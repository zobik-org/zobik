package networkmonitor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/grpc"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/claim"
	"zobik.org/zobik/internal/globalconfig"
	"zobik.org/zobik/internal/role"
	contextv1 "zobik.org/zobik/proto/zobik/context/v1"
)

// Options are what the role reaches besides the Bus.
type Options struct {
	// Fleet reads the Active Node Registry through docker_reader
	// (implementation §1.2.14).
	Fleet func(ctx context.Context) ([]Node, error)
	// Watch calls changed on each change to the Active Node Registry, from the
	// engine's /events through docker_reader; it returns when ctx ends or the
	// stream breaks (implementation §1.2.8).
	Watch func(ctx context.Context, changed func()) error
	// ContextAddr is the Context Store's address, where it writes the census.
	ContextAddr string
}

// The keys of the role's bucket: one per node of the roster, and one per family
// with how far the role applied it (implementation §1.2.8).
const (
	keyNode   = "node."
	keyStream = "stream."
)

const (
	fetchBatch = 64
	fetchWait  = 2 * time.Second
	cycle      = time.Second
)

// state is the aggregate the role keeps in memory and in its bucket.
type state struct {
	nodes map[string]*Aggregate
	marks map[string]mark
}

type monitor struct {
	id       string
	js       jetstream.JetStream
	kv       jetstream.KeyValue
	o        Options
	claimant *claim.Claimant
	context  contextv1.ContextStoreClient
	// leaseTTL is task_lease_ttl as of the last head read, in nanoseconds.
	leaseTTL  atomic.Int64
	answering sync.WaitGroup

	mu     sync.Mutex
	st     state
	params Params
	clock  admission
	// read is, per family, the server's time before which every stored event
	// has been applied.
	read map[string]time.Time
	// resets are the topics whose roster changed while the role was down: they
	// are applied once the task family has been reread (implementation §1.2.8).
	resets map[string]bool
}

// Run is the network_monitor role: it observes the Bus and measures each node
// of the roster (architecture §3.12.1, §3.12.2), and answers node_query: the
// census from the Active Node Registry, the point form from its aggregate
// (architecture §3.12.3). It is ready
// once it reads the Global Configuration and its history, binds its consumers,
// loads its aggregate and reads the roster.
func Run(ctx context.Context, rt *role.Runtime, nc *nats.Conn, o Options) error {
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	l := rt.MainLoop()
	head, err := role.AwaitHead(ctx, l, js)
	if err != nil {
		return err
	}
	consumers := map[string]jetstream.Consumer{}
	for _, f := range families {
		if consumers[f], err = role.AwaitConsumer(ctx, l, js, rt, f); err != nil {
			return err
		}
	}
	conn, err := grpc.NewClient(o.ContextAddr, grpc.WithTransportCredentials(rt.RPCCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	m := &monitor{
		id: rt.ID, js: js, o: o,
		claimant: claim.New(rt.ID, rt.Credential, js, nc, Topic),
		context:  contextv1.NewContextStoreClient(conn),
		params:   ReadParams(head),
		read:     map[string]time.Time{},
		resets:   map[string]bool{},
	}
	m.leaseTTL.Store(int64(head.Duration("task_lease_ttl")))
	if err := role.Retry(ctx, l, opTimeout, m.load); err != nil {
		return err
	}
	if err := role.Retry(ctx, l, opTimeout, func(ctx context.Context) error { return m.clock.follow(ctx, js) }); err != nil {
		return err
	}
	// A change that happened with the role down counts after what it rereads
	// (implementation §1.2.8).
	if err := role.Retry(ctx, l, opTimeout, func(ctx context.Context) error { return m.syncRoster(ctx, true) }); err != nil {
		return err
	}
	var workers sync.WaitGroup
	defer m.answering.Wait()
	defer workers.Wait()
	for f, c := range consumers {
		workers.Go(func() { m.consume(ctx, rt.Loop(f, time.Minute), f, c) })
	}
	if o.Watch != nil {
		workers.Go(func() { m.watch(ctx) })
	}
	rt.SetReady(true)

	t := time.NewTicker(cycle)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			l.Mark()
			if err := m.tick(ctx); err != nil {
				log.Printf("network_monitor: %v", err)
			}
		}
	}
}

// load reads the aggregate from the bucket.
func (m *monitor) load(ctx context.Context) error {
	kv, err := m.js.KeyValue(ctx, bus.BucketMonitor)
	if err != nil {
		return err
	}
	st := state{nodes: map[string]*Aggregate{}, marks: map[string]mark{}}
	lister, err := kv.ListKeys(ctx)
	if err != nil {
		return err
	}
	var keys []string
	for k := range lister.Keys() {
		keys = append(keys, k)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, k := range keys {
		e, err := kv.Get(ctx, k)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		switch {
		case strings.HasPrefix(k, keyNode):
			var a Aggregate
			if err := json.Unmarshal(e.Value(), &a); err != nil {
				return fmt.Errorf("%s: %w", k, err)
			}
			if a.Applied == nil {
				a.Applied = map[string]uint64{}
			}
			st.nodes[a.NodeID] = &a
		case strings.HasPrefix(k, keyStream):
			var mk mark
			if err := json.Unmarshal(e.Value(), &mk); err != nil {
				return fmt.Errorf("%s: %w", k, err)
			}
			st.marks[strings.TrimPrefix(k, keyStream)] = mk
		}
	}
	m.kv, m.st = kv, st
	return nil
}

// consume applies a family's events. A pull that returns less than it asked for
// read everything stored before it started.
func (m *monitor) consume(ctx context.Context, l *role.Loop, family string, c jetstream.Consumer) {
	for ctx.Err() == nil {
		l.Mark()
		start := time.Now()
		batch, err := c.Fetch(fetchBatch, jetstream.FetchMaxWait(fetchWait))
		if err != nil {
			log.Printf("network_monitor: fetching from %s: %v", family, err)
			time.Sleep(time.Second)
			continue
		}
		n := 0
		for msg := range batch.Messages() {
			l.Mark()
			n++
			if ctx.Err() != nil {
				msg.Nak() // for whoever reads next
				continue
			}
			m.handle(ctx, l, family, msg)
		}
		if err := batch.Error(); err != nil {
			if ctx.Err() == nil {
				log.Printf("network_monitor: fetching from %s: %v", family, err)
			}
			continue
		}
		if n < fetchBatch {
			m.mu.Lock()
			m.read[family] = start
			if family == "task" && len(m.resets) > 0 {
				if err := m.applyResets(); err != nil {
					log.Printf("network_monitor: roster: %v", err)
				}
			}
			m.mu.Unlock()
		}
	}
}

// handle applies an event to the aggregate and, if it belongs to the cycle of
// node_query, takes part in it. It is acknowledged once the keys it moves are
// written: a write that fails is retried, and the event is not left behind
// a later one (implementation §1.2.8).
func (m *monitor) handle(ctx context.Context, l *role.Loop, family string, msg jetstream.Msg) {
	meta, err := msg.Metadata()
	if err != nil {
		msg.Ack()
		return
	}
	var e *bus.Event
	var ev bus.Event
	if json.Unmarshal(msg.Data(), &ev) == nil && ev.Validate() == nil && ev.Subject != "" {
		e = &ev
	}
	for {
		err := m.apply(ctx, family, meta.Sequence.Stream, meta.Timestamp, e, msg.Subject())
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			return
		}
		log.Printf("network_monitor: applying %s %d: %v", family, meta.Sequence.Stream, err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
			l.Mark()
		}
	}
	if family == "task" && e != nil {
		if err := m.claim(ctx, e, msg.Subject()); err != nil {
			log.Printf("network_monitor: proposing on %s: %v", e.Subject, err)
			msg.NakWithDelay(time.Second)
			return
		}
	}
	msg.Ack()
}

// apply computes what the event moves, writes those keys and then the
// family's mark, and only then keeps the change in memory.
func (m *monitor) apply(ctx context.Context, family string, seq uint64, stored time.Time, e *bus.Event, subject string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	d := m.st.observe(family, seq, stored, e, subject, m.params)
	for id, a := range d.nodes {
		a.Applied[family] = seq
		if err := m.put(ctx, keyNode+id, a); err != nil {
			return err
		}
	}
	mk := m.st.marks[family]
	if seq > mk.Seq {
		mk = mark{Seq: seq, Stored: stored}
		if err := m.put(ctx, keyStream+family, mk); err != nil {
			return err
		}
	}
	for id, a := range d.nodes {
		m.st.nodes[id] = a
	}
	m.st.marks[family] = mk
	return nil
}

func (m *monitor) put(ctx context.Context, key string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	_, err = m.kv.Put(ctx, key, b)
	return err
}

// claim takes part in the cycle of node_query: it proposes on each census
// announcement, by auction without similarity (architecture §3.7.6, §3.12.3),
// and on each point one, whose target_topic its single instance owns
// (implementation thesis 3), and answers what it is awarded.
func (m *monitor) claim(ctx context.Context, e *bus.Event, subject string) error {
	switch {
	case e.Type == bus.TypeAnnounced && subject == bus.SubjectAnnounced(Topic):
		opCtx, cancel := context.WithTimeout(ctx, opTimeout)
		defer cancel()
		return m.claimant.Propose(opCtx, e, nil)
	case e.Type == bus.TypeRejected && subject == bus.SubjectAddressed(bus.TypeRejected, m.id):
		m.claimant.Rejected(e)
	case e.Type == bus.TypeAssigned && subject == bus.SubjectAddressed(bus.TypeAssigned, m.id):
		if a, ok := m.claimant.Assigned(e); ok {
			m.answering.Go(func() { m.answer(ctx, a) })
		}
	}
	return nil
}

// tick is the role's cycle: it reads the head and the history, and classifies
// the closes whose grace_period has passed.
func (m *monitor) tick(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	head, _, err := globalconfig.Head(ctx, m.js)
	if err != nil {
		return err
	}
	m.leaseTTL.Store(int64(head.Duration("task_lease_ttl")))
	m.mu.Lock()
	defer m.mu.Unlock()
	m.params = ReadParams(head)
	if err := m.clock.follow(ctx, m.js); err != nil {
		return err
	}
	return m.classifyClean(ctx)
}

// classifyClean classifies clean every task.completed whose grace_period of
// admission_clock has passed with no reopen (architecture §3.12.1). What
// decides is how far the task family has been read, not the wall clock: a
// reopen still unread would otherwise be lost.
func (m *monitor) classifyClean(ctx context.Context) error {
	read, ok := m.read["task"]
	if !ok {
		return nil
	}
	now := m.clock.at(read)
	for id, a := range m.st.nodes {
		var ready, keep []Observation
		for _, o := range a.Pending {
			if now-m.clock.at(o.Closed) >= m.params.GracePeriod {
				ready = append(ready, o)
			} else {
				keep = append(keep, o)
			}
		}
		if len(ready) == 0 {
			continue
		}
		c := a.clone()
		c.Pending = keep
		for _, o := range ready {
			c.classify(o)
		}
		c.trimObservations(time.Now(), m.params)
		if err := m.put(ctx, keyNode+id, c); err != nil {
			return err
		}
		m.st.nodes[id] = c
	}
	return nil
}

// watch follows the changes to the Active Node Registry. A stream that breaks
// is opened again, and the roster read again in case it missed one.
func (m *monitor) watch(ctx context.Context) {
	for ctx.Err() == nil {
		err := m.o.Watch(ctx, func() {
			if err := m.syncRoster(ctx, false); err != nil {
				log.Printf("network_monitor: roster: %v", err)
			}
		})
		if ctx.Err() != nil {
			return
		}
		log.Printf("network_monitor: watching the Active Node Registry: %v", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
		if err := m.syncRoster(ctx, false); err != nil {
			log.Printf("network_monitor: roster: %v", err)
		}
	}
}

// syncRoster brings the keys to the Active Node Registry: a node that enters
// gets its key and one that leaves takes its key with it (architecture
// §3.12.1). Every addition or removal leaves the usage window of the topic's
// nodes waiting for usage_window new assignments (implementation §5.4); with
// deferred, once the task family has been reread.
func (m *monitor) syncRoster(ctx context.Context, deferred bool) error {
	fleet, err := m.o.Fleet(ctx)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	live := map[string]Node{}
	for _, n := range fleet {
		if n.NodeID != "" && n.Topic != "" {
			live[n.NodeID] = n
		}
	}
	changed := map[string]bool{}
	now := time.Now().UTC()
	for id, n := range live {
		if _, ok := m.st.nodes[id]; ok {
			continue
		}
		a := &Aggregate{NodeID: id, Topic: n.Topic, Applied: map[string]uint64{}, Since: now, DemandFrom: now}
		if err := m.put(ctx, keyNode+id, a); err != nil {
			return err
		}
		m.st.nodes[id] = a
		changed[n.Topic] = true
	}
	for id, a := range m.st.nodes {
		if _, ok := live[id]; ok {
			continue
		}
		dctx, cancel := context.WithTimeout(ctx, opTimeout)
		err := m.kv.Delete(dctx, keyNode+id)
		cancel()
		if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
			return err
		}
		delete(m.st.nodes, id)
		changed[a.Topic] = true
	}
	for topic := range changed {
		m.resets[topic] = true
	}
	if deferred {
		return nil
	}
	return m.applyResets()
}

// applyResets restarts the count of assignments since the roster change on
// every node of the topics that changed (implementation §5.4).
func (m *monitor) applyResets() error {
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	for topic := range m.resets {
		for _, id := range m.st.ofTopic(topic) {
			c := m.st.nodes[id].clone()
			c.SinceRoster = 0
			if err := m.put(ctx, keyNode+id, c); err != nil {
				return err
			}
			m.st.nodes[id] = c
		}
		delete(m.resets, topic)
	}
	return nil
}

// ReadAggregate reads a node's aggregate from the role's bucket.
func ReadAggregate(ctx context.Context, js jetstream.JetStream, nodeID string) (*Aggregate, error) {
	kv, err := js.KeyValue(ctx, bus.BucketMonitor)
	if err != nil {
		return nil, err
	}
	e, err := kv.Get(ctx, keyNode+nodeID)
	if err != nil {
		return nil, err
	}
	var a Aggregate
	if err := json.Unmarshal(e.Value(), &a); err != nil {
		return nil, err
	}
	return &a, nil
}
