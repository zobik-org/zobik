package tracingcollector

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/claim"
	"zobik.org/zobik/internal/globalconfig"
	"zobik.org/zobik/internal/role"
	contextv1 "zobik.org/zobik/proto/zobik/context/v1"
)

// Options are what the role runs with.
type Options struct {
	// Network is the network's name, the resource's service.name (implementation §1.2.6).
	Network string
	Backend Backend
	// DataDir is the volume where it keeps the live traces.
	DataDir string
	// ContextAddr is the Context Store's address, where it writes the answers
	// to trace_query (implementation §6, The Docker network).
	ContextAddr string
}

// families are the streams it reads: every family whose events carry
// traceparent (architecture §3.2, implementation §1.2.1).
var families = []string{"task", "prop", "notice"}

const (
	opTimeout  = 5 * time.Second
	fetchBatch = 64
	fetchWait  = 2 * time.Second
	// settleMargin is how long past a span's close every family has to
	// be read before it is exported: it covers the task.rejected the Task Broker
	// publishes after the award, which can be stored after a fast close.
	settleMargin = 5 * time.Second
	exportEvery  = time.Second
	sweepEvery   = time.Minute
)

// collector holds the live traces. Each event is on disk before it is
// acknowledged, and each span stays until the backend accepts it.
type collector struct {
	store   *traces
	backend Backend
	res     *resource.Resource

	mu     sync.Mutex
	traces map[string]*trace
	// read is, per family, the server's time before which every stored event
	// has been applied.
	read map[string]time.Time

	// The half that answers trace_query (architecture §3.10).
	id        string
	claimant  *claim.Claimant
	context   contextv1.ContextStoreClient
	answering sync.WaitGroup
	// leaseTTL is task_lease_ttl as of the last head read, in nanoseconds.
	leaseTTL atomic.Int64
}

// Run is the tracing_collector role: it rebuilds the traces from what crosses
// the Bus and exports them, and answers trace_query from them (architecture
// §3.2, §3.10, implementation §1.2.6). It is ready once it reads the Global
// Configuration, binds its consumers, loads the live traces and reaches the
// tracing backend.
func Run(ctx context.Context, rt *role.Runtime, nc *nats.Conn, o Options) error {
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	res, err := networkResource(o.Network)
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
	store, err := openTraces(o.DataDir)
	if err != nil {
		return err
	}
	loaded, err := store.load()
	if err != nil {
		return err
	}
	if err := role.Retry(ctx, l, opTimeout, o.Backend.Reach); err != nil {
		return err
	}
	conn, err := grpc.NewClient(o.ContextAddr, grpc.WithTransportCredentials(rt.RPCCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	c := &collector{
		store: store, backend: o.Backend, res: res, traces: loaded, read: map[string]time.Time{},
		id: rt.ID, claimant: claim.New(rt.ID, rt.Credential, js, nc, Topic), context: contextv1.NewContextStoreClient(conn),
	}
	c.leaseTTL.Store(int64(head.Duration("task_lease_ttl")))
	// The role returns once its readers and its answers have stopped, so a new
	// instance over the same volume never shares the consumers with one still
	// applying.
	var readers sync.WaitGroup
	defer c.answering.Wait()
	defer readers.Wait()
	for f, cons := range consumers {
		readers.Go(func() { c.consume(ctx, rt.Loop(f, time.Minute), f, cons) })
	}
	rt.SetReady(true)

	t := time.NewTicker(exportEvery)
	defer t.Stop()
	next := time.Now()
	for {
		select {
		case <-ctx.Done():
			return nil
		case now := <-t.C:
			l.Mark()
			c.export(ctx)
			if now.Before(next) {
				continue
			}
			next = now.Add(sweepEvery)
			if err := c.sweep(ctx, js, now); err != nil {
				log.Printf("tracing_collector: sweep: %v", err)
			}
		}
	}
}

// consume applies a family's events. A pull that returns less than it asked
// for read everything stored before it started.
func (c *collector) consume(ctx context.Context, l *role.Loop, family string, cons jetstream.Consumer) {
	for ctx.Err() == nil {
		l.Mark()
		start := time.Now()
		batch, err := cons.Fetch(fetchBatch, jetstream.FetchMaxWait(fetchWait))
		if err != nil {
			log.Printf("tracing_collector: fetching from %s: %v", family, err)
			time.Sleep(time.Second)
			continue
		}
		n := 0
		for m := range batch.Messages() {
			l.Mark()
			n++
			if ctx.Err() != nil {
				m.Nak() // for whoever reads next
				continue
			}
			c.handle(ctx, m)
		}
		if err := batch.Error(); err != nil {
			if ctx.Err() == nil {
				log.Printf("tracing_collector: fetching from %s: %v", family, err)
			}
			continue
		}
		if n < fetchBatch {
			c.mu.Lock()
			c.read[family] = start
			c.mu.Unlock()
		}
	}
}

// handle applies one event and acknowledges it once it is on disk and, if it
// belongs to the cycle of trace_query, once the role has taken part. What is
// not an event of a trace is acknowledged and left out.
func (c *collector) handle(ctx context.Context, m jetstream.Msg) {
	var e bus.Event
	if err := json.Unmarshal(m.Data(), &e); err != nil || e.Validate() != nil {
		m.Ack()
		return
	}
	meta, err := m.Metadata()
	if err != nil {
		m.Ack()
		return
	}
	f, traceID, ok := newFact(&e, m.Subject(), meta.Timestamp)
	if !ok {
		m.Ack()
		return
	}
	if err := c.apply(traceID, f); err != nil {
		log.Printf("tracing_collector: keeping %s: %v", e.ID, err)
		m.Nak()
		return
	}
	if err := c.claimEvent(ctx, &e, f, m.Subject()); err != nil {
		log.Printf("tracing_collector: proposing on %s: %v", e.Subject, err)
		m.NakWithDelay(time.Second)
		return
	}
	m.Ack()
}

func (c *collector) apply(traceID string, f fact) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := c.traces[traceID]
	if t == nil {
		t = &trace{ID: traceID}
	}
	if !t.add(f) {
		return nil
	}
	last := t.Last
	t.Last = time.Now()
	if err := c.store.save(t); err != nil {
		t.Facts, t.Last = t.Facts[:len(t.Facts)-1], last
		return err
	}
	c.traces[traceID] = t
	if s := spanWith(t.spans(), f.ID); s != nil && t.Exported[s.ID] {
		log.Printf("tracing_collector: %s %s arrived after span %s was exported; the backend will not show it", f.Type, f.ID, s.ID)
	}
	return nil
}

// spanWith is the span the fact id went into, or nil.
func spanWith(spans []*span, id string) *span {
	for _, s := range spans {
		for _, f := range []*fact{s.Assigned, s.Terminal} {
			if f != nil && f.ID == id {
				return s
			}
		}
		for _, f := range s.Events {
			if f.ID == id {
				return s
			}
		}
	}
	return nil
}

// export sends the backend every closed span that is settled: every family
// has been read past its close by settleMargin (implementation §1.2.6). A span
// the backend does not accept stays, and goes again on the next pass.
func (c *collector) export(ctx context.Context) {
	type item struct {
		t  *trace
		id string
	}
	c.mu.Lock()
	read := time.Time{}
	for i, f := range families {
		r, ok := c.read[f]
		if !ok {
			c.mu.Unlock()
			return
		}
		if i == 0 || r.Before(read) {
			read = r
		}
	}
	var batch []sdktrace.ReadOnlySpan
	var items []item
	for _, t := range c.traces {
		for _, s := range t.spans() {
			if !s.closed() || t.Exported[s.ID] || s.Settle.IsZero() || read.Before(s.Settle.Add(settleMargin)) {
				continue
			}
			ro, err := s.readOnly(t.ID, c.res)
			if err != nil {
				log.Printf("tracing_collector: span %s of %s cannot be exported: %v", s.ID, t.ID, err)
				markExported(t, s.ID)
				continue
			}
			batch, items = append(batch, ro), append(items, item{t, s.ID})
		}
	}
	c.mu.Unlock()
	if len(batch) == 0 {
		return
	}
	ectx, cancel := context.WithTimeout(ctx, opTimeout)
	err := c.backend.ExportSpans(ectx, batch)
	cancel()
	if err != nil {
		log.Printf("tracing_collector: exporting %d spans: %v", len(batch), err)
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	touched := map[*trace]bool{}
	for _, it := range items {
		markExported(it.t, it.id)
		touched[it.t] = true
	}
	for t := range touched {
		if err := c.store.save(t); err != nil {
			log.Printf("tracing_collector: recording the export of %s: %v", t.ID, err)
		}
	}
}

func markExported(t *trace, id string) {
	if t.Exported == nil {
		t.Exported = map[string]bool{}
	}
	t.Exported[id] = true
}

// sweep discards a trace context_retention_grace after its last event, once
// none of its awards is open: a trace whose root never closes, like one opened
// with zobik publish, does not stay forever. It reads the window from the head
// on each pass.
func (c *collector) sweep(ctx context.Context, js jetstream.JetStream, now time.Time) error {
	ctx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	head, _, err := globalconfig.Head(ctx, js)
	if err != nil {
		return err
	}
	var graceMs int64
	if err := json.Unmarshal(head.Values["context_retention_grace"], &graceMs); err != nil {
		return fmt.Errorf("context_retention_grace: %w", err)
	}
	grace := time.Duration(graceMs) * time.Millisecond
	c.leaseTTL.Store(int64(head.Duration("task_lease_ttl")))
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, t := range c.traces {
		if now.Sub(t.Last) < grace || t.awardOpen() {
			continue
		}
		if err := c.store.remove(id); err != nil {
			return err
		}
		delete(c.traces, id)
	}
	return nil
}

// awardOpen says whether a task of the trace holds an award that has not closed.
func (t *trace) awardOpen() bool {
	for _, s := range t.spans() {
		if s.Assigned != nil && !s.closed() {
			return true
		}
	}
	return false
}
