package taskbroker

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

	"github.com/nats-io/nats.go/jetstream"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/embedding"
	"zobik.org/zobik/internal/signing"
)

// Each task is driven by the goroutine of its topic, which keeps the CAS from
// failing within a single instance (implementation §1.2.7). Everything that
// moves a task—an event, a renewal, a timer—reaches it as an op.

const (
	// opTimeout bounds each op's reads and writes on the Bus.
	opTimeout = 5 * time.Second
	// retryDelay is when a timer-driven step that failed on the Bus runs again.
	retryDelay = time.Second
)

// signer issues the scope token with the Task Broker's key and its
// task_broker role certificate (implementation §1.2.7).
type signer func(typ signing.Type, claims any) (string, error)

type broker struct {
	ctx    context.Context
	id     string
	js     jetstream.JetStream
	tasks  store
	creds  *credentials
	pinned *pinned
	sign   signer
	params atomic.Pointer[params]
	// since is when this instance took the range: a lease it rearms lasts at
	// least one task_lease_ttl from then (implementation §1.2.7).
	since time.Time

	mu      sync.Mutex
	workers map[string]*worker
}

func newBroker(ctx context.Context, id, root string, js jetstream.JetStream, kv jetstream.KeyValue, sign signer, p *params) *broker {
	b := &broker{
		ctx: ctx, id: id, js: js, tasks: store{kv}, creds: newCredentials(root),
		pinned: &pinned{js: js, by: map[string]int{}}, sign: sign,
		since: time.Now(), workers: map[string]*worker{},
	}
	b.params.Store(p)
	return b
}

// worker is a topic's goroutine and the state only it touches.
type worker struct {
	b        *broker
	topic    string
	ops      chan func(context.Context, *worker)
	auctions map[string]*auction
	timers   map[string]*time.Timer
}

// auction collects the proposals of one round, which closes at deadline.
type auction struct {
	task      announced
	deadline  time.Time
	proposals map[string]proposal
	// decimalsErr is set when the pinned snapshot could not be read: the round
	// closes without awarding.
	decimalsErr error
}

// do runs op on topic's goroutine, waiting for room in its queue.
func (b *broker) do(topic string, op func(context.Context, *worker)) {
	b.mu.Lock()
	w, ok := b.workers[topic]
	if !ok {
		w = &worker{b: b, topic: topic, ops: make(chan func(context.Context, *worker), 64),
			auctions: map[string]*auction{}, timers: map[string]*time.Timer{}}
		b.workers[topic] = w
		go w.run()
	}
	b.mu.Unlock()
	select {
	case w.ops <- op:
	case <-b.ctx.Done():
	}
}

func (w *worker) run() {
	for {
		select {
		case <-w.b.ctx.Done():
			return
		case op := <-w.ops:
			ctx, cancel := context.WithTimeout(w.b.ctx, opTimeout)
			op(ctx, w)
			cancel()
		}
	}
}

// arm sets the task's single timer, replacing the one it had: a task waits
// either for its auction to close, for its next announcement or for its lease.
// Each step checks the stored state again, so a timer that fires late is harmless.
func (w *worker) arm(taskID string, at time.Time, step func(*worker, context.Context, string)) {
	w.disarm(taskID)
	w.timers[taskID] = time.AfterFunc(time.Until(at), func() {
		w.b.do(w.topic, func(ctx context.Context, w *worker) { step(w, ctx, taskID) })
	})
}

func (w *worker) disarm(taskID string) {
	if t, ok := w.timers[taskID]; ok {
		t.Stop()
		delete(w.timers, taskID)
	}
}

// announcementData is what the Task Broker reads from a task.announced.
type announcementData struct {
	Topic             string  `json:"topic"`
	TaskEmbedding     *string `json:"task_embedding"`
	ModelID           string  `json:"model_id"`
	RequesterIdentity string  `json:"requester_identity"`
}

// onAnnounced creates the task in UNASSIGNED only if its key does not exist,
// and opens its auction. A key that exists is the redelivery of an announcement
// or the Task Broker's own re-announcement (implementation §1.2.7).
func (w *worker) onAnnounced(ctx context.Context, m jetstream.Msg) {
	e, err := readEvent(m, 2, w.topic)
	if err != nil {
		log.Printf("task_broker: dropping %s: %v", m.Subject(), err)
		m.Ack()
		return
	}
	if bus.TraceID(e.TraceParent) == "" {
		log.Printf("task_broker: dropping %s %s: traceparent names no trace", m.Subject(), e.Subject)
		m.Ack()
		return
	}
	now := time.Now()
	t := &task{State: stateUnassigned, LastAnnounced: now, Announcement: e}
	switch err := w.b.tasks.create(ctx, taskKey(w.topic, e.Subject), t); {
	case errors.Is(err, jetstream.ErrKeyExists):
	case errors.Is(err, jetstream.ErrInvalidKey):
		log.Printf("task_broker: dropping %s: %q is not a task key", m.Subject(), taskKey(w.topic, e.Subject))
	case err != nil:
		log.Printf("task_broker: creating %s: %v", e.Subject, err)
		m.NakWithDelay(retryDelay)
		return
	default:
		w.openAuction(ctx, e.Subject, e, now)
	}
	m.Ack()
}

// openAuction collects proposals for auction_timeout (architecture §3.7.5, step 5).
func (w *worker) openAuction(ctx context.Context, taskID string, e *bus.Event, now time.Time) {
	var d announcementData
	json.Unmarshal(e.Data, &d)
	a := &auction{
		task:      announced{topic: w.topic, taskID: taskID, modelID: d.ModelID},
		deadline:  now.Add(w.b.params.Load().auctionTimeout),
		proposals: map[string]proposal{},
	}
	if d.TaskEmbedding != nil {
		// An embedding that does not decode leaves only the structural proposals.
		a.task.embedding, _ = embedding.Decode(*d.TaskEmbedding)
	}
	a.task.decimals, a.decimalsErr = w.b.pinned.decimals(ctx, e.ConfigVersion)
	w.auctions[taskID] = a
	w.arm(taskID, a.deadline, (*worker).closeAuction)
}

// proposalData is what the Task Broker reads from a task.proposed.
type proposalData struct {
	DeclaredSimilarity *float64 `json:"declared_similarity"`
	Credential         string   `json:"credential"`
}

// onProposed adds a proposal to the task's open round. One that arrives
// outside a round is discarded: the task is already out of UNASSIGNED, or it
// is waiting to be announced again (architecture §3.1).
func (w *worker) onProposed(m jetstream.Msg, identity string) {
	defer m.Ack()
	e, err := readEvent(m, 2, w.topic)
	if err != nil {
		log.Printf("task_broker: dropping %s: %v", m.Subject(), err)
		return
	}
	a, ok := w.auctions[e.Subject]
	if !ok {
		return
	}
	var d proposalData
	if err := json.Unmarshal(e.Data, &d); err != nil {
		return
	}
	a.proposals[identity] = proposal{identity: identity, declared: d.DeclaredSimilarity, credential: d.Credential}
}

// closeAuction awards the round's highest admissible proposal, or leaves the
// task vacant (architecture §3.7.5, steps 4 and 5).
func (w *worker) closeAuction(ctx context.Context, taskID string) {
	a, ok := w.auctions[taskID]
	if !ok {
		return
	}
	if now := time.Now(); now.Before(a.deadline) {
		w.arm(taskID, a.deadline, (*worker).closeAuction)
		return
	}
	delete(w.auctions, taskID)
	key := taskKey(w.topic, taskID)
	t, _, err := w.b.tasks.get(ctx, key)
	if err != nil {
		log.Printf("task_broker: closing the round of %s: %v", taskID, err)
		w.arm(taskID, time.Now().Add(retryDelay), (*worker).announce)
		return
	}
	if t.State != stateUnassigned {
		return
	}
	p := w.b.params.Load()
	// A paused network grants nothing and emits no event: the task stays
	// UNASSIGNED and is re-announced with backoff (architecture §3.15.3, §3.18).
	if !admits(p.networkAdmission, w.topic) {
		w.scheduleAnnouncement(t, taskID)
		return
	}
	if a.decimalsErr != nil {
		log.Printf("task_broker: %s pins config_version %q: %v", taskID, t.Announcement.ConfigVersion, a.decimalsErr)
		w.scheduleAnnouncement(t, taskID)
		return
	}
	var verdicts []verdict
	for _, prop := range a.proposals {
		v, err := judge(w.b.creds, &a.task, prop)
		if err != nil {
			log.Printf("task_broker: %s on %s: proposal of %s %v", taskID, w.topic, prop.identity, err)
			continue
		}
		verdicts = append(verdicts, v)
	}
	winner, ok := award(taskID, verdicts)
	if !ok {
		w.reject(ctx, t.Announcement, verdicts, -1)
		w.scheduleAnnouncement(t, taskID)
		return
	}

	now := time.Now()
	won := verdicts[winner]
	t, err = w.b.tasks.transition(ctx, key, func(t *task) error {
		if t.State != stateUnassigned {
			return errNoChange
		}
		t.State, t.Assignee, t.LeaseExpiry = stateClaimed, won.identity, now.Add(p.leaseTTL)
		return nil
	})
	if errors.Is(err, errNoChange) {
		return
	}
	if err != nil {
		log.Printf("task_broker: awarding %s: %v", taskID, err)
		w.arm(taskID, time.Now().Add(retryDelay), (*worker).announce)
		return
	}
	// The award is the CAS; if publishing it fails, the winner never renews and
	// the lease returns the task to UNASSIGNED.
	if err := w.assign(ctx, t.Announcement, won); err != nil {
		log.Printf("task_broker: publishing the award of %s: %v", taskID, err)
	}
	w.reject(ctx, t.Announcement, verdicts, winner)
	w.arm(taskID, w.leaseDeadline(t), (*worker).checkLease)
}

// assignedData is the data of a task.assigned (architecture §3.8.1).
type assignedData struct {
	Topic             string   `json:"topic"`
	Similarity        *float64 `json:"similarity,omitempty"`
	RequesterIdentity string   `json:"requester_identity,omitempty"`
	ScopeToken        string   `json:"scope_token"`
}

// rejectedData is the data of a task.rejected.
type rejectedData struct {
	Topic      string   `json:"topic"`
	Similarity *float64 `json:"similarity,omitempty"`
}

// assign publishes the task.assigned with the scope token it mints for the
// winner: read under the TraceID, write under the TaskID (architecture §3.14.3).
func (w *worker) assign(ctx context.Context, ann *bus.Event, won verdict) error {
	var d announcementData
	json.Unmarshal(ann.Data, &d)
	token, err := w.b.sign(signing.TypeScopeToken, signing.ScopeToken{
		Sub: won.identity, Iat: time.Now().Unix(), TraceID: bus.TraceID(ann.TraceParent), Section: ann.Subject,
	})
	if err != nil {
		return err
	}
	return w.publish(ctx, bus.TypeAssigned, won.identity, ann, assignedData{
		Topic: w.topic, Similarity: won.similarity, RequesterIdentity: d.RequesterIdentity, ScopeToken: token,
	})
}

// reject answers every judged proposal but the winner's with task.rejected and
// the similarity it lost with (architecture §3.8.1).
func (w *worker) reject(ctx context.Context, ann *bus.Event, verdicts []verdict, winner int) {
	for i, v := range verdicts {
		if i == winner {
			continue
		}
		if err := w.publish(ctx, bus.TypeRejected, v.identity, ann, rejectedData{Topic: w.topic, Similarity: v.similarity}); err != nil {
			log.Printf("task_broker: rejecting %s for %s: %v", v.identity, ann.Subject, err)
		}
	}
}

// publish emits an event addressed to identity about the announced task. It
// carries the trace's traceparent and config_version, like every event of the
// trace (architecture §3.8.1).
func (w *worker) publish(ctx context.Context, typ, identity string, ann *bus.Event, data any) error {
	e, err := bus.NewEvent(typ, w.b.id, ann.Subject, data)
	if err != nil {
		return err
	}
	e.TraceParent, e.ConfigVersion = ann.TraceParent, ann.ConfigVersion
	return bus.Publish(ctx, w.b.js, bus.SubjectAddressed(typ, identity), e)
}

// scheduleAnnouncement arms the next announcement of an UNASSIGNED task:
// reannounce_backoff_base after the previous one, doubled on each successive
// re-announcement up to max_reannounce_backoff (architecture §3.1).
func (w *worker) scheduleAnnouncement(t *task, taskID string) {
	w.arm(taskID, w.nextAnnouncement(t), (*worker).announce)
}

func (w *worker) nextAnnouncement(t *task) time.Time {
	return t.LastAnnounced.Add(w.b.params.Load().backoff(t.Reannouncements))
}

// announce re-announces an UNASSIGNED task, with the same data and the same
// task_embedding, on its topic, and opens a new round (architecture §3.1).
func (w *worker) announce(ctx context.Context, taskID string) {
	now := time.Now()
	t, err := w.b.tasks.transition(ctx, taskKey(w.topic, taskID), func(t *task) error {
		if t.State != stateUnassigned || now.Before(w.nextAnnouncement(t)) {
			return errNoChange
		}
		t.Reannouncements++
		t.LastAnnounced = now
		return nil
	})
	switch {
	case errors.Is(err, errNoChange):
		w.resume(t, taskID)
		return
	case errors.Is(err, jetstream.ErrKeyNotFound):
		return
	case err != nil:
		log.Printf("task_broker: re-announcing %s: %v", taskID, err)
		w.arm(taskID, time.Now().Add(retryDelay), (*worker).announce)
		return
	}
	ann := t.Announcement
	e, err := bus.NewEvent(bus.TypeAnnounced, w.b.id, ann.Subject, ann.Data)
	if err == nil {
		e.TraceParent, e.ConfigVersion = ann.TraceParent, ann.ConfigVersion
		err = bus.Publish(ctx, w.b.js, bus.SubjectAnnounced(w.topic), e)
	}
	if err != nil {
		log.Printf("task_broker: re-announcing %s: %v", taskID, err)
	}
	w.openAuction(ctx, taskID, ann, now)
}

// leaseDeadline is when a CLAIMED task's lease expires for this instance: the
// stored date, and no earlier than one task_lease_ttl after it took the range
// (implementation §1.2.7).
func (w *worker) leaseDeadline(t *task) time.Time {
	floor := w.b.since.Add(w.b.params.Load().leaseTTL)
	if t.LeaseExpiry.Before(floor) {
		return floor
	}
	return t.LeaseExpiry
}

// checkLease returns to UNASSIGNED a task whose assignee stopped renewing, and
// re-announces it (architecture §3.1). If a renewal won the CAS, the lease is
// extended and the expiry reads it again.
func (w *worker) checkLease(ctx context.Context, taskID string) {
	t, err := w.b.tasks.transition(ctx, taskKey(w.topic, taskID), func(t *task) error {
		if t.State != stateClaimed || time.Now().Before(w.leaseDeadline(t)) {
			return errNoChange
		}
		t.State, t.Assignee, t.LeaseExpiry = stateUnassigned, "", time.Time{}
		return nil
	})
	switch {
	case errors.Is(err, jetstream.ErrKeyNotFound):
		return
	case err != nil && !errors.Is(err, errNoChange):
		log.Printf("task_broker: expiring the lease of %s: %v", taskID, err)
		w.arm(taskID, time.Now().Add(retryDelay), (*worker).checkLease)
		return
	}
	w.resume(t, taskID)
}

// onRenewal extends the lease only if the task is CLAIMED and the identity the
// subject attributes is the assignee's (architecture §3.1).
func (w *worker) onRenewal(ctx context.Context, identity, taskID string) {
	t, err := w.b.tasks.transition(ctx, taskKey(w.topic, taskID), func(t *task) error {
		if t.State != stateClaimed || t.Assignee != identity {
			return errNoChange
		}
		t.LeaseExpiry = time.Now().Add(w.b.params.Load().leaseTTL)
		return nil
	})
	if err == nil {
		w.arm(taskID, w.leaseDeadline(t), (*worker).checkLease)
	} else if !errors.Is(err, errNoChange) && !errors.Is(err, jetstream.ErrKeyNotFound) {
		log.Printf("task_broker: renewing %s for %s: %v", taskID, identity, err)
	}
}

// onClosed moves the task to DONE on its terminal event, the only terminal
// state (architecture §3.1), and drops the announcement with its embedding
// (implementation §1.2.7). A close of a task without a key—a trace's root—
// moves nothing.
func (w *worker) onClosed(ctx context.Context, m jetstream.Msg) {
	e, err := readEvent(m, 3, w.topic)
	if err != nil {
		log.Printf("task_broker: dropping %s: %v", m.Subject(), err)
		m.Ack()
		return
	}
	_, err = w.b.tasks.transition(ctx, taskKey(w.topic, e.Subject), func(t *task) error {
		if t.State == stateDone {
			return errNoChange
		}
		t.State, t.LeaseExpiry, t.Announcement = stateDone, time.Time{}, nil
		return nil
	})
	if err != nil && !errors.Is(err, errNoChange) && !errors.Is(err, jetstream.ErrKeyNotFound) && !errors.Is(err, jetstream.ErrInvalidKey) {
		log.Printf("task_broker: closing %s: %v", e.Subject, err)
		m.NakWithDelay(retryDelay)
		return
	}
	delete(w.auctions, e.Subject)
	w.disarm(e.Subject)
	m.Ack()
}

// resume arms what a task waits for from its stored state: its next
// announcement or the expiry of its lease.
func (w *worker) resume(t *task, taskID string) {
	if _, open := w.auctions[taskID]; open {
		return
	}
	switch t.State {
	case stateUnassigned:
		w.scheduleAnnouncement(t, taskID)
	case stateClaimed:
		w.arm(taskID, w.leaseDeadline(t), (*worker).checkLease)
	default:
		w.disarm(taskID)
	}
}

// readEvent decodes and validates an event, and checks that the topic its
// subject names at topicAt is the worker's.
func readEvent(m jetstream.Msg, topicAt int, topic string) (*bus.Event, error) {
	var e bus.Event
	if err := json.Unmarshal(m.Data(), &e); err != nil {
		return nil, err
	}
	if err := e.Validate(); err != nil {
		return nil, err
	}
	if e.Subject == "" {
		return nil, errors.New("the event names no task")
	}
	if tokens := strings.Split(m.Subject(), "."); len(tokens) <= topicAt || tokens[topicAt] != topic {
		return nil, fmt.Errorf("subject %q is not of topic %q", m.Subject(), topic)
	}
	return &e, nil
}
