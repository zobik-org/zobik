package sidecar

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/claim"
	"zobik.org/zobik/internal/egress"
	"zobik.org/zobik/internal/signing"
	nriv1 "zobik.org/zobik/proto/zobik/nri/v1"
)

// opTimeout bounds each read and write on the Bus.
const opTimeout = 5 * time.Second

// codeLogicContainerLost closes the tasks of a session that was cut (architecture §3.6.1).
const codeLogicContainerLost = "logic_container_lost"

// node is the Bus side of the Integration Sidecar: what it decides on its
// topic's announcements, the tasks it holds for its Logic Container and the
// session that delivers them.
type node struct {
	id   string
	cred *signing.Capability
	// channel is a node on a hitl_contact_* topic: it holds every query it wins,
	// and its topic is exempt from similarity (architecture §3.6.1, §3.7.2).
	channel  bool
	audience string
	js       jetstream.JetStream
	claimant *claim.Claimant
	cfg      *config

	st  *state
	out *outbox
	// areas is the shared volume, where the work areas go (implementation §3),
	// opened as a root: the Logic Container writes inside it, so no path under
	// it is resolved outside it. private is the Integration Sidecar's own volume.
	areas   *os.Root
	private string
	// root is the root's public key, which a provision grant's proof verifies
	// against, and egress executes the calls (architecture §3.16.4).
	root   string
	egress *egress.Executor

	mu      sync.Mutex
	session *session
	// stopping is set when the process ends: it takes no new session.
	stopping bool
	tasks    map[string]*task // by TaskID
	// held are the deliveries waiting for the Logic Container's Received, by
	// delivery_id: the Bus's message is acknowledged on it (implementation §3,
	// The session).
	held map[string]*held
	// subtasks are the lineages the node's tasks opened, by each attempt's TaskID.
	subtasks map[string]*lineage
	// unsent counts the closed tasks whose terminal event the Bus has not
	// confirmed yet.
	unsent int
	// waiting is the TaskID of the proposal a worker waits on, until its
	// answer or waitUntil: it proposes on no other meanwhile, so it does not
	// win two tasks at once.
	waiting   string
	waitUntil time.Time
}

// task is an assignment the node holds for its Logic Container.
type task struct {
	a *claim.Assignment
	// deliveryID names its delivery, the task.assigned's id.
	deliveryID string
	stopLease  func()
	// closing marks a close in flight, and detached a task whose session was
	// cut meanwhile: if that close fails, the task fails as the session's.
	closing, detached bool
	// calls are the calls in flight on the task: its terminal event waits for
	// them, so their charge is in its metrics (architecture §3.10).
	calls sync.WaitGroup
}

// held is a delivery waiting for its Received: an assignment, or the result
// of a subtask of the task parent.
type held struct {
	msg       jetstream.Msg
	parent    string
	confirmed chan struct{}
}

func newNode(id, credential string, cred *signing.Capability, js jetstream.JetStream, nc *nats.Conn, cfg *config, st *state, out *outbox, areas *os.Root) *node {
	n := &node{
		id: id, cred: cred, js: js, cfg: cfg, st: st, out: out, areas: areas,
		claimant: claim.New(id, credential, js, nc, cred.Topic),
		tasks:    map[string]*task{}, held: map[string]*held{}, subtasks: map[string]*lineage{},
	}
	if strings.HasPrefix(cred.Topic, signing.HITLPrefix) {
		n.channel, n.audience = true, strings.TrimPrefix(cred.Topic, signing.HITLPrefix)
	}
	return n
}

// bind pulls from the node's durable consumer while the session lasts
// (implementation §3, The session). The consumer is the Spawner's: binding is
// reading from it, and unbinding, stopping.
func (n *node) bind(ctx context.Context) {
	var c jetstream.Consumer
	for c == nil {
		opCtx, cancel := context.WithTimeout(ctx, opTimeout)
		var err error
		c, err = n.js.Consumer(opCtx, "task", n.id)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("sidecar: binding the consumer: %v", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
		}
	}
	for ctx.Err() == nil {
		pull, cancel := context.WithTimeout(ctx, 5*time.Second)
		batch, err := c.Fetch(64, jetstream.FetchContext(pull))
		if err != nil {
			cancel()
			if ctx.Err() == nil {
				log.Printf("sidecar: fetching: %v", err)
				time.Sleep(time.Second)
			}
			continue
		}
		for m := range batch.Messages() {
			n.handle(ctx, m)
		}
		cancel()
	}
}

func (n *node) handle(ctx context.Context, m jetstream.Msg) {
	if ctx.Err() != nil {
		return // unbound: the Bus redelivers it to the next session
	}
	var e bus.Event
	if err := json.Unmarshal(m.Data(), &e); err != nil || e.Validate() != nil || e.Subject == "" {
		m.Ack()
		return
	}
	// What reaches the Logic Container is deduplicated by id before it is
	// delivered (architecture §3.6.4).
	if e.Type != bus.TypeAnnounced && e.Type != bus.TypeRejected && n.st.delivered(e.ID) {
		m.Ack()
		return
	}
	opCtx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	switch e.Type {
	case bus.TypeAnnounced:
		if err := n.onAnnounced(opCtx, &e); err != nil {
			log.Printf("sidecar: proposing on %s: %v", e.Subject, err)
		}
	case bus.TypeAssigned:
		if n.onAssigned(&e, m) {
			return // acknowledged on the Logic Container's Received
		}
	case bus.TypeRejected:
		n.claimant.Rejected(&e)
		n.answered(e.Subject)
	case bus.TypeCompleted, bus.TypeFailed, bus.TypeAborted:
		if n.onResult(&e, m) {
			return
		}
	}
	m.Ack()
}

// onAnnounced proposes when the node is eligible for the announcement
// (architecture §3.6.1, §3.7.5, step 3).
func (n *node) onAnnounced(ctx context.Context, e *bus.Event) error {
	auction := n.cfg.auctionTimeout()
	// An announcement older than an auction has no round open to propose in.
	if time.Since(e.Time) > auction {
		return nil
	}
	r := claim.ReadRound(e)
	if !claim.Admits(n.cred.AttemptWindow, r) {
		return nil
	}
	n.mu.Lock()
	attached := n.session != nil
	free := attached && len(n.tasks) == 0 && n.unsent == 0 && (n.waiting == "" || time.Now().After(n.waitUntil))
	n.mu.Unlock()
	if n.channel {
		if !attached {
			return nil
		}
		// It owns no TraceID until it opens traces, so round 1 of its own
		// audience is never its turn (architecture §3.5.2).
		if r.ReservedTo(n.audience) {
			return nil
		}
		return n.claimant.Propose(ctx, e, nil)
	}
	if !free {
		return nil
	}
	decimals, err := n.cfg.decimals(ctx, e.ConfigVersion)
	if err != nil {
		return err
	}
	s, ok := claim.Similarity(n.cred, e, decimals)
	if !ok {
		return nil
	}
	n.mu.Lock()
	n.waiting, n.waitUntil = e.Subject, time.Now().Add(auction)
	n.mu.Unlock()
	return n.claimant.Propose(ctx, e, &s)
}

// answered ends the wait on a proposal.
func (n *node) answered(taskID string) {
	n.mu.Lock()
	if n.waiting == taskID {
		n.waiting = ""
	}
	n.mu.Unlock()
}

// onAssigned takes a task the node won and delivers it to the session. It does
// not take one it did not propose on in this session, nor a worker's second
// task: it neither delivers it nor renews its lease, so the lease expires and
// the task is re-announced (architecture §3.1). It reports whether it holds m
// until the Logic Container confirms the delivery.
func (n *node) onAssigned(e *bus.Event, m jetstream.Msg) bool {
	a, ok := n.claimant.Assigned(e)
	n.answered(e.Subject)
	if !ok {
		return false
	}
	n.mu.Lock()
	s := n.session
	if s == nil || (!n.channel && len(n.tasks) > 0) {
		n.mu.Unlock()
		log.Printf("sidecar: %s won and let go", a.TaskID)
		return false
	}
	// The work area is created empty before the delivery (implementation §3,
	// Context Store and egress); a TaskID that cannot name it is not taken.
	if err := n.newArea(a.TaskID); err != nil {
		n.mu.Unlock()
		log.Printf("sidecar: %s won and let go: %v", a.TaskID, err)
		return false
	}
	t := &task{a: a, deliveryID: e.ID}
	t.stopLease = n.claimant.KeepLease(a, n.cfg.leaseTTL)
	n.tasks[a.TaskID] = t
	n.hold(e.ID, "", m, s.done)
	n.mu.Unlock()

	s.send(&nriv1.SidecarMessage{Body: &nriv1.SidecarMessage_Assignment{Assignment: delivery(t)}})
	return true
}

// hold keeps m unacknowledged until the Logic Container confirms the delivery
// id. Called with n.mu held.
func (n *node) hold(id, parent string, m jetstream.Msg, done <-chan struct{}) {
	h := &held{msg: m, parent: parent, confirmed: make(chan struct{})}
	n.held[id] = h
	go holdUnconfirmed(m, h.confirmed, done)
}

// holdUnconfirmed keeps the Bus from redelivering a task.assigned while its
// delivery waits for the Logic Container's Received.
func holdUnconfirmed(m jetstream.Msg, confirmed, done <-chan struct{}) {
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-confirmed:
			return
		case <-done:
			return
		case <-tick.C:
			_ = m.InProgress() // a lost one is made up by the next
		}
	}
}

// delivery is the assignment as the Logic Container receives it: its
// task.assigned without the scope token, which the node keeps, and its
// task.announced (implementation §3, The session).
func delivery(t *task) *nriv1.Delivery {
	var data map[string]json.RawMessage
	_ = json.Unmarshal(t.a.Assigned.Data, &data) // it passed its validation
	delete(data, "scope_token")
	raw, _ := json.Marshal(data)
	e := *t.a.Assigned
	e.Data = raw
	event, _ := json.Marshal(e)
	ann, _ := json.Marshal(t.a.Announcement)
	return &nriv1.Delivery{DeliveryId: t.deliveryID, Event: event, Announcement: ann}
}

// received settles the delivery the Logic Container confirmed.
func (n *node) received(deliveryID string) {
	n.mu.Lock()
	h := n.unhold(deliveryID)
	n.mu.Unlock()
	if h != nil {
		n.settle(deliveryID, h.msg)
	}
}

// unhold takes a held delivery out, or returns nil. Called with n.mu held.
func (n *node) unhold(id string) *held {
	h := n.held[id]
	if h == nil {
		return nil
	}
	delete(n.held, id)
	close(h.confirmed)
	return h
}

// settle marks the event id as processed and only then acknowledges it, so a
// crash in between delivers it again only to be discarded (architecture §3.6.4).
func (n *node) settle(id string, m jetstream.Msg) {
	if err := n.st.mark(id); err != nil {
		log.Printf("sidecar: marking %s: %v", id, err)
	}
	m.Ack()
}

// begin reserves a task the node holds for closing it, or returns nil: two
// requests do not close a task twice.
func (n *node) begin(taskID string) *task {
	n.mu.Lock()
	defer n.mu.Unlock()
	t := n.tasks[taskID]
	if t == nil || t.closing {
		return nil
	}
	t.closing = true
	// A call waiting for a gate's answer returns without executing, so the
	// close does not wait for the person.
	n.endGates(taskID)
	return t
}

// abort releases a task whose close failed.
func (n *node) abort(t *task) {
	n.mu.Lock()
	t.closing = false
	detached := t.detached
	n.mu.Unlock()
	if detached {
		n.lost(t)
	}
}

// closed forgets a task whose terminal event is in the outbox, with its work
// area and its subtasks. Its lease is renewed until the Bus confirms the event.
func (n *node) closed(t *task, sent <-chan struct{}) {
	n.mu.Lock()
	delete(n.tasks, t.a.TaskID)
	n.forgetSubtasks(t.a.TaskID)
	// The task's delivery and its subtasks' results are settled: no one
	// waits on them.
	settled := map[string]*held{}
	for id, h := range n.held {
		if id == t.deliveryID || h.parent == t.a.TaskID {
			settled[id] = n.unhold(id)
		}
	}
	n.unsent++
	n.mu.Unlock()
	for id, h := range settled {
		n.settle(id, h.msg)
	}
	n.removeArea(t.a.TaskID)
	go func() {
		// Until the Bus confirms the terminal event, the task is still the
		// node's: its lease is renewed and a worker proposes on nothing
		// (architecture §3.6.1). The end of the process ends the wait.
		select {
		case <-sent:
		case <-n.out.ctx.Done():
		}
		t.stopLease()
		n.mu.Lock()
		n.unsent--
		n.mu.Unlock()
	}()
}

// attach starts a session, unless one is open or the process is ending.
func (n *node) attach(s *session) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	switch {
	case n.stopping:
		return status.Error(codes.Unavailable, "the Integration Sidecar is stopping")
	case n.session != nil:
		return status.Error(codes.FailedPrecondition, "a session is already open")
	}
	n.session = s
	return nil
}

// cut ends the session: it closes each task with task.failed, stops renewing
// their leases and forgets the proposals in flight. A later session starts with
// no tasks (architecture §3.6.1).
func (n *node) cut(s *session) {
	n.mu.Lock()
	if n.session != s {
		n.mu.Unlock()
		return
	}
	n.session = nil
	s.unbind()
	n.waiting = ""
	tasks := n.tasks
	n.tasks = map[string]*task{}
	// The terminal events of the subtasks reach no one: their parents fail.
	n.endGates("")
	n.subtasks = map[string]*lineage{}
	unconfirmed := n.held
	n.held = map[string]*held{}
	for _, h := range unconfirmed {
		close(h.confirmed)
	}
	// Under the lock, so a session that opens next finds its areas intact.
	n.clearAreas()
	n.mu.Unlock()
	n.claimant.Forget()
	for id, h := range unconfirmed {
		n.settle(id, h.msg)
	}

	for _, t := range tasks {
		n.mu.Lock()
		t.detached = t.closing
		n.mu.Unlock()
		if !t.detached {
			n.lost(t)
		}
	}
}

// cutOpen cuts the open session, if there is one, and takes no other.
func (n *node) cutOpen() {
	n.mu.Lock()
	n.stopping = true
	s := n.session
	n.mu.Unlock()
	if s != nil {
		n.cut(s)
	}
}

// lost closes a task of a cut session with task.failed (architecture §3.6.1).
func (n *node) lost(t *task) {
	// The cut ends the session's calls with it.
	t.calls.Wait()
	e, subject, err := n.claimant.Terminal(t.a, bus.TypeFailed, map[string]any{
		"failure_reason": "the Node Runtime Interface session was cut", "failure_code": codeLogicContainerLost,
	})
	if err == nil {
		var sent <-chan struct{}
		if sent, err = n.out.put(&pending{Subject: subject, Event: e}); err == nil {
			n.closed(t, sent)
			return
		}
	}
	log.Printf("sidecar: closing %s: %v", t.a.TaskID, err)
	t.stopLease()
}
