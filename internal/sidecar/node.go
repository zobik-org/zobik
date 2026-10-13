package sidecar

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/claim"
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

	mu      sync.Mutex
	session *session
	tasks   map[string]*task // by TaskID
	// waiting is the TaskID of the proposal a worker waits on, until its
	// answer or waitUntil: it proposes on no other meanwhile, so it does not
	// win two tasks at once.
	waiting   string
	waitUntil time.Time
}

// task is an assignment the node holds for its Logic Container.
type task struct {
	a *claim.Assignment
	// deliveryID names its delivery; msg is the task.assigned, acknowledged to
	// the Bus when the Logic Container confirms it (implementation §3, The session).
	deliveryID string
	msg        jetstream.Msg
	confirmed  chan struct{}
	stopLease  func()
	// closing marks a close in flight, and detached a task whose session was
	// cut meanwhile: if that close fails, the task fails as the session's.
	closing, detached bool
}

func newNode(id, credential string, cred *signing.Capability, js jetstream.JetStream, nc *nats.Conn, cfg *config) *node {
	n := &node{
		id: id, cred: cred, js: js, cfg: cfg,
		claimant: claim.New(id, credential, js, nc, cred.Topic),
		tasks:    map[string]*task{},
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
	free := attached && len(n.tasks) == 0 && (n.waiting == "" || time.Now().After(n.waitUntil))
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
	t := &task{a: a, deliveryID: e.ID, msg: m, confirmed: make(chan struct{})}
	t.stopLease = n.claimant.KeepLease(a, n.cfg.leaseTTL)
	n.tasks[a.TaskID] = t
	n.mu.Unlock()

	go holdUnconfirmed(m, t.confirmed, s.done)
	s.send(&nriv1.SidecarMessage{Body: &nriv1.SidecarMessage_Assignment{Assignment: delivery(t)}})
	return true
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

// received acknowledges to the Bus the delivery the Logic Container confirmed.
func (n *node) received(deliveryID string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, t := range n.tasks {
		if t.deliveryID == deliveryID && t.msg != nil {
			t.msg.Ack()
			t.msg = nil
			close(t.confirmed)
			return
		}
	}
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

// closed forgets a task whose terminal event is published.
func (n *node) closed(t *task) {
	n.mu.Lock()
	delete(n.tasks, t.a.TaskID)
	if t.msg != nil {
		t.msg.Ack()
		t.msg = nil
		close(t.confirmed)
	}
	n.mu.Unlock()
	t.stopLease()
}

// open starts a session, unless one is open.
func (n *node) open(s *session) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.session != nil {
		return false
	}
	n.session = s
	return true
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
	n.waiting = ""
	tasks := n.tasks
	n.tasks = map[string]*task{}
	n.mu.Unlock()
	n.claimant.Forget()

	for _, t := range tasks {
		n.mu.Lock()
		t.detached = t.closing
		n.mu.Unlock()
		if !t.detached {
			n.lost(t)
		}
	}
}

// cutOpen cuts the open session, if there is one.
func (n *node) cutOpen() {
	n.mu.Lock()
	s := n.session
	n.mu.Unlock()
	if s != nil {
		n.cut(s)
	}
}

// lost closes a task of a cut session with task.failed (architecture §3.6.1).
func (n *node) lost(t *task) {
	t.stopLease()
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	if err := n.claimant.Fail(ctx, t.a, codeLogicContainerLost, "the Node Runtime Interface session was cut"); err != nil {
		log.Printf("sidecar: closing %s: %v", t.a.TaskID, err)
	}
	n.mu.Lock()
	if t.msg != nil {
		t.msg.Ack()
		t.msg = nil
	}
	n.mu.Unlock()
}
