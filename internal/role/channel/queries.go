package channel

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/google/uuid"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/claim"
	channelv1 "zobik.org/zobik/proto/zobik/channel/v1"
	nriv1 "zobik.org/zobik/proto/zobik/nri/v1"
)

// The failure codes of a query closed without an answer (architecture §3.5.2).
const (
	codeUnanswered  = "unanswered"
	codeUndelivered = "undelivered"
)

// query is a hitl_contact_* task the channel won and retains until the person
// answers it, the window expires or, outside the final round, the session closes.
type query struct {
	a     *claim.Assignment
	final bool
	shown bool
	// deliveryID names it before the console.
	deliveryID string
	stopLease  func()
	timer      *time.Timer
}

// session is the console's Attach (implementation §6, The service between the
// console and the channels).
type session struct {
	out  chan *channelv1.ChannelMessage
	done chan struct{}
}

func (s *session) send(m *channelv1.ChannelMessage) {
	select {
	case s.out <- m:
	case <-s.done:
	}
}

// onAnnounced proposes on a query of its audience when it is its turn: its
// attempt_window includes the round, and outside the final round, only with the
// console attached. In round 1 of the trace's origin audience only the owner of
// the TraceID proposes (architecture §3.5.2, implementation §6, The operator console).
func (c *channel) onAnnounced(ctx context.Context, e *bus.Event) error {
	r := claim.ReadRound(e)
	c.mu.Lock()
	attached := c.session != nil
	_, owner := c.open[bus.TraceID(e.TraceParent)]
	_, retained := c.queries[e.Subject]
	c.mu.Unlock()
	switch {
	case retained, !claim.Admits(c.window, r):
		return nil
	case !r.FinalRound && !attached:
		return nil
	case r.ReservedTo(c.audience) && !owner:
		return nil
	}
	return c.claimant.Propose(ctx, e, nil)
}

// onAssigned retains a query it won, keeps its lease and shows it to the console.
func (c *channel) onAssigned(ctx context.Context, e *bus.Event) error {
	a, ok := c.claimant.Assigned(e)
	if !ok {
		return nil
	}
	r := claim.ReadRound(a.Announcement)
	window, err := c.responseWindow(ctx, a.Announcement.ConfigVersion)
	if err != nil {
		// Without the window, the query cannot be bounded: the lease is let
		// go and the task comes back with its re-announcement.
		log.Printf("channel: %s: %v", a.TaskID, err)
		return nil
	}
	q := &query{a: a, final: r.FinalRound, deliveryID: e.ID}
	q.stopLease = c.claimant.KeepLease(a, c.leaseTTL)
	q.timer = time.AfterFunc(window, func() { c.expire(a.TaskID, "hitl_response_window expired without an answer") })
	c.mu.Lock()
	c.queries[a.TaskID] = q
	s := c.session
	c.mu.Unlock()
	if s != nil {
		s.send(consultation(q))
	}
	return nil
}

// consultation is the query as the console receives it: its task.assigned,
// without the scope token, with its task.announced (implementation §3, The session).
func consultation(q *query) *channelv1.ChannelMessage {
	var data map[string]json.RawMessage
	_ = json.Unmarshal(q.a.Assigned.Data, &data) // the Task Broker validated it
	delete(data, "scope_token")
	raw, _ := json.Marshal(data)
	e := *q.a.Assigned
	e.Data = raw
	b, _ := json.Marshal(e)
	ann, _ := json.Marshal(q.a.Announcement)
	return &channelv1.ChannelMessage{Body: &channelv1.ChannelMessage_Consultation{
		Consultation: &nriv1.Delivery{DeliveryId: q.deliveryID, Event: b, Announcement: ann},
	}}
}

// take removes a retained query and stops its lease and its window.
func (c *channel) take(taskID string) (*query, bool) {
	c.mu.Lock()
	q, ok := c.queries[taskID]
	delete(c.queries, taskID)
	c.mu.Unlock()
	if ok {
		q.stopLease()
		q.timer.Stop()
	}
	return q, ok
}

// expire closes a query without an answer: unanswered if the console showed it,
// undelivered if not, and tells the console with an expired it confirms
// (architecture §3.5.2, implementation §6, The service between the console and the channels).
func (c *channel) expire(taskID, reason string) {
	q, ok := c.take(taskID)
	if !ok {
		return
	}
	code := codeUndelivered
	if q.shown {
		code = codeUnanswered
	}
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	if err := c.claimant.Fail(ctx, q.a, code, reason); err != nil {
		log.Printf("channel: closing %s: %v", taskID, err)
	}
	ex := &channelv1.Expired{DeliveryId: uuid.NewString(), TaskId: taskID, FailureCode: code}
	c.mu.Lock()
	c.expired = append(c.expired, ex)
	s := c.session
	c.mu.Unlock()
	if s != nil {
		s.send(&channelv1.ChannelMessage{Body: &channelv1.ChannelMessage_Expired{Expired: ex}})
	}
}

// attach opens the console's session: it replaces the previous one, and sends
// each query the role retains and each expired not yet confirmed.
func (c *channel) attach() *session {
	s := &session{out: make(chan *channelv1.ChannelMessage, 64), done: make(chan struct{})}
	c.mu.Lock()
	c.session = s
	var pending []*channelv1.ChannelMessage
	for _, q := range c.queries {
		pending = append(pending, consultation(q))
	}
	for _, ex := range c.expired {
		pending = append(pending, &channelv1.ChannelMessage{Body: &channelv1.ChannelMessage_Expired{Expired: ex}})
	}
	c.mu.Unlock()
	go func() {
		for _, m := range pending {
			s.send(m)
		}
	}()
	return s
}

// detach closes a session. Without the console, the queries outside the final
// round are let go, so the absence escalates to the audience's other channels
// (implementation §6, The operator console).
func (c *channel) detach(s *session) {
	close(s.done)
	c.mu.Lock()
	if c.session != s {
		c.mu.Unlock()
		return
	}
	c.session = nil
	var release []string
	for id, q := range c.queries {
		if !q.final {
			release = append(release, id)
		}
	}
	c.mu.Unlock()
	for _, id := range release {
		c.expire(id, "the console's session closed without an answer")
	}
}

func (c *channel) shown(taskID string) {
	c.mu.Lock()
	if q, ok := c.queries[taskID]; ok {
		q.shown = true
	}
	c.mu.Unlock()
}

func (c *channel) received(deliveryID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, ex := range c.expired {
		if ex.DeliveryId == deliveryID {
			c.expired = append(c.expired[:i], c.expired[i+1:]...)
			return
		}
	}
}

var errNotRetained = errors.New("the role does not retain that query")

// answer closes a query with task.completed and the data the console assembles
// (implementation §6, The service between the console and the channels).
func (c *channel) answer(ctx context.Context, taskID string, data []byte) error {
	var fields map[string]any
	if err := json.Unmarshal(data, &fields); err != nil || fields == nil {
		return errors.New("the answer is not a JSON object")
	}
	q, ok := c.take(taskID)
	if !ok {
		return errNotRetained
	}
	// A close that fails leaves the query without a lease: it comes back with
	// its re-announcement.
	return c.claimant.Complete(ctx, q.a, fields)
}
