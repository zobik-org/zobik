// Package claim is a node's half of the CNP cycle on its topic: it proposes with
// the node's credential, keeps the lease of what the node wins and closes it
// toward the requester (architecture §3.1, §3.7.5). The Integration Sidecar does
// it on behalf of its Logic Container (architecture §3.6.1), and a structural
// role that claims a topic does it itself.
package claim

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"zobik.org/zobik/internal/bus"
)

// Claimant is the cycle on one topic, under one platform identity.
type Claimant struct {
	id, credential string
	js             jetstream.JetStream
	nc             *nats.Conn
	topic          string

	mu sync.Mutex
	// proposed are the announcements it proposed on, by TaskID: the assignment
	// does not repeat the announcement's data, so the claimant retains it.
	proposed map[string]proposed
}

type proposed struct {
	ann *bus.Event
	at  time.Time
}

// forgetAfter is how long an announcement is retained without an answer.
const forgetAfter = 10 * time.Minute

// New is the claimant of the identity id, whose capability credential is credential.
func New(id, credential string, js jetstream.JetStream, nc *nats.Conn, topic string) *Claimant {
	return &Claimant{id: id, credential: credential, js: js, nc: nc, topic: topic, proposed: map[string]proposed{}}
}

// Propose publishes a task.proposed with the credential by value and, if
// declared is not nil, the similarity it declares (architecture §3.7.5,
// implementation §1.2.4).
func (c *Claimant) Propose(ctx context.Context, ann *bus.Event, declared *float64) error {
	data := map[string]any{"topic": c.topic, "credential": c.credential}
	if declared != nil {
		data["declared_similarity"] = *declared
	}
	e, err := bus.NewEvent(bus.TypeProposed, c.id, ann.Subject, data)
	if err != nil {
		return err
	}
	e.TraceParent, e.ConfigVersion = ann.TraceParent, ann.ConfigVersion
	c.mu.Lock()
	now := time.Now()
	for id, p := range c.proposed {
		if now.Sub(p.at) > forgetAfter {
			delete(c.proposed, id)
		}
	}
	c.proposed[ann.Subject] = proposed{ann: ann, at: now}
	c.mu.Unlock()
	return bus.Publish(ctx, c.js, bus.SubjectProposed(c.id, c.topic), e)
}

// Assignment is a task the claimant won.
type Assignment struct {
	Announcement *bus.Event
	// Assigned is the task.assigned the Task Broker published.
	Assigned *bus.Event
	TaskID   string
	// Token is the scope token the Task Broker minted for this task (architecture §3.14.3).
	Token     string
	Requester string
	Started   time.Time
}

// Inherited is what the task's events inherit from its announcement:
// workspace and origin_audience (architecture §3.6.1).
func (a *Assignment) Inherited() map[string]any {
	var d struct {
		Workspace      *string `json:"workspace"`
		OriginAudience *string `json:"origin_audience"`
	}
	_ = json.Unmarshal(a.Announcement.Data, &d) // a field of another type reads as absent
	out := map[string]any{}
	if d.Workspace != nil {
		out["workspace"] = *d.Workspace
	}
	if d.OriginAudience != nil {
		out["origin_audience"] = *d.OriginAudience
	}
	return out
}

// Assigned matches a task.assigned with the announcement it answers. It
// returns false for one it did not propose on, or no longer retains.
func (c *Claimant) Assigned(e *bus.Event) (*Assignment, bool) {
	c.mu.Lock()
	p, ok := c.proposed[e.Subject]
	delete(c.proposed, e.Subject)
	c.mu.Unlock()
	if !ok {
		return nil, false
	}
	var d struct {
		ScopeToken        string `json:"scope_token"`
		RequesterIdentity string `json:"requester_identity"`
	}
	_ = json.Unmarshal(e.Data, &d) // a field of another type reads as absent
	return &Assignment{Announcement: p.ann, Assigned: e, TaskID: e.Subject, Token: d.ScopeToken,
		Requester: d.RequesterIdentity, Started: time.Now()}, true
}

// Rejected forgets the announcement of a proposal that lost.
func (c *Claimant) Rejected(e *bus.Event) {
	c.mu.Lock()
	delete(c.proposed, e.Subject)
	c.mu.Unlock()
}

// Forget forgets every announcement it proposed on: an assignment that answers
// one of them afterwards is not taken.
func (c *Claimant) Forget() {
	c.mu.Lock()
	c.proposed = map[string]proposed{}
	c.mu.Unlock()
}

// KeepLease renews the lease at a third of task_lease_ttl, which ttl reads in its
// present value, until stop (implementation §1.2.1, §6, The initial Global
// Configuration). The renewal is a bodiless message on NATS core.
func (c *Claimant) KeepLease(a *Assignment, ttl func() time.Duration) (stop func()) {
	done := make(chan struct{})
	subject := "lease." + c.id + "." + c.topic + "." + a.TaskID
	go func() {
		for {
			_ = c.nc.Publish(subject, nil) // a lost renewal is made up by the next one
			select {
			case <-done:
				return
			case <-time.After(ttl() / 3):
			}
		}
	}()
	var once sync.Once
	return func() { once.Do(func() { close(done) }) }
}

// Complete closes the task with task.completed toward its requester. data
// carries the topic's fields.
func (c *Claimant) Complete(ctx context.Context, a *Assignment, data map[string]any) error {
	return c.Close(ctx, a, bus.TypeCompleted, data)
}

// Fail closes the task with task.failed, its reason and, where the architecture
// fixes the cause, its code (architecture §3.3).
func (c *Claimant) Fail(ctx context.Context, a *Assignment, code, reason string) error {
	data := map[string]any{"failure_reason": reason}
	if code != "" {
		data["failure_code"] = code
	}
	return c.Close(ctx, a, bus.TypeFailed, data)
}

// Close publishes the terminal event typ of the task toward its requester.
func (c *Claimant) Close(ctx context.Context, a *Assignment, typ string, data map[string]any) error {
	e, subject, err := c.Terminal(a, typ, data)
	if err != nil {
		return err
	}
	return bus.Publish(ctx, c.js, subject, e)
}

// Terminal assembles and validates the terminal event typ of the task, on the
// task's identifier, with what the trace inherits copied from the task
// (architecture §3.6.1), and returns it with the subject of its requester. The
// metrics are those of a task that spent no tokens and no money (architecture §3.10).
func (c *Claimant) Terminal(a *Assignment, typ string, data map[string]any) (*bus.Event, string, error) {
	for k, v := range a.Inherited() {
		data[k] = v
	}
	data["topic"] = c.topic
	data["metrics"] = map[string]any{
		"tokens_in": 0, "tokens_out": 0, "cost": 0,
		"duration_ms": time.Since(a.Started).Milliseconds(),
	}
	e, err := bus.NewEvent(typ, c.id, a.TaskID, data)
	if err != nil {
		return nil, "", err
	}
	e.TraceParent, e.ConfigVersion = a.Assigned.TraceParent, a.Assigned.ConfigVersion
	if err := e.Validate(); err != nil {
		return nil, "", err
	}
	if err := e.ValidateTopic(); err != nil {
		return nil, "", err
	}
	return e, bus.SubjectTerminal(typ, a.Requester, c.topic), nil
}
