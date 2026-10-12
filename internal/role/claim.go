package role

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/rpcauth"
)

// A structural node that claims a topic takes part in the CNP cycle itself:
// it proposes with its credential, keeps the lease of what it wins and closes
// it toward the requester (architecture §3.1, §3.7.5). What a node's Integration
// Sidecar does on its behalf (architecture §3.6.1), a role does here.

// Claimant is a role's half of the cycle on one topic.
type Claimant struct {
	rt    *Runtime
	js    jetstream.JetStream
	nc    *nats.Conn
	topic string

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

func NewClaimant(rt *Runtime, js jetstream.JetStream, nc *nats.Conn, topic string) *Claimant {
	return &Claimant{rt: rt, js: js, nc: nc, topic: topic, proposed: map[string]proposed{}}
}

// Propose publishes the single task.proposed of a structural claimant, with its
// credential by value (architecture §3.7.5, step 4, implementation §1.2.4).
func (c *Claimant) Propose(ctx context.Context, ann *bus.Event) error {
	e, err := bus.NewEvent(bus.TypeProposed, c.rt.ID, ann.Subject, map[string]any{
		"topic":      c.topic,
		"credential": c.rt.Credential,
	})
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
	return bus.Publish(ctx, c.js, bus.SubjectProposed(c.rt.ID, c.topic), e)
}

// Assignment is a task the claimant won.
type Assignment struct {
	Announcement *bus.Event
	TaskID       string
	// Token is the scope token the Task Broker minted for this task (architecture §3.14.3).
	Token     string
	Requester string
	Started   time.Time
	assigned  *bus.Event
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
	json.Unmarshal(e.Data, &d)
	return &Assignment{Announcement: p.ann, TaskID: e.Subject, Token: d.ScopeToken,
		Requester: d.RequesterIdentity, Started: time.Now(), assigned: e}, true
}

// Rejected forgets the announcement of a proposal that lost.
func (c *Claimant) Rejected(e *bus.Event) {
	c.mu.Lock()
	delete(c.proposed, e.Subject)
	c.mu.Unlock()
}

// KeepLease renews the lease at a third of task_lease_ttl, which ttl reads in its
// present value, until stop (implementation §1.2.1, §6, The initial Global
// Configuration). The renewal is a bodiless message on NATS core.
func (c *Claimant) KeepLease(a *Assignment, ttl func() time.Duration) (stop func()) {
	done := make(chan struct{})
	subject := "lease." + c.rt.ID + "." + c.topic + "." + a.TaskID
	go func() {
		for {
			c.nc.Publish(subject, nil)
			select {
			case <-done:
				return
			case <-time.After(ttl() / 3):
			}
		}
	}()
	return func() { close(done) }
}

// Complete closes the task with task.completed toward its requester. data
// carries the topic's fields; the metrics are those of a role, which spends no
// tokens and no money (architecture §3.10).
func (c *Claimant) Complete(ctx context.Context, a *Assignment, data map[string]any) error {
	return c.close(ctx, a, bus.TypeCompleted, data)
}

// Fail closes the task with task.failed, its reason and, where the architecture
// fixes the cause, its code (architecture §3.3).
func (c *Claimant) Fail(ctx context.Context, a *Assignment, code, reason string) error {
	data := map[string]any{"failure_reason": reason}
	if code != "" {
		data["failure_code"] = code
	}
	return c.close(ctx, a, bus.TypeFailed, data)
}

func (c *Claimant) close(ctx context.Context, a *Assignment, typ string, data map[string]any) error {
	data["topic"] = c.topic
	data["metrics"] = map[string]any{
		"tokens_in": 0, "tokens_out": 0, "cost": 0,
		"duration_ms": time.Since(a.Started).Milliseconds(),
	}
	e, err := bus.NewEvent(typ, c.rt.ID, a.TaskID, data)
	if err != nil {
		return err
	}
	e.TraceParent, e.ConfigVersion = a.assigned.TraceParent, a.assigned.ConfigVersion
	if err := e.ValidateTopic(); err != nil {
		return err
	}
	return bus.Publish(ctx, c.js, bus.SubjectTerminal(typ, a.Requester, c.topic), e)
}

// RPCCredentials authenticate the role to an off-Bus interface with its
// platform identity, reading its JWT from the volume on each connection
// (implementation §6, The off-Bus interfaces).
func (rt *Runtime) RPCCredentials() *rpcauth.Client {
	return &rpcauth.Client{
		JWT: func() (string, error) {
			b, err := os.ReadFile(filepath.Join(rt.dir, FileBusJWT))
			return strings.TrimSpace(string(b)), err
		},
		Key: rt.key,
	}
}
