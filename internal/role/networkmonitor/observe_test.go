package networkmonitor_test

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/globalconfig"
	"zobik.org/zobik/internal/role"
	"zobik.org/zobik/internal/role/networkmonitor"
	"zobik.org/zobik/internal/testnet"
)

// The harness writes the stream by hand, as the network would, and the role
// measures the nodes of a fleet it controls (architecture §3.12.1,
// implementation §1.2.8).
const (
	topic     = "translate"
	requester = "UREQUESTER"
	nodeA     = "UNODEA"
	nodeB     = "UNODEB"
	nodeC     = "UNODEC"
)

type observer struct {
	*testnet.Net
	js   jetstream.JetStream
	stop func()
	run  role.Body

	mu      sync.Mutex
	fleet   []networkmonitor.Node
	changed chan struct{}
}

func newObserver(t *testing.T, nodes ...string) *observer {
	n := testnet.New(t, func(v globalconfig.Values) {
		testnet.Set(v, map[string]any{
			"network_admission": "open", "grace_period": 300,
			"quality_window": 8, "min_observations": 2, "usage_window": 3,
		})
	})
	o := &observer{Net: n, js: n.JetStream(n.Conn(bus.PublishScope.Role, nil)), changed: make(chan struct{}, 8)}
	for _, id := range nodes {
		o.fleet = append(o.fleet, networkmonitor.Node{NodeID: id, Topic: topic})
	}
	o.run = func(ctx context.Context, rt *role.Runtime, nc *nats.Conn) error {
		return networkmonitor.Run(ctx, rt, nc, networkmonitor.Options{
			Fleet: func(context.Context) ([]networkmonitor.Node, error) {
				o.mu.Lock()
				defer o.mu.Unlock()
				return append([]networkmonitor.Node(nil), o.fleet...), nil
			},
			Watch: func(ctx context.Context, changed func()) error {
				for {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-o.changed:
						changed()
					}
				}
			},
			ContextAddr: n.ContextAddr,
		})
	}
	o.start()
	return o
}

func (o *observer) start() { _, o.stop = o.StartStoppable("network_monitor", o.run) }

// provision adds a node to the Active Node Registry and signals the change.
func (o *observer) provision(id string) {
	o.mu.Lock()
	o.fleet = append(o.fleet, networkmonitor.Node{NodeID: id, Topic: topic})
	o.mu.Unlock()
	o.changed <- struct{}{}
}

// publish writes an event on the stream and returns its sequence.
func (o *observer) publish(subject, typ, source, task, trace string, data map[string]any) uint64 {
	o.T.Helper()
	e, err := bus.NewEvent(typ, source, task, data)
	if err != nil {
		o.T.Fatal(err)
	}
	if trace != "" {
		e.TraceParent, e.ConfigVersion = "00-"+trace+"-00f067aa0ba902b7-01", globalconfig.V0Version
	}
	if err := e.Validate(); err != nil {
		o.T.Fatal(err)
	}
	b, _ := json.Marshal(e)
	ack, err := o.js.Publish(o.Ctx, subject, b)
	if err != nil {
		o.T.Fatal(err)
	}
	return ack.Sequence
}

func newTrace() string { return strings.ReplaceAll(uuid.NewString(), "-", "") }

var someMetrics = func(cost float64, ms int) map[string]any {
	return map[string]any{"tokens_in": 100, "tokens_out": 20, "cost": cost, "duration_ms": ms}
}

// announce opens a task of by on topic; previous names the attempt it reopens.
func (o *observer) announce(by, on, trace, previous string) string {
	task := uuid.NewString()
	data := map[string]any{"topic": on, "requester_identity": by, "task_text": "translate this"}
	if previous != "" {
		data["previous_attempt"] = previous
	}
	o.publish(bus.SubjectAnnounced(on), bus.TypeAnnounced, by, task, trace, data)
	return task
}

func (o *observer) propose(node, task, trace string) {
	o.publish(bus.SubjectProposed(node, topic), bus.TypeProposed, node, task, trace, map[string]any{"topic": topic})
}

func (o *observer) assign(node, on, task, trace, by string, similarity float64) uint64 {
	return o.publish(bus.SubjectAddressed(bus.TypeAssigned, node), bus.TypeAssigned, "UBROKER", task, trace,
		map[string]any{"topic": on, "similarity": similarity, "requester_identity": by})
}

func (o *observer) close(typ, node, on, task, trace, by string, cost float64) uint64 {
	data := map[string]any{"topic": on, "metrics": someMetrics(cost, 1000)}
	if typ == bus.TypeFailed {
		data["failure_reason"] = "cannot translate this format"
	}
	return o.publish(typ+"."+by+"."+on, typ, node, task, trace, data)
}

// work is a task of the requester that node wins and closes with typ.
func (o *observer) work(node, typ string, cost float64) (task, trace string) {
	trace = newTrace()
	task = o.announce(requester, topic, trace, "")
	o.propose(node, task, trace)
	o.assign(node, topic, task, trace, requester, 0.8)
	o.close(typ, node, topic, task, trace, requester, cost)
	return task, trace
}

// await polls node's aggregate until ok holds.
func (o *observer) await(node string, ok func(*networkmonitor.Aggregate) bool) *networkmonitor.Aggregate {
	o.T.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var last *networkmonitor.Aggregate
	for time.Now().Before(deadline) {
		if a, err := networkmonitor.ReadAggregate(o.Ctx, o.Console, node); err == nil {
			last = a
			if ok(a) {
				return a
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	o.T.Fatalf("%s's aggregate did not reach the state: %+v", node, last)
	return nil
}

func (o *observer) params() networkmonitor.Params {
	head, _, err := globalconfig.Head(o.Ctx, o.Console)
	if err != nil {
		o.T.Fatal(err)
	}
	return networkmonitor.ReadParams(head)
}

func (o *observer) vector(a *networkmonitor.Aggregate) networkmonitor.Vector {
	return a.Vector(o.params(), time.Now())
}

// settled waits until the role acknowledged everything stored in every family.
func (o *observer) settled() {
	o.T.Helper()
	id := o.ID("network_monitor")
	deadline := time.Now().Add(30 * time.Second)
	for _, f := range []string{"task", "prop", "notice"} {
		for {
			c, err := o.Console.Consumer(o.Ctx, f, id)
			if err == nil {
				if info, err := c.Info(o.Ctx); err == nil && info.NumPending == 0 && info.NumAckPending == 0 {
					break
				}
			}
			if time.Now().After(deadline) {
				o.T.Fatalf("the role did not settle on %s", f)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// The vector of a node with clean, reopened and failed closes: a reopen by
// the requester of the attempt and a task.failed both count as unresolved, and
// the subtasks it opens add to its subtree (architecture §3.12.2).
func TestVectorFromStream(t *testing.T) {
	o := newObserver(t, nodeA, nodeB)
	o.work(nodeA, bus.TypeCompleted, 0.01)
	reopened, trace := o.work(nodeA, bus.TypeCompleted, 0.01)
	o.announce(requester, topic, trace, reopened)
	// A reopen by another identity does not count (architecture §3.12.2).
	other, otherTrace := o.work(nodeA, bus.TypeCompleted, 0.01)
	o.announce("USTRANGER", topic, otherTrace, other)
	o.work(nodeA, bus.TypeFailed, 0.02)

	// A task during which nodeA opens a subtask that nodeB resolves.
	trace = newTrace()
	task := o.announce(requester, topic, trace, "")
	o.propose(nodeA, task, trace)
	o.propose(nodeB, task, trace)
	o.assign(nodeA, topic, task, trace, requester, 0.9)
	sub := o.announce(nodeA, topic, trace, "")
	o.assign(nodeB, topic, sub, trace, nodeA, 0.85)
	o.close(bus.TypeCompleted, nodeB, topic, sub, trace, nodeA, 0.05)
	o.close(bus.TypeCompleted, nodeA, topic, task, trace, requester, 0.01)

	a := o.await(nodeA, func(a *networkmonitor.Aggregate) bool { return len(a.Observations) == 5 && len(a.Pending) == 0 })
	v := o.vector(a)
	if v.NObservations != 5 || *v.ReopenRate != 2.0/5 || *v.Score != 0 {
		t.Errorf("quality n=%d reopen_rate=%v score=%v", v.NObservations, *v.ReopenRate, *v.Score)
	}
	if *v.CostSubtreeP90 < 0.06-1e-9 || *v.AvgAttempts != 1 {
		t.Errorf("subtree p90=%v avg_attempts=%v", *v.CostSubtreeP90, *v.AvgAttempts)
	}
	// Six assignments on the topic: nodeA won five and nodeB one; the window is
	// the last three.
	if *v.UsageShare != 2.0/3 || *v.WinRate != 1 {
		t.Errorf("usage usage_share=%v win_rate=%v", *v.UsageShare, *v.WinRate)
	}
	// nodeB was free in the last three but the one it won, while nodeA held
	// its task.
	b := o.await(nodeB, func(a *networkmonitor.Aggregate) bool { return len(a.Observations) == 1 })
	if vb := o.vector(b); *vb.IdleShare != 2.0/3 || *vb.UsageShare != 1.0/3 || *vb.WinRate != 0.5 {
		t.Errorf("nodeB idle_share=%v usage_share=%v win_rate=%v", *vb.IdleShare, *vb.UsageShare, *vb.WinRate)
	}
}

// An event the role rereads does not count twice: each key recognizes it by
// its sequence (architecture §3.12.1, implementation §1.2.8).
func TestRereadEvent(t *testing.T) {
	o := newObserver(t, nodeA)
	o.work(nodeA, bus.TypeCompleted, 0.01)
	o.work(nodeA, bus.TypeFailed, 0.01)
	before := o.await(nodeA, func(a *networkmonitor.Aggregate) bool { return len(a.Observations) == 2 })
	o.settled()
	o.stop()

	// The role's consumer goes back to the start of the stream, as after a
	// restart between applying an event and acknowledging it.
	id := o.ID("network_monitor")
	if err := o.Console.DeleteConsumer(o.Ctx, "task", id); err != nil {
		t.Fatal(err)
	}
	if _, err := o.Console.CreateConsumer(o.Ctx, "task", jetstream.ConsumerConfig{
		Durable: id, FilterSubjects: []string{"task.>"}, AckPolicy: jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
	}); err != nil {
		t.Fatal(err)
	}
	o.start()
	o.settled()
	after := o.await(nodeA, func(*networkmonitor.Aggregate) bool { return true })
	if len(after.Observations) != len(before.Observations) || len(after.Usage) != len(before.Usage) {
		t.Errorf("reread: %d observations and %d assignments, want %d and %d",
			len(after.Observations), len(after.Usage), len(before.Observations), len(before.Usage))
	}
}

// A gap leaves the windows incomplete: what came before it does not count, and
// the signals are absent until the window fills again (architecture §3.12.1).
func TestObservationGap(t *testing.T) {
	o := newObserver(t, nodeA)
	o.work(nodeA, bus.TypeCompleted, 0.01)
	o.work(nodeA, bus.TypeCompleted, 0.01)
	o.await(nodeA, func(a *networkmonitor.Aggregate) bool { return len(a.Observations) == 2 })
	o.settled()
	o.stop()

	// While the role is down, the stream loses a task's events.
	trace := newTrace()
	task := o.announce(requester, topic, trace, "")
	lost := []uint64{o.assign(nodeA, topic, task, trace, requester, 0.8), o.close(bus.TypeCompleted, nodeA, topic, task, trace, requester, 0.01)}
	s, err := o.Console.Stream(o.Ctx, "task")
	if err != nil {
		t.Fatal(err)
	}
	for _, seq := range lost {
		if err := s.DeleteMsg(o.Ctx, seq); err != nil {
			t.Fatal(err)
		}
	}
	o.work(nodeA, bus.TypeCompleted, 0.01)
	o.start()
	a := o.await(nodeA, func(a *networkmonitor.Aggregate) bool { return len(a.Observations) == 1 && len(a.Pending) == 0 })
	if v := o.vector(a); v.NObservations != 1 || v.CostPerResolutionOwn != nil || v.UsageShare != nil {
		t.Errorf("after the gap %+v", v)
	}
}

// A change to the topic's roster restarts the count of assignments since the
// change on all its nodes (implementation §5.4).
func TestRosterChange(t *testing.T) {
	o := newObserver(t, nodeA, nodeB)
	o.work(nodeA, bus.TypeCompleted, 0.01)
	o.work(nodeB, bus.TypeCompleted, 0.01)
	o.await(nodeA, func(a *networkmonitor.Aggregate) bool { return a.SinceRoster == 2 })
	o.settled()
	o.provision(nodeC)
	for _, id := range []string{nodeA, nodeB, nodeC} {
		o.await(id, func(a *networkmonitor.Aggregate) bool { return a.SinceRoster == 0 })
	}
	o.work(nodeC, bus.TypeCompleted, 0.01)
	a := o.await(nodeA, func(a *networkmonitor.Aggregate) bool { return a.SinceRoster == 1 })
	// The signal keeps its window: only the trigger waits.
	if len(a.Usage) != 3 {
		t.Errorf("nodeA's usage window has %d assignments", len(a.Usage))
	}
}
