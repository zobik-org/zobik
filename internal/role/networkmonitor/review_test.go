package networkmonitor_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/role/networkmonitor"
)

// review waits for the next node_review the role announces.
func (o *observer) review(announced chan *nats.Msg, within time.Duration) (*bus.Event, map[string]any) {
	o.T.Helper()
	select {
	case m := <-announced:
		var e bus.Event
		var d map[string]any
		json.Unmarshal(m.Data, &e)
		json.Unmarshal(e.Data, &d)
		return &e, d
	case <-time.After(within):
		return nil, nil
	}
}

// A node that violates the constraint is reviewed in a trace the role opens:
// the announcement carries the trigger, the vector and the reopens by
// requester, and the root closes when the Spawner closes the review. A node
// snoozed is not reviewed again until it gathers the observations of its
// snooze_until (architecture §3.5.1, §3.12.3, implementation §5.9).
func TestReviewCycle(t *testing.T) {
	o := newObserver(t, nodeA)
	nm := o.ID("network_monitor")
	announced := o.Tap(bus.SubjectAnnounced(networkmonitor.TopicReview))
	roots := o.Tap(bus.SubjectTerminal(bus.TypeCompleted, nm, networkmonitor.TopicReview))

	reopened, trace := o.work(nodeA, bus.TypeCompleted, 0.01)
	o.announce(requester, topic, trace, reopened)
	o.work(nodeA, bus.TypeFailed, 0.01)

	e, d := o.review(announced, 30*time.Second)
	if e == nil {
		t.Fatal("no review")
	}
	if d["trigger_reason"] != networkmonitor.ReasonLowScore || d["node_id"] != nodeA || d["target_topic"] != topic ||
		d["n_observations"] != 2.0 || d["requester_identity"] != nm || d["origin_audience"] != "operator" {
		t.Errorf("the review %v", d)
	}
	if signals, _ := d["signals"].(map[string]any); signals["score"] != 0.0 {
		t.Errorf("the review's signals %v", d["signals"])
	}
	reopens, _ := d["reopens_by_requester"].([]any)
	if len(reopens) != 1 || reopens[0].(map[string]any)["requester_identity"] != requester || reopens[0].(map[string]any)["reopens"] != 1.0 {
		t.Errorf("reopens by requester %v", d["reopens_by_requester"])
	}
	traceID := bus.TraceID(e.TraceParent)
	if traceID == "" || e.ConfigVersion == "" {
		t.Fatalf("the review's trace %q %q", e.TraceParent, e.ConfigVersion)
	}

	// The Spawner closes it snoozed for two observations.
	o.publish(bus.SubjectTerminal(bus.TypeCompleted, nm, networkmonitor.TopicReview), bus.TypeCompleted, "USPAWNER", e.Subject, traceID,
		map[string]any{"topic": networkmonitor.TopicReview, "action": "snoozed", "snooze_until": 2, "metrics": someMetrics(0, 10)})
	closed := false
	for deadline := time.After(15 * time.Second); !closed; {
		select {
		case m := <-roots:
			var r bus.Event
			json.Unmarshal(m.Data, &r)
			closed = r.Subject == traceID
		case <-deadline:
			t.Fatal("the review's root did not close")
		}
	}
	if e, d := o.review(announced, 3*time.Second); e != nil {
		t.Fatalf("reviewed while snoozed: %v", d)
	}

	o.work(nodeA, bus.TypeFailed, 0.01)
	if e, d := o.review(announced, 3*time.Second); e != nil {
		t.Fatalf("reviewed with one observation of the two: %v", d)
	}
	o.work(nodeA, bus.TypeFailed, 0.01)
	if e, d := o.review(announced, 30*time.Second); e == nil || d["trigger_reason"] != networkmonitor.ReasonLowScore {
		t.Fatalf("not reviewed once the snooze was used up: %v", d)
	}
}

// A node with a review open is not reviewed again, and an open review survives
// a restart: the successor closes its root when the review closes, even after
// the node left the roster (implementation §1.2.8, §5.9).
func TestReviewSurvivesRestart(t *testing.T) {
	o := newObserver(t, nodeA)
	nm := o.ID("network_monitor")
	announced := o.Tap(bus.SubjectAnnounced(networkmonitor.TopicReview))
	roots := o.Tap(bus.SubjectTerminal(bus.TypeCompleted, nm, networkmonitor.TopicReview))
	o.work(nodeA, bus.TypeFailed, 0.01)
	o.work(nodeA, bus.TypeFailed, 0.01)
	e, _ := o.review(announced, 30*time.Second)
	if e == nil {
		t.Fatal("no review")
	}
	if e, d := o.review(announced, 3*time.Second); e != nil {
		t.Fatalf("reviewed again with one open: %v", d)
	}
	o.settled()
	o.stop()

	// The Spawner archives the node and closes the review while the role is down.
	o.mu.Lock()
	o.fleet = nil
	o.mu.Unlock()
	traceID := bus.TraceID(e.TraceParent)
	o.publish(bus.SubjectTerminal(bus.TypeCompleted, nm, networkmonitor.TopicReview), bus.TypeCompleted, "USPAWNER", e.Subject, traceID,
		map[string]any{"topic": networkmonitor.TopicReview, "action": "archived", "metrics": someMetrics(0, 10)})
	o.start()
	for deadline := time.After(30 * time.Second); ; {
		select {
		case m := <-roots:
			var r bus.Event
			json.Unmarshal(m.Data, &r)
			if r.Subject == traceID {
				return
			}
		case <-deadline:
			t.Fatal("the review's root did not close")
		}
	}
}
