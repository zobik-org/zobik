package spawner

import (
	"testing"
	"time"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/role/catalog"
)

// The observer counts the rounds a task goes without an award, and an award
// restarts the count; a node is busy from its award until the terminal event,
// a re-announcement of the task or the expiry of its lease, which a renewal
// extends (architecture §3.13.2, §3.13.10).
func TestObserver(t *testing.T) {
	o := newObserver()
	now := time.Now()
	ann := &bus.Event{Subject: "T1"}
	for want := range 3 {
		if got := o.announced(ann, "echo", now); got != want {
			t.Fatalf("announcement %d counts %d rounds", want+1, got)
		}
	}
	lease := time.Second
	if topic := o.assigned("T1", "N1", now, lease); topic != "echo" {
		t.Errorf("topic %q", topic)
	}
	if !o.busy("N1", now) {
		t.Error("N1 is free right after its award")
	}
	if got := o.announced(ann, "echo", now.Add(2*lease)); got != 0 {
		t.Errorf("the award did not restart the count: %d", got)
	}
	if o.busy("N1", now) {
		t.Error("the re-announcement did not end the assignment")
	}

	o.assigned("T2", "N2", now, lease)
	o.renewed("lease.N2.echo.T2", now.Add(lease/2), lease)
	if !o.busy("N2", now.Add(lease+lease/4)) {
		t.Error("the renewal did not extend the lease")
	}
	if o.busy("N2", now.Add(3*lease)) {
		t.Error("the lease did not expire")
	}
	o.assigned("T3", "N2", now, lease)
	o.closed(&bus.Event{Subject: "T3", Source: "N2"})
	if o.busy("N2", now) {
		t.Error("the terminal event did not free N2")
	}
	if last, ok := o.lastAssigned("N2"); !ok || !last.Equal(now) {
		t.Errorf("last assignment %v %v", last, ok)
	}

	o.hold(now.Add(lease))
	if !o.busy("N9", now) || o.busy("N9", now.Add(2*lease)) {
		t.Error("the start's window")
	}
}

// Topic governance covers the emergent topics only (architecture §3.7.6, §3.7.7).
func TestEmergent(t *testing.T) {
	for topic, want := range map[string]bool{
		"lookup": true, bus.TopicIntake: false, "node_review": false, "hitl_contact_operator": false,
	} {
		if got := catalog.Emergent(topic); got != want {
			t.Errorf("%s: %v", topic, got)
		}
	}
}
