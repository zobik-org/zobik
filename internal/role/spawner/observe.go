package spawner

import (
	"strings"
	"sync"
	"time"

	"zobik.org/zobik/internal/bus"
)

// observer is what the Spawner derives by passive observation of the stream
// (architecture §3.13.1): how many successive rounds each task went without a
// valid proposal (architecture §3.13.2), and which node is busy and when it
// received its last assignment (architecture §3.13.10). Nothing of it travels
// in a payload, and a restart loses it: the counts start again, and every node
// counts as busy for a task_lease_ttl (implementation §1.2.4).
type observer struct {
	mu    sync.Mutex
	tasks map[string]*observed
	nodes map[string]*occupancy
	// holdUntil is the end of the window after a start in which every node
	// counts as busy.
	holdUntil time.Time
}

// observed is a task as the stream shows it.
type observed struct {
	topic string
	// announcement is its latest task.announced.
	announcement *bus.Event
	// announced counts the announcements since its last award: the rounds that
	// closed without a valid proposal are one fewer.
	announced int
	assignee  string
	seen      time.Time
}

// occupancy is a node's: the tasks it holds, each with the expiry of its
// lease, and when it received its last assignment.
type occupancy struct {
	tasks        map[string]time.Time
	lastAssigned time.Time
}

// forgetTask is how long a task is retained without any event: longer than
// max_reannounce_backoff, so a vacant task keeps its count.
const forgetTask = time.Hour

func newObserver() *observer {
	return &observer{tasks: map[string]*observed{}, nodes: map[string]*occupancy{}}
}

// hold makes every node count as busy until until.
func (o *observer) hold(until time.Time) {
	o.mu.Lock()
	o.holdUntil = until
	o.mu.Unlock()
}

func (o *observer) node(id string) *occupancy {
	n := o.nodes[id]
	if n == nil {
		n = &occupancy{tasks: map[string]time.Time{}}
		o.nodes[id] = n
	}
	return n
}

// announced records an announcement of topic and returns how many successive
// rounds the task already went without a valid proposal. A re-announcement
// after an award ends that assignment: its lease expired (architecture §3.1).
func (o *observer) announced(e *bus.Event, topic string, now time.Time) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.prune(now)
	t := o.tasks[e.Subject]
	if t == nil {
		t = &observed{topic: topic}
		o.tasks[e.Subject] = t
	}
	if t.assignee != "" {
		if n := o.nodes[t.assignee]; n != nil {
			delete(n.tasks, e.Subject)
		}
		t.assignee = ""
	}
	t.announcement, t.seen = e, now
	t.announced++
	return t.announced - 1
}

// assigned records the award of a task to assignee, busy until its lease
// expires unless renewed, and returns the task's topic if it was observed.
func (o *observer) assigned(taskID, assignee string, now time.Time, lease time.Duration) string {
	o.mu.Lock()
	defer o.mu.Unlock()
	t := o.tasks[taskID]
	if t == nil {
		t = &observed{}
		o.tasks[taskID] = t
	}
	t.assignee, t.announced, t.seen = assignee, 0, now
	n := o.node(assignee)
	n.tasks[taskID] = now.Add(lease)
	n.lastAssigned = now
	return t.topic
}

// renewed records a lease renewal, whose subject is
// lease.<identity>.<topic>.<TaskID> (implementation §1.2.1).
func (o *observer) renewed(subject string, now time.Time, lease time.Duration) {
	parts := strings.Split(subject, ".")
	if len(parts) != 4 {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if n := o.nodes[parts[1]]; n != nil {
		if _, ok := n.tasks[parts[3]]; ok {
			n.tasks[parts[3]] = now.Add(lease)
		}
	}
}

// closed records a task's terminal event: its source no longer holds it.
func (o *observer) closed(e *bus.Event) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if n := o.nodes[e.Source]; n != nil {
		delete(n.tasks, e.Subject)
	}
	if t := o.tasks[e.Subject]; t != nil && (t.assignee == e.Source || t.assignee == "") {
		delete(o.tasks, e.Subject)
	}
}

// busy reports whether the node holds a task whose lease has not expired.
func (o *observer) busy(node string, now time.Time) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	if now.Before(o.holdUntil) {
		return true
	}
	n := o.nodes[node]
	if n == nil {
		return false
	}
	for id, expiry := range n.tasks {
		if now.After(expiry) {
			delete(n.tasks, id)
			continue
		}
		return true
	}
	return false
}

// lastAssigned is when the node received its last assignment, if it received any.
func (o *observer) lastAssigned(node string) (time.Time, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	n := o.nodes[node]
	if n == nil || n.lastAssigned.IsZero() {
		return time.Time{}, false
	}
	return n.lastAssigned, true
}

// forgetNode drops what is known of a node that left the fleet.
func (o *observer) forgetNode(node string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.nodes, node)
}

func (o *observer) prune(now time.Time) {
	for id, t := range o.tasks {
		if now.Sub(t.seen) > forgetTask {
			delete(o.tasks, id)
		}
	}
}
