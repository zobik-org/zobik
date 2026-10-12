package taskbroker

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"zobik.org/zobik/internal/bus"
)

// The states of a task (architecture §3.1).
const (
	stateUnassigned = "UNASSIGNED"
	stateClaimed    = "CLAIMED"
	stateDone       = "DONE"
)

// task is the value of a task's key in the tasks bucket: everything an instance
// needs to resume it (implementation §1.2.7).
type task struct {
	State    string `json:"state"`
	Assignee string `json:"assignee,omitempty"`
	// LeaseExpiry is a date in the value, not the KV's TTL.
	LeaseExpiry time.Time `json:"lease_expiry,omitzero"`
	// Reannouncements is how many times the task was re-announced, which the
	// backoff comes from; LastAnnounced, when it was last announced.
	Reannouncements int       `json:"reannouncements"`
	LastAnnounced   time.Time `json:"last_announced"`
	// Announcement is the task.announced the Task Broker re-announces, with its
	// task_embedding; the value drops it on passing to DONE.
	Announcement *bus.Event `json:"announcement,omitempty"`
}

// taskKey is a task's key, <topic>.<TaskID> (implementation §1.2.7).
func taskKey(topic, taskID string) string { return topic + "." + taskID }

// store is the tasks bucket. Only the Task Broker writes to it.
type store struct{ kv jetstream.KeyValue }

func (s store) get(ctx context.Context, key string) (*task, uint64, error) {
	e, err := s.kv.Get(ctx, key)
	if err != nil {
		return nil, 0, err
	}
	var t task
	if err := json.Unmarshal(e.Value(), &t); err != nil {
		return nil, 0, err
	}
	return &t, e.Revision(), nil
}

// create writes a new task only if its key does not exist.
func (s store) create(ctx context.Context, key string, t *task) error {
	b, err := json.Marshal(t)
	if err != nil {
		return err
	}
	_, err = s.kv.Create(ctx, key, b)
	return err
}

// errNoChange is what a transition returns to leave the key as it is.
var errNoChange = errors.New("no change")

// transition is a CAS on the revision read (implementation §1.2.7): it reads the
// key, applies change and writes only if the key is still at that revision; on
// losing, it reads again. It returns the task as it was written, or as it was
// read if change returned errNoChange.
func (s store) transition(ctx context.Context, key string, change func(*task) error) (*task, error) {
	for {
		t, rev, err := s.get(ctx, key)
		if err != nil {
			return nil, err
		}
		if err := change(t); err != nil {
			return t, err
		}
		b, err := json.Marshal(t)
		if err != nil {
			return nil, err
		}
		_, err = s.kv.Update(ctx, key, b, rev)
		if errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
			continue
		}
		return t, err
	}
}
