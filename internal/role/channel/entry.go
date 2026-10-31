package channel

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/google/uuid"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/embedding"
)

// The retries of a pending announcement: from retryMin, doubling, up to retryMax.
const (
	retryMin = 500 * time.Millisecond
	retryMax = 30 * time.Second
	// embedTimeout bounds one call to the embeddings endpoint.
	embedTimeout = 30 * time.Second
)

// decisionReject is the answer to a delivery that rejects its outcome
// (architecture §3.5.4).
const decisionReject = "reject"

// embedded reports whether an entry carries a task_embedding: an emergent node
// claims it by similarity, while a structural one awards without it
// (architecture §3.7.7).
func embedded(topic string) bool { return !bus.StructuralTopics[topic] }

// send publishes the root's pending attempt of the entry subtask, retrying
// until the embeddings endpoint answers and the Bus confirms it
// (implementation §6, The service between the console and the channels).
func (c *channel) send(r *root) {
	wait := retryMin
	for {
		err := c.trySend(r)
		if err == nil {
			return
		}
		log.Printf("channel: announcing the entry subtask of %s: %v", r.TraceID, err)
		select {
		case <-c.ctx.Done():
			return // it stays pending in the root, for the next run
		case <-time.After(wait):
		}
		wait = min(2*wait, retryMax)
	}
}

func (c *channel) trySend(r *root) error {
	c.mu.Lock()
	if r.Pending == nil {
		c.mu.Unlock()
		return nil
	}
	e := *r.Pending
	vector, modelID := r.Vector, r.ModelID
	c.mu.Unlock()

	if embedded(r.EntryTopic) {
		var d map[string]json.RawMessage
		if err := json.Unmarshal(e.Data, &d); err != nil {
			return err
		}
		var text string
		if err := json.Unmarshal(d["task_text"], &text); err != nil {
			return err
		}
		// The embedding is generated under the current embedding_model, and an
		// earlier one of the same text is reused only in that space
		// (architecture §3.7.4).
		space, err := embedding.Resolve(c.head.Load().Values)
		if err != nil {
			return err
		}
		if modelID != space.ModelID {
			ctx, cancel := context.WithTimeout(c.ctx, embedTimeout)
			v, err := space.Query(ctx, c.egress, text)
			cancel()
			if err != nil {
				return err
			}
			vector, modelID = embedding.Encode(v), space.ModelID
		}
		d["task_embedding"], _ = json.Marshal(vector)
		d["model_id"], _ = json.Marshal(modelID)
		if e.Data, err = json.Marshal(d); err != nil {
			return err
		}
	}
	ctx, cancel := context.WithTimeout(c.ctx, opTimeout)
	defer cancel()
	if err := bus.Publish(ctx, c.js, bus.SubjectAnnounced(r.EntryTopic), &e); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	// A reopen may have replaced the attempt meanwhile; that one is still to send.
	if r.Pending != nil && r.Pending.ID == e.ID {
		r.Pending = nil
	}
	r.Vector, r.ModelID = vector, modelID
	if err := c.roots.save(r); err != nil {
		// Published already: a restart that finds it pending publishes it again
		// under the same id, which the stream deduplicates.
		log.Printf("channel: keeping %s: %v", r.TraceID, err)
	}
	return nil
}

// reopen announces a new attempt of the entry subtask with the rejection's
// reason, and the delivery of its outcome starts again (architecture §3.3, §3.5.4).
func (c *channel) reopen(r *root, reason, code string) error {
	var data map[string]any
	if err := json.Unmarshal(r.Entry.Data, &data); err != nil {
		return err
	}
	data["failure_reason"] = reason
	if code != "" {
		data["failure_code"] = code
	}
	data["current_attempts"] = r.EntryAttempts + 1
	data["previous_attempt"] = r.EntryTaskID
	e, err := bus.NewEvent(bus.TypeAnnounced, c.rt.ID, uuid.NewString(), data)
	if err != nil {
		return err
	}
	// The new attempt hangs from the root, like the first one.
	e.TraceParent, e.ConfigVersion = r.TraceParent, r.Version

	c.mu.Lock()
	defer c.mu.Unlock()
	prevEntry, prevDelivery := r.EntryTaskID, r.Delivery
	r.EntryTaskID, r.EntryAttempts, r.Pending = e.Subject, r.EntryAttempts+1, e
	r.Outcome, r.Delivery, r.Attempts, r.FinalRound, r.Options = nil, "", 0, false, nil
	if err := c.roots.save(r); err != nil {
		return err
	}
	delete(c.byEntry, prevEntry)
	delete(c.byDelivery, prevDelivery)
	c.byEntry[r.EntryTaskID] = r.TraceID
	go c.send(r)
	return nil
}
