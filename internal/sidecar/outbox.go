package sidecar

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/egress"
	"zobik.org/zobik/internal/embedding"
)

// The outbox's retries: from retryMin, doubling, up to retryMax.
const (
	retryMin = 500 * time.Millisecond
	retryMax = 30 * time.Second
	// embedTimeout bounds one call to the embeddings endpoint.
	embedTimeout = 30 * time.Second
)

// outbox publishes what the node emits: each event is persisted before
// trying, and retried until the Bus confirms it (architecture §3.6.4). Each
// event is retried on its own, so one waiting for its task_embedding holds no
// other.
type outbox struct {
	ctx context.Context
	// sending counts the events in flight, which the end of the process
	// waits for a moment.
	sending sync.WaitGroup
	st      *state
	js      jetstream.JetStream
	cfg     *config
	egress  *egress.Executor

	mu sync.Mutex
	// vectors are the last task_embeddings generated, by model_id and text: a
	// reopen that does not change the text reuses its vector (architecture §3.7.4).
	vectors map[string]string
}

// maxVectors bounds the vectors kept for reuse.
const maxVectors = 256

func newOutbox(ctx context.Context, st *state, js jetstream.JetStream, cfg *config, x *egress.Executor) *outbox {
	return &outbox{ctx: ctx, st: st, js: js, cfg: cfg, egress: x, vectors: map[string]string{}}
}

// resume sends what a previous run left in the outbox.
func (o *outbox) resume() error {
	unsent, err := o.st.unsent()
	if err != nil {
		return err
	}
	for _, p := range unsent {
		o.sending.Add(1)
		go o.send(p, make(chan struct{}))
	}
	return nil
}

// errTooLarge is an event the Bus would refuse every time: the request that
// produced it is rejected instead of retried forever.
var errTooLarge = errors.New("the event exceeds the Bus's largest message")

// put persists p and starts sending it. The channel closes when the Bus
// confirms it.
func (o *outbox) put(p *pending) (<-chan struct{}, error) {
	b, err := json.Marshal(p.Event)
	if err != nil {
		return nil, err
	}
	size := int64(len(b))
	if p.Embed {
		size += embedding.Room
	}
	if size > o.js.Conn().MaxPayload() {
		return nil, errTooLarge
	}
	if err := o.st.put(p); err != nil {
		return nil, err
	}
	sent := make(chan struct{})
	o.sending.Add(1)
	go o.send(p, sent)
	return sent, nil
}

// drain waits up to d for the events in flight: what does not go out stays in
// the outbox for the next run.
func (o *outbox) drain(d time.Duration) {
	done := make(chan struct{})
	go func() {
		o.sending.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(d):
	}
}

func (o *outbox) send(p *pending, sent chan struct{}) {
	defer o.sending.Done()
	wait := retryMin
	for {
		err := o.try(p)
		if err == nil {
			if err := o.st.sent(p.Event.ID); err != nil {
				log.Printf("sidecar: removing %s from the outbox: %v", p.Event.ID, err)
			}
			close(sent)
			return
		}
		log.Printf("sidecar: sending %s %s: %v", p.Event.Type, p.Event.Subject, err)
		select {
		case <-o.ctx.Done():
			return // it stays in the outbox for the next run
		case <-time.After(wait):
		}
		wait = min(2*wait, retryMax)
	}
}

func (o *outbox) try(p *pending) error {
	e := *p.Event
	if p.Embed {
		data, err := o.embed(e.Data)
		if err != nil {
			return err
		}
		e.Data = data
	}
	ctx, cancel := context.WithTimeout(o.ctx, opTimeout)
	defer cancel()
	return bus.Publish(ctx, o.js, p.Subject, &e)
}

// embed stamps the task_embedding of data's task_text under the current
// embedding_model, with its model_id (architecture §3.7.4).
func (o *outbox) embed(raw json.RawMessage) (json.RawMessage, error) {
	var d map[string]json.RawMessage
	if err := json.Unmarshal(raw, &d); err != nil {
		return nil, err
	}
	var text string
	if err := json.Unmarshal(d["task_text"], &text); err != nil || text == "" {
		return nil, errors.New("the announcement carries no task_text")
	}
	space, err := embedding.Resolve(o.cfg.head().Values)
	if err != nil {
		return nil, err
	}
	key := space.ModelID + "\x00" + text
	o.mu.Lock()
	vector, ok := o.vectors[key]
	o.mu.Unlock()
	if !ok {
		ctx, cancel := context.WithTimeout(o.ctx, embedTimeout)
		v, err := space.Query(ctx, o.egress, text)
		cancel()
		if err != nil {
			return nil, err
		}
		vector = embedding.Encode(v)
		o.mu.Lock()
		if len(o.vectors) >= maxVectors {
			clear(o.vectors)
		}
		o.vectors[key] = vector
		o.mu.Unlock()
	}
	d["task_embedding"], _ = json.Marshal(vector)
	d["model_id"], _ = json.Marshal(space.ModelID)
	return json.Marshal(d)
}
