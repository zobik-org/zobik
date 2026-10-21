package console

import (
	"encoding/json"
	"fmt"
	"sync"
)

// Outcome is the delivery of a trace's outcome, as the event stream carries it
// (architecture §3.5.4). The console carries the details as they arrived.
type Outcome struct {
	// Channel is the audience of the channel that delivered it.
	Channel     string          `json:"channel"`
	TraceID     string          `json:"trace_id"`
	EntryTaskID string          `json:"entry_task_id"`
	Details     json.RawMessage `json:"details"`
}

// hub passes what reaches the panel without being asked for to the open event
// streams (implementation §6, The operator console). An outcome is delivered
// once some stream has it, so one that arrives with no stream open waits for
// the next.
type hub struct {
	mu      sync.Mutex
	streams map[*stream]struct{}
	pending []pendingOutcome
}

type pendingOutcome struct {
	frame []byte
	ack   func()
}

// stream is an open event stream of a session.
type stream struct {
	session string
	frames  chan []byte
}

func frame(event string, v any) []byte {
	b, _ := json.Marshal(v)
	return fmt.Appendf(nil, "event: %s\ndata: %s\n\n", event, b)
}

// outcome passes o to the open streams and acks it, or keeps it for the next.
func (h *hub) outcome(o Outcome, ack func()) {
	f := frame("outcome", o)
	h.mu.Lock()
	if len(h.streams) == 0 {
		h.pending = append(h.pending, pendingOutcome{f, ack})
		h.mu.Unlock()
		return
	}
	for s := range h.streams {
		s.send(f)
	}
	h.mu.Unlock()
	go ack()
}

// open opens a stream for session, which receives first what was waiting.
func (h *hub) open(session string) *stream {
	s := &stream{session: session, frames: make(chan []byte, 64)}
	h.mu.Lock()
	if h.streams == nil {
		h.streams = map[*stream]struct{}{}
	}
	h.streams[s] = struct{}{}
	pending := h.pending
	h.pending = nil
	for _, p := range pending {
		s.send(p.frame)
	}
	h.mu.Unlock()
	for _, p := range pending {
		go p.ack()
	}
	return s
}

func (h *hub) close(s *stream) {
	h.mu.Lock()
	delete(h.streams, s)
	h.mu.Unlock()
}

// reset drops what was waiting, when the last session closes: the channels
// retain the deliveries not acked, and send them again with the next session.
func (h *hub) reset() {
	h.mu.Lock()
	h.pending = nil
	h.mu.Unlock()
}

// send queues f; a stream that does not keep up loses it.
func (s *stream) send(f []byte) {
	select {
	case s.frames <- f:
	default:
	}
}
