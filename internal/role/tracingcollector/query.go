package tracingcollector

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/claim"
	contextv1 "zobik.org/zobik/proto/zobik/context/v1"
)

// Topic is the fixed-convention topic the Tracing Collector claims (architecture §3.10).
const Topic = "trace_query"

// portion is where the reconstruction goes, in the query task's section
// (implementation §10.1).
const portion = "reconstruction"

// chunkSize bounds each message of a write, well under gRPC's default limit.
const chunkSize = 64 << 10

// codeRequestInvalid closes a query whose announcement does not validate
// (implementation §10.1, §10.2).
const codeRequestInvalid = "request_invalid"

// claimEvent takes part in the cycle of trace_query with an event of the task
// family, already applied to its trace. Deterministic routing: the only
// instance owns the whole ring, so it always proposes (architecture §3.10).
func (c *collector) claimEvent(ctx context.Context, e *bus.Event, f fact, subject string) error {
	switch {
	case e.Type == bus.TypeAnnounced && subject == bus.SubjectAnnounced(Topic):
		opCtx, cancel := context.WithTimeout(ctx, opTimeout)
		defer cancel()
		return c.claimant.Propose(opCtx, e, nil)
	case e.Type == bus.TypeRejected && f.Node == c.id:
		c.claimant.Rejected(e)
	case e.Type == bus.TypeAssigned && f.Node == c.id:
		if a, ok := c.claimant.Assigned(e); ok {
			c.answering.Go(func() { c.answer(ctx, a) })
		}
	}
	return nil
}

// answer reconstructs the query's trace from the role's state, writes it to
// the Context Store and closes the query (architecture §3.10, implementation
// §1.2.6). A failure of the Bus or of the Context Store leaves the query
// without closing: the lease expires and it comes back with its re-announcement.
func (c *collector) answer(ctx context.Context, a *claim.Assignment) {
	stop := c.claimant.KeepLease(a, func() time.Duration { return time.Duration(c.leaseTTL.Load()) })
	defer stop()
	opCtx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	if err := a.Announcement.ValidateTopic(); err != nil {
		if err := c.claimant.Fail(opCtx, a, codeRequestInvalid, err.Error()); err != nil {
			log.Printf("tracing_collector: closing %s: %v", a.TaskID, err)
		}
		return
	}
	traceID := bus.TraceID(a.Announcement.TraceParent)
	raw, err := c.reconstruction(traceID, strings.Split(a.Announcement.TraceParent, "-")[2])
	if err == nil {
		err = c.write(opCtx, a.Token, raw)
	}
	if err == nil {
		err = c.claimant.Complete(opCtx, a, map[string]any{
			"context_ref": map[string]any{"trace_id": traceID, "task_id": a.TaskID, "name": portion},
		})
	}
	if err != nil {
		log.Printf("tracing_collector: %s left to its re-announcement: %v", a.TaskID, err)
	}
}

// entry is a span as the reconstruction delivers it: the list of what passes,
// without node identities or span events (architecture §3.10, implementation
// §1.2.6). Its fields are those of trace_query.reconstruction.schema.json.
type entry struct {
	TaskID          string     `json:"task_id"`
	SpanID          string     `json:"span_id"`
	ParentSpanID    string     `json:"parent_span_id,omitempty"`
	Topic           *string    `json:"topic,omitempty"`
	TaskText        *string    `json:"task_text,omitempty"`
	CurrentAttempts *int64     `json:"current_attempts,omitempty"`
	PreviousAttempt *string    `json:"previous_attempt,omitempty"`
	Outcome         string     `json:"outcome"`
	FailureCode     *string    `json:"failure_code,omitempty"`
	FailureReason   *string    `json:"failure_reason,omitempty"`
	Metrics         *metrics   `json:"metrics,omitempty"`
	StartedAt       time.Time  `json:"started_at"`
	EndedAt         *time.Time `json:"ended_at,omitempty"`
}

// outcomeOpen is the outcome of a span that has not ended (implementation §1.2.6).
const outcomeOpen = "open"

// reconstruction is the portion for traceID, validated against its schema. The
// state holds the whole trace while any of its tasks is assigned, and the
// requester's is: from requester, the span the query was announced from
// (architecture §3.10). A state without that span lost the trace's history,
// and what is left of it is not answered.
func (c *collector) reconstruction(traceID, requester string) ([]byte, error) {
	c.mu.Lock()
	var spans []*span
	if t := c.traces[traceID]; t != nil {
		spans = t.spans()
	}
	c.mu.Unlock()
	tasks := make([]entry, 0, len(spans))
	found := false
	for _, s := range spans {
		tasks = append(tasks, s.entry())
		found = found || s.ID == requester
	}
	if !found {
		return nil, fmt.Errorf("the state of %s has no span %s, the query's requester", traceID, requester)
	}
	sort.SliceStable(tasks, func(i, j int) bool { return tasks[i].StartedAt.Before(tasks[j].StartedAt) })
	raw, err := json.Marshal(map[string]any{"trace_id": traceID, "tasks": tasks})
	if err != nil {
		return nil, err
	}
	schema, err := bus.Schema("trace_query.reconstruction.schema.json")
	if err != nil {
		return nil, err
	}
	if err := schema.ValidateJSON(raw); err != nil {
		return nil, fmt.Errorf("the reconstruction of %s: %w", traceID, err)
	}
	return raw, nil
}

func (s *span) entry() entry {
	e := entry{TaskID: s.Task, SpanID: s.ID, Outcome: s.Outcome, StartedAt: s.Start.UTC()}
	if !s.Root {
		e.ParentSpanID = s.Parent
	}
	if a := s.Announcement; a != nil && !s.Root {
		e.Topic, e.TaskText = a.Data.Topic, a.Data.TaskText
		e.CurrentAttempts, e.PreviousAttempt = a.Data.CurrentAttempts, a.Data.PreviousAttempt
	}
	if t := s.Terminal; t != nil {
		e.FailureCode, e.FailureReason, e.Metrics = t.Data.FailureCode, t.Data.FailureReason, t.Data.Metrics
	}
	if s.closed() {
		end := s.End.UTC()
		e.EndedAt = &end
	} else {
		e.Outcome = outcomeOpen
	}
	return e
}

// write leaves raw in the portion of the query task's section, with the scope
// token of its award (architecture §3.14.3).
func (c *collector) write(ctx context.Context, token string, raw []byte) error {
	w, err := c.context.Write(ctx)
	if err != nil {
		return err
	}
	if err := w.Send(&contextv1.StoreWrite{Body: &contextv1.StoreWrite_Header{Header: &contextv1.StoreWriteHeader{Token: token, Name: portion}}}); err != nil {
		return err
	}
	for len(raw) > 0 {
		n := min(len(raw), chunkSize)
		if err := w.Send(&contextv1.StoreWrite{Body: &contextv1.StoreWrite_Chunk{Chunk: raw[:n]}}); err != nil {
			return err
		}
		raw = raw[n:]
	}
	_, err = w.CloseAndRecv()
	return err
}
