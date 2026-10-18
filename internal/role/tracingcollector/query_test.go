package tracingcollector_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/google/uuid"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/role/tracingcollector"
	"zobik.org/zobik/internal/signing"
	contextv1 "zobik.org/zobik/proto/zobik/context/v1"
	nriv1 "zobik.org/zobik/proto/zobik/nri/v1"
)

// A trace_query is answered from the role's state: the whole trace, with
// spans exported, closed and open, without node identities, in the portion
// reconstruction of the query task's section (architecture §3.10,
// implementation §1.2.6).
func TestTraceQuery(t *testing.T) {
	f := newFixture(t, &backend{}, nil)
	f.start()
	collector := f.ID("tracing_collector")
	proposals := f.Tap(bus.SubjectProposed(collector, tracingcollector.Topic))
	closes := f.Tap(bus.SubjectTerminal(bus.TypeCompleted, store, tracingcollector.Topic))

	// The Config Store holds the entry and opens a subtask, which another node
	// completes and the role exports.
	assigned := f.opening()
	entrySpan := bus.SpanID(assigned.ID)
	done := uuid.NewString()
	f.emit(bus.TypeAnnounced, store, done, bus.SubjectAnnounced("echo"), entrySpan,
		map[string]any{"topic": "echo", "task_text": "say hi", "requester_identity": store})
	doneAssigned := f.emit(bus.TypeAssigned, broker, done, bus.SubjectAddressed(bus.TypeAssigned, other), entrySpan,
		map[string]any{"topic": "echo", "similarity": 0.8, "requester_identity": store})
	f.emit(bus.TypeCompleted, other, done, bus.SubjectTerminal(bus.TypeCompleted, store, "echo"), entrySpan,
		map[string]any{"topic": "echo", "metrics": metrics})
	f.await(1)
	// A second one fails, and its span is not exported yet when the query arrives.
	failed := uuid.NewString()
	f.emit(bus.TypeAnnounced, store, failed, bus.SubjectAnnounced("echo"), entrySpan,
		map[string]any{"topic": "echo", "task_text": "say bye", "requester_identity": store})
	f.emit(bus.TypeAssigned, broker, failed, bus.SubjectAddressed(bus.TypeAssigned, other), entrySpan,
		map[string]any{"topic": "echo", "similarity": 0.7, "requester_identity": store})
	f.emit(bus.TypeFailed, other, failed, bus.SubjectTerminal(bus.TypeFailed, store, "echo"), entrySpan,
		map[string]any{"topic": "echo", "failure_reason": "no", "metrics": metrics})

	// The query, from the entry; the test plays the Task Broker's award.
	query := uuid.NewString()
	f.emit(bus.TypeAnnounced, store, query, bus.SubjectAnnounced(tracingcollector.Topic), entrySpan,
		map[string]any{"topic": tracingcollector.Topic, "requester_identity": store})
	select {
	case <-proposals:
	case <-time.After(10 * time.Second):
		t.Fatal("the role did not propose")
	}
	brokerKey := f.Identity("task_broker")
	brokerID := f.ID("task_broker")
	cert := f.Sign(f.Root, signing.TypeRoleCertificate, signing.RoleCertificate{Sub: brokerID, Role: signing.RoleTaskBroker}, "")
	token := f.Sign(brokerKey, signing.TypeScopeToken, signing.ScopeToken{
		Sub: collector, Iat: time.Now().Unix(), TraceID: f.trace, Section: query,
	}, cert)
	queryAssigned := f.emit(bus.TypeAssigned, broker, query, bus.SubjectAddressed(bus.TypeAssigned, collector), entrySpan,
		map[string]any{"topic": tracingcollector.Topic, "similarity": 1, "requester_identity": store, "scope_token": token})

	var ref struct {
		ContextRef struct {
			TraceID string `json:"trace_id"`
			TaskID  string `json:"task_id"`
			Name    string `json:"name"`
		} `json:"context_ref"`
	}
	select {
	case m := <-closes:
		var e bus.Event
		json.Unmarshal(m.Data, &e)
		if e.Subject != query || e.Source != collector {
			t.Fatalf("close %+v", e)
		}
		json.Unmarshal(e.Data, &ref)
	case <-time.After(10 * time.Second):
		t.Fatal("the query did not close")
	}
	if ref.ContextRef.TraceID != f.trace || ref.ContextRef.TaskID != query || ref.ContextRef.Name != "reconstruction" {
		t.Fatalf("context_ref %+v", ref.ContextRef)
	}
	raw := f.read(ref.ContextRef.TaskID, ref.ContextRef.Name)
	for _, id := range []string{store, other, broker, channel, collector} {
		if bytes.Contains(raw, []byte(id)) {
			t.Errorf("the reconstruction names %s: %s", id, raw)
		}
	}

	var rec struct {
		TraceID string `json:"trace_id"`
		Tasks   []struct {
			TaskID   string         `json:"task_id"`
			SpanID   string         `json:"span_id"`
			Parent   string         `json:"parent_span_id"`
			Topic    string         `json:"topic"`
			TaskText string         `json:"task_text"`
			Outcome  string         `json:"outcome"`
			Reason   string         `json:"failure_reason"`
			Metrics  map[string]any `json:"metrics"`
			EndedAt  string         `json:"ended_at"`
		} `json:"tasks"`
	}
	if err := json.Unmarshal(raw, &rec); err != nil || rec.TraceID != f.trace {
		t.Fatalf("reconstruction %s: %v", raw, err)
	}
	type want struct{ span, parent, topic, text, outcome string }
	wants := map[string]want{
		f.trace: {f.root, "", "", "", "open"},
		f.entry: {entrySpan, f.root, "config_change", "set auction_timeout", "open"},
		done:    {bus.SpanID(doneAssigned.ID), entrySpan, "echo", "say hi", "completed"},
		failed:  {"", entrySpan, "echo", "say bye", "failed"},
		query:   {bus.SpanID(queryAssigned.ID), entrySpan, tracingcollector.Topic, "", "open"},
	}
	if len(rec.Tasks) != len(wants) {
		t.Fatalf("tasks %s", raw)
	}
	for _, got := range rec.Tasks {
		w, ok := wants[got.TaskID]
		if !ok || (w.span != "" && got.SpanID != w.span) || got.Parent != w.parent || got.Topic != w.topic ||
			got.TaskText != w.text || got.Outcome != w.outcome {
			t.Errorf("task %+v, want %+v", got, w)
		}
		if (got.Outcome == "open") != (got.EndedAt == "") {
			t.Errorf("task %s: outcome %s, ended_at %q", got.TaskID, got.Outcome, got.EndedAt)
		}
		if got.TaskID == done && got.Metrics["duration_ms"] != 12.0 {
			t.Errorf("metrics %v", got.Metrics)
		}
		if got.TaskID == failed && got.Reason != "no" {
			t.Errorf("failure_reason %q", got.Reason)
		}
	}
}

// read reads a portion of the trace as the channel that opened it, with the
// root's token (architecture §3.14.3).
func (f *fixture) read(section, name string) []byte {
	f.T.Helper()
	ch := f.Identity("channel_operator")
	chID := f.ID("channel_operator")
	token := f.Sign(ch, signing.TypeScopeToken, signing.ScopeToken{Sub: chID, TraceID: f.trace, Section: f.trace}, f.Credential("channel_operator"))
	s, err := f.ContextClient(bus.ChannelScope.Role, ch).Read(f.Ctx, &contextv1.StoreRead{
		Token: token, Ref: &nriv1.ContextRef{TraceId: f.trace, TaskId: section, Name: name},
	})
	if err != nil {
		f.T.Fatal(err)
	}
	var out []byte
	for {
		c, err := s.Recv()
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			f.T.Fatalf("reading %s/%s: %v", section, name, err)
		}
		out = append(out, c.GetData()...)
	}
}
