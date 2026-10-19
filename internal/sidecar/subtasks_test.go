package sidecar_test

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/embedding/embeddingtest"
	nriv1 "zobik.org/zobik/proto/zobik/nri/v1"
)

const subtopic = "lookup"

// endpoint is the fake embeddings endpoint behind a switch that takes it down.
type endpoint struct {
	*embeddingtest.Server
	down atomic.Bool
	URL  string
}

func newEndpoint(t *testing.T) *endpoint {
	e := &endpoint{Server: embeddingtest.NewServer(t)}
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if e.down.Load() {
			http.Error(w, "down", http.StatusServiceUnavailable)
			return
		}
		e.Config.Handler.ServeHTTP(w, r)
	}))
	t.Cleanup(s.Close)
	e.URL = s.URL
	return e
}

// take has the node win a task the test assigns as the Task Broker, delivered
// and confirmed; extra goes in its announcement's data.
func (f *fixture) take(l *logic, extra map[string]any) string {
	f.t.Helper()
	proposals := f.Tap("prop.>")
	taskID := f.announce(near, extra)
	await(f.t, proposals, 5*time.Second)
	f.assign(taskID)
	d, _ := assignment(f.t, l.next(f.t, 5*time.Second))
	l.received(f.t, d.GetDeliveryId())
	return taskID
}

// terminal publishes, as the node that resolved it, the terminal event typ of
// a subtask the node opened on topic.
func (f *fixture) terminal(typ, taskID, topic string, extra map[string]any) {
	f.t.Helper()
	data := map[string]any{"topic": topic, "metrics": map[string]any{"tokens_in": 0, "tokens_out": 0, "cost": 0, "duration_ms": 1}}
	for k, v := range extra {
		data[k] = v
	}
	e, err := bus.NewEvent(typ, "worker", taskID, data)
	if err != nil {
		f.t.Fatal(err)
	}
	e.TraceParent, e.ConfigVersion = traceparent(), "v0"
	f.publish(f.pub, bus.SubjectTerminal(typ, f.id, topic), e)
}

func announceReq(taskID, topic, previous, data string) *nriv1.PublishRequest {
	return &nriv1.PublishRequest{TaskId: taskID, Body: &nriv1.PublishRequest_Announce{Announce: &nriv1.AnnounceTask{
		Topic: topic, Data: []byte(data), PreviousAttempt: previous,
	}}}
}

func askReq(taskID, previous, data string) *nriv1.PublishRequest {
	return &nriv1.PublishRequest{TaskId: taskID, Body: &nriv1.PublishRequest_Ask{Ask: &nriv1.AskRequester{
		Data: []byte(data), PreviousAttempt: previous,
	}}}
}

func dataOf(t *testing.T, e *bus.Event) map[string]any {
	t.Helper()
	var d map[string]any
	if err := json.Unmarshal(e.Data, &d); err != nil {
		t.Fatal(err)
	}
	return d
}

// result reads a result delivery: the terminal event without its source.
func result(t *testing.T, m *nriv1.SidecarMessage) (*nriv1.Delivery, map[string]any) {
	t.Helper()
	d := m.GetResult()
	if d == nil {
		t.Fatalf("got %v, want a result", m)
	}
	var doc map[string]any
	if err := json.Unmarshal(d.GetEvent(), &doc); err != nil {
		t.Fatal(err)
	}
	if _, ok := doc["source"]; ok {
		t.Errorf("the result carries its source: %s", d.GetEvent())
	}
	return d, doc
}

// A subtask carries what the Integration Sidecar stamps, its terminal event
// reaches the Logic Container, and a reopen continues its lineage.
func TestSubtask(t *testing.T) {
	ep := newEndpoint(t)
	f := newFixture(t, options{embeddings: ep.URL})
	announced := f.Tap(bus.SubjectAnnounced(subtopic))
	assigned := f.Tap(bus.SubjectAddressed(bus.TypeAssigned, f.id))
	l, err := f.attach(0)
	if err != nil {
		t.Fatal(err)
	}
	parent := f.take(l, nil)
	a := await(t, assigned, 5*time.Second)

	ack, err := l.client.Publish(f.Ctx, announceReq(parent, subtopic, "", `{"task_text":"find it","detail":1}`))
	if err != nil {
		t.Fatal(err)
	}
	e := await(t, announced, 5*time.Second)
	// The subtask names as its parent the span of the task it is announced from (architecture §3.2).
	if want := "00-" + bus.TraceID(a.TraceParent) + "-" + bus.SpanID(a.ID) + "-01"; e.TraceParent != want {
		t.Errorf("traceparent %s, want %s", e.TraceParent, want)
	}
	d := dataOf(t, e)
	if e.Subject != ack.GetTaskId() || e.Source != f.id || d["requester_identity"] != f.id || d["workspace"] != "repo" ||
		d["task_embedding"] == nil || d["model_id"] == nil || d["detail"] != 1.0 || d["current_attempts"] != nil {
		t.Errorf("announced %s from %s: %s", e.Subject, e.Source, e.Data)
	}
	if r := ep.Requests(); len(r) != 1 || r[0].Input != "q: find it" {
		t.Errorf("embedded %v", r)
	}
	sub := ack.GetTaskId()

	reject := func(name string, req *nriv1.PublishRequest, code codes.Code) {
		t.Helper()
		if _, err := l.client.Publish(f.Ctx, req); status.Code(err) != code {
			t.Errorf("%s: %v, want %s", name, err, code)
		}
	}
	reject("a reopen before the terminal event", announceReq(parent, subtopic, sub, `{"task_text":"again"}`), codes.FailedPrecondition)
	f.terminal(bus.TypeFailed, sub, subtopic, map[string]any{"failure_reason": "not found"})
	del, doc := result(t, l.next(t, 5*time.Second))
	if doc["subject"] != sub || doc["type"] != bus.TypeFailed {
		t.Errorf("delivered %s", del.GetEvent())
	}
	l.received(t, del.GetDeliveryId())

	for _, c := range []struct {
		name string
		req  *nriv1.PublishRequest
		code codes.Code
	}{
		{"a task it does not hold", announceReq("other", subtopic, "", `{"task_text":"x"}`), codes.FailedPrecondition},
		{"no topic", announceReq(parent, "", "", `{"task_text":"x"}`), codes.InvalidArgument},
		{"a topic of the hitl_contact_* family", announceReq(parent, "hitl_contact_user", "", `{"task_text":"x"}`), codes.InvalidArgument},
		{"no task_text", announceReq(parent, subtopic, "", `{}`), codes.InvalidArgument},
		{"a task_text longer than max_text_size", announceReq(parent, subtopic, "", `{"task_text":"`+strings.Repeat("x", 64)+`"}`), codes.InvalidArgument},
		{"a requester_identity", announceReq(parent, subtopic, "", `{"task_text":"x","requester_identity":"me"}`), codes.InvalidArgument},
		{"a previous_attempt in data", announceReq(parent, subtopic, "", `{"task_text":"x","previous_attempt":"y"}`), codes.InvalidArgument},
		{"a query in a trace without origin_audience", askReq(parent, "", `{}`), codes.FailedPrecondition},
		{"a reopen of an unknown attempt", announceReq(parent, subtopic, "other", `{"task_text":"x"}`), codes.FailedPrecondition},
		{"a reopen on another topic", announceReq(parent, "other_topic", sub, `{"task_text":"x"}`), codes.FailedPrecondition},
	} {
		reject(c.name, c.req, c.code)
	}

	ack2, err := l.client.Publish(f.Ctx, announceReq(parent, subtopic, sub, `{"task_text":"find it, with the traceback"}`))
	if err != nil {
		t.Fatal(err)
	}
	e = await(t, announced, 5*time.Second)
	if d := dataOf(t, e); e.Subject != ack2.GetTaskId() || d["current_attempts"] != 2.0 || d["previous_attempt"] != sub {
		t.Errorf("reopened %s: %s", e.Subject, e.Data)
	}
	reject("a reopen of an attempt that is not the last", announceReq(parent, subtopic, sub, `{"task_text":"x"}`), codes.FailedPrecondition)
}

// A subtask left outside the vector space is reopened by the Integration
// Sidecar, and the Logic Container keeps naming it by its first TaskID.
func TestStaleVectorSpace(t *testing.T) {
	ep := newEndpoint(t)
	f := newFixture(t, options{embeddings: ep.URL})
	announced := f.Tap(bus.SubjectAnnounced(subtopic))
	l, err := f.attach(0)
	if err != nil {
		t.Fatal(err)
	}
	parent := f.take(l, nil)
	ack, err := l.client.Publish(f.Ctx, announceReq(parent, subtopic, "", `{"task_text":"find it"}`))
	if err != nil {
		t.Fatal(err)
	}
	sub := ack.GetTaskId()
	await(t, announced, 5*time.Second)

	f.terminal(bus.TypeFailed, sub, subtopic, map[string]any{"failure_reason": "stale", "failure_code": "stale_vector_space"})
	e := await(t, announced, 5*time.Second)
	if d := dataOf(t, e); d["current_attempts"] != 2.0 || d["previous_attempt"] != sub || d["task_text"] != "find it" {
		t.Errorf("reopened %s: %s", e.Subject, e.Data)
	}
	if m := l.next(t, 2*auction); m != nil {
		t.Fatalf("delivered %v", m)
	}

	f.terminal(bus.TypeCompleted, e.Subject, subtopic, nil)
	del, doc := result(t, l.next(t, 5*time.Second))
	if doc["subject"] != sub {
		t.Errorf("delivered %s", del.GetEvent())
	}
	l.received(t, del.GetDeliveryId())
	if _, err := l.client.Publish(f.Ctx, announceReq(parent, subtopic, sub, `{"task_text":"find it"}`)); err != nil {
		t.Fatal(err)
	}
	if d := dataOf(t, await(t, announced, 5*time.Second)); d["current_attempts"] != 3.0 || d["previous_attempt"] != e.Subject {
		t.Errorf("reopened with %v", d)
	}
	// The same text in the same space reuses its vector (architecture §3.7.4).
	if r := ep.Requests(); len(r) != 1 {
		t.Errorf("embedded %d times", len(r))
	}
}

// A query goes on the trace's origin audience, and an answered one is not reopened.
func TestAsk(t *testing.T) {
	f := newFixture(t, options{})
	topic := "hitl_contact_user"
	queries := f.Tap(bus.SubjectAnnounced(topic))
	l, err := f.attach(0)
	if err != nil {
		t.Fatal(err)
	}
	parent := f.take(l, map[string]any{"origin_audience": "user"})

	ack, err := l.client.Publish(f.Ctx, askReq(parent, "", `{"question":"which one?"}`))
	if err != nil {
		t.Fatal(err)
	}
	e := await(t, queries, 5*time.Second)
	if d := dataOf(t, e); e.Subject != ack.GetTaskId() || d["topic"] != topic || d["origin_audience"] != "user" || d["requester_identity"] != f.id {
		t.Errorf("asked %s: %s", e.Subject, e.Data)
	}
	f.terminal(bus.TypeFailed, ack.GetTaskId(), topic, map[string]any{"failure_reason": "nobody", "failure_code": "unanswered"})
	d, _ := result(t, l.next(t, 5*time.Second))
	l.received(t, d.GetDeliveryId())

	again, err := l.client.Publish(f.Ctx, askReq(parent, ack.GetTaskId(), `{"question":"which one?"}`))
	if err != nil {
		t.Fatal(err)
	}
	if d := dataOf(t, await(t, queries, 5*time.Second)); d["current_attempts"] != 2.0 {
		t.Errorf("reopened with %v", d)
	}
	f.terminal(bus.TypeCompleted, again.GetTaskId(), topic, map[string]any{"decision": "the first"})
	d, _ = result(t, l.next(t, 5*time.Second))
	l.received(t, d.GetDeliveryId())
	if _, err := l.client.Publish(f.Ctx, askReq(parent, again.GetTaskId(), `{}`)); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("reopening an answered query: %v", err)
	}
}

// An announcement waits in the outbox while the embeddings endpoint is down,
// across a restart, and goes out when it comes back.
func TestOutboxRestart(t *testing.T) {
	ep := newEndpoint(t)
	ep.down.Store(true)
	f := newFixture(t, options{embeddings: ep.URL})
	announced := f.Tap(bus.SubjectAnnounced(subtopic))
	l, err := f.attach(0)
	if err != nil {
		t.Fatal(err)
	}
	parent := f.take(l, nil)
	ack, err := l.client.Publish(f.Ctx, announceReq(parent, subtopic, "", `{"task_text":"find it"}`))
	if err != nil {
		t.Fatal(err)
	}
	silent(t, announced, time.Second, "with the endpoint down")

	f.restart()
	silent(t, announced, time.Second, "after the restart")
	ep.down.Store(false)
	if e := await(t, announced, 10*time.Second); e.Subject != ack.GetTaskId() {
		t.Errorf("announced %s, want %s", e.Subject, ack.GetTaskId())
	}
}

// The Context Store, with the task's token; the work area lives with the task.
func TestContext(t *testing.T) {
	f := newFixture(t, options{broker: true})
	l, err := f.attach(0)
	if err != nil {
		t.Fatal(err)
	}
	taskID := f.announce(near, nil)
	d, e := assignment(t, l.next(t, 5*time.Second))
	l.received(t, d.GetDeliveryId())
	area := filepath.Join(f.shared, "tasks", taskID)
	if info, err := os.Stat(area); err != nil || !info.IsDir() {
		t.Fatalf("the work area: %v", err)
	}

	write := func(task, name string, chunks ...string) (*nriv1.ContextRef, error) {
		s, err := l.client.WriteContext(f.Ctx)
		if err != nil {
			return nil, err
		}
		if err := s.Send(&nriv1.ContextWrite{Body: &nriv1.ContextWrite_Header{Header: &nriv1.ContextWriteHeader{TaskId: task, Name: name}}}); err != nil {
			return nil, err
		}
		for _, c := range chunks {
			if err := s.Send(&nriv1.ContextWrite{Body: &nriv1.ContextWrite_Chunk{Chunk: []byte(c)}}); err != nil {
				break
			}
		}
		return s.CloseAndRecv()
	}
	read := func(task string, ref *nriv1.ContextRef) (string, error) {
		s, err := l.client.ReadContext(f.Ctx, &nriv1.ContextRead{TaskId: task, Ref: ref})
		if err != nil {
			return "", err
		}
		var b strings.Builder
		for {
			c, err := s.Recv()
			if errors.Is(err, io.EOF) {
				return b.String(), nil
			}
			if err != nil {
				return "", err
			}
			b.Write(c.GetData())
		}
	}

	ref, err := write(taskID, "notes", "hello ", "world")
	if err != nil {
		t.Fatal(err)
	}
	if ref.GetTaskId() != taskID || ref.GetTraceId() != bus.TraceID(e.TraceParent) || ref.GetName() != "notes" {
		t.Errorf("wrote %v", ref)
	}
	if got, err := read(taskID, ref); err != nil || got != "hello world" {
		t.Errorf("read %q, %v", got, err)
	}
	listing, err := l.client.ListContext(f.Ctx, &nriv1.ContextList{TaskId: taskID, Section: taskID})
	if err != nil || len(listing.GetRefs()) != 1 || listing.GetRefs()[0].GetName() != "notes" {
		t.Errorf("listed %v, %v", listing, err)
	}

	if _, err := write(taskID, "zobik/payload", "x"); status.Code(err) != codes.InvalidArgument {
		t.Errorf("writing a reserved name: %v", err)
	}
	if _, err := write("other", "notes", "x"); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("writing on a task it does not hold: %v", err)
	}
	if _, err := read("other", ref); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("reading with a task it does not hold: %v", err)
	}
	other := &nriv1.ContextRef{TraceId: strings.ReplaceAll(uuid.NewString(), "-", ""), TaskId: taskID, Name: "notes"}
	if _, err := read(taskID, other); status.Code(err) != codes.PermissionDenied {
		t.Errorf("reading another trace: %v", err)
	}

	if _, err := l.client.Publish(f.Ctx, &nriv1.PublishRequest{TaskId: taskID, Body: &nriv1.PublishRequest_Complete{Complete: &nriv1.CompleteTask{}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(area); !os.IsNotExist(err) {
		t.Errorf("the work area outlives its task: %v", err)
	}
}
