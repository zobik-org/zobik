package bus

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

func announced(t *testing.T, data map[string]any) *Event {
	t.Helper()
	e, err := NewEvent(TypeAnnounced, "node-a", "task-1", data)
	if err != nil {
		t.Fatal(err)
	}
	e.TraceParent = traceparent
	e.ConfigVersion = "v0"
	return e
}

func TestValidate(t *testing.T) {
	if err := announced(t, map[string]any{"topic": "echo", "task_embedding": "AAAAAA=="}).Validate(); err != nil {
		t.Fatalf("valid announcement: %v", err)
	}

	metrics := map[string]any{"tokens_in": 0, "tokens_out": 0, "cost": 0, "duration_ms": 5}
	cases := map[string]struct {
		mutate func(e *Event)
		want   string
	}{
		"unknown type":                {func(e *Event) { e.Type = "task.paused" }, "envelope: type"},
		"no traceparent":              {func(e *Event) { e.TraceParent = ""; e.ConfigVersion = "" }, "traceparent"},
		"configversion alone":         {func(e *Event) { e.ConfigVersion = "" }, "configversion"},
		"data not an object":          {func(e *Event) { e.Data = json.RawMessage(`"x"`) }, "data: expected object"},
		"null field":                  {func(e *Event) { e.Data = json.RawMessage(`{"topic":null}`) }, "data: topic: null"},
		"wrong field type":            {func(e *Event) { e.Data = json.RawMessage(`{"current_attempts":"2"}`) }, "current_attempts"},
		"embedding outside announced": {func(e *Event) { e.Type = TypeProposed }, "task_embedding"},
		"failed without reason": {func(e *Event) {
			e.Type = TypeFailed
			e.Data, _ = json.Marshal(map[string]any{"metrics": metrics})
		}, "failure_reason"},
		"completed without metrics": {func(e *Event) {
			e.Type = TypeCompleted
			e.Data = json.RawMessage(`{}`)
		}, "metrics"},
		"notice with metrics": {func(e *Event) {
			e.Type = TypeNotice
			e.Data, _ = json.Marshal(map[string]any{"metrics": metrics})
		}, "notice"},
	}
	for name, c := range cases {
		e := announced(t, map[string]any{"topic": "echo", "task_embedding": "AAAAAA=="})
		c.mutate(e)
		if err := e.Validate(); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: got %v, want %q", name, err, c.want)
		}
	}

	notice, _ := NewEvent(TypeNotice, "network_monitor", "partition-3", map[string]any{"kind": "coverage_lost"})
	if err := notice.Validate(); err != nil {
		t.Errorf("network-scope notice: %v", err)
	}
}

func TestCheckSubject(t *testing.T) {
	e := announced(t, map[string]any{"topic": "echo"})
	for subject, ok := range map[string]bool{
		SubjectAnnounced("echo"):     true,
		SubjectAnnounced("other"):    false,
		"task.announced.echo.extra":  false,
		"task.proposed.echo":         false,
		SubjectAnnounced("*"):        false,
		SubjectNotice("echo"):        false,
		SubjectProposed("UABC", "x"): false,
	} {
		if err := checkSubject(subject, e); (err == nil) != ok {
			t.Errorf("%s: got %v", subject, err)
		}
	}
	done, _ := NewEvent(TypeCompleted, "node-b", "task-1", map[string]any{})
	if err := checkSubject(SubjectTerminal(TypeCompleted, "UREQ", "echo"), done); err != nil {
		t.Errorf("terminal subject: %v", err)
	}
}

func TestPublish(t *testing.T) {
	a := newTestAccount(t)
	s := startServer(t, a)
	year := time.Now().Add(365 * 24 * time.Hour)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	console, _, err := connect(t, s, a, ConsoleScope.Role, year)
	if err != nil {
		t.Fatal(err)
	}
	cjs, _ := jetstream.New(console)
	if err := EnsureStreams(ctx, cjs); err != nil {
		t.Fatal(err)
	}
	if err := EnsureStreams(ctx, cjs); err != nil {
		t.Fatalf("again: %v", err)
	}

	harness, _, err := connect(t, s, a, harnessScope.Role, year)
	if err != nil {
		t.Fatal(err)
	}
	js, _ := jetstream.New(harness)
	e := announced(t, map[string]any{"topic": "echo"})
	if err := Publish(ctx, js, SubjectAnnounced("echo"), e); err != nil {
		t.Fatal(err)
	}
	if err := Publish(ctx, js, SubjectAnnounced("echo"), e); err != nil {
		t.Fatalf("republishing: %v", err)
	}
	if err := Publish(ctx, js, SubjectAnnounced("other"), e); err == nil {
		t.Error("published on another topic's subject")
	}

	stream, err := cjs.Stream(ctx, "task")
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 1 {
		t.Errorf("stream holds %d events, want 1: the republication is deduplicated by id", info.State.Msgs)
	}
}
