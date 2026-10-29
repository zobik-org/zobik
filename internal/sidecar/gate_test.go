package sidecar_test

import (
	"context"
	"encoding/json"
	"maps"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/embedding"
	"zobik.org/zobik/internal/globalconfig"
	"zobik.org/zobik/internal/signing"
	nriv1 "zobik.org/zobik/proto/zobik/nri/v1"
)

// gateRules send every call the model's entry does not deny to requester.
var gateRules = []map[string]string{
	{"when": "has(call.max_tokens) && call.max_tokens > 100", "then": "deny"},
	{"then": "requester"},
}

// gateFixture is a node whose model slot is associated with an entry whose
// rules end in requester, holding a task whose trace's origin audience is user.
func gateFixture(t *testing.T) (*fixture, *provider, *logic, string) {
	p := newProvider(t)
	f := newFixture(t, options{
		cap: signing.Capability{Topic: topic, ArtifactRef: "sha256:test", ModelID: modelID,
			CapabilityEmbedding: embedding.Encode(capability), SimilarityFloor: 0.6,
			EgressGrants: []signing.Grant{{Slot: "model", Entry: "model", Interface: interfaceDigest(t, modelInterface)}}},
		registry: map[string]any{"model": entry(modelInterface, p.URL, "provider_key", map[string]any{"rules": gateRules})},
		secrets:  map[string][]byte{"provider_key": []byte("sk-test")},
	})
	l, err := f.attach(0)
	if err != nil {
		t.Fatal(err)
	}
	return f, p, l, f.take(l, map[string]any{"origin_audience": "user"})
}

// gateTopic is the audience requester resolves to in the fixture.
const gateTopic = "hitl_contact_user"

// calling runs a call of the model slot with prompt and returns its error.
func calling(ctx context.Context, l *logic, taskID, prompt string) chan error {
	done := make(chan error, 1)
	go func() {
		fields, _ := json.Marshal(map[string]any{"prompt": prompt})
		_, err := l.client.Call(ctx, &nriv1.EgressRequest{TaskId: taskID, Slot: "model", Operation: "complete", Fields: fields})
		done <- err
	}()
	return done
}

func waitCall(t *testing.T, done chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("the call did not return")
		return nil
	}
}

func (p *provider) received() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.bodies...)
}

// setRegistry publishes a version of the Global Configuration with registry as
// its egress_registry, and waits for the Integration Sidecar to read the head.
func (f *fixture) setRegistry(registry map[string]any) {
	f.t.Helper()
	head, rev, err := globalconfig.Head(f.Ctx, f.Console)
	if err != nil {
		f.t.Fatal(err)
	}
	next, err := globalconfig.NextVersion(head.ConfigVersion)
	if err != nil {
		f.t.Fatal(err)
	}
	values := maps.Clone(head.Values)
	values["egress_registry"], _ = json.Marshal(registry)
	if err := globalconfig.Publish(f.Ctx, f.Console, rev, globalconfig.Record{
		ConfigVersion: next, PublishedAt: time.Now().UTC(), AuthorizedBy: globalconfig.AuthorizedByDeployment, Values: values,
	}); err != nil {
		f.t.Fatal(err)
	}
	time.Sleep(3 * time.Second) // the Integration Sidecar reads the head every two seconds
}

// The gate of architecture §3.16.4: the call goes out only once approved, a
// round without an answer is reopened, and an approval executes only the
// request of its own query.
func TestGate(t *testing.T) {
	f, p, l, taskID := gateFixture(t)
	queries := f.Tap(bus.SubjectAnnounced(gateTopic))

	first := calling(f.Ctx, l, taskID, "first")
	q := await(t, queries, 5*time.Second)
	d := dataOf(t, q)
	details, _ := d["details"].(map[string]any)
	call, _ := details["call"].(map[string]any)
	evidence, _ := d["evidence"].(map[string]any)
	if d["kind"] != "gate_approval" || d["scope"] != "call" || d["outcome_if_silent"] != "deny" || d["origin_audience"] != "user" ||
		details["entry"] != "model" || !strings.Contains(details["rule"].(string), "requester") || details["node_id"] != f.id ||
		call["target"] != p.URL || call["operation"] != "complete" || evidence["task_id"] != taskID {
		t.Fatalf("the gate: %s", q.Data)
	}
	if !strings.HasPrefix(details["digest"].(string), "sha256:") {
		t.Errorf("the digest: %v", details["digest"])
	}

	// A round without an answer is reopened.
	f.terminal(bus.TypeFailed, q.Subject, gateTopic, map[string]any{"failure_reason": "the session closed", "failure_code": "unanswered"})
	again := await(t, queries, 5*time.Second)
	if a := dataOf(t, again); a["current_attempts"] != 2.0 || a["previous_attempt"] != q.Subject {
		t.Errorf("reopened with %s", again.Data)
	}

	// A second call opens its own query, and approving it executes only it.
	second := calling(f.Ctx, l, taskID, "second")
	q2 := await(t, queries, 5*time.Second)
	if dataOf(t, q2)["details"].(map[string]any)["digest"] == details["digest"] {
		t.Error("two different requests have the same digest")
	}
	f.terminal(bus.TypeCompleted, q2.Subject, gateTopic, map[string]any{"decision": "approve"})
	if err := waitCall(t, second); err != nil {
		t.Fatal(err)
	}
	if got := p.received(); len(got) != 1 || !strings.Contains(got[0], "second") {
		t.Fatalf("the provider received %q", got)
	}
	select {
	case err := <-first:
		t.Fatalf("the first call returned on the second's approval: %v", err)
	default:
	}
	f.terminal(bus.TypeCompleted, again.Subject, gateTopic, map[string]any{"decision": "approve"})
	if err := waitCall(t, first); err != nil {
		t.Fatal(err)
	}
	if got := p.received(); len(got) != 2 {
		t.Fatalf("the provider received %q", got)
	}
}

// What is not an approval does not execute: a deny, an answer outside the
// options, an aborted query, a call the Logic Container stops waiting for and
// one whose task closes.
func TestGateNotApproved(t *testing.T) {
	f, p, l, taskID := gateFixture(t)
	queries := f.Tap(bus.SubjectAnnounced(gateTopic))

	answers := []struct {
		name, typ string
		data      map[string]any
		want      codes.Code
	}{
		{"a deny", bus.TypeCompleted, map[string]any{"decision": "deny"}, codes.PermissionDenied},
		{"an answer outside options", bus.TypeCompleted, map[string]any{"decision": "yes"}, codes.PermissionDenied},
		{"no decision", bus.TypeCompleted, map[string]any{"answer": "approve"}, codes.PermissionDenied},
		{"an aborted query", bus.TypeAborted, map[string]any{"failure_reason": "budget"}, codes.Aborted},
	}
	for _, a := range answers {
		done := calling(f.Ctx, l, taskID, a.name)
		q := await(t, queries, 5*time.Second)
		f.terminal(a.typ, q.Subject, gateTopic, a.data)
		if err := waitCall(t, done); status.Code(err) != a.want {
			t.Errorf("%s: %v, want %s", a.name, err, a.want)
		}
	}

	// The Logic Container stops waiting: a later approval executes nothing.
	ctx, cancel := context.WithCancel(f.Ctx)
	done := calling(ctx, l, taskID, "abandoned")
	q := await(t, queries, 5*time.Second)
	cancel()
	if err := waitCall(t, done); status.Code(err) != codes.Canceled {
		t.Errorf("a cancelled call: %v", err)
	}
	f.terminal(bus.TypeCompleted, q.Subject, gateTopic, map[string]any{"decision": "approve"})

	// The task closes while a call waits: the call returns and the close
	// does not wait for the person.
	closes := f.Tap("task.completed." + f.requester + ".>")
	done = calling(f.Ctx, l, taskID, "closing")
	await(t, queries, 5*time.Second)
	if _, err := l.client.Publish(f.Ctx, &nriv1.PublishRequest{TaskId: taskID, Body: &nriv1.PublishRequest_Complete{Complete: &nriv1.CompleteTask{}}}); err != nil {
		t.Fatal(err)
	}
	if err := waitCall(t, done); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("a call whose task closed: %v", err)
	}
	await(t, closes, 5*time.Second)

	time.Sleep(200 * time.Millisecond)
	if got := p.received(); len(got) != 0 {
		t.Errorf("the provider received %q", got)
	}
}

// Once approved, the call is checked again against the current registry: a
// rule edited to deny or a target edited during the wait does not execute.
func TestGateEdited(t *testing.T) {
	f, p, l, taskID := gateFixture(t)
	other := newProvider(t)
	queries := f.Tap(bus.SubjectAnnounced(gateTopic))

	done := calling(f.Ctx, l, taskID, "moved")
	q := await(t, queries, 5*time.Second)
	f.setRegistry(map[string]any{"model": entry(modelInterface, other.URL, "provider_key", map[string]any{"rules": gateRules})})
	f.terminal(bus.TypeCompleted, q.Subject, gateTopic, map[string]any{"decision": "approve"})
	if err := waitCall(t, done); status.Code(err) != codes.PermissionDenied || !strings.Contains(status.Convert(err).Message(), "changed") {
		t.Errorf("a target edited during the wait: %v", err)
	}

	done = calling(f.Ctx, l, taskID, "denied")
	q = await(t, queries, 5*time.Second)
	f.setRegistry(map[string]any{"model": entry(modelInterface, other.URL, "provider_key", map[string]any{
		"rules": []map[string]string{{"then": "deny"}},
	})})
	f.terminal(bus.TypeCompleted, q.Subject, gateTopic, map[string]any{"decision": "approve"})
	if err := waitCall(t, done); status.Code(err) != codes.PermissionDenied || !strings.Contains(status.Convert(err).Message(), "then deny") {
		t.Errorf("a rule edited to deny during the wait: %v", err)
	}
	if got := append(p.received(), other.received()...); len(got) != 0 {
		t.Errorf("the providers received %q", got)
	}
}
