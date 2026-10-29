package sidecar_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/digest"
	"zobik.org/zobik/internal/embedding"
	"zobik.org/zobik/internal/signing"
	nriv1 "zobik.org/zobik/proto/zobik/nri/v1"
)

// modelInterface is a model provider's interface: a completion with its usage.
const modelInterface = `{
  "protocol": "https", "direction": "egress",
  "operations": {
    "complete": {
      "fields": {"type": "object", "required": ["prompt"], "properties": {
        "prompt": {"type": "string"}, "max_tokens": {"type": "integer"}}},
      "response": {"type": "object", "properties": {
        "text": {"type": "string"},
        "usage": {"type": "object", "properties": {"input_tokens": {"type": "integer"}, "output_tokens": {"type": "integer"}}}}},
      "https": {"method": "POST", "path": "/v1/complete", "headers": {"Content-Type": "application/json"},
        "fields": {"prompt": {"in": "json", "at": "/prompt"}, "max_tokens": {"in": "json", "at": "/max_tokens"}},
        "response": {"text": {"from": "json", "at": "/text"}, "usage": {"from": "json", "at": "/usage"}}}
    }
  }
}`

// storeInterface uploads a file as the call's body.
const storeInterface = `{
  "protocol": "https", "direction": "egress",
  "operations": {
    "put": {
      "fields": {"type": "object", "required": ["name"], "properties": {"name": {"type": "string"}}},
      "body": {"request": true, "response": false},
      "https": {"method": "PUT", "path": "/files/{name}", "fields": {"name": {"in": "path"}}, "response": {}}
    }
  }
}`

// provider is the destination of both entries: it records what it receives.
type provider struct {
	*httptest.Server
	mu       sync.Mutex
	auth     []string
	uploaded []string
}

func newProvider(t *testing.T) *provider {
	p := &provider{}
	p.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		p.mu.Lock()
		p.auth = append(p.auth, r.Header.Get("Authorization"))
		if r.Method == http.MethodPut {
			p.uploaded = append(p.uploaded, string(body))
		}
		p.mu.Unlock()
		if strings.Contains(string(body), "slow") {
			time.Sleep(500 * time.Millisecond)
		}
		if strings.Contains(string(body), "no usage") {
			io.WriteString(w, `{"text":"done"}`)
			return
		}
		io.WriteString(w, `{"text":"done","usage":{"input_tokens":100,"output_tokens":20}}`)
	}))
	t.Cleanup(p.Close)
	return p
}

func entry(iface, target, handle string, extra map[string]any) map[string]any {
	var in map[string]any
	_ = json.Unmarshal([]byte(iface), &in)
	ops := []string{}
	billing := map[string]any{}
	for op := range in["operations"].(map[string]any) {
		ops = append(ops, op)
		billing[op] = map[string]string{"cost": "0.0"}
	}
	e := map[string]any{
		"interface": in, "target": target, "operations": ops, "billing_rule": billing,
		"credential_handle": handle, "credential_placement": map[string]string{"header": "Authorization", "scheme": "Bearer"},
		"rules": []map[string]string{{"then": "allow"}},
	}
	for k, v := range extra {
		e[k] = v
	}
	return e
}

func interfaceDigest(t *testing.T, iface string) string {
	t.Helper()
	d, err := digest.JSON([]byte(iface))
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// callFixture is a node whose grants associate the model and the store slots,
// and a slot built against an older interface of the model's entry.
func callFixture(t *testing.T) (*fixture, *provider) {
	p := newProvider(t)
	model := interfaceDigest(t, modelInterface)
	f := newFixture(t, options{
		cap: signing.Capability{Topic: topic, ArtifactRef: "sha256:test", ModelID: modelID,
			CapabilityEmbedding: embedding.Encode(capability), SimilarityFloor: 0.6,
			EgressGrants: []signing.Grant{
				{Slot: "model", Entry: "model", Interface: model},
				{Slot: "store", Entry: "store", Interface: interfaceDigest(t, storeInterface)},
				{Slot: "older", Entry: "model", Interface: "sha256:older"},
			}},
		registry: map[string]any{
			"model": entry(modelInterface, p.URL, "provider_key", map[string]any{
				"rules": []map[string]string{
					{"when": "has(call.max_tokens) && call.max_tokens > 100", "then": "deny"},
					{"then": "allow"},
				},
				"billing_rule": map[string]any{"complete": map[string]string{
					"cost":      `double(response.usage.input_tokens) * rate["m"]["input"] + double(response.usage.output_tokens) * rate["m"]["output"]`,
					"tokens_in": "response.usage.input_tokens", "tokens_out": "response.usage.output_tokens",
				}},
				"rate_source": "provider",
			}),
			"store": entry(storeInterface, p.URL, "store_key", map[string]any{"max_body_size": 8}),
		},
		secrets: map[string][]byte{"provider_key": []byte("sk-test"), "store_key": []byte("st-test")},
	})
	kv, err := f.Console.KeyValue(f.Ctx, bus.BucketRates)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kv.Put(f.Ctx, "provider", []byte(`{"as_of":"2026-10-01","max_per_call":1.5,"rates":{"m":{"input":0.01,"output":0.1}}}`)); err != nil {
		t.Fatal(err)
	}
	return f, p
}

// held makes the node win a task and returns its TaskID.
func (f *fixture) held(l *logic) string {
	f.t.Helper()
	proposals := f.Tap("prop.>")
	taskID := f.announce(near, nil)
	await(f.t, proposals, 5*time.Second)
	f.assign(taskID)
	d, _ := assignment(f.t, l.next(f.t, 5*time.Second))
	l.received(f.t, d.GetDeliveryId())
	return taskID
}

// The procedure of architecture §3.16.4 up to the execution: what is not
// associated, not admitted, invalid or denied does not go out; what is allowed
// goes out with its credential and leaves its cost in metrics.
func TestCall(t *testing.T) {
	f, p := callFixture(t)
	closes := f.Tap("task.completed." + f.requester + ".>")
	l, err := f.attach(0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.client.Call(f.Ctx, &nriv1.EgressRequest{TaskId: "none", Slot: "model", Operation: "complete", Fields: []byte(`{"prompt":"x"}`)}); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("a task the node does not hold: %v", err)
	}
	taskID := f.held(l)

	rejected := []struct {
		name, slot, op, fields string
		want                   codes.Code
	}{
		{"a slot without association", "repo", "complete", `{"prompt":"x"}`, codes.PermissionDenied},
		{"an entry that no longer implements the slot's interface", "older", "complete", `{"prompt":"x"}`, codes.PermissionDenied},
		{"an operation the entry does not admit", "model", "delete_all", `{}`, codes.InvalidArgument},
		{"invalid fields", "model", "complete", `{"prompt":5}`, codes.InvalidArgument},
		{"a deny", "model", "complete", `{"prompt":"x","max_tokens":500}`, codes.PermissionDenied},
	}
	for _, c := range rejected {
		_, err := l.client.Call(f.Ctx, &nriv1.EgressRequest{TaskId: taskID, Slot: c.slot, Operation: c.op, Fields: []byte(c.fields)})
		if status.Code(err) != c.want {
			t.Errorf("%s: %v, want %s", c.name, err, c.want)
		}
		if c.name == "a deny" && !strings.Contains(status.Convert(err).Message(), "call.max_tokens > 100") {
			t.Errorf("the deny does not name its rule: %v", err)
		}
	}
	p.mu.Lock()
	if len(p.auth) != 0 {
		t.Errorf("%d rejected calls went out", len(p.auth))
	}
	p.mu.Unlock()

	resp, err := l.client.Call(f.Ctx, &nriv1.EgressRequest{TaskId: taskID, Slot: "model", Operation: "complete", Fields: []byte(`{"prompt":"x","max_tokens":50}`)})
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	_ = json.Unmarshal(resp.GetFields(), &fields)
	if fields["text"] != "done" || fields["status"] != float64(200) {
		t.Errorf("response %s", resp.GetFields())
	}
	// A response without usage charges max_per_call.
	if _, err := l.client.Call(f.Ctx, &nriv1.EgressRequest{TaskId: taskID, Slot: "model", Operation: "complete", Fields: []byte(`{"prompt":"no usage"}`)}); err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	if len(p.auth) != 2 || p.auth[0] != "Bearer sk-test" {
		t.Errorf("the provider received %v", p.auth)
	}
	p.mu.Unlock()

	if _, err := l.client.Publish(f.Ctx, &nriv1.PublishRequest{TaskId: taskID, Body: &nriv1.PublishRequest_Complete{Complete: &nriv1.CompleteTask{}}}); err != nil {
		t.Fatal(err)
	}
	done := await(t, closes, 5*time.Second)
	var data struct {
		Metrics struct {
			TokensIn      int64   `json:"tokens_in"`
			TokensOut     int64   `json:"tokens_out"`
			Cost          float64 `json:"cost"`
			BillingErrors int64   `json:"billing_errors"`
		} `json:"metrics"`
	}
	_ = json.Unmarshal(done.Data, &data)
	m := data.Metrics
	want := 100*0.01 + 20*0.1 + 1.5
	if m.TokensIn != 100 || m.TokensOut != 20 || m.BillingErrors != 1 || m.Cost < want-1e-9 || m.Cost > want+1e-9 {
		t.Errorf("metrics %s, want cost %g", done.Data, want)
	}
}

// The body is a file of the task's work area: a path that leaves it, a
// symlink or one over max_body_size is rejected, and what goes out is the copy.
func TestCallBody(t *testing.T) {
	f, p := callFixture(t)
	l, err := f.attach(0)
	if err != nil {
		t.Fatal(err)
	}
	taskID := f.held(l)
	area := filepath.Join(f.shared, "tasks", taskID)
	os.WriteFile(filepath.Join(area, "small"), []byte("uploaded"), 0o600)
	os.WriteFile(filepath.Join(area, "large"), []byte("too large"), 0o600)
	os.WriteFile(filepath.Join(f.shared, "outside"), []byte("secret"), 0o600)
	os.Symlink(filepath.Join(f.shared, "outside"), filepath.Join(area, "link"))
	if err := syscall.Mkfifo(filepath.Join(area, "fifo"), 0o600); err != nil {
		t.Fatal(err)
	}

	call := func(body string) error {
		_, err := l.client.Call(f.Ctx, &nriv1.EgressRequest{TaskId: taskID, Slot: "store", Operation: "put", Fields: []byte(`{"name":"a"}`), Body: body})
		return err
	}
	for _, body := range []string{"", "missing", "../../outside", "link", "large", "fifo"} {
		if err := call(body); status.Code(err) != codes.InvalidArgument {
			t.Errorf("body %q: %v", body, err)
		}
	}
	if err := call("small"); err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.uploaded) != 1 || p.uploaded[0] != "uploaded" || p.auth[0] != "Bearer st-test" {
		t.Errorf("uploaded %q with %v", p.uploaded, p.auth)
	}
}

// A work area the Logic Container replaced with a symlink out of the shared
// volume does not open: nothing of the Integration Sidecar's own goes out.
func TestCallAreaSymlink(t *testing.T) {
	f, p := callFixture(t)
	l, err := f.attach(0)
	if err != nil {
		t.Fatal(err)
	}
	taskID := f.held(l)
	private := t.TempDir()
	os.WriteFile(filepath.Join(private, "startup.json"), []byte("seed"), 0o600)
	area := filepath.Join(f.shared, "tasks", taskID)
	if err := os.RemoveAll(area); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(private, area); err != nil {
		t.Fatal(err)
	}
	_, err = l.client.Call(f.Ctx, &nriv1.EgressRequest{TaskId: taskID, Slot: "store", Operation: "put", Fields: []byte(`{"name":"a"}`), Body: "startup.json"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("a symlinked work area: %v", err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.uploaded) != 0 {
		t.Errorf("uploaded %q", p.uploaded)
	}
}

// The terminal event waits for the task's calls in flight, so their charge is
// in its metrics.
func TestCloseWaitsForCalls(t *testing.T) {
	f, _ := callFixture(t)
	closes := f.Tap("task.completed." + f.requester + ".>")
	l, err := f.attach(0)
	if err != nil {
		t.Fatal(err)
	}
	taskID := f.held(l)
	called := make(chan error, 1)
	go func() {
		_, err := l.client.Call(f.Ctx, &nriv1.EgressRequest{TaskId: taskID, Slot: "model", Operation: "complete", Fields: []byte(`{"prompt":"slow"}`)})
		called <- err
	}()
	time.Sleep(100 * time.Millisecond) // the call is at the provider
	if _, err := l.client.Publish(f.Ctx, &nriv1.PublishRequest{TaskId: taskID, Body: &nriv1.PublishRequest_Complete{Complete: &nriv1.CompleteTask{}}}); err != nil {
		t.Fatal(err)
	}
	if err := <-called; err != nil {
		t.Fatal(err)
	}
	done := await(t, closes, 5*time.Second)
	var data struct {
		Metrics struct {
			TokensIn int64 `json:"tokens_in"`
		} `json:"metrics"`
	}
	_ = json.Unmarshal(done.Data, &data)
	if data.Metrics.TokensIn != 100 {
		t.Errorf("metrics %s: the call in flight is missing", done.Data)
	}
}
