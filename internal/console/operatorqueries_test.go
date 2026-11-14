package console_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/console"
)

// A query of the operator audience reaches the operation area: the console
// lists it, tells the channel the panel showed it, refuses an answer outside
// its options and closes it with the one the person chose (architecture
// §3.17.1; implementation §6, The operator console).
func TestOperatorQuery(t *testing.T) {
	f := newFixture(t, nil)
	cookie := f.cookie()
	worker := f.Conn(bus.NodeScope.Role, nil)
	closes := f.Tap("task.*.worker.>")

	trace := strings.ReplaceAll(uuid.NewString(), "-", "")
	taskID := uuid.NewString()
	ev, err := bus.NewEvent(bus.TypeAnnounced, "worker", taskID, map[string]any{
		"topic": "hitl_contact_operator", "requester_identity": "worker",
		"kind": "gate_approval", "scope": "call", "since": time.Now().UTC().Format(time.RFC3339Nano),
		"options": []string{"approve", "deny"}, "outcome_if_silent": "deny",
		"evidence": map[string]any{"trace_id": trace, "task_id": uuid.NewString()},
		"details":  map[string]any{"entry": "mail", "rule": 0, "topic": "mailer", "node_id": "worker"},
	})
	if err != nil {
		t.Fatal(err)
	}
	ev.TraceParent, ev.ConfigVersion = "00-"+trace+"-00f067aa0ba902b7-01", "v0"
	if err := bus.Publish(f.Ctx, f.JetStream(worker), bus.SubjectAnnounced("hitl_contact_operator"), ev); err != nil {
		t.Fatal(err)
	}

	var list console.OperatorQueries
	deadline := time.Now().Add(15 * time.Second)
	for len(list.Queries) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the query did not reach the operation area")
		}
		time.Sleep(100 * time.Millisecond)
		f.callJSON(cookie, http.MethodGet, console.PathOperatorQueries, nil, &list)
	}
	q := list.Queries[0]
	if q.TaskID != taskID || q.Kind != "gate_approval" || q.TraceID != trace || strings.Join(q.Options, ",") != "approve,deny" || q.Silent != "deny" {
		t.Fatalf("query %+v", q)
	}
	if code := f.callJSON(cookie, http.MethodPost, console.PathOperatorShown, console.TaskIDs{TaskIDs: []string{taskID}}, nil); code != http.StatusNoContent {
		t.Errorf("shown: %d", code)
	}
	if code := f.callJSON(cookie, http.MethodPost, console.PathOperatorAnswers, console.OperatorAnswer{TaskID: taskID, Decision: "maybe"}, nil); code != http.StatusUnprocessableEntity {
		t.Errorf("an answer outside the options: %d", code)
	}
	if code := f.callJSON(cookie, http.MethodPost, console.PathOperatorAnswers, console.OperatorAnswer{TaskID: taskID, Decision: "approve"}, nil); code != http.StatusNoContent {
		t.Fatalf("answer: %d", code)
	}
	timeout := time.After(10 * time.Second)
	for {
		select {
		case m := <-closes:
			var e bus.Event
			json.Unmarshal(m.Data, &e)
			if e.Subject != taskID {
				continue
			}
			var d struct {
				Decision string `json:"decision"`
			}
			json.Unmarshal(e.Data, &d)
			if e.Type != bus.TypeCompleted || d.Decision != "approve" {
				t.Errorf("close %s %s", e.Type, e.Data)
			}
			f.callJSON(cookie, http.MethodGet, console.PathOperatorQueries, nil, &list)
			if len(list.Queries) != 0 {
				t.Errorf("the answered query is still listed: %+v", list.Queries)
			}
			return
		case <-timeout:
			t.Fatal("the query was not closed")
		}
	}
}

// The schema the form is generated from carries every key with its
// declaration (architecture §3.17.1).
func TestConfigSchema(t *testing.T) {
	f := newFixture(t, nil)
	var out struct {
		Keys map[string]struct {
			Type    string `json:"type"`
			Storage string `json:"storage"`
		} `json:"keys"`
	}
	if code := f.callJSON(f.cookie(), http.MethodGet, console.PathConfigSchema, nil, &out); code != http.StatusOK {
		t.Fatalf("schema: %d", code)
	}
	if k := out.Keys["task_lease_ttl"]; k.Type != "integer" || k.Storage != "versioned" {
		t.Errorf("task_lease_ttl %+v", k)
	}
	if k := out.Keys["egress_registry"]; k.Type != "object" {
		t.Errorf("egress_registry %+v", k)
	}
}

// cookie signs in and returns the session's cookie.
func (f *fixture) cookie() *http.Cookie {
	f.T.Helper()
	b, _ := json.Marshal(console.Credentials{Username: user, Password: password})
	resp, err := http.Post(f.base+console.PathSession, "application/json", bytes.NewReader(b))
	if err != nil {
		f.T.Fatal(err)
	}
	resp.Body.Close()
	for _, c := range resp.Cookies() {
		if c.Name == console.CookieSession {
			return c
		}
	}
	f.T.Fatalf("sign-in: %s", resp.Status)
	return nil
}

// callJSON calls the API with cookie and decodes the response into out.
func (f *fixture) callJSON(cookie *http.Cookie, method, path string, in, out any) int {
	f.T.Helper()
	var body []byte
	if in != nil {
		body, _ = json.Marshal(in)
	}
	req, _ := http.NewRequestWithContext(f.Ctx, method, f.base+path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(cookie)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		f.T.Fatal(err)
	}
	defer resp.Body.Close()
	if out != nil && resp.StatusCode < 300 {
		json.NewDecoder(resp.Body).Decode(out)
	}
	return resp.StatusCode
}
