package egress

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// table is the OpenAI rate table of implementation §8.5.
var table = &Table{AsOf: "2026-10-01", MaxPerCall: 2.0, Rates: map[string]map[string]float64{
	"gpt-5": {"input": 0.00000125, "cached_input": 0.000000125, "output": 0.00001},
}}

func executor(credential string) *Executor {
	return &Executor{
		HTTP:       HTTPClient(nil),
		Credential: func(context.Context, string) ([]byte, error) { return []byte(credential), nil },
		Rates: func(name string) (*Table, bool) {
			if name == "openai" {
				return table, true
			}
			return nil, false
		},
	}
}

// The adapter builds the request from the mapping, under the target's root,
// with the credential where the entry places it, and the charge comes from
// the response's usage and the rate.
func TestExecute(t *testing.T) {
	var got struct {
		path, auth string
		body       map[string]any
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got.path, got.auth = r.URL.Path, r.Header.Get("Authorization")
		_ = json.NewDecoder(r.Body).Decode(&got.body)
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"model":"gpt-5","output":[{"type":"message"}],"usage":{"input_tokens":1000,"input_tokens_details":{"cached_tokens":200},"output_tokens":50}}`)
	}))
	defer srv.Close()
	e, err := ParseEntry(openAIEntry(t, func(e map[string]any) { e["target"] = srv.URL + "/openai/" }))
	if err != nil {
		t.Fatal(err)
	}
	c, err := e.NewCall("create_response", []byte(`{"model":"gpt-5","input":["hi"],"text_format":{"type":"json_schema"}}`), false)
	if err != nil {
		t.Fatal(err)
	}
	res, err := executor("sk-1").Execute(context.Background(), e, c, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.path != "/openai/v1/responses" || got.auth != "Bearer sk-1" {
		t.Errorf("sent %s with %q", got.path, got.auth)
	}
	if text, _ := got.body["text"].(map[string]any); got.body["model"] != "gpt-5" || text["format"] == nil {
		t.Errorf("sent the body %v", got.body)
	}
	if _, ok := got.body["max_output_tokens"]; ok {
		t.Error("an absent optional field was sent")
	}
	if res.Response["status"] != json.Number("200") || res.Response["model"] != "gpt-5" || res.Response["output"] == nil {
		t.Errorf("response %v", res.Response)
	}
	ch := res.Charge
	want := 800*0.00000125 + 200*0.000000125 + 50*0.00001
	if ch.TokensIn != 1000 || ch.TokensOut != 50 || ch.BillingErrors != 0 || ch.Cost < want-1e-12 || ch.Cost > want+1e-12 {
		t.Errorf("charge %+v, want cost %g", ch, want)
	}
}

// A response without usage charges max_per_call, adds no tokens and counts
// once in billing_errors (implementation §8.5); an error response does not charge.
func TestBillWithoutUsage(t *testing.T) {
	e, err := ParseEntry(openAIEntry(t, nil))
	if err != nil {
		t.Fatal(err)
	}
	c, _ := e.NewCall("create_response", []byte(`{"model":"gpt-5","input":[]}`), false)
	ch := e.Bill(c, map[string]any{"status": json.Number("200"), "model": "gpt-5"}, table)
	if ch.Cost != 2.0 || ch.TokensIn != 0 || ch.TokensOut != 0 || ch.BillingErrors != 1 || len(ch.Errors) != 3 {
		t.Errorf("without usage: %+v", ch)
	}
	ch = e.Bill(c, map[string]any{"status": json.Number("200"), "model": "gpt-9", "usage": map[string]any{
		"input_tokens": json.Number("1"), "input_tokens_details": map[string]any{"cached_tokens": json.Number("0")}, "output_tokens": json.Number("1"),
	}}, table)
	if ch.Cost != 2.0 || ch.TokensIn != 1 || ch.BillingErrors != 1 {
		t.Errorf("a model the table does not have: %+v", ch)
	}
	ch = e.Bill(c, map[string]any{"status": json.Number("429")}, table)
	if ch.Cost != 0 || ch.BillingErrors != 0 {
		t.Errorf("an error response: %+v", ch)
	}
}

// The rules give the first outcome whose condition holds, an evaluation error
// is deny, and each outcome names its rule (architecture §3.16.2, §3.16.4).
func TestDecide(t *testing.T) {
	e, err := ParseEntry(openAIEntry(t, func(e map[string]any) {
		e["rules"] = []map[string]string{
			{"when": `model == "gpt-4"`, "then": "deny"},
			{"when": `has(call.max_output_tokens) && call.max_output_tokens > 4000`, "then": "requester"},
			{"when": `max_output_tokens > 2000`, "then": "operator"},
			{"then": "allow"},
		}
	}))
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		fields, then, rule string
		unevaluable        bool
	}{
		{`{"model":"gpt-4","input":[]}`, "deny", `when model == "gpt-4" then deny`, false},
		{`{"model":"gpt-5","input":[],"max_output_tokens":8000}`, "requester", "then requester", false},
		{`{"model":"gpt-5","input":[],"max_output_tokens":100}`, "allow", "then allow", false},
		// A missing field makes the condition not evaluable, never false.
		{`{"model":"gpt-5","input":[]}`, "deny", "max_output_tokens > 2000", true},
	}
	for _, c := range cases {
		call, err := e.NewCall("create_response", []byte(c.fields), false)
		if err != nil {
			t.Fatal(err)
		}
		o := e.Decide(call, Lists, time.Now())
		if o.Then != c.then || !strings.Contains(o.Rule, c.rule) || (o.Unevaluable != nil) != c.unevaluable {
			t.Errorf("%s: %+v, want %s by %q", c.fields, o, c.then, c.rule)
		}
	}
}

// What does not validate against the interface is rejected, not completed or
// corrected; a field the interface does not declare is not part of the call.
func TestNewCallRejects(t *testing.T) {
	e, err := ParseEntry(openAIEntry(t, nil))
	if err != nil {
		t.Fatal(err)
	}
	c, err := e.NewCall("create_response", []byte(`{"model":"gpt-5","input":[],"temperature":1}`), false)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := c.Fields["temperature"]; ok {
		t.Error("the call keeps a field the interface does not declare")
	}
	for _, c := range []struct {
		name, op, fields string
		body             bool
	}{
		{"an operation the entry does not admit", "delete_all", `{}`, false},
		{"a required field missing", "create_response", `{"model":"gpt-5"}`, false},
		{"a field of another type", "create_response", `{"model":5,"input":[]}`, false},
		{"not JSON", "create_response", `model`, false},
		{"a body on an operation without one", "create_response", `{"model":"gpt-5","input":[]}`, true},
	} {
		if _, err := e.NewCall(c.op, []byte(c.fields), c.body); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: %v", c.name, err)
		}
	}
}

// A path field is a single segment, the adapter follows no redirect, a body
// travels as the HTTP body and the credential can go in a query parameter.
func TestAdapterMapping(t *testing.T) {
	var gotPath, gotKey, gotType string
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotKey, gotType = r.URL.EscapedPath(), r.URL.Query().Get("key"), r.Header.Get("Content-Type")
		gotBody, _ = io.ReadAll(r.Body)
		if strings.HasSuffix(r.URL.Path, "/moved") {
			http.Redirect(w, r, "https://elsewhere.example/", http.StatusFound)
			return
		}
		w.Header().Set("ETag", `"v1"`)
		io.WriteString(w, "downloaded")
	}))
	defer srv.Close()
	in := `{"protocol":"https","direction":"egress","operations":{
	  "put":{"fields":{"type":"object","required":["name"],"properties":{"name":{"type":"string"},"kind":{"type":"string"}}},
	    "body":{"request":true,"response":false},"response":{"type":"object","properties":{"etag":{"type":"string"}}},
	    "https":{"method":"PUT","path":"/files/{name}","fields":{"name":{"in":"path"},"kind":{"in":"header","at":"Content-Type"}},
	      "response":{"etag":{"from":"header","at":"ETag"}}}},
	  "get":{"fields":{"type":"object","required":["name"],"properties":{"name":{"type":"string"}}},
	    "body":{"request":false,"response":true},"response":{"type":"object","properties":{}},
	    "https":{"method":"GET","path":"/files/{name}","fields":{"name":{"in":"path"}},"response":{}}}}}`
	raw, _ := json.Marshal(map[string]any{
		"interface": json.RawMessage(in), "target": srv.URL, "operations": []string{"put", "get"},
		"credential_handle": "store", "credential_placement": map[string]string{"query": "key"},
		"rules": []map[string]string{{"then": "allow"}}, "max_body_size": 1024,
		"billing_rule": map[string]any{"put": map[string]string{"cost": "0.0"}, "get": map[string]string{"cost": "0.0"}},
	})
	e, err := ParseEntry(raw)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	file := filepath.Join(dir, "body")
	os.WriteFile(file, []byte("uploaded"), 0o600)
	body, _ := os.Open(file)
	defer body.Close()
	c, err := e.NewCall("put", []byte(`{"name":"../a/b","kind":"text/plain"}`), true)
	if err != nil {
		t.Fatal(err)
	}
	res, err := executor("k").Execute(context.Background(), e, c, body, nil)
	if err != nil {
		t.Fatal(err)
	}
	if gotPath != "/files/..%2Fa%2Fb" || gotKey != "k" || gotType != "text/plain" || string(gotBody) != "uploaded" {
		t.Errorf("sent %s key %q type %q body %q", gotPath, gotKey, gotType, gotBody)
	}
	if res.Response["etag"] != `"v1"` || res.Response["status"] != json.Number("200") {
		t.Errorf("response %v", res.Response)
	}

	c, _ = e.NewCall("get", []byte(`{"name":".."}`), false)
	var out bytes.Buffer
	if _, err := executor("k").Execute(context.Background(), e, c, nil, &out); err != nil || out.String() != "downloaded" {
		t.Fatalf("download: %q %v", out.String(), err)
	}
	if gotPath != "/files/%2E%2E" {
		t.Errorf("a .. segment went as %s", gotPath)
	}

	c, _ = e.NewCall("get", []byte(`{"name":"moved"}`), false)
	res, err = executor("k").Execute(context.Background(), e, c, nil, io.Discard)
	if err != nil || res.Response["status"] != json.Number("302") {
		t.Errorf("a redirect: %v %v", res, err)
	}

	// A response body over max_body_size makes the call fail.
	e.MaxBodySize = 4
	c, _ = e.NewCall("get", []byte(`{"name":"x"}`), false)
	if _, err := executor("k").Execute(context.Background(), e, c, nil, io.Discard); !errors.Is(err, ErrUnreadable) {
		t.Errorf("a response over max_body_size: %v", err)
	}
}

// An entry whose rate_source the bucket does not have does not execute
// (implementation §8.5).
func TestExecuteWithoutTable(t *testing.T) {
	e, err := ParseEntry(openAIEntry(t, func(e map[string]any) { e["rate_source"] = "missing" }))
	if err != nil {
		t.Fatal(err)
	}
	c, _ := e.NewCall("create_response", []byte(`{"model":"gpt-5","input":[]}`), false)
	if _, err := executor("k").Execute(context.Background(), e, c, nil, nil); !errors.Is(err, ErrNoTable) {
		t.Errorf("got %v", err)
	}
}

func TestParseTable(t *testing.T) {
	if _, err := ParseTable([]byte(`{"as_of":"2026-10-01","max_per_call":2,"rates":{"gpt-5":{"input":0.1}}}`)); err != nil {
		t.Errorf("a valid table: %v", err)
	}
	for _, raw := range []string{
		`{"as_of":"October","max_per_call":2,"rates":{}}`,
		`{"as_of":"2026-10-01","max_per_call":-1,"rates":{}}`,
		`{"as_of":"2026-10-01","max_per_call":1,"rates":{"m":{"input":-0.1}}}`,
		`{"as_of":"2026-10-01","max_per_call":1,"price":3}`,
	} {
		if _, err := ParseTable([]byte(raw)); err == nil {
			t.Errorf("%s was accepted", raw)
		}
	}
}

// A credential placed in the query never reaches the error a failed call
// returns, and a header field cannot carry a control character.
func TestCredentialStaysOutOfErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {}))
	target := srv.URL
	srv.Close() // nothing listens: the call gets no response
	in := `{"protocol":"https","direction":"egress","operations":{
	  "get":{"fields":{"type":"object","properties":{"trace":{"type":"string"}}},
	    "https":{"method":"GET","path":"/x","fields":{"trace":{"in":"header","at":"X-Trace"}},"response":{}}}}}`
	raw, _ := json.Marshal(map[string]any{
		"interface": json.RawMessage(in), "target": target, "operations": []string{"get"},
		"credential_handle": "k", "credential_placement": map[string]string{"query": "api_key"},
		"rules": []map[string]string{{"then": "allow"}}, "billing_rule": map[string]any{"get": map[string]string{"cost": "0.0"}},
	})
	e, err := ParseEntry(raw)
	if err != nil {
		t.Fatal(err)
	}
	c, _ := e.NewCall("get", []byte(`{}`), false)
	_, err = executor("SECRET123").Execute(context.Background(), e, c, nil, nil)
	if !errors.Is(err, ErrNoResponse) || strings.Contains(err.Error(), "SECRET123") {
		t.Errorf("a call without response: %v", err)
	}
	c, _ = e.NewCall("get", []byte("{\"trace\":\"a\\r\\nX-Other: b\"}"), false)
	_, err = executor("SECRET123").Execute(context.Background(), e, c, nil, nil)
	if !errors.Is(err, ErrInvalid) || strings.Contains(err.Error(), "SECRET123") {
		t.Errorf("a header field with CRLF: %v", err)
	}
}

// A response that cannot be read whole was executed: it is charged max_per_call.
func TestUnreadableResponseCharges(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write(bytes.Repeat([]byte(" "), maxJSONResponse+1))
	}))
	defer srv.Close()
	e, err := ParseEntry(openAIEntry(t, func(e map[string]any) { e["target"] = srv.URL }))
	if err != nil {
		t.Fatal(err)
	}
	c, _ := e.NewCall("create_response", []byte(`{"model":"gpt-5","input":[]}`), false)
	res, err := executor("k").Execute(context.Background(), e, c, nil, nil)
	if !errors.Is(err, ErrUnreadable) || res == nil || res.Charge.Cost != 2.0 || res.Charge.BillingErrors != 1 {
		t.Errorf("got %+v, %v", res, err)
	}
}

// An empty body goes with Content-Length: 0.
func TestEmptyBody(t *testing.T) {
	var length int64 = -1
	var chunked bool
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		length, chunked = r.ContentLength, len(r.TransferEncoding) > 0
	}))
	defer srv.Close()
	in := `{"protocol":"https","direction":"egress","operations":{
	  "put":{"fields":{"type":"object","properties":{}},"body":{"request":true,"response":false},
	    "https":{"method":"PUT","path":"/f","fields":{},"response":{}}}}}`
	raw, _ := json.Marshal(map[string]any{
		"interface": json.RawMessage(in), "target": srv.URL, "operations": []string{"put"}, "max_body_size": 8,
		"rules": []map[string]string{{"then": "allow"}}, "billing_rule": map[string]any{"put": map[string]string{"cost": "0.0"}},
	})
	e, err := ParseEntry(raw)
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "empty")
	os.WriteFile(file, nil, 0o600)
	body, _ := os.Open(file)
	defer body.Close()
	c, _ := e.NewCall("put", []byte(`{}`), true)
	if _, err := executor("").Execute(context.Background(), e, c, body, nil); err != nil {
		t.Fatal(err)
	}
	if length != 0 || chunked {
		t.Errorf("an empty body went with length %d, chunked %v", length, chunked)
	}
}
