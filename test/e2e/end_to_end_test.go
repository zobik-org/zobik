//go:build e2e

package e2e

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"zobik.org/zobik/internal/console"
)

// De punta a punta: a request written in the panel comes back with its answer,
// with the link to its trace. The test walks the console's HTTP API as the
// panel calls it: a request answered by echo on intake is accepted; one that
// starts with "?" brings echo's query, which is answered, and the answer comes
// back in the outcome; the first request's trace is read through the console's
// port for Jaeger's UI, with the panel's session.
func TestEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	z := newNetwork(t, ctx)
	z.build(t, ctx, "images/zobik-sidecar.Dockerfile", "zobik-sidecar:dev")
	z.build(t, ctx, "images/zobik-python.Dockerfile", "zobik-python:dev")
	z.build(t, ctx, "examples/blueprints/echo/Dockerfile", "zobik-echo:dev")
	z.build(t, ctx, "test/e2e/embeddings/Dockerfile", embeddingsImage)
	link, _ := z.init(t, ctx)
	p := signIn(t, ctx, link)

	blueprint := filepath.Join(z.root, "examples/blueprints/echo/blueprint.json")
	raw, err := os.ReadFile(blueprint)
	if err != nil {
		t.Fatal(err)
	}
	var b struct {
		CapabilityText string `json:"capability_text"`
	}
	if err := json.Unmarshal(raw, &b); err != nil {
		t.Fatal(err)
	}
	// The requests' texts and echo's capability share a vector: similarity 1,
	// above the default task_similarity_floor.
	text, question := "repeat this", "?is it ready"
	same := []float32{1, 0}
	z.embeddings(t, ctx, map[string][]float32{documentPrefix + b.CapabilityText: same, queryPrefix + text: same, queryPrefix + question: same})

	z.run(t, ctx, credentials, "config", "set",
		embeddingsRegistry,
		`embedding_model={"entry":"embeddings","model":"fake","document_prefix":"`+documentPrefix+`","query_prefix":"`+queryPrefix+`","max_text_size":8192}`)
	z.run(t, ctx, credentials, "config", "set", "network_admission=open")
	if out := z.run(t, ctx, "", "node", "up", "--topic", "intake", blueprint); !strings.Contains(out, "provisioned node") {
		t.Fatalf("node up said: %s", out)
	}

	changed := p.events(t, ctx)
	var conv console.Conversation
	p.call(t, ctx, http.MethodPost, console.PathConversations, nil, http.StatusCreated, &conv)

	// The request comes back with echo's answer, which the person accepts.
	request := p.send(t, ctx, conv.ID, text)
	outcome := p.wait(t, ctx, changed, conv.ID, func(m *console.Message) bool {
		return m.Kind == console.KindOutcome && m.TraceID == request.TraceID && m.State == console.StateAwaiting
	})
	if outcome.Text != text || !slices.Equal(outcome.Options, []string{"accept", "reject"}) {
		t.Errorf("the outcome of %q: %+v", text, outcome)
	}
	p.accept(t, ctx, conv.ID, outcome)

	// A request that starts with "?" brings echo's query, and the answer comes
	// back in the outcome.
	asked := p.send(t, ctx, conv.ID, question)
	query := p.wait(t, ctx, changed, conv.ID, func(m *console.Message) bool {
		return m.Kind == console.KindQuery && m.TraceID == asked.TraceID && m.State == console.StateAwaiting
	})
	if !slices.Equal(query.Options, []string{"yes", "no"}) {
		t.Errorf("echo's query: %+v", query)
	}
	p.shown(t, ctx, query.TaskID)
	p.call(t, ctx, http.MethodPost, console.PathAnswers, console.Answer{TaskID: query.TaskID, Decision: "yes"}, http.StatusNoContent, nil)
	outcome = p.wait(t, ctx, changed, conv.ID, func(m *console.Message) bool {
		return m.Kind == console.KindOutcome && m.TraceID == asked.TraceID && m.State == console.StateAwaiting
	})
	if want := "is it ready\n→ yes"; outcome.Text != want {
		t.Errorf("the outcome of %q is %q, want %q", question, outcome.Text, want)
	}
	p.accept(t, ctx, conv.ID, outcome)

	// The link to the trace: Jaeger's UI on the console's port, behind the
	// session. Accepting closed the root, so its span reaches Jaeger with the
	// inbound subtask and the delivery hanging from it.
	var session console.SessionJSON
	p.call(t, ctx, http.MethodGet, console.PathSession, nil, http.StatusOK, &session)
	if session.TraceUI == "" {
		t.Fatal("the console serves no link to the traces")
	}
	spans := jaegerTrace(t, ctx, &jaegerClient{base: session.TraceUI, c: p.c}, request.TraceID, "root", "intake", "hitl_contact_user")
	root := spans["root"]
	if root.ParentSpanID != "" {
		t.Errorf("the root span has a parent: %+v", root)
	}
	for _, name := range []string{"intake", "hitl_contact_user"} {
		if s := spans[name]; s.ParentSpanID != root.SpanID {
			t.Errorf("the span %s hangs from %s, not from the root %s", name, s.ParentSpanID, root.SpanID)
		}
	}
	// The page the panel links to opens with the session, and only with it.
	page := session.TraceUI + "/trace/" + request.TraceID
	if code := status(t, ctx, p.c, page); code != http.StatusOK {
		t.Errorf("Jaeger's UI with the session answered %d, want %d", code, http.StatusOK)
	}
	if code := status(t, ctx, http.DefaultClient, page); code != http.StatusUnauthorized {
		t.Errorf("Jaeger's UI without a session answered %d, want %d", code, http.StatusUnauthorized)
	}
}

// panel is a browser on the panel: it holds the session's cookie in its jar.
type panel struct {
	base string
	c    *http.Client
}

// signIn spends the single-use link of zobik init, as the browser it opens.
func signIn(t *testing.T, ctx context.Context, link string) *panel {
	t.Helper()
	u, err := url.Parse(link)
	if err != nil {
		t.Fatal(err)
	}
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	p := &panel{base: u.Scheme + "://" + u.Host, c: &http.Client{Jar: jar}}
	if code := status(t, ctx, p.c, link); code != http.StatusOK {
		t.Fatalf("the panel's link led to %d", code)
	}
	return p
}

func status(t *testing.T, ctx context.Context, c *http.Client, u string) int {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

// call sends body as JSON, expects want and decodes the answer into out.
func (p *panel) call(t *testing.T, ctx context.Context, method, path string, body any, want int, out any) {
	t.Helper()
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, p.base+path, r)
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	p.do(t, req, want, out)
}

func (p *panel) do(t *testing.T, req *http.Request, want int, out any) {
	t.Helper()
	resp, err := p.c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != want {
		t.Fatalf("%s %s answered %s, want %d: %s", req.Method, req.URL.Path, resp.Status, want, raw)
	}
	if out != nil {
		if err := json.Unmarshal(raw, out); err != nil {
			t.Fatalf("%s %s: %v: %s", req.Method, req.URL.Path, err, raw)
		}
	}
}

// send writes a request in the conversation, as the panel's form posts it.
func (p *panel) send(t *testing.T, ctx context.Context, conv, text string) console.Message {
	t.Helper()
	var body bytes.Buffer
	form := multipart.NewWriter(&body)
	if err := form.WriteField("text", text); err != nil {
		t.Fatal(err)
	}
	form.Close()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.base+console.PathConversations+"/"+conv+"/messages", &body)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", form.FormDataContentType())
	var m console.Message
	p.do(t, req, http.StatusCreated, &m)
	if m.Kind != console.KindRequest || m.TraceID == "" {
		t.Fatalf("sending %q: %+v", text, m)
	}
	return m
}

// events opens the event stream and returns the ids of the conversations it
// says changed.
func (p *panel) events(t *testing.T, ctx context.Context) <-chan string {
	t.Helper()
	ctx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.base+console.PathEvents, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := p.c.Do(req) //nolint:bodyclose // the stream's goroutine closes it
	if err != nil {
		t.Fatal(err)
	}
	r := bufio.NewReader(resp.Body)
	if l, err := r.ReadString('\n'); resp.StatusCode != http.StatusOK || err != nil || !strings.HasPrefix(l, ":") {
		resp.Body.Close()
		t.Fatalf("the event stream did not open: %s %q %v", resp.Status, l, err)
	}
	changed := make(chan string, 64)
	go func() {
		defer close(changed)
		defer resp.Body.Close()
		var event string
		for {
			l, err := r.ReadString('\n')
			if err != nil {
				return
			}
			l = strings.TrimRight(l, "\r\n")
			switch {
			case strings.HasPrefix(l, "event: "):
				event = strings.TrimPrefix(l, "event: ")
			case strings.HasPrefix(l, "data: ") && event == "conversation":
				var d struct {
					ID string `json:"id"`
				}
				if json.Unmarshal([]byte(strings.TrimPrefix(l, "data: ")), &d) == nil {
					select {
					case changed <- d.ID:
					default:
					}
				}
			case l == "":
				event = ""
			}
		}
	}()
	return changed
}

// wait reads the conversation, again each time the event stream says it
// changed, until one of its messages satisfies match, and returns it. Those
// reads are the panel's own, after an event, so they are not activity; a
// periodic read covers an event the stream dropped.
func (p *panel) wait(t *testing.T, ctx context.Context, changed <-chan string, conv string, match func(*console.Message) bool) *console.Message {
	t.Helper()
	timeout := time.After(2 * time.Minute)
	for {
		c := p.conversation(t, ctx, conv, true)
		for _, m := range c.Messages {
			if match(m) {
				return m
			}
		}
		for again := false; !again; {
			select {
			case id, ok := <-changed:
				if !ok {
					t.Fatal("the event stream ended")
				}
				again = id == conv
			case <-time.After(10 * time.Second):
				again = true
			case <-timeout:
				t.Fatalf("the conversation never had the message awaited: %s", mustJSON(c))
			case <-ctx.Done():
				t.Fatalf("waiting on the conversation: %v", ctx.Err())
			}
		}
	}
}

// conversation reads a conversation; refresh marks the read as one the panel
// makes on its own.
func (p *panel) conversation(t *testing.T, ctx context.Context, conv string, refresh bool) console.Conversation {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.base+console.PathConversations+"/"+conv, nil)
	if err != nil {
		t.Fatal(err)
	}
	if refresh {
		req.Header.Set(console.HeaderRefresh, "1")
	}
	var c console.Conversation
	p.do(t, req, http.StatusOK, &c)
	return c
}

func mustJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// shown tells the console the panel showed the task, as the panel does when
// it renders it.
func (p *panel) shown(t *testing.T, ctx context.Context, task string) {
	t.Helper()
	p.call(t, ctx, http.MethodPost, console.PathShown, console.TaskIDs{TaskIDs: []string{task}}, http.StatusNoContent, nil)
}

// accept shows the outcome and accepts it, and the conversation records the
// answer.
func (p *panel) accept(t *testing.T, ctx context.Context, conv string, outcome *console.Message) {
	t.Helper()
	p.shown(t, ctx, outcome.TaskID)
	p.call(t, ctx, http.MethodPost, console.PathAnswers, console.Answer{TaskID: outcome.TaskID, Decision: "accept"}, http.StatusNoContent, nil)
	c := p.conversation(t, ctx, conv, false)
	for _, m := range c.Messages {
		if m.ID == outcome.ID && m.State != console.StateAnswered {
			t.Errorf("the accepted outcome is %s", m.State)
		}
	}
}
