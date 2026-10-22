package console_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/console"
	"zobik.org/zobik/internal/embedding/embeddingtest"
	"zobik.org/zobik/internal/globalconfig"
	"zobik.org/zobik/internal/role"
	"zobik.org/zobik/internal/role/channel"
	"zobik.org/zobik/internal/role/config"
	"zobik.org/zobik/internal/role/taskbroker"
	"zobik.org/zobik/internal/signing"
	"zobik.org/zobik/internal/testnet"
	contextv1 "zobik.org/zobik/proto/zobik/context/v1"
	nriv1 "zobik.org/zobik/proto/zobik/nri/v1"
)

// workFixture is the console's service with its work area against a network
// with the Config Store, the Task Broker and the Entry Node running; the test
// plays the node that resolves intake.
type workFixture struct {
	*testnet.Net
	base, dir string
	channel   string
	cookie    *http.Cookie
	intake    chan *nats.Msg
	closes    chan *nats.Msg
	worker    *nats.Conn
}

func newWorkFixture(t *testing.T) *workFixture {
	ep := embeddingtest.NewServer(t)
	n := testnet.New(t, func(v globalconfig.Values) {
		testnet.Set(v, map[string]any{
			"auction_timeout":   200,
			"network_admission": "open",
			"egress_registry":   map[string]any{"embeddings": map[string]any{"target": ep.URL}},
			"embedding_model":   map[string]any{"entry": "embeddings", "model": "test", "document_prefix": "", "query_prefix": "", "max_text_size": 256},
		})
	})
	addr := freeAddr(t)
	n.Start("config", func(ctx context.Context, rt *role.Runtime, nc *nats.Conn) error {
		return config.Run(ctx, rt, nc, config.Options{ContextAddr: n.ContextAddr})
	})
	n.Start("task_broker", taskbroker.Run)
	n.Start("channel_user", func(ctx context.Context, rt *role.Runtime, nc *nats.Conn) error {
		return channel.Run(ctx, rt, nc, channel.Options{Audience: channel.AudienceUser, Listen: addr, ContextAddr: n.ContextAddr})
	})
	id := n.ID("channel_user")
	f := &workFixture{
		Net: n, dir: t.TempDir(), channel: id,
		intake: n.Tap(bus.SubjectAnnounced("intake")), closes: n.Tap("task.*." + id + ".>"),
		worker: n.Conn(bus.NodeScope.Role, nil),
	}
	key := bytes.Repeat([]byte{7}, 32)
	svc := console.New(console.Options{
		Open: func(u string, pw []byte) (*console.Keys, error) {
			if u != user || string(pw) != password {
				return nil, errors.New("wrong")
			}
			pub, _ := n.ConsoleKey.PublicKey()
			return &console.Keys{
				Root: n.Root, Console: n.ConsoleKey, Conversations: key,
				JWT: func(exp time.Time) (string, error) {
					return bus.IssueUser(n.Account.SigningKeys[bus.ConsoleScope.Role], n.Account.Public, pub, bus.ConsoleScope.Role, exp)
				},
				Close: func() {},
			}, nil
		},
		Channels:         []console.Channel{{Audience: console.AudienceUser, ID: id, Addr: addr}},
		IdleTimeout:      time.Minute,
		ConversationsDir: f.dir,
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f.base = "http://" + ln.Addr().String()
	ctx, cancel := context.WithCancel(n.Ctx)
	done := make(chan struct{})
	go func() { defer close(done); svc.Serve(ctx, ln) }()
	t.Cleanup(func() { cancel(); <-done })
	return f
}

func (f *workFixture) signIn() {
	f.T.Helper()
	b, _ := json.Marshal(console.Credentials{Username: user, Password: password})
	resp, err := http.Post(f.base+console.PathSession, "application/json", bytes.NewReader(b))
	if err != nil {
		f.T.Fatal(err)
	}
	resp.Body.Close()
	for _, c := range resp.Cookies() {
		if c.Name == console.CookieSession {
			f.cookie = c
		}
	}
	if f.cookie == nil {
		f.T.Fatalf("sign-in: %s", resp.Status)
	}
}

// call calls the API and decodes the response into out, if there is one.
func (f *workFixture) call(method, path, contentType string, body []byte, out any) int {
	f.T.Helper()
	req, _ := http.NewRequestWithContext(f.Ctx, method, f.base+path, bytes.NewReader(body))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	req.AddCookie(f.cookie)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		f.T.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if out != nil && resp.StatusCode < 300 {
		if err := json.Unmarshal(b, out); err != nil {
			f.T.Fatalf("%s %s: %v: %s", method, path, err, b)
		}
	}
	return resp.StatusCode
}

func (f *workFixture) callJSON(method, path string, in, out any) int {
	f.T.Helper()
	b, _ := json.Marshal(in)
	return f.call(method, path, "application/json", b, out)
}

// send sends a request with its attachments, name → content.
func (f *workFixture) send(conv, text string, files map[string]string) (*console.Message, int) {
	f.T.Helper()
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	w.WriteField("text", text)
	for name, content := range files {
		p, _ := w.CreateFormFile("file", name)
		p.Write([]byte(content))
	}
	w.Close()
	var m console.Message
	code := f.call(http.MethodPost, console.PathConversations+"/"+conv+"/messages", w.FormDataContentType(), buf.Bytes(), &m)
	return &m, code
}

// announced waits for the announcement of the entry subtask taskID.
func (f *workFixture) announced(match func(*bus.Event) bool) *bus.Event {
	f.T.Helper()
	timeout := time.After(10 * time.Second)
	for {
		select {
		case m := <-f.intake:
			var e bus.Event
			json.Unmarshal(m.Data, &e)
			if match(&e) {
				return &e
			}
		case <-timeout:
			f.T.Fatal("no announcement on intake")
		}
	}
}

// complete closes the entry subtask as the node that resolved it.
func (f *workFixture) complete(e *bus.Event, typ string) {
	f.T.Helper()
	data := map[string]any{"topic": "intake", "metrics": map[string]any{"tokens_in": 0, "tokens_out": 0, "cost": 0, "duration_ms": 1}}
	if typ == bus.TypeFailed {
		data["failure_reason"] = "nothing found"
	}
	ev, err := bus.NewEvent(typ, "worker", e.Subject, data)
	if err != nil {
		f.T.Fatal(err)
	}
	ev.TraceParent, ev.ConfigVersion = e.TraceParent, "v0"
	if err := bus.Publish(f.Ctx, f.JetStream(f.worker), bus.SubjectTerminal(typ, f.channel, "intake"), ev); err != nil {
		f.T.Fatal(err)
	}
}

// until reads the conversation until ok accepts it.
func (f *workFixture) until(conv string, ok func(c *console.Conversation) bool) *console.Conversation {
	f.T.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var c console.Conversation
		f.callJSON(http.MethodGet, console.PathConversations+"/"+conv, nil, &c)
		if ok(&c) {
			return &c
		}
		if time.Now().After(deadline) {
			b, _ := json.Marshal(c)
			f.T.Fatalf("conversation %s", b)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func last(c *console.Conversation, kind string) *console.Message {
	for i := len(c.Messages) - 1; i >= 0; i-- {
		if c.Messages[i].Kind == kind {
			return c.Messages[i]
		}
	}
	return nil
}

type bodyOf struct {
	Name    string `json:"name"`
	Context bool   `json:"context"`
	Ref     struct {
		TraceID string `json:"trace_id"`
		TaskID  string `json:"task_id"`
		Name    string `json:"name"`
	} `json:"ref"`
}

func bodies(t *testing.T, e *bus.Event) []bodyOf {
	t.Helper()
	var d struct {
		Bodies []bodyOf `json:"bodies"`
	}
	json.Unmarshal(e.Data, &d)
	return d.Bodies
}

// The work area opens a request with its attachments and the conversation as
// context, shows the outcome to accept or reject, keeps the conversation
// encrypted, and deleting it purges its traces and receives the outcome still
// on its way without showing it (implementation §6, The operator console).
func TestWorkArea(t *testing.T) {
	f := newWorkFixture(t)
	f.signIn()
	var conv console.Conversation
	if code := f.callJSON(http.MethodPost, console.PathConversations, nil, &conv); code != http.StatusCreated {
		t.Fatalf("create: %d", code)
	}

	first, code := f.send(conv.ID, "find the report", map[string]string{"notes.txt": "it is blue"})
	if code != http.StatusCreated {
		t.Fatalf("send: %d", code)
	}
	e := f.announced(func(e *bus.Event) bool { return e.Subject == first.EntryTaskID })
	if b := bodies(t, e); len(b) != 1 || b[0].Name != "notes.txt" || b[0].Context {
		t.Errorf("the request's bodies %+v", b)
	}

	// The outcome waits for the person, who rejects it with a reason.
	f.complete(e, bus.TypeFailed)
	c := f.until(conv.ID, func(c *console.Conversation) bool { return last(c, console.KindOutcome) != nil })
	out := last(c, console.KindOutcome)
	if out.State != console.StateAwaiting || out.FailureReason != "nothing found" || strings.Join(out.Options, ",") != "accept,reject" {
		t.Fatalf("outcome %+v", out)
	}
	f.callJSON(http.MethodPost, console.PathShown, console.TaskIDs{TaskIDs: []string{out.TaskID}}, nil)
	if code := f.callJSON(http.MethodPost, console.PathAnswers, console.Answer{TaskID: out.TaskID, Decision: "reject"}, nil); code != http.StatusUnprocessableEntity {
		t.Errorf("a reject without its reason: %d", code)
	}
	if code := f.callJSON(http.MethodPost, console.PathAnswers, console.Answer{TaskID: out.TaskID, Decision: "reject", FailureReason: "look again"}, nil); code != http.StatusNoContent {
		t.Fatalf("reject: %d", code)
	}
	e = f.announced(func(e *bus.Event) bool { return bus.TraceID(e.TraceParent) == first.TraceID })
	f.complete(e, bus.TypeCompleted)
	c = f.until(conv.ID, func(c *console.Conversation) bool {
		m := last(c, console.KindOutcome)
		return m != nil && m.State == console.StateAwaiting && m.Outcome == bus.TypeCompleted
	})
	out = last(c, console.KindOutcome)
	if code := f.callJSON(http.MethodPost, console.PathAnswers, console.Answer{TaskID: out.TaskID, Decision: "accept"}, nil); code != http.StatusNoContent {
		t.Fatalf("accept: %d", code)
	}
	if e := rootClose(t, f.closes, first.TraceID); e.Type != bus.TypeCompleted {
		t.Errorf("root close %s", e.Type)
	}

	// The next request carries the conversation as context, from the oldest
	// message to the newest, each followed by its attachments and each answer
	// right after what it answers.
	second, _ := f.send(conv.ID, "and the other one", nil)
	e = f.announced(func(e *bus.Event) bool { return e.Subject == second.EntryTaskID })
	var got []string
	for _, b := range bodies(t, e) {
		if !b.Context {
			t.Errorf("a request without attachments sends %s", b.Name)
		}
		got = append(got, b.Name)
	}
	if want := "message.json,notes.txt,message.json,message.json,message.json,message.json"; strings.Join(got, ",") != want {
		t.Fatalf("context %s, want %s", strings.Join(got, ","), want)
	}
	var msgs []string
	for _, b := range bodies(t, e) {
		if b.Name == "message.json" {
			msgs = append(msgs, f.readBody(e, b))
		}
	}
	want := []string{
		`{"from":"person","text":"find the report","attachments":["notes.txt"]}`,
		`{"from":"network","text":"nothing found","attachments":[]}`,
		`{"from":"person","text":"look again","decision":"reject","attachments":[]}`,
		`{"from":"network","text":"","attachments":[]}`,
		`{"from":"person","text":"","decision":"accept","attachments":[]}`,
	}
	if strings.Join(msgs, "\n") != strings.Join(want, "\n") {
		t.Errorf("context messages\n%s\nwant\n%s", strings.Join(msgs, "\n"), strings.Join(want, "\n"))
	}

	// Nothing of the conversation is on disk in the clear.
	filepath.Walk(f.dir, func(p string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			if b, _ := os.ReadFile(p); bytes.Contains(b, []byte("find the report")) || bytes.Contains(b, []byte("it is blue")) {
				t.Errorf("%s is in the clear", p)
			}
		}
		return nil
	})

	// A new session reads the conversation back.
	f.callJSON(http.MethodDelete, console.PathSession, nil, nil)
	f.signIn()
	f.until(conv.ID, func(c *console.Conversation) bool { return len(c.Messages) == 4 })

	// Deleting the conversation with the second request on its way: its outcome
	// is received without being shown and the root closes.
	if code := f.callJSON(http.MethodDelete, console.PathConversations+"/"+conv.ID, nil, nil); code != http.StatusNoContent {
		t.Fatalf("delete: %d", code)
	}
	f.complete(e, bus.TypeCompleted)
	if e := rootClose(t, f.closes, second.TraceID); e.Type != bus.TypeCompleted {
		t.Errorf("root close of the deleted conversation %s", e.Type)
	}
	var list []console.Summary
	if f.callJSON(http.MethodGet, console.PathConversations, nil, &list); len(list) != 0 {
		t.Errorf("conversations %+v", list)
	}
}

func rootClose(t *testing.T, closes chan *nats.Msg, traceID string) *bus.Event {
	t.Helper()
	timeout := time.After(10 * time.Second)
	for {
		select {
		case m := <-closes:
			var e bus.Event
			json.Unmarshal(m.Data, &e)
			if e.Subject == traceID {
				return &e
			}
		case <-timeout:
			t.Fatalf("the root %s did not close", traceID)
		}
	}
}

// Deleting a conversation whose outcome waits for the person receives it
// without showing it, so the root closes then and not when the response
// window expires (implementation §6, The service between the console and the channels).
func TestDeleteAwaiting(t *testing.T) {
	f := newWorkFixture(t)
	f.signIn()
	var conv console.Conversation
	f.callJSON(http.MethodPost, console.PathConversations, nil, &conv)
	req, _ := f.send(conv.ID, "find the report", nil)
	f.complete(f.announced(func(e *bus.Event) bool { return e.Subject == req.EntryTaskID }), bus.TypeCompleted)
	f.until(conv.ID, func(c *console.Conversation) bool {
		m := last(c, console.KindOutcome)
		return m != nil && m.State == console.StateAwaiting
	})
	if code := f.callJSON(http.MethodDelete, console.PathConversations+"/"+conv.ID, nil, nil); code != http.StatusNoContent {
		t.Fatalf("delete: %d", code)
	}
	if e := rootClose(t, f.closes, req.TraceID); e.Type != bus.TypeCompleted {
		t.Errorf("root close %s", e.Type)
	}
}

// readBody reads a body of e from the Context Store, with a token of its root
// as the user channel mints it.
func (f *workFixture) readBody(e *bus.Event, b bodyOf) string {
	f.T.Helper()
	ch := f.Identity("channel_user")
	trace := bus.TraceID(e.TraceParent)
	token := f.Sign(ch, signing.TypeScopeToken, signing.ScopeToken{Sub: f.channel, TraceID: trace, Section: trace}, f.Credential("channel_user"))
	r, err := f.ContextClient(bus.ChannelScope.Role, ch).Read(f.Ctx, &contextv1.StoreRead{
		Token: token, Ref: &nriv1.ContextRef{TraceId: b.Ref.TraceID, TaskId: b.Ref.TaskID, Name: b.Ref.Name},
	})
	if err != nil {
		f.T.Fatal(err)
	}
	var out []byte
	for {
		c, err := r.Recv()
		if err != nil {
			break
		}
		out = append(out, c.GetData()...)
	}
	return string(out)
}
