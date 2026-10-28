package channel_test

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/egress"
	"zobik.org/zobik/internal/embedding/embeddingtest"
	"zobik.org/zobik/internal/globalconfig"
	"zobik.org/zobik/internal/role"
	"zobik.org/zobik/internal/role/channel"
	"zobik.org/zobik/internal/role/config"
	"zobik.org/zobik/internal/role/taskbroker"
	"zobik.org/zobik/internal/testnet"
	channelv1 "zobik.org/zobik/proto/zobik/channel/v1"
	nriv1 "zobik.org/zobik/proto/zobik/nri/v1"
)

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

// entryFixture is a network, with an open vector space if it has one, the Config Store, the
// Task Broker and the Entry Node running, and the console played by the test.
type entryFixture struct {
	*fixture
	ep       *endpoint
	intake   chan *nats.Msg
	stop     func()
	listen   string
	worker   *nats.Conn
	maxBytes int
}

func newEntryFixture(t *testing.T, space bool) *entryFixture {
	ep := newEndpoint(t)
	const maxBytes = 64
	n := testnet.New(t, func(v globalconfig.Values) {
		testnet.Set(v, map[string]any{"auction_timeout": 200})
		if space {
			testnet.Set(v, map[string]any{
				"network_admission": "open",
				"egress_registry":   map[string]any{"embeddings": egress.EmbeddingsEntry(ep.URL)},
				"embedding_model":   map[string]any{"entry": "embeddings", "model": "test", "document_prefix": "", "query_prefix": "q: ", "max_text_size": maxBytes},
			})
		}
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	n.Start("config", func(ctx context.Context, rt *role.Runtime, nc *nats.Conn) error {
		return config.Run(ctx, rt, nc, config.Options{ContextAddr: n.ContextAddr})
	})
	n.Start("task_broker", taskbroker.Run)
	id := n.ID("channel_user")
	f := &entryFixture{
		fixture: &fixture{Net: n, addr: addr, channel: id, closes: n.Tap("task.*." + id + ".>")},
		ep:      ep, intake: n.Tap(bus.SubjectAnnounced("intake")), listen: addr,
		worker: n.Conn(bus.NodeScope.Role, nil), maxBytes: maxBytes,
	}
	f.start()
	return f
}

// start brings up the Entry Node over its volume.
func (f *entryFixture) start() {
	_, f.stop = f.StartStoppable("channel_user", func(ctx context.Context, rt *role.Runtime, nc *nats.Conn) error {
		return channel.Run(ctx, rt, nc, channel.Options{Audience: channel.AudienceUser, Listen: f.listen, ContextAddr: f.ContextAddr})
	})
}

// open opens a request with data and returns its ack.
func (f *entryFixture) open(c channelv1.ChannelClient, data string) (*nriv1.OpenAck, error) {
	f.T.Helper()
	s, err := c.Open(f.Ctx)
	if err != nil {
		f.T.Fatal(err)
	}
	s.Send(&channelv1.OpenTrace{Part: &channelv1.OpenTrace_Header{Header: &channelv1.OpenHeader{Topic: "intake", Data: []byte(data)}}})
	return s.CloseAndRecv()
}

// request opens a request and returns its TraceID and its entry subtask's TaskID.
func (f *entryFixture) request(c channelv1.ChannelClient, text string) (string, string) {
	f.T.Helper()
	ack, err := f.open(c, `{"task_text":"`+text+`"}`)
	if err != nil {
		f.T.Fatal(err)
	}
	return ack.GetTraceId(), ack.GetEntryTaskId()
}

// announced waits for an announcement on intake that match accepts: the Task
// Broker re-announces those without a candidate.
func (f *entryFixture) announced(within time.Duration, match func(*bus.Event) bool) *bus.Event {
	f.T.Helper()
	timeout := time.After(within)
	for {
		select {
		case m := <-f.intake:
			var e bus.Event
			json.Unmarshal(m.Data, &e)
			if match(&e) {
				return &e
			}
		case <-timeout:
			return nil
		}
	}
}

func subject(id string) func(*bus.Event) bool {
	return func(e *bus.Event) bool { return e.Subject == id }
}

// complete closes the entry subtask taskID as the node that resolved it.
func (f *entryFixture) complete(taskID, traceparent string) {
	f.T.Helper()
	e, err := bus.NewEvent(bus.TypeCompleted, "worker", taskID, map[string]any{
		"topic": "intake", "metrics": map[string]any{"tokens_in": 0, "tokens_out": 0, "cost": 0, "duration_ms": 1},
	})
	if err != nil {
		f.T.Fatal(err)
	}
	e.TraceParent, e.ConfigVersion = traceparent, "v0"
	if err := bus.Publish(f.Ctx, f.JetStream(f.worker), bus.SubjectTerminal(bus.TypeCompleted, f.channel, "intake"), e); err != nil {
		f.T.Fatal(err)
	}
}

func dataOf(t *testing.T, e *bus.Event) map[string]any {
	t.Helper()
	var d map[string]any
	if err := json.Unmarshal(e.Data, &d); err != nil {
		t.Fatal(err)
	}
	return d
}

// The announcement on intake carries the task_embedding of its task_text with
// its model_id, and Open rejects what no retry would embed (implementation §6,
// The service between the console and the channels).
func TestIntake(t *testing.T) {
	f := newEntryFixture(t, true)
	c := f.client(f.ConsoleKey)
	traceID, entry := f.request(c, "find it")
	e := f.announced(5*time.Second, subject(entry))
	if e == nil {
		t.Fatal("the entry subtask was not announced")
	}
	d := dataOf(t, e)
	if bus.TraceID(e.TraceParent) != traceID || d["origin_audience"] != "user" || d["requester_identity"] != f.channel ||
		d["task_embedding"] == nil || d["model_id"] == nil || d["current_attempts"] != nil {
		t.Errorf("announced %s in %s: %s", e.Subject, e.TraceParent, e.Data)
	}
	if r := f.ep.Requests(); len(r) != 1 || r[0].Input != "q: find it" {
		t.Errorf("embedded %v", r)
	}

	for name, data := range map[string]string{
		"no task_text":                      `{}`,
		"a task_text longer than its space": `{"task_text":"` + strings.Repeat("x", f.maxBytes) + `"}`,
		"a task_embedding":                  `{"task_text":"x","task_embedding":"AAAA"}`,
		"a model_id":                        `{"task_text":"x","model_id":"m"}`,
		"invalid UTF-8":                     "{\"task_text\":\"\xff\"}",
	} {
		if _, err := f.open(c, data); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// With the embeddings endpoint down, Open returns and the announcement waits
// in the role's volume, across a restart, until the endpoint answers.
func TestIntakeWaits(t *testing.T) {
	f := newEntryFixture(t, true)
	f.ep.down.Store(true)
	_, entry := f.request(f.client(f.ConsoleKey), "find it")
	if e := f.announced(time.Second, subject(entry)); e != nil {
		t.Fatal("announced with the endpoint down")
	}
	f.stop()
	f.start()
	if e := f.announced(time.Second, subject(entry)); e != nil {
		t.Fatal("announced after the restart with the endpoint down")
	}
	f.ep.down.Store(false)
	if e := f.announced(10*time.Second, subject(entry)); e == nil {
		t.Error("the announcement did not go out when the endpoint came back")
	}
}

// A reject of the outcome reopens the entry subtask with its reason, and the
// new attempt's outcome is delivered again; a reject without its reason is
// silence, and the root closes (architecture §3.5.4).
func TestReject(t *testing.T) {
	f := newEntryFixture(t, true)
	c := f.client(f.ConsoleKey)
	session, err := c.Attach(f.Ctx)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	traceID, entry := f.request(c, "find it")
	first := f.announced(5*time.Second, subject(entry))
	if first == nil {
		t.Fatal("the entry subtask was not announced")
	}

	f.complete(entry, first.TraceParent)
	q := asConsultation(t, receive(t, session))
	if q.Data.Kind != "result_delivery" || strings.Join(q.Data.Options, ",") != "accept,reject" {
		t.Fatalf("delivery %+v", q.Data)
	}
	if _, err := c.Answer(f.Ctx, &channelv1.AnswerRequest{TaskId: q.TaskID, Data: []byte(`{"decision":"reject","failure_reason":"it is not it"}`)}); err != nil {
		t.Fatal(err)
	}
	again := f.announced(5*time.Second, func(e *bus.Event) bool { return dataOf(t, e)["previous_attempt"] == entry })
	if again == nil {
		t.Fatal("the entry subtask was not reopened")
	}
	d, before := dataOf(t, again), dataOf(t, first)
	if d["failure_reason"] != "it is not it" || d["current_attempts"] != 2.0 || d["task_text"] != "find it" ||
		d["task_embedding"] != before["task_embedding"] || again.TraceParent != first.TraceParent {
		t.Errorf("reopened %s: %s", again.Subject, again.Data)
	}
	// The same text in the same space reuses its vector (architecture §3.7.4).
	if r := f.ep.Requests(); len(r) != 1 {
		t.Errorf("embedded %d times", len(r))
	}

	f.complete(again.Subject, again.TraceParent)
	q = asConsultation(t, receive(t, session))
	if q.Data.Details["type"] != bus.TypeCompleted {
		t.Fatalf("second delivery %+v", q.Data)
	}
	if _, err := c.Answer(f.Ctx, &channelv1.AnswerRequest{TaskId: q.TaskID, Data: []byte(`{"decision":"reject"}`)}); err != nil {
		t.Fatal(err)
	}
	if e := f.rootClose(traceID, 5*time.Second); e == nil || e.Type != bus.TypeCompleted {
		t.Errorf("root close %+v", e)
	}
}

// The role keeps a closed root's token for the purge until
// context_retention_grace expires (architecture §3.6.1, §3.4), and an expired
// carries its query's trace (implementation §6, The service between the
// console and the channels).
func TestPurgeAfterCloseAndExpired(t *testing.T) {
	f := newEntryFixture(t, true)
	c := f.client(f.ConsoleKey)
	ctx, detach := context.WithCancel(f.Ctx)
	session, err := c.Attach(ctx)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)

	traceID, entry := f.request(c, "find it")
	e := f.announced(5*time.Second, subject(entry))
	if e == nil {
		t.Fatal("the entry subtask was not announced")
	}
	f.complete(entry, e.TraceParent)
	q := asConsultation(t, receive(t, session))
	if _, err := c.Answer(f.Ctx, &channelv1.AnswerRequest{TaskId: q.TaskID, Data: []byte(`{"decision":"accept"}`)}); err != nil {
		t.Fatal(err)
	}
	if e := f.rootClose(traceID, 5*time.Second); e == nil {
		t.Fatal("the root did not close")
	}
	if _, err := c.Purge(f.Ctx, &nriv1.PurgeRequest{TaskId: traceID}); err != nil {
		t.Errorf("purge after the close: %v", err)
	}

	// A delivery the console did not show expires undelivered when the session closes.
	traceID, entry = f.request(c, "find it again")
	if e = f.announced(5*time.Second, subject(entry)); e == nil {
		t.Fatal("the second entry subtask was not announced")
	}
	f.complete(entry, e.TraceParent)
	q = asConsultation(t, receive(t, session))
	detach()
	time.Sleep(200 * time.Millisecond)
	session, err = c.Attach(f.Ctx)
	if err != nil {
		t.Fatal(err)
	}
	var expired, retaken bool
	for !expired || !retaken {
		m := receive(t, session)
		if ex := m.GetExpired(); ex != nil {
			if ex.GetTaskId() != q.TaskID || ex.GetTraceId() != traceID || ex.GetFailureCode() != "undelivered" {
				t.Errorf("expired %v", ex)
			}
			expired = true
			continue
		}
		// Until the Spawner closes vacant rounds, the panel's channel takes the
		// delivery's next round when the console is back.
		if r := asConsultation(t, m); r.TaskID != q.TaskID && r.Data.Kind == "result_delivery" {
			retaken = true
		}
	}
}

// Release lets go a query as the session closing does: unanswered once shown,
// and the delivery's silence closes the root (implementation §6, The service
// between the console and the channels).
func TestRelease(t *testing.T) {
	f := newEntryFixture(t, true)
	c := f.client(f.ConsoleKey)
	session, err := c.Attach(f.Ctx)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	traceID, entry := f.request(c, "find it")
	e := f.announced(5*time.Second, subject(entry))
	if e == nil {
		t.Fatal("the entry subtask was not announced")
	}
	f.complete(entry, e.TraceParent)
	q := asConsultation(t, receive(t, session))
	for _, m := range []*channelv1.ConsoleMessage{
		{Body: &channelv1.ConsoleMessage_Shown{Shown: &channelv1.Shown{TaskId: q.TaskID}}},
		{Body: &channelv1.ConsoleMessage_Release{Release: &channelv1.Release{TaskId: q.TaskID}}},
	} {
		if err := session.Send(m); err != nil {
			t.Fatal(err)
		}
	}
	ex := receive(t, session).GetExpired()
	if ex == nil || ex.GetTaskId() != q.TaskID || ex.GetFailureCode() != "unanswered" {
		t.Errorf("expired %v", ex)
	}
	if e := f.rootClose(traceID, 5*time.Second); e == nil || e.Type != bus.TypeCompleted {
		t.Errorf("root close %+v", e)
	}
}

// Without a vector space nothing bounds or embeds the request's text, so Open
// on intake fails instead of waiting (implementation §6, The service between
// the console and the channels).
func TestIntakeWithoutSpace(t *testing.T) {
	f := newEntryFixture(t, false)
	if _, err := f.open(f.client(f.ConsoleKey), `{"task_text":"find it"}`); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("Open without a vector space: %v", err)
	}
}
