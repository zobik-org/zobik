package sidecar_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/egress"
	"zobik.org/zobik/internal/embedding"
	"zobik.org/zobik/internal/globalconfig"
	"zobik.org/zobik/internal/role/taskbroker"
	"zobik.org/zobik/internal/sidecar"
	"zobik.org/zobik/internal/signing"
	"zobik.org/zobik/internal/testnet"
	nriv1 "zobik.org/zobik/proto/zobik/nri/v1"
)

const (
	topic   = "echo"
	modelID = "test-space"
	// auction is auction_timeout and lease task_lease_ttl, shortened for a test.
	auction = 300 * time.Millisecond
	lease   = time.Second
)

// The vectors: the node's capability, a task near it and one far from it.
var (
	capability = []float32{1, 0, 0, 0}
	near       = []float32{0.8, 0.6, 0, 0}
	far        = []float32{0, 1, 0, 0}
)

// fixture is a network with one node: its Integration Sidecar runs in the
// test, and the test plays its Logic Container and, unless the Task Broker
// runs, the Task Broker.
type fixture struct {
	*testnet.Net
	t         *testing.T
	id        string
	topic     string
	requester string
	socket    string
	// run is Run's result.
	run chan error
	// broker publishes as the Task Broker when it does not run.
	broker *nats.Conn
	pub    *nats.Conn

	startup             *sidecar.Startup
	path, shared, state string
	speaks              sidecar.Range
	nc                  *nats.Conn
	stop                context.CancelFunc
}

// start runs the Integration Sidecar until the test ends or stop.
func (f *fixture) start() {
	ctx, stop := context.WithCancel(f.Ctx)
	run := make(chan error, 1)
	f.run, f.stop = run, stop
	go func() {
		run <- sidecar.Run(ctx, f.nc, f.startup, sidecar.Options{
			Shared: f.shared, State: f.state, Speaks: f.speaks, StartupPath: f.path, ContextAddr: f.ContextAddr,
		})
	}()
	f.t.Cleanup(func() {
		stop()
		select {
		case err := <-run:
			run <- err
		case <-time.After(10 * time.Second):
			f.t.Error("the Integration Sidecar did not stop")
		}
	})
}

// restart stops the Integration Sidecar and runs it again on the same volumes.
func (f *fixture) restart() {
	f.t.Helper()
	f.stop()
	select {
	case err := <-f.run:
		f.run <- err
	case <-time.After(10 * time.Second):
		f.t.Fatal("the Integration Sidecar did not stop")
	}
	f.start()
}

type options struct {
	broker  bool // run the Task Broker
	cap     signing.Capability
	version string
	speaks  sidecar.Range
	// embeddings is the target of the embeddings endpoint, if the network has
	// a vector space.
	embeddings string
}

func newFixture(t *testing.T, o options) *fixture {
	t.Helper()
	n := testnet.New(t, func(v globalconfig.Values) {
		testnet.Set(v, map[string]any{
			"auction_timeout": auction.Milliseconds(), "task_lease_ttl": lease.Milliseconds(),
			"reannounce_backoff_base": 600, "max_reannounce_backoff": 2400, "network_admission": "open",
		})
		if o.embeddings != "" {
			testnet.Set(v, map[string]any{
				"egress_registry": map[string]any{"embeddings": egress.EmbeddingsEntry(o.embeddings)},
				"embedding_model": map[string]any{"entry": "embeddings", "model": "test", "document_prefix": "", "query_prefix": "q: ", "max_text_size": 64},
			})
		}
	})
	if o.cap.Topic == "" {
		o.cap = signing.Capability{Topic: topic, ArtifactRef: "sha256:test", ModelID: modelID,
			CapabilityEmbedding: embedding.Encode(capability), SimilarityFloor: 0.6}
	}
	if o.version == "" {
		o.version = "1.0"
	}
	if o.broker {
		n.Start("task_broker", taskbroker.Run)
	}
	st := n.MintNode(o.cap, o.version)
	dir, err := os.MkdirTemp("", "nri")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	path := filepath.Join(dir, "startup.json")
	b, _ := json.Marshal(st)
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	nc, err := sidecar.Connect(n.Srv.ClientURL(), path)
	if err != nil {
		t.Fatal(err)
	}
	requester, _ := nkeys.CreateUser()
	f := &fixture{
		Net: n, t: t, topic: o.cap.Topic, socket: filepath.Join(dir, "nri.sock"),
		pub: n.Conn(bus.PublishScope.Role, nil),
	}
	f.requester, _ = requester.PublicKey()
	kp, _ := nkeys.FromSeed([]byte(st.Identity))
	f.id, _ = kp.PublicKey()
	if !o.broker {
		f.broker = n.Conn(bus.TaskBrokerScope.Role, nil)
	}
	f.startup, f.path, f.shared, f.state, f.speaks, f.nc = &st, path, dir, t.TempDir(), o.speaks, nc
	t.Cleanup(nc.Close)
	f.start()
	return f
}

// logic is the Logic Container's side of the session.
type logic struct {
	client nriv1.NodeRuntimeClient
	stream nriv1.NodeRuntime_AttachClient
	cut    context.CancelFunc
	// msgs are the deliveries, read by a single reader; closed with the stream.
	msgs chan *nriv1.SidecarMessage
}

func (l *logic) read() {
	defer close(l.msgs)
	for {
		m, err := l.stream.Recv()
		if err != nil {
			return
		}
		l.msgs <- m
	}
}

// attach dials the socket until it exists, for up to ten seconds, and opens
// the session with a Hello of minor; it returns the error of the first answer.
func (f *fixture) attach(minor uint32) (*logic, error) {
	f.t.Helper()
	// A short backoff, so the socket is dialed soon after it appears.
	conn, err := grpc.NewClient("unix://"+f.socket, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff:           backoff.Config{BaseDelay: 50 * time.Millisecond, Multiplier: 1.6, MaxDelay: 200 * time.Millisecond},
			MinConnectTimeout: time.Second,
		}))
	if err != nil {
		f.t.Fatal(err)
	}
	f.t.Cleanup(func() { conn.Close() })
	ctx, cut := context.WithCancel(f.Ctx)
	f.t.Cleanup(cut)
	client := nriv1.NewNodeRuntimeClient(conn)
	var stream nriv1.NodeRuntime_AttachClient
	deadline := time.Now().Add(10 * time.Second)
	for {
		stream, err = client.Attach(ctx)
		if err == nil {
			err = stream.Send(&nriv1.LogicMessage{Body: &nriv1.LogicMessage_Hello{Hello: &nriv1.Hello{Minor: minor}}})
		}
		var m *nriv1.SidecarMessage
		if err == nil {
			m, err = stream.Recv()
		}
		if err == nil {
			if m.GetAccepted() == nil {
				f.t.Fatalf("the session opened with %v", m)
			}
			l := &logic{client: client, stream: stream, cut: cut, msgs: make(chan *nriv1.SidecarMessage, 64)}
			go l.read()
			return l, nil
		}
		if status.Code(err) != codes.Unavailable || f.Ctx.Err() != nil || time.Now().After(deadline) {
			return nil, err
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// next is the next delivery, or nil if none arrives within d.
func (l *logic) next(t *testing.T, d time.Duration) *nriv1.SidecarMessage {
	t.Helper()
	select {
	case m := <-l.msgs:
		return m
	case <-time.After(d):
		return nil
	}
}

func (l *logic) received(t *testing.T, deliveryID string) {
	t.Helper()
	if err := l.stream.Send(&nriv1.LogicMessage{Body: &nriv1.LogicMessage_Received{Received: &nriv1.Received{DeliveryId: deliveryID}}}); err != nil {
		t.Fatal(err)
	}
}

// announce publishes a task.announced on the fixture's topic with the vector v
// and extra fields in data, and returns its TaskID.
func (f *fixture) announce(v []float32, extra map[string]any) string {
	f.t.Helper()
	return f.announceAt(time.Now(), v, extra)
}

// announceAt publishes it as if it had been published at.
func (f *fixture) announceAt(at time.Time, v []float32, extra map[string]any) string {
	f.t.Helper()
	taskID := uuid.NewString()
	data := map[string]any{
		"topic": f.topic, "task_text": "say it back", "requester_identity": f.requester,
		"task_embedding": embedding.Encode(v), "model_id": modelID, "workspace": "repo",
	}
	for k, val := range extra {
		data[k] = val
	}
	e, err := bus.NewEvent(bus.TypeAnnounced, f.requester, taskID, data)
	if err != nil {
		f.t.Fatal(err)
	}
	e.TraceParent, e.ConfigVersion = traceparent(), globalconfig.V0Version
	e.Time = at
	f.publish(f.pub, bus.SubjectAnnounced(f.topic), e)
	return taskID
}

func traceparent() string {
	return "00-" + strings.ReplaceAll(uuid.NewString(), "-", "") + "-0000000000000001-01"
}

func (f *fixture) publish(nc *nats.Conn, subject string, e *bus.Event) {
	f.t.Helper()
	if err := bus.Publish(f.Ctx, f.JetStream(nc), subject, e); err != nil {
		f.t.Fatal(err)
	}
}

// assign publishes, as the Task Broker, the task.assigned of taskID to the node.
func (f *fixture) assign(taskID string) {
	f.t.Helper()
	e, err := bus.NewEvent(bus.TypeAssigned, "task_broker", taskID, map[string]any{
		"topic": f.topic, "requester_identity": f.requester, "scope_token": "token", "similarity": 0.8,
	})
	if err != nil {
		f.t.Fatal(err)
	}
	e.TraceParent, e.ConfigVersion = traceparent(), globalconfig.V0Version
	f.publish(f.broker, bus.SubjectAddressed(bus.TypeAssigned, f.id), e)
}

// await reads the next message of ch, or fails after d.
func await(t *testing.T, ch chan *nats.Msg, d time.Duration) *bus.Event {
	t.Helper()
	select {
	case m := <-ch:
		var e bus.Event
		if err := json.Unmarshal(m.Data, &e); err != nil {
			t.Fatal(err)
		}
		return &e
	case <-time.After(d):
		t.Fatal("nothing arrived")
		return nil
	}
}

// silent fails if anything arrives on ch within d.
func silent(t *testing.T, ch chan *nats.Msg, d time.Duration, what string) {
	t.Helper()
	select {
	case m := <-ch:
		t.Fatalf("%s: %s arrived", what, m.Subject)
	case <-time.After(d):
	}
}

func assignment(t *testing.T, m *nriv1.SidecarMessage) (*nriv1.Delivery, *bus.Event) {
	t.Helper()
	d := m.GetAssignment()
	if d == nil {
		t.Fatalf("got %v, want an assignment", m)
	}
	var e bus.Event
	if err := json.Unmarshal(d.GetEvent(), &e); err != nil {
		t.Fatal(err)
	}
	return d, &e
}

// The cycle with the Task Broker: the node proposes on a task near its
// capability, receives it with its announcement, keeps its lease beyond
// task_lease_ttl and closes it toward the requester.
func TestCycle(t *testing.T) {
	f := newFixture(t, options{broker: true})
	closes := f.Tap("task.completed." + f.requester + ".>")
	announced := f.Tap(bus.SubjectAnnounced(topic))
	l, err := f.attach(0)
	if err != nil {
		t.Fatal(err)
	}
	taskID := f.announce(near, nil)
	await(t, announced, 5*time.Second)

	m := l.next(t, 5*time.Second)
	if m == nil {
		t.Fatal("no assignment")
	}
	d, e := assignment(t, m)
	if e.Type != bus.TypeAssigned || e.Subject != taskID {
		t.Fatalf("delivered %s %s", e.Type, e.Subject)
	}
	if strings.Contains(string(e.Data), "scope_token") {
		t.Error("the assignment carries the scope token")
	}
	var ann bus.Event
	if err := json.Unmarshal(d.GetAnnouncement(), &ann); err != nil || ann.Subject != taskID || !strings.Contains(string(ann.Data), "say it back") {
		t.Fatalf("the announcement: %s, %v", d.GetAnnouncement(), err)
	}
	l.received(t, d.GetDeliveryId())

	// Three lifetimes of the lease: the Task Broker does not re-announce it.
	silent(t, announced, 3*lease, "the lease expired")

	if _, err := l.client.Publish(f.Ctx, &nriv1.PublishRequest{TaskId: taskID, Body: &nriv1.PublishRequest_Complete{
		Complete: &nriv1.CompleteTask{Data: []byte(`{"answer":"said"}`)}}}); err != nil {
		t.Fatal(err)
	}
	done := await(t, closes, 5*time.Second)
	var data map[string]any
	_ = json.Unmarshal(done.Data, &data)
	if done.Type != bus.TypeCompleted || done.Subject != taskID || done.Source != f.id ||
		data["answer"] != "said" || data["topic"] != topic || data["workspace"] != "repo" || data["metrics"] == nil {
		t.Errorf("closed with %s %s from %s: %s", done.Type, done.Subject, done.Source, done.Data)
	}
	_, err = l.client.Publish(f.Ctx, &nriv1.PublishRequest{TaskId: taskID, Body: &nriv1.PublishRequest_Complete{Complete: &nriv1.CompleteTask{}}})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("closing it again: %v", err)
	}
}

// The node does not propose on a task below its floor, on an announcement
// older than an auction, nor without a session.
func TestNoProposal(t *testing.T) {
	f := newFixture(t, options{})
	proposals := f.Tap("prop.>")
	f.announce(near, nil)
	silent(t, proposals, 2*auction, "without a session")

	if _, err := f.attach(0); err != nil {
		t.Fatal(err)
	}
	f.announce(far, nil)
	silent(t, proposals, 2*auction, "below the floor")
	f.announceAt(time.Now().Add(-2*auction), near, nil)
	silent(t, proposals, 2*auction, "an old announcement")
	f.announce(near, nil)
	p := await(t, proposals, 5*time.Second)
	var data map[string]any
	_ = json.Unmarshal(p.Data, &data)
	if s, _ := data["declared_similarity"].(float64); s < 0.79 || s > 0.81 || data["credential"] == nil {
		t.Errorf("proposed with %s", p.Data)
	}
}

// A busy worker does not propose, and one waiting on a proposal neither.
func TestBusy(t *testing.T) {
	f := newFixture(t, options{})
	proposals := f.Tap("prop.>")
	l, err := f.attach(0)
	if err != nil {
		t.Fatal(err)
	}
	first := f.announce(near, nil)
	await(t, proposals, 5*time.Second)
	f.announce(near, nil)
	silent(t, proposals, auction/2, "waiting on a proposal")

	f.assign(first)
	d, _ := assignment(t, l.next(t, 5*time.Second))
	l.received(t, d.GetDeliveryId())
	f.announce(near, nil)
	silent(t, proposals, 2*auction, "busy")
}

// A second task won while busy is neither delivered nor renewed.
func TestSecondAssignment(t *testing.T) {
	f := newFixture(t, options{})
	proposals := f.Tap("prop.>")
	leases := f.Tap("lease.>")
	l, err := f.attach(0)
	if err != nil {
		t.Fatal(err)
	}
	first := f.announce(near, nil)
	await(t, proposals, 5*time.Second)
	time.Sleep(auction + 50*time.Millisecond)
	second := f.announce(near, nil)
	await(t, proposals, 5*time.Second)

	f.assign(first)
	f.assign(second)
	_, e := assignment(t, l.next(t, 5*time.Second))
	if e.Subject != first {
		t.Fatalf("delivered %s, want %s", e.Subject, first)
	}
	if m := l.next(t, lease); m != nil {
		t.Fatalf("delivered %v while busy", m)
	}
	deadline := time.After(lease)
	for {
		select {
		case m := <-leases:
			if strings.HasSuffix(m.Subject, second) {
				t.Fatalf("renewed %s", m.Subject)
			}
		case <-deadline:
			return
		}
	}
}

// A cut session closes its tasks with logic_container_lost, and the node
// proposes again only with a new session.
func TestCut(t *testing.T) {
	f := newFixture(t, options{})
	proposals := f.Tap("prop.>")
	failures := f.Tap("task.failed." + f.requester + ".>")
	l, err := f.attach(0)
	if err != nil {
		t.Fatal(err)
	}
	taskID := f.announce(near, nil)
	await(t, proposals, 5*time.Second)
	f.assign(taskID)
	d, _ := assignment(t, l.next(t, 5*time.Second))
	l.received(t, d.GetDeliveryId())

	l.cut()
	e := await(t, failures, 5*time.Second)
	var data map[string]any
	_ = json.Unmarshal(e.Data, &data)
	if e.Subject != taskID || data["failure_code"] != "logic_container_lost" {
		t.Errorf("failed with %s: %s", e.Subject, e.Data)
	}
	f.announce(near, nil)
	silent(t, proposals, 2*auction, "without a session")

	if _, err := f.attach(0); err != nil {
		t.Fatal(err)
	}
	f.announce(near, nil)
	await(t, proposals, 5*time.Second)
}

// A Logic Container whose Hello the binary does not speak, or that does not
// match its image's label, ends the Integration Sidecar.
func TestVersion(t *testing.T) {
	cases := []struct {
		name    string
		o       options
		minor   uint32
		attachs bool
	}{
		{"a minor it does not speak", options{}, 1, true},
		{"a Hello that is not the label's", options{version: "1.1", speaks: sidecar.Range{"1": 1}}, 0, true},
		{"a label outside what it speaks", options{version: "2.0"}, 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, tc.o)
			if tc.attachs {
				if _, err := f.attach(tc.minor); status.Code(err) != codes.FailedPrecondition {
					t.Errorf("attached with %v", err)
				}
			}
			select {
			case err := <-f.run:
				if err == nil {
					t.Error("the Integration Sidecar ended without an error")
				}
				f.run <- err
			case <-time.After(5 * time.Second):
				t.Error("the Integration Sidecar did not end")
			}
		})
	}
}

// What a publication request cannot carry.
func TestPublishRejected(t *testing.T) {
	f := newFixture(t, options{})
	proposals := f.Tap("prop.>")
	l, err := f.attach(0)
	if err != nil {
		t.Fatal(err)
	}
	taskID := f.announce(near, nil)
	await(t, proposals, 5*time.Second)
	f.assign(taskID)
	assignment(t, l.next(t, 5*time.Second))

	complete := func(data string) *nriv1.PublishRequest {
		return &nriv1.PublishRequest{TaskId: taskID, Body: &nriv1.PublishRequest_Complete{Complete: &nriv1.CompleteTask{Data: []byte(data)}}}
	}
	fail := func(reason, code, data string) *nriv1.PublishRequest {
		return &nriv1.PublishRequest{TaskId: taskID, Body: &nriv1.PublishRequest_Fail{Fail: &nriv1.FailTask{FailureReason: reason, FailureCode: code, Data: []byte(data)}}}
	}
	cases := []struct {
		name string
		req  *nriv1.PublishRequest
		code codes.Code
	}{
		{"a task it does not hold", &nriv1.PublishRequest{TaskId: "other", Body: &nriv1.PublishRequest_Complete{Complete: &nriv1.CompleteTask{}}}, codes.FailedPrecondition},
		{"data that is not an object", complete(`[1]`), codes.InvalidArgument},
		{"a key the sidecar stamps", complete(`{"topic":"other"}`), codes.InvalidArgument},
		{"metrics", complete(`{"metrics":{}}`), codes.InvalidArgument},
		{"more than the Bus carries", complete(`{"x":"` + strings.Repeat("a", 2<<20) + `"}`), codes.InvalidArgument},
		{"a delivery_id without an Inbound", &nriv1.PublishRequest{TaskId: taskID, Body: &nriv1.PublishRequest_Complete{Complete: &nriv1.CompleteTask{DeliveryId: "x"}}}, codes.FailedPrecondition},
		{"a failure without its reason", fail("", "", ""), codes.InvalidArgument},
		{"a worker's failure_code", fail("broke", "unanswered", ""), codes.InvalidArgument},
		{"a failure_code in data", fail("broke", "", `{"failure_code":"x"}`), codes.InvalidArgument},
	}
	for _, tc := range cases {
		if _, err := l.client.Publish(f.Ctx, tc.req); status.Code(err) != tc.code {
			t.Errorf("%s: %v, want %s", tc.name, err, tc.code)
		}
	}
	if _, err := l.client.Publish(f.Ctx, fail("broke", "", `{"detail":"x"}`)); err != nil {
		t.Errorf("a valid failure: %v", err)
	}
}

// A channel holds every query it wins, does not propose in round 1 of its own
// audience, and closes a query with undelivered or unanswered.
func TestChannel(t *testing.T) {
	f := newFixture(t, options{cap: signing.Capability{Topic: "hitl_contact_ops", ArtifactRef: "sha256:channel"}})
	proposals := f.Tap("prop.>")
	failures := f.Tap("task.failed." + f.requester + ".>")
	l, err := f.attach(0)
	if err != nil {
		t.Fatal(err)
	}
	query := map[string]any{"origin_audience": "user", "kind": "question"}
	f.announce(far, map[string]any{"origin_audience": "ops"})
	silent(t, proposals, 2*auction, "round 1 of its own audience")

	first := f.announce(far, query)
	await(t, proposals, 5*time.Second)
	second := f.announce(far, query)
	await(t, proposals, 5*time.Second)
	f.assign(first)
	f.assign(second)
	for range 2 {
		d, _ := assignment(t, l.next(t, 5*time.Second))
		l.received(t, d.GetDeliveryId())
	}

	fail := func(code string) error {
		_, err := l.client.Publish(f.Ctx, &nriv1.PublishRequest{TaskId: first, Body: &nriv1.PublishRequest_Fail{Fail: &nriv1.FailTask{FailureReason: "nobody", FailureCode: code}}})
		return err
	}
	if err := fail(""); status.Code(err) != codes.InvalidArgument {
		t.Errorf("a query failed without its code: %v", err)
	}
	if err := fail("unanswered"); err != nil {
		t.Fatal(err)
	}
	if e := await(t, failures, 5*time.Second); e.Subject != first {
		t.Errorf("failed %s", e.Subject)
	}
	if _, err := l.client.Publish(f.Ctx, &nriv1.PublishRequest{TaskId: second, Body: &nriv1.PublishRequest_Complete{
		Complete: &nriv1.CompleteTask{Data: []byte(`{"decision":"yes"}`)}}}); err != nil {
		t.Error(err)
	}
}
