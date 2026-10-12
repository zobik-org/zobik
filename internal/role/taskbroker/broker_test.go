package taskbroker_test

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/configschema"
	"zobik.org/zobik/internal/deploy"
	"zobik.org/zobik/internal/embedding"
	"zobik.org/zobik/internal/globalconfig"
	"zobik.org/zobik/internal/role"
	"zobik.org/zobik/internal/role/taskbroker"
	"zobik.org/zobik/internal/signing"
)

// nodeScope is an emergent node's, as implementation §1.2.1 writes it; the
// Spawner issues it once it enters, and until then the harness does.
var nodeScope = bus.Scope{
	Role: "node",
	Publish: []string{
		"task.announced.>", "prop.{{subject()}}.>", "lease.{{subject()}}.>",
		"task.completed.>", "task.failed.>",
	},
	Subscribe: []string{
		"task.assigned.{{subject()}}", "task.rejected.{{subject()}}",
		"task.announced.>", "_INBOX.{{subject()}}.>",
	},
}

// fixture is a Bus in operator mode with the substrate of zobik init and a Task
// Broker running under its own scope, with times short enough for a test.
type fixture struct {
	t       *testing.T
	ctx     context.Context
	srv     *server.Server
	account *bus.Account
	root    nkeys.KeyPair
	rootPub string
	spawner nkeys.KeyPair
	cert    string
	console jetstream.JetStream
	broker  *role.Runtime
	stop    context.CancelFunc
	dir     string
}

// testValues are v0 with the network open and the times of the cycle shortened.
func testValues(t *testing.T, admission string) globalconfig.Values {
	t.Helper()
	s, err := configschema.Load()
	if err != nil {
		t.Fatal(err)
	}
	v := s.V0()
	for key, val := range map[string]any{
		"auction_timeout":         300,
		"reannounce_backoff_base": 600,
		"max_reannounce_backoff":  2400,
		"task_lease_ttl":          1000,
		"network_admission":       admission,
	} {
		v[key], _ = json.Marshal(val)
	}
	return v
}

func newFixture(t *testing.T, admission string) *fixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	root, _ := nkeys.CreateOperator()
	rootPub, _ := root.PublicKey()
	a, err := bus.NewAccount(root, "test", append([]bus.Scope{bus.ConsoleScope, nodeScope}, bus.RoleScopes...))
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{t: t, ctx: ctx, account: a, root: root, rootPub: rootPub}
	f.srv = startServer(t, a)

	f.console = must(t, f.connect(bus.ConsoleScope.Role, nil))
	if err := bus.EnsureStreams(ctx, f.console); err != nil {
		t.Fatal(err)
	}
	if err := bus.EnsureBuckets(ctx, f.console, time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := globalconfig.WriteV0(ctx, f.console, globalconfig.Record{
		ConfigVersion: globalconfig.V0Version, PublishedAt: time.Now(),
		AuthorizedBy: globalconfig.AuthorizedByDeployment, Values: testValues(t, admission),
	}); err != nil {
		t.Fatal(err)
	}

	f.spawner, _ = nkeys.CreateUser()
	spawnerPub, _ := f.spawner.PublicKey()
	f.cert = f.sign(f.root, signing.TypeRoleCertificate, signing.RoleCertificate{Sub: spawnerPub, Role: signing.RoleSpawner}, "")

	// The Task Broker's material, as zobik init leaves it in its volume.
	f.dir = t.TempDir()
	kp, _ := nkeys.CreateUser()
	pub, _ := kp.PublicKey()
	seed, _ := kp.Seed()
	userJWT, err := bus.IssueUser(a.SigningKeys[bus.TaskBrokerScope.Role], a.Public, pub, "task_broker", time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	for file, data := range map[string]string{
		role.FileIdentity:        string(seed),
		role.FileBusJWT:          userJWT,
		role.FileRootPub:         rootPub,
		role.FileAccount:         a.JWT,
		role.FileRoleCertificate: f.sign(root, signing.TypeRoleCertificate, signing.RoleCertificate{Sub: pub, Role: signing.RoleTaskBroker}, ""),
	} {
		if err := os.WriteFile(filepath.Join(f.dir, file), []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	var consumers []bus.Consumer
	for _, r := range deploy.Roles {
		if r.Unit == "task_broker" {
			consumers = r.Consumers(pub)
		}
	}
	if err := bus.EnsureConsumers(ctx, f.console, consumers); err != nil {
		t.Fatal(err)
	}
	f.start()
	return f
}

// start runs the Task Broker and waits until it is ready.
func (f *fixture) start() {
	f.t.Helper()
	rt, err := role.Load("task_broker", f.dir)
	if err != nil {
		f.t.Fatal(err)
	}
	nc, err := rt.Connect(f.srv.ClientURL())
	if err != nil {
		f.t.Fatal(err)
	}
	ctx, stop := context.WithCancel(f.ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := taskbroker.Run(ctx, rt, nc); err != nil {
			f.t.Errorf("task_broker: %v", err)
		}
	}()
	f.broker = rt
	f.stop = func() { stop(); <-done; nc.Close() }
	f.t.Cleanup(f.stop)
	for !rt.Ready() {
		select {
		case <-f.ctx.Done():
			f.t.Fatal("the Task Broker did not become ready")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func startServer(t *testing.T, a *bus.Account) *server.Server {
	t.Helper()
	op, err := jwt.DecodeOperatorClaims(a.Operator)
	if err != nil {
		t.Fatal(err)
	}
	resolver := &server.MemAccResolver{}
	for _, j := range []string{a.System, a.JWT} {
		c, err := jwt.DecodeAccountClaims(j)
		if err != nil {
			t.Fatal(err)
		}
		resolver.Store(c.Subject, j)
	}
	s, err := server.NewServer(&server.Options{
		Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(),
		TrustedOperators: []*jwt.OperatorClaims{op}, SystemAccount: op.SystemAccount,
		AccountResolver: resolver, NoLog: true, NoSigs: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	go s.Start()
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatal("server not ready")
	}
	t.Cleanup(s.Shutdown)
	return s
}

// connect opens a connection under scope; kp is its identity, or a new one.
func (f *fixture) connect(scope string, kp nkeys.KeyPair) *nats.Conn {
	f.t.Helper()
	if kp == nil {
		kp, _ = nkeys.CreateUser()
	}
	pub, _ := kp.PublicKey()
	seed, _ := kp.Seed()
	token, err := bus.IssueUser(f.account.SigningKeys[scope], f.account.Public, pub, scope, time.Now().Add(time.Hour))
	if err != nil {
		f.t.Fatal(err)
	}
	nc, err := nats.Connect(f.srv.ClientURL(), nats.UserJWTAndSeed(token, string(seed)), nats.CustomInboxPrefix(bus.InboxPrefix(pub)))
	if err != nil {
		f.t.Fatal(err)
	}
	f.t.Cleanup(nc.Close)
	return nc
}

func (f *fixture) sign(kp nkeys.KeyPair, typ signing.Type, claims any, signer string) string {
	f.t.Helper()
	tok, err := signing.Sign(kp, typ, claims, signer)
	if err != nil {
		f.t.Fatal(err)
	}
	return tok
}

// node is an emergent node on the Bus: its identity, its credential and what it receives.
type node struct {
	f          *fixture
	id         string
	nc         *nats.Conn
	js         jetstream.JetStream
	credential string
	inbox      chan *nats.Msg
}

func (f *fixture) node(c signing.Capability, structural bool) *node {
	f.t.Helper()
	kp, _ := nkeys.CreateUser()
	pub, _ := kp.PublicKey()
	c.Sub = pub
	n := &node{f: f, id: pub, nc: f.connect(nodeScope.Role, kp), inbox: make(chan *nats.Msg, 64)}
	n.js = must(f.t, n.nc)
	if structural {
		n.credential = f.sign(f.root, signing.TypeCapability, c, "")
	} else {
		n.credential = f.sign(f.spawner, signing.TypeCapability, c, f.cert)
	}
	for _, typ := range []string{bus.TypeAssigned, bus.TypeRejected} {
		if _, err := n.nc.ChanSubscribe(bus.SubjectAddressed(typ, pub), n.inbox); err != nil {
			f.t.Fatal(err)
		}
	}
	n.nc.Flush()
	return n
}

// unit is a normalized vector whose similarity with unit(1) is cos.
func unit(cos float64) string {
	return embedding.Encode([]float32{float32(cos), float32(math.Sqrt(1 - cos*cos)), 0})
}

// announce publishes a task.announced as a requester's Integration Sidecar does.
func (n *node) announce(topic string, data map[string]any) (taskID, traceID string) {
	n.f.t.Helper()
	taskID, traceID = uuid.NewString(), "4bf92f3577b34da6a3ce929d0e0e4736"
	data["topic"] = topic
	data["requester_identity"] = n.id
	e, err := bus.NewEvent(bus.TypeAnnounced, n.id, taskID, data)
	if err != nil {
		n.f.t.Fatal(err)
	}
	e.TraceParent, e.ConfigVersion = "00-"+traceID+"-00f067aa0ba902b7-01", globalconfig.V0Version
	if err := bus.Publish(n.f.ctx, n.js, bus.SubjectAnnounced(topic), e); err != nil {
		n.f.t.Fatal(err)
	}
	return taskID, traceID
}

func (n *node) propose(topic, taskID string, declared *float64) {
	n.f.t.Helper()
	data := map[string]any{"topic": topic, "credential": n.credential}
	if declared != nil {
		data["declared_similarity"] = *declared
	}
	e, err := bus.NewEvent(bus.TypeProposed, n.id, taskID, data)
	if err != nil {
		n.f.t.Fatal(err)
	}
	e.TraceParent, e.ConfigVersion = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", globalconfig.V0Version
	if err := bus.Publish(n.f.ctx, n.js, bus.SubjectProposed(n.id, topic), e); err != nil {
		n.f.t.Fatal(err)
	}
}

func (n *node) renew(topic, taskID string) {
	n.nc.Publish("lease."+n.id+"."+topic+"."+taskID, nil)
	n.nc.Flush()
}

func (n *node) complete(requester, topic, taskID string) {
	n.f.t.Helper()
	e, err := bus.NewEvent(bus.TypeCompleted, n.id, taskID, map[string]any{
		"topic":   topic,
		"metrics": map[string]any{"tokens_in": 0, "tokens_out": 0, "cost": 0, "duration_ms": 1},
	})
	if err != nil {
		n.f.t.Fatal(err)
	}
	e.TraceParent, e.ConfigVersion = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", globalconfig.V0Version
	if err := bus.Publish(n.f.ctx, n.js, bus.SubjectTerminal(bus.TypeCompleted, requester, topic), e); err != nil {
		n.f.t.Fatal(err)
	}
}

// receive waits for the next event addressed to n.
func (n *node) receive(within time.Duration) *bus.Event {
	n.f.t.Helper()
	select {
	case m := <-n.inbox:
		var e bus.Event
		if err := json.Unmarshal(m.Data, &e); err != nil {
			n.f.t.Fatal(err)
		}
		return &e
	case <-time.After(within):
		return nil
	}
}

type addressed struct {
	Topic             string   `json:"topic"`
	Similarity        *float64 `json:"similarity"`
	RequesterIdentity string   `json:"requester_identity"`
	ScopeToken        string   `json:"scope_token"`
}

func dataOf(t *testing.T, e *bus.Event) addressed {
	t.Helper()
	var d addressed
	if err := json.Unmarshal(e.Data, &d); err != nil {
		t.Fatal(err)
	}
	return d
}

// announcements collects every task.announced of topic.
func (f *fixture) announcements(topic string) chan *nats.Msg {
	ch := make(chan *nats.Msg, 64)
	nc := f.connect(nodeScope.Role, nil)
	if _, err := nc.ChanSubscribe(bus.SubjectAnnounced(topic), ch); err != nil {
		f.t.Fatal(err)
	}
	nc.Flush()
	return ch
}

func (f *fixture) state(topic, taskID string) map[string]any {
	f.t.Helper()
	kv, err := f.console.KeyValue(f.ctx, bus.BucketTasks)
	if err != nil {
		f.t.Fatal(err)
	}
	e, err := kv.Get(f.ctx, topic+"."+taskID)
	if err != nil {
		f.t.Fatal(err)
	}
	var v map[string]any
	json.Unmarshal(e.Value(), &v)
	return v
}

func ptr[T any](v T) *T { return &v }

// The highest recomputed similarity wins; the rest receive task.rejected with
// the similarity they lost with; the winner receives its scope token.
func TestAuction(t *testing.T) {
	f := newFixture(t, "open")
	capOf := func(cos, floor float64) signing.Capability {
		return signing.Capability{Topic: "echo", ModelID: "m1", CapabilityEmbedding: unit(cos), SimilarityFloor: floor}
	}
	requester := f.node(signing.Capability{}, false)
	best := f.node(capOf(0.9, 0.6), false)
	second := f.node(capOf(0.8, 0.6), false)
	below := f.node(capOf(0.5, 0.6), false)

	taskID, traceID := requester.announce("echo", map[string]any{"task_embedding": unit(1), "model_id": "m1"})
	best.propose("echo", taskID, ptr(0.85))
	second.propose("echo", taskID, ptr(0.99))
	below.propose("echo", taskID, nil)

	won := best.receive(3 * time.Second)
	if won == nil || won.Type != bus.TypeAssigned || won.Subject != taskID {
		t.Fatalf("the best proposal: %+v", won)
	}
	d := dataOf(t, won)
	if *d.Similarity != 0.85 || d.RequesterIdentity != requester.id {
		t.Errorf("task.assigned data %+v", d)
	}
	token, err := signing.VerifyScopeToken(d.ScopeToken, f.rootPub)
	if err != nil {
		t.Fatal(err)
	}
	if token.Claims.Sub != best.id || token.Claims.TraceID != traceID || token.Claims.Section != taskID {
		t.Errorf("scope token %+v", token.Claims)
	}
	for _, c := range []struct {
		n    *node
		want float64
	}{{second, 0.8}, {below, 0.5}} {
		e := c.n.receive(time.Second)
		if e == nil || e.Type != bus.TypeRejected || *dataOf(t, e).Similarity != c.want {
			t.Errorf("rejected: %+v", e)
		}
	}
	if s := f.state("echo", taskID); s["state"] != "CLAIMED" || s["assignee"] != best.id {
		t.Errorf("state %v", s)
	}

	// A proposal after the award is discarded.
	second.propose("echo", taskID, nil)
	if e := second.receive(time.Second); e != nil {
		t.Errorf("a late proposal was answered: %+v", e)
	}
}

// Only the assignee renews; without renewals the lease expires and the task is
// re-announced; the terminal event moves it to DONE and drops the announcement.
func TestLease(t *testing.T) {
	f := newFixture(t, "open")
	announced := f.announcements("echo")
	requester := f.node(signing.Capability{}, false)
	worker := f.node(signing.Capability{Topic: "echo", ModelID: "m1", CapabilityEmbedding: unit(0.9), SimilarityFloor: 0.6}, false)
	other := f.node(signing.Capability{Topic: "echo", ModelID: "m1", CapabilityEmbedding: unit(0.9), SimilarityFloor: 0.6}, false)

	taskID, _ := requester.announce("echo", map[string]any{"task_embedding": unit(1), "model_id": "m1"})
	<-announced
	worker.propose("echo", taskID, nil)
	if e := worker.receive(3 * time.Second); e == nil || e.Type != bus.TypeAssigned {
		t.Fatalf("not assigned: %+v", e)
	}

	// The assignee renews for twice the lease, and nothing is re-announced; a
	// renewal by another identity does not count.
	deadline := time.Now().Add(2 * time.Second)
	var last time.Time
	for time.Now().Before(deadline) {
		worker.renew("echo", taskID)
		last = time.Now()
		other.renew("echo", taskID)
		time.Sleep(300 * time.Millisecond)
	}
	select {
	case m := <-announced:
		t.Fatalf("re-announced while renewed: %s", m.Subject)
	default:
	}
	// Only the other renews: the lease expires and the task is re-announced.
	start := time.Now()
	for {
		other.renew("echo", taskID)
		select {
		case m := <-announced:
			var e bus.Event
			json.Unmarshal(m.Data, &e)
			if e.Subject != taskID || e.Source != f.broker.ID {
				t.Errorf("re-announcement %+v", e)
			}
			if time.Since(last) < 900*time.Millisecond {
				t.Errorf("re-announced %s after the last renewal", time.Since(last))
			}
			goto expired
		case <-time.After(300 * time.Millisecond):
		}
		if time.Since(start) > 5*time.Second {
			t.Fatal("the lease did not expire")
		}
	}
expired:
	if s := f.state("echo", taskID); s["state"] != "UNASSIGNED" {
		t.Errorf("after expiry: %v", s)
	}
	other.propose("echo", taskID, nil)
	if e := other.receive(3 * time.Second); e == nil || e.Type != bus.TypeAssigned {
		t.Fatalf("not reassigned: %+v", e)
	}
	other.complete(requester.id, "echo", taskID)
	time.Sleep(300 * time.Millisecond)
	if s := f.state("echo", taskID); s["state"] != "DONE" || s["announcement"] != nil {
		t.Errorf("after the close: %v", s)
	}
}

// A task without candidates is re-announced with backoff: base, doubled, up to
// the ceiling.
func TestVacancyBackoff(t *testing.T) {
	f := newFixture(t, "open")
	announced := f.announcements("nobody")
	requester := f.node(signing.Capability{}, false)
	taskID, _ := requester.announce("nobody", map[string]any{})
	var times []time.Time
	for len(times) < 5 {
		select {
		case m := <-announced:
			var e bus.Event
			json.Unmarshal(m.Data, &e)
			if e.Subject != taskID {
				t.Fatalf("another task %s", e.Subject)
			}
			times = append(times, time.Now())
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d announcements", len(times))
		}
	}
	want := []time.Duration{600, 1200, 2400, 2400}
	for i, w := range want {
		got := times[i+1].Sub(times[i])
		if d := got - w*time.Millisecond; d < -100*time.Millisecond || d > 400*time.Millisecond {
			t.Errorf("interval %d: %s, want %dms", i, got, w)
		}
	}
	if s := f.state("nobody", taskID); s["reannouncements"] != 4.0 {
		t.Errorf("state %v", s)
	}
}

// A frozen network admits nothing on an emergent topic and emits no event, and
// keeps awarding the fixed topics with a structural claimant (architecture §3.18).
func TestFrozen(t *testing.T) {
	f := newFixture(t, "frozen")
	announced := f.announcements("echo")
	requester := f.node(signing.Capability{}, false)
	worker := f.node(signing.Capability{Topic: "echo", ModelID: "m1", CapabilityEmbedding: unit(0.9), SimilarityFloor: 0.6}, false)

	taskID, _ := requester.announce("echo", map[string]any{"task_embedding": unit(1), "model_id": "m1"})
	<-announced
	worker.propose("echo", taskID, nil)
	if e := worker.receive(time.Second); e != nil {
		t.Errorf("a frozen network answered: %+v", e)
	}
	select {
	case <-announced:
	case <-time.After(2 * time.Second):
		t.Error("the paused task was not re-announced")
	}

	config := f.node(signing.Capability{Topic: "config_change"}, true)
	changeID, _ := requester.announce("config_change", map[string]any{})
	config.propose("config_change", changeID, nil)
	if e := config.receive(3 * time.Second); e == nil || e.Type != bus.TypeAssigned {
		t.Errorf("config_change in a frozen network: %+v", e)
	}
}

// A restarted Task Broker resumes its range from the bucket: the lease of a task
// it awarded before stopping still expires.
func TestResume(t *testing.T) {
	f := newFixture(t, "open")
	announced := f.announcements("echo")
	requester := f.node(signing.Capability{}, false)
	worker := f.node(signing.Capability{Topic: "echo", ModelID: "m1", CapabilityEmbedding: unit(0.9), SimilarityFloor: 0.6}, false)
	taskID, _ := requester.announce("echo", map[string]any{"task_embedding": unit(1), "model_id": "m1"})
	<-announced
	worker.propose("echo", taskID, nil)
	if e := worker.receive(3 * time.Second); e == nil || e.Type != bus.TypeAssigned {
		t.Fatalf("not assigned: %+v", e)
	}
	f.stop()
	f.start()
	select {
	case <-announced:
	case <-time.After(4 * time.Second):
		t.Fatal("the restarted Task Broker did not expire the lease")
	}
}

func must(t *testing.T, nc *nats.Conn) jetstream.JetStream {
	t.Helper()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	return js
}
