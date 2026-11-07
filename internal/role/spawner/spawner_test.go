package spawner_test

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/digest"
	"zobik.org/zobik/internal/egress"
	"zobik.org/zobik/internal/embedding"
	"zobik.org/zobik/internal/globalconfig"
	"zobik.org/zobik/internal/role"
	"zobik.org/zobik/internal/role/catalog"
	"zobik.org/zobik/internal/role/spawner"
	"zobik.org/zobik/internal/role/taskbroker"
	"zobik.org/zobik/internal/signing"
	"zobik.org/zobik/internal/testnet"
)

const (
	network = "test"
	ruleset = "sha256:rules"
	echo    = "sha256:" + "e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0"
)

// record is a qualified record of the fake Catalog.
type record struct {
	value    json.RawMessage
	revision int64
	expires  time.Time
}

// fakeCatalog is the Catalog in memory, with the write-if-absent, the revision
// and the expiry of implementation §1.2.11.
type fakeCatalog struct {
	mu      sync.Mutex
	entries map[string]*catalog.Entry
	// occupied and capacity are what the status reports of the storage.
	occupied, capacity int64
	records            map[string]*record // network/key
	rev                int64
	// onPresence runs when a presence mark is written.
	onPresence func()
}

func newCatalog() *fakeCatalog {
	return &fakeCatalog{entries: map[string]*catalog.Entry{}, records: map[string]*record{}}
}

func (c *fakeCatalog) add(e catalog.Entry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[e.ArtifactRef] = &e
}

func (c *fakeCatalog) Entry(_ context.Context, digest string) (*catalog.Entry, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[digest]
	if !ok {
		return nil, false, nil
	}
	cp := *e
	return &cp, true, nil
}

func (c *fakeCatalog) Entries(context.Context) ([]catalog.Entry, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []catalog.Entry
	for _, e := range c.entries {
		out = append(out, *e)
	}
	return out, nil
}

func (c *fakeCatalog) Status(context.Context) (*catalog.Status, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return &catalog.Status{RulesetVersion: ruleset, Occupied: c.occupied, Capacity: c.capacity}, nil
}

func (c *fakeCatalog) live(k string) *record {
	r := c.records[k]
	if r == nil || (!r.expires.IsZero() && time.Now().After(r.expires)) {
		return nil
	}
	return r
}

func (c *fakeCatalog) Record(_ context.Context, network, key string, out any) (int64, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	r := c.live(network + "/" + key)
	if r == nil {
		return 0, false, nil
	}
	if out != nil {
		if err := json.Unmarshal(r.value, out); err != nil {
			return 0, false, err
		}
	}
	return r.revision, true, nil
}

func (c *fakeCatalog) Records(_ context.Context, prefix, network string) ([]catalog.Record, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []catalog.Record
	for k := range c.records {
		n, key, _ := strings.Cut(k, "/")
		if r := c.live(k); r != nil && (network == "" || n == network) && strings.HasPrefix(key, prefix) {
			out = append(out, catalog.Record{Network: n, Key: key, Value: r.value, Revision: r.revision})
		}
	}
	return out, nil
}

func (c *fakeCatalog) PutRecord(_ context.Context, network, key string, value any, w catalog.Write) error {
	c.mu.Lock()
	k := network + "/" + key
	if w.IfAbsent && c.live(k) != nil {
		c.mu.Unlock()
		return catalog.ErrPrecondition
	}
	b, _ := json.Marshal(value)
	c.rev++
	r := &record{value: b, revision: c.rev}
	if w.Expires > 0 {
		r.expires = time.Now().Add(w.Expires)
	}
	c.records[k] = r
	hook := c.onPresence
	c.mu.Unlock()
	if strings.HasPrefix(key, "presence:") && hook != nil {
		hook()
	}
	return nil
}

func (c *fakeCatalog) DeleteRecord(_ context.Context, network, key string, revision int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	k := network + "/" + key
	r := c.live(k)
	if r == nil || r.revision != revision {
		return catalog.ErrPrecondition
	}
	delete(c.records, k)
	return nil
}

func (c *fakeCatalog) SetDeletionMark(_ context.Context, digest string, on bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[digest]
	if !ok {
		return fmt.Errorf("no entry %s", digest)
	}
	e.DeletionMark = on
	return nil
}

func (c *fakeCatalog) has(key string) bool {
	_, ok, _ := c.Record(context.Background(), network, key, nil)
	return ok
}

// fakeDriver is the deployment platform in memory.
type fakeDriver struct {
	mu       sync.Mutex
	fleet    []spawner.Running
	acts     []spawner.Provisioning
	renewals map[string]int
	refusal  error
	// decommissioned and images are the nodes and the images it removed.
	decommissioned []string
	images         []string
	// capacityMB is the fleet's capacity, zero for none; each node reserves
	// nodeMB.
	capacityMB int64
	// modelID is the network's space, vectors the embedding of each text, and
	// down takes the model down.
	modelID string
	vectors map[string][]float32
	down    bool
}

// nodeMB is what each node of the fake reserves, both halves.
const nodeMB = 288

func (d *fakeDriver) Shortfall(_ context.Context, p spawner.Provisioning) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.capacityMB == 0 {
		return 0, nil
	}
	if nodeMB > d.capacityMB {
		return 0, spawner.Refuse(spawner.CodeCapacityExhausted, "does not fit")
	}
	var used int64
	for _, n := range d.fleet {
		used += n.MemoryMB
	}
	return max(0, used+nodeMB-d.capacityMB), nil
}

func (d *fakeDriver) Document(_ context.Context, text string) (string, string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.down {
		return "", "", fmt.Errorf("the model is down")
	}
	v, ok := d.vectors[text]
	if !ok {
		return "", "", fmt.Errorf("no vector for %q", text)
	}
	return d.modelID, embedding.Encode(v), nil
}

// defaultFloor is the floor the fake brings a node up with when the act names none.
const defaultFloor = 0.5

func (d *fakeDriver) Provision(_ context.Context, p spawner.Provisioning) (*spawner.Node, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.acts = append(d.acts, p)
	if d.refusal != nil {
		return nil, d.refusal
	}
	id := fmt.Sprintf("UNODE%d", len(d.acts))
	floor := defaultFloor
	if p.Floor != nil {
		floor = *p.Floor
	}
	d.fleet = append(d.fleet, spawner.Running{NodeID: id, Topic: p.Topic, ArtifactRef: p.Entry.ArtifactRef, ModelID: d.modelID,
		SimilarityFloor: floor, MemoryMB: nodeMB, Provisioned: time.Now()})
	vector := "AAAA"
	if v, ok := d.vectors[p.Entry.CapabilityText]; ok {
		vector = embedding.Encode(v)
	}
	return &spawner.Node{ID: id, ModelID: d.modelID, Embedding: vector, MemoryMB: 256}, nil
}

func (d *fakeDriver) Decommission(_ context.Context, n spawner.Running) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.fleet = slices.DeleteFunc(d.fleet, func(r spawner.Running) bool { return r.NodeID == n.NodeID })
	d.decommissioned = append(d.decommissioned, n.NodeID)
	return nil
}

func (d *fakeDriver) DeleteImage(_ context.Context, digest string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.images = append(d.images, digest)
	return nil
}

// fakeRegistry records what purge deletes.
type fakeRegistry struct {
	mu      sync.Mutex
	deleted []string
}

func (r *fakeRegistry) Delete(_ context.Context, digest string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.deleted = append(r.deleted, digest)
	return nil
}

func (d *fakeDriver) Fleet(context.Context) ([]spawner.Running, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]spawner.Running(nil), d.fleet...), nil
}

func (d *fakeDriver) Renew(_ context.Context, n spawner.Running) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.renewals[n.NodeID]++
	return nil
}

func (d *fakeDriver) actCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.acts)
}

type fixture struct {
	*testnet.Net
	cat      *fakeCatalog
	driver   *fakeDriver
	registry *fakeRegistry
	channel  string
	closes   chan *nats.Msg
	// failures are the task.failed toward the requester of the vacant tasks.
	failures chan *nats.Msg
}

// embeddingModel is the network's embedding_model: the act resolves the space
// from it, and the fake driver generates the vectors.
var embeddingModel = map[string]any{"entry": "embeddings", "model": "test", "document_prefix": "", "query_prefix": "", "max_text_size": 256}

func newFixture(t *testing.T, lifetime time.Duration, noCatalog bool, edits ...func(globalconfig.Values)) *fixture {
	var modelID string
	n := testnet.New(t, func(v globalconfig.Values) {
		testnet.Set(v, map[string]any{
			"auction_timeout": 200, "embedding_model": embeddingModel,
			"egress_registry": map[string]any{"embeddings": egress.EmbeddingsEntry("http://embeddings.test")},
		})
		for _, edit := range edits {
			edit(v)
		}
		space, err := embedding.Resolve(v)
		if err != nil {
			t.Fatal(err)
		}
		modelID = space.ModelID
	})
	f := &fixture{
		Net: n, cat: newCatalog(), registry: &fakeRegistry{},
		driver:  &fakeDriver{renewals: map[string]int{}, modelID: modelID, vectors: map[string][]float32{}},
		channel: n.ID("channel_operator"),
		closes:  n.Tap("task.*." + n.ID("channel_operator") + ".*"),
	}
	f.cat.add(catalog.Entry{
		Registration: catalog.Registration{Topic: "echo", ArtifactRef: echo, CapabilityText: "repeats", AuthorID: "seeding", Requires: map[string]json.RawMessage{}},
		ScanResult:   catalog.ScanResult{Verdict: "pass", RulesetVersion: ruleset},
	})
	n.Start("spawner", func(ctx context.Context, rt *role.Runtime, nc *nats.Conn) error {
		return spawner.Run(ctx, rt, nc, spawner.Options{
			Network: network, Driver: f.driver, Registry: f.registry, IdentityLifetime: lifetime,
			Catalog: func(context.Context) (spawner.Catalog, error) {
				if noCatalog {
					return nil, spawner.ErrNoCatalog
				}
				return f.cat, nil
			},
		})
	})
	n.Start("task_broker", taskbroker.Run)
	return f
}

// request is a node_provision as the console assembles it; each field can be spoiled.
type request struct {
	targetTopic, artifactRef string
	associations             []signing.Association
	entryTopic               string
	proofAct, proofArtifact  string
	tamperData               bool
}

func (f *fixture) send(r request) (taskID string) {
	f.T.Helper()
	traceID := strings.ReplaceAll(uuid.NewString(), "-", "")
	taskID = uuid.NewString()
	if r.targetTopic == "" {
		r.targetTopic = "echo"
	}
	if r.artifactRef == "" {
		r.artifactRef = echo
	}
	data := map[string]any{
		"topic": spawner.TopicProvision, "requester_identity": f.channel, "origin_audience": "operator",
		"target_topic": r.targetTopic, "artifact_ref": r.artifactRef,
	}
	if r.associations != nil {
		data["egress_associations"] = r.associations
	}
	if r.entryTopic != "" {
		data["entry_topic"] = r.entryTopic
	}
	raw, _ := json.Marshal(data)
	dataDigest, err := digest.RequestData(raw)
	if err != nil {
		f.T.Fatal(err)
	}
	act, artifact := spawner.TopicProvision, r.artifactRef
	if r.proofAct != "" {
		act = r.proofAct
	}
	if r.proofArtifact != "" {
		artifact = r.proofArtifact
	}
	data["operator_proof"] = f.Sign(f.Root, signing.TypeOperatorProof, signing.OperatorProof{
		Act: act, TaskID: taskID, DataDigest: dataDigest, ArtifactRef: artifact,
	}, "")
	if r.tamperData {
		data["workspace"] = "another"
	}
	ev, err := bus.NewEvent(bus.TypeAnnounced, f.channel, taskID, data)
	if err != nil {
		f.T.Fatal(err)
	}
	ev.TraceParent, ev.ConfigVersion = "00-"+traceID+"-00f067aa0ba902b7-01", globalconfig.V0Version
	js := f.JetStream(f.Conn(bus.ChannelScope.Role, f.Identity("channel_operator")))
	if err := bus.Publish(f.Ctx, js, bus.SubjectAnnounced(spawner.TopicProvision), ev); err != nil {
		f.T.Fatal(err)
	}
	return taskID
}

func (f *fixture) outcome(taskID string) (typ string, data map[string]any) {
	f.T.Helper()
	timeout := time.After(15 * time.Second)
	for {
		select {
		case m := <-f.closes:
			var e bus.Event
			json.Unmarshal(m.Data, &e)
			if e.Subject != taskID {
				continue
			}
			json.Unmarshal(e.Data, &data)
			return e.Type, data
		case <-timeout:
			f.T.Fatalf("%s did not close", taskID)
		}
	}
}

// node_provision instantiates the entry it names: the presence mark, the
// network's row and the request's outcome are written, and the close names the
// node (architecture §3.13.3, §3.13.8).
func TestProvision(t *testing.T) {
	f := newFixture(t, time.Hour, false)
	assoc := []signing.Association{{Slot: "api", Entry: "a"}}
	taskID := f.send(request{associations: assoc})
	typ, data := f.outcome(taskID)
	if typ != bus.TypeCompleted || data["node_id"] != "UNODE1" {
		t.Fatalf("%s %v", typ, data)
	}
	act := f.driver.acts[0]
	if act.Topic != "echo" || act.Entry.ArtifactRef != echo || act.Associations["api"] != "a" || act.MemoryMB != 0 {
		t.Errorf("the act: %+v", act)
	}
	for _, key := range []string{"presence:" + echo, "embedding:" + echo, "memory:" + echo, "operator_request:" + taskID, "topic:echo"} {
		if !f.cat.has(key) {
			t.Errorf("%s was not written", key)
		}
	}

	// The second act of the entry is brought up with its effective reservation.
	f.cat.PutRecord(f.Ctx, network, "memory:"+echo, map[string]int64{"memory_required": 512}, catalog.Write{})
	if typ, _ := f.outcome(f.send(request{})); typ != bus.TypeCompleted || f.driver.acts[1].MemoryMB != 512 {
		t.Errorf("second: %s %+v", typ, f.driver.acts[1])
	}
}

// A request whose outcome is already stamped under its key returns it instead
// of executing again (architecture §3.13.8).
func TestStampedOutcome(t *testing.T) {
	f := newFixture(t, time.Hour, false)
	taskID := uuid.NewString()
	f.cat.PutRecord(f.Ctx, network, "operator_request:"+taskID,
		map[string]any{"type": bus.TypeCompleted, "data": map[string]any{"node_id": "UEARLIER"}}, catalog.Write{})
	sent := f.sendWithTask(taskID)
	typ, data := f.outcome(sent)
	if typ != bus.TypeCompleted || data["node_id"] != "UEARLIER" || f.driver.actCount() != 0 {
		t.Fatalf("%s %v, %d acts", typ, data, f.driver.actCount())
	}
}

// sendWithTask is send with a TaskID chosen by the test.
func (f *fixture) sendWithTask(taskID string) string {
	f.T.Helper()
	data := map[string]any{
		"topic": spawner.TopicProvision, "requester_identity": f.channel, "origin_audience": "operator",
		"target_topic": "echo", "artifact_ref": echo,
	}
	raw, _ := json.Marshal(data)
	dataDigest, _ := digest.RequestData(raw)
	data["operator_proof"] = f.Sign(f.Root, signing.TypeOperatorProof, signing.OperatorProof{
		Act: spawner.TopicProvision, TaskID: taskID, DataDigest: dataDigest, ArtifactRef: echo,
	}, "")
	ev, err := bus.NewEvent(bus.TypeAnnounced, f.channel, taskID, data)
	if err != nil {
		f.T.Fatal(err)
	}
	ev.TraceParent, ev.ConfigVersion = "00-"+strings.ReplaceAll(uuid.NewString(), "-", "")+"-00f067aa0ba902b7-01", globalconfig.V0Version
	js := f.JetStream(f.Conn(bus.ChannelScope.Role, f.Identity("channel_operator")))
	if err := bus.Publish(f.Ctx, js, bus.SubjectAnnounced(spawner.TopicProvision), ev); err != nil {
		f.T.Fatal(err)
	}
	return taskID
}

// Each layer rejects with its code, and nothing is provisioned.
func TestRejections(t *testing.T) {
	f := newFixture(t, time.Hour, false)
	other := "sha256:" + strings.Repeat("0f", 32)
	f.cat.add(catalog.Entry{
		Registration: catalog.Registration{Topic: "failing", ArtifactRef: other, CapabilityText: "x", AuthorID: "x", Requires: map[string]json.RawMessage{}},
		ScanResult:   catalog.ScanResult{Verdict: "fail", RulesetVersion: ruleset, Findings: []string{"secret"}},
	})
	stale := "sha256:" + strings.Repeat("1f", 32)
	f.cat.add(catalog.Entry{
		Registration: catalog.Registration{Topic: "stale", ArtifactRef: stale, CapabilityText: "x", AuthorID: "x", Requires: map[string]json.RawMessage{}},
		ScanResult:   catalog.ScanResult{Verdict: "pass", RulesetVersion: "sha256:older"},
	})
	vetoed := "sha256:" + strings.Repeat("2f", 32)
	f.cat.add(catalog.Entry{
		Registration: catalog.Registration{Topic: "vetoed", ArtifactRef: vetoed, CapabilityText: "x", AuthorID: "x", Requires: map[string]json.RawMessage{}},
		ScanResult:   catalog.ScanResult{Verdict: "pass", RulesetVersion: ruleset},
	})
	f.cat.PutRecord(f.Ctx, network, "veto:"+vetoed, map[string]any{}, catalog.Write{})

	for _, c := range []struct {
		name string
		req  request
		code string
	}{
		{"proof of another act", request{proofAct: "node_retire"}, "proof_invalid"},
		{"proof of another artifact", request{proofArtifact: other}, "proof_invalid"},
		{"data the proof does not cover", request{tamperData: true}, "proof_invalid"},
		{"a target_topic other than the entry's", request{targetTopic: "other"}, "request_invalid"},
		{"an entry the Catalog lacks", request{artifactRef: "sha256:" + strings.Repeat("ab", 32)}, "request_invalid"},
		{"an entry that failed its analysis", request{targetTopic: "failing", artifactRef: other}, "admission_failed"},
		{"an analysis of an earlier ruleset", request{targetTopic: "stale", artifactRef: stale}, "request_invalid"},
		{"a vetoed artifact", request{targetTopic: "vetoed", artifactRef: vetoed}, "artifact_vetoed"},
		{"a channel", request{targetTopic: "hitl_contact_slack"}, "request_invalid"},
		{"an entry topic", request{entryTopic: "intake"}, "request_invalid"},
		{"a slot associated twice", request{associations: []signing.Association{{Slot: "a", Entry: "x"}, {Slot: "a", Entry: "y"}}}, "request_invalid"},
	} {
		typ, data := f.outcome(f.send(c.req))
		if typ != bus.TypeFailed || data["failure_code"] != c.code {
			t.Errorf("%s: %s %v, want %s", c.name, typ, data, c.code)
		}
	}
	if n := f.driver.actCount(); n != 0 {
		t.Errorf("%d acts executed", n)
	}

	// A deletion stamped while the presence mark is written stops the act: the
	// mark goes before the deletion mark is read (architecture §3.13.8).
	f.cat.onPresence = func() {
		f.cat.mu.Lock()
		f.cat.entries[echo].DeletionMark = true
		f.cat.mu.Unlock()
	}
	if typ, data := f.outcome(f.send(request{})); typ != bus.TypeFailed || data["failure_code"] != "request_invalid" || f.driver.actCount() != 0 {
		t.Errorf("deleted: %s %v", typ, data)
	}
	f.cat.onPresence = nil
	f.cat.entries[echo].DeletionMark = false

	// The driver's refusal closes the request with its code.
	f.driver.refusal = spawner.Refuse(spawner.CodeCapacityExhausted, "full")
	if typ, data := f.outcome(f.send(request{})); typ != bus.TypeFailed || data["failure_code"] != "capacity_exhausted" {
		t.Errorf("capacity: %s %v", typ, data)
	}
}

// A network without Catalog credentials provisions nothing.
func TestNoCatalog(t *testing.T) {
	f := newFixture(t, time.Hour, true)
	if typ, data := f.outcome(f.send(request{})); typ != bus.TypeFailed || data["failure_code"] != "request_invalid" {
		t.Errorf("%s %v", typ, data)
	}
}

// The renewal renews each node's identity at a third of its life, rewrites
// the presence of what runs and removes that of what no longer runs
// (architecture §3.13.3, implementation §1.2.4).
func TestRenewal(t *testing.T) {
	gone := "sha256:" + strings.Repeat("3f", 32)
	f := newFixture(t, 6*time.Second, false)
	f.cat.PutRecord(f.Ctx, network, "presence:"+gone, map[string]any{}, catalog.Write{Expires: time.Hour})
	if typ, _ := f.outcome(f.send(request{})); typ != bus.TypeCompleted {
		t.Fatal(typ)
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		f.driver.mu.Lock()
		renewals := f.driver.renewals["UNODE1"]
		f.driver.mu.Unlock()
		if renewals >= 2 && !f.cat.has("presence:"+gone) && f.cat.has("presence:"+echo) {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Errorf("renewals %v, stale presence %v, own presence %v", f.driver.renewals, f.cat.has("presence:"+gone), f.cat.has("presence:"+echo))
}
