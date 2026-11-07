package spawner_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/embedding"
	"zobik.org/zobik/internal/globalconfig"
	"zobik.org/zobik/internal/role/catalog"
	"zobik.org/zobik/internal/role/spawner"
	"zobik.org/zobik/internal/signing"
	"zobik.org/zobik/internal/testnet"
)

// requester is the identity the vacant tasks of the tests close toward.
const requester = "requester"

// fast makes the vacancy quick: one vacant round is enough, the backoff and
// the lease are short, and admission is open.
func fast(v globalconfig.Values) {
	testnet.Set(v, map[string]any{
		"network_admission": "open", "vacancy_rounds_threshold": 1,
		"reannounce_backoff_base": 200, "max_reannounce_backoff": 400, "task_lease_ttl": 1000,
		"catalog_presence_ttl": 5000,
	})
}

// vectors are the fixed embeddings of the tests: the task's, close to near's
// and far from far's.
var (
	taskVector = []float32{1, 0, 0, 0}
	nearVector = []float32{0.99, 0.14, 0, 0}
	farVector  = []float32{0.8, 0.6, 0, 0}
	echoVector = []float32{0.97, 0, 0.24, 0}
)

func newVacancy(t *testing.T, edits ...func(globalconfig.Values)) *fixture {
	f := newFixture(t, time.Hour, false, append([]func(globalconfig.Values){fast}, edits...)...)
	f.driver.vectors["repeats"] = echoVector
	f.failures = f.Tap(bus.TypeFailed + "." + requester + ".>")
	return f
}

// announce publishes a task on topic with the task's vector under modelID.
func (f *fixture) announce(topic, modelID string) string {
	f.T.Helper()
	taskID := uuid.NewString()
	data := map[string]any{
		"topic": topic, "requester_identity": requester,
		"task_embedding": embedding.Encode(taskVector), "model_id": modelID,
	}
	e, err := bus.NewEvent(bus.TypeAnnounced, requester, taskID, data)
	if err != nil {
		f.T.Fatal(err)
	}
	e.TraceParent = "00-" + strings.ReplaceAll(uuid.NewString(), "-", "") + "-00f067aa0ba902b7-01"
	e.ConfigVersion = globalconfig.V0Version
	if err := bus.Publish(f.Ctx, f.JetStream(f.Conn(bus.PublishScope.Role, nil)), bus.SubjectAnnounced(topic), e); err != nil {
		f.T.Fatal(err)
	}
	return taskID
}

// award publishes the award of a task to node, as if it had won it.
func (f *fixture) award(taskID, node, topic string) {
	f.T.Helper()
	e, err := bus.NewEvent(bus.TypeAssigned, "task_broker", taskID, map[string]any{"topic": topic, "scope_token": "x"})
	if err != nil {
		f.T.Fatal(err)
	}
	e.TraceParent = "00-" + strings.ReplaceAll(uuid.NewString(), "-", "") + "-00f067aa0ba902b7-01"
	e.ConfigVersion = globalconfig.V0Version
	if err := bus.Publish(f.Ctx, f.JetStream(f.Conn(bus.PublishScope.Role, nil)), bus.SubjectAddressed(bus.TypeAssigned, node), e); err != nil {
		f.T.Fatal(err)
	}
}

// failure waits for the task.failed of taskID toward the requester.
func (f *fixture) failure(taskID string, within time.Duration) map[string]any {
	f.T.Helper()
	timeout := time.After(within)
	for {
		select {
		case m := <-f.failures:
			var e bus.Event
			json.Unmarshal(m.Data, &e)
			if e.Subject != taskID {
				continue
			}
			var data map[string]any
			json.Unmarshal(e.Data, &data)
			return data
		case <-timeout:
			return nil
		}
	}
}

// eventually waits for cond.
func eventually(t *testing.T, within time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

func (f *fixture) entry(topic, digest, text string, w *signing.AttemptWindow) {
	f.cat.add(catalog.Entry{
		Registration: catalog.Registration{Topic: topic, ArtifactRef: digest, CapabilityText: text, AuthorID: "seeding",
			AttemptWindow: w, Requires: map[string]json.RawMessage{}},
		ScanResult: catalog.ScanResult{Verdict: "pass", RulesetVersion: ruleset},
	})
}

func (f *fixture) runs(id, topic, digest string, provisioned time.Time) {
	f.driver.mu.Lock()
	defer f.driver.mu.Unlock()
	f.driver.fleet = append(f.driver.fleet, spawner.Running{NodeID: id, Topic: topic, ArtifactRef: digest,
		ModelID: f.driver.modelID, SimilarityFloor: 0.5, MemoryMB: nodeMB, Provisioned: provisioned})
}

// A task vacant on a topic with an entry that would have proposed brings up a
// node of it; while the niche stays vacant each intervention counts, and at
// max_successive_candidates the niche is capped. An award to another node
// resets it (architecture §3.13.2, §3.13.5).
func TestVacancyProvisions(t *testing.T) {
	f := newVacancy(t)
	taskID := f.announce("echo", f.driver.modelID)
	if !eventually(t, 10*time.Second, func() bool { return f.driver.actCount() >= 1 }) {
		t.Fatal("no node was provisioned")
	}
	if act := f.driver.acts[0]; act.Topic != "echo" || act.Entry.ArtifactRef != echo {
		t.Errorf("the act: %+v", act)
	}
	if !f.cat.has("topic:echo") {
		t.Error("the topic was not registered")
	}

	// Nobody proposes, so the niche stays vacant until it is capped.
	var n struct {
		SuccessiveCandidates int  `json:"successive_candidates"`
		Capped               bool `json:"capped"`
	}
	capped := eventually(t, 30*time.Second, func() bool {
		_, ok, _ := f.cat.Record(f.Ctx, network, "niche:echo", &n)
		return ok && n.Capped
	})
	if !capped || n.SuccessiveCandidates != 3 || f.driver.actCount() != 3 {
		t.Fatalf("niche %+v, %d acts", n, f.driver.actCount())
	}
	time.Sleep(3 * time.Second)
	if c := f.driver.actCount(); c != 3 {
		t.Errorf("%d acts on a capped niche", c)
	}

	f.award(taskID, "UNODE1", "echo")
	if !eventually(t, 5*time.Second, func() bool { return !f.cat.has("niche:echo") }) {
		t.Error("the award did not reset the niche")
	}
}

// A task whose model_id is not the current one fails with stale_vector_space
// (architecture §3.13.2).
func TestVacancyStaleSpace(t *testing.T) {
	f := newVacancy(t)
	taskID := f.announce("echo", "sha256:old")
	data := f.failure(taskID, 10*time.Second)
	if data == nil || data["failure_code"] != "stale_vector_space" {
		t.Fatalf("%v", data)
	}
	if f.driver.actCount() != 0 {
		t.Error("it provisioned")
	}
}

// An emergent topic no entry claims and the Topic Registry lacks is redundant
// with a covered topic its task reaches; a topic an entry claims, even vetoed,
// and intake are not checked (architecture §3.13.2, §3.7.7).
func TestVacancyRedundantTopic(t *testing.T) {
	f := newVacancy(t)
	f.runs("UECHO", "echo", echo, time.Now())
	f.cat.PutRecord(f.Ctx, network, "topic:echo",
		map[string]any{"topic": "echo", "embedding": embedding.Encode(nearVector), "model_id": f.driver.modelID, "description": "repeats"},
		catalog.Write{})

	data := f.failure(f.announce("lookup", f.driver.modelID), 10*time.Second)
	if data == nil || data["failure_code"] != "redundant_topic" || data["suggested_topic"] != "echo" {
		t.Fatalf("lookup: %v", data)
	}

	vetoed := "sha256:" + strings.Repeat("4f", 32)
	f.entry("claimed", vetoed, "claims", nil)
	f.cat.PutRecord(f.Ctx, network, "veto:"+vetoed, map[string]any{}, catalog.Write{})
	for _, topic := range []string{"claimed", "intake"} {
		if data := f.failure(f.announce(topic, f.driver.modelID), 4*time.Second); data != nil {
			t.Errorf("%s: %v", topic, data)
		}
	}
	if f.driver.actCount() != 0 {
		t.Error("it provisioned")
	}
}

// When a stale row of the Topic Registry cannot be regenerated and nothing
// else answers, the Spawner abstains and stamps the vector space key
// (architecture §3.13.3, §3.13.7).
func TestVacancyAbstains(t *testing.T) {
	f := newVacancy(t)
	f.driver.down = true
	f.cat.PutRecord(f.Ctx, network, "topic:echo",
		map[string]any{"topic": "echo", "embedding": embedding.Encode(nearVector), "model_id": "sha256:old", "description": "repeats"},
		catalog.Write{})
	if data := f.failure(f.announce("lookup", f.driver.modelID), 4*time.Second); data != nil {
		t.Errorf("it failed the task: %v", data)
	}
	var key struct {
		Pending []string `json:"pending"`
	}
	if _, ok, _ := f.cat.Record(f.Ctx, network, "vector_space", &key); !ok || len(key.Pending) != 1 || key.Pending[0] != "topic:echo" {
		t.Errorf("vector space key %v %+v", ok, key)
	}
}

// Among the entries that would have proposed, one that runs goes first, then
// the closest; one whose attempt_window excludes the round does not compete
// (implementation §4.3).
func TestVacancyChooses(t *testing.T) {
	near, far, late := "sha256:"+strings.Repeat("5f", 32), "sha256:"+strings.Repeat("6f", 32), "sha256:"+strings.Repeat("7f", 32)
	from := 2
	for _, c := range []struct {
		name    string
		running string
		want    string
	}{
		{"the closest", "", near},
		{"the one that runs", far, far},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newVacancy(t)
			f.driver.vectors["near"], f.driver.vectors["far"], f.driver.vectors["late"] = nearVector, farVector, taskVector
			f.entry("pick", near, "near", nil)
			f.entry("pick", far, "far", nil)
			f.entry("pick", late, "late", &signing.AttemptWindow{From: &from})
			if c.running != "" {
				f.runs("URUNNING", "pick", c.running, time.Now())
			}
			f.announce("pick", f.driver.modelID)
			if !eventually(t, 10*time.Second, func() bool { return f.driver.actCount() >= 1 }) {
				t.Fatal("no node was provisioned")
			}
			if got := f.driver.acts[0].Entry.ArtifactRef; got != c.want {
				t.Errorf("provisioned %s, want %s", got, c.want)
			}
		})
	}
}

// Without room, node_provision evicts the free node with the oldest
// assignment, which archives with capacity_eviction and stamps the pressure;
// a busy node is not evicted, and without a free one the request closes with
// capacity_exhausted (architecture §3.13.10).
func TestEviction(t *testing.T) {
	other := "sha256:" + strings.Repeat("8f", 32)
	f := newVacancy(t, func(v globalconfig.Values) {
		testnet.Set(v, map[string]any{"eviction_pressure_threshold": 0})
	})
	f.driver.capacityMB = 2 * nodeMB
	f.runs("UOLD", "other", other, time.Now().Add(-2*time.Hour))
	f.runs("UNEW", "other", other, time.Now().Add(-time.Hour))
	time.Sleep(1500 * time.Millisecond) // the start's window, a task_lease_ttl

	if typ, data := f.outcome(f.send(request{})); typ != bus.TypeCompleted {
		t.Fatalf("%s %v", typ, data)
	}
	if got := f.driver.decommissioned; len(got) != 1 || got[0] != "UOLD" {
		t.Errorf("evicted %v", got)
	}
	var row struct {
		TriggerReason string `json:"trigger_reason"`
	}
	if _, ok, _ := f.cat.Record(f.Ctx, network, "floor:"+other+":0.5", &row); !ok || row.TriggerReason != "capacity_eviction" {
		t.Errorf("floor row %v %+v", ok, row)
	}
	if !f.cat.has("capacity_pressure") {
		t.Error("the capacity pressure was not stamped")
	}

	// UNODE1 never got a task, so it counts its provisioning: UNEW goes first.
	// Busy, UNEW stays and UNODE1 goes.
	f.award(uuid.NewString(), "UNEW", "other")
	time.Sleep(300 * time.Millisecond)
	if typ, data := f.outcome(f.send(request{})); typ != bus.TypeCompleted {
		t.Fatalf("%s %v", typ, data)
	}
	if got := f.driver.decommissioned; len(got) != 2 || got[1] != "UNODE1" {
		t.Errorf("evicted %v", got)
	}

	// The lease of an award nobody renews lasts a task_lease_ttl.
	f.award(uuid.NewString(), "UNEW", "other")
	f.award(uuid.NewString(), "UNODE2", "echo")
	time.Sleep(300 * time.Millisecond)
	if typ, data := f.outcome(f.send(request{})); typ != bus.TypeFailed || data["failure_code"] != "capacity_exhausted" {
		t.Errorf("all busy: %s %v", typ, data)
	}
}

// The renewal stamps the storage pressure above artifact_storage_pressure_share
// of the capacity, and deletes it below (architecture §3.13.3).
func TestStoragePressure(t *testing.T) {
	f := newVacancy(t)
	f.cat.mu.Lock()
	f.cat.occupied, f.cat.capacity = 90, 100
	f.cat.mu.Unlock()
	if !eventually(t, 10*time.Second, func() bool { return f.cat.has("storage_pressure") }) {
		t.Fatal("the storage pressure was not stamped")
	}
	f.cat.mu.Lock()
	f.cat.occupied = 10
	f.cat.mu.Unlock()
	if !eventually(t, 10*time.Second, func() bool { return !f.cat.has("storage_pressure") }) {
		t.Error("the storage pressure was not deleted")
	}
}
