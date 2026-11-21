package spawner_test

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/digest"
	"zobik.org/zobik/internal/globalconfig"
	"zobik.org/zobik/internal/role/catalog"
	"zobik.org/zobik/internal/role/spawner"
	"zobik.org/zobik/internal/signing"
)

// retire publishes a node_retire of kind with the fields it names, signed by
// the operator's root, and returns its TaskID.
func (f *fixture) retire(kind string, fields map[string]any) string {
	f.T.Helper()
	taskID := uuid.NewString()
	data := map[string]any{
		"topic": spawner.TopicRetire, "requester_identity": f.channel, "origin_audience": "operator",
		"kind": kind, "target_topic": "echo",
	}
	for k, v := range fields {
		data[k] = v
	}
	raw, _ := json.Marshal(data)
	dataDigest, err := digest.RequestData(raw)
	if err != nil {
		f.T.Fatal(err)
	}
	data["operator_proof"] = f.Sign(f.Root, signing.TypeOperatorProof, signing.OperatorProof{
		Act: spawner.TopicRetire, TaskID: taskID, DataDigest: dataDigest,
	}, "")
	ev, err := bus.NewEvent(bus.TypeAnnounced, f.channel, taskID, data)
	if err != nil {
		f.T.Fatal(err)
	}
	ev.TraceParent, ev.ConfigVersion = "00-"+strings.ReplaceAll(uuid.NewString(), "-", "")+"-00f067aa0ba902b7-01", globalconfig.V0Version
	js := f.JetStream(f.Conn(bus.ChannelScope.Role, f.Identity("channel_operator")))
	if err := bus.Publish(f.Ctx, js, bus.SubjectAnnounced(spawner.TopicRetire), ev); err != nil {
		f.T.Fatal(err)
	}
	return taskID
}

// provisioned brings up a node of echo and returns its node_id.
func (f *fixture) provisioned() string {
	f.T.Helper()
	typ, data := f.outcome(f.send(request{}))
	if typ != bus.TypeCompleted {
		f.T.Fatalf("provisioning: %s %v", typ, data)
	}
	return data["node_id"].(string)
}

// remove archives the node in the order of architecture §3.13.3: the node
// leaves the fleet, the floor history gets its row with operator_removal and
// the snapshot the point query returned before the decommissioning, and
// without another instance the presence mark and the engine's image go.
func TestRemove(t *testing.T) {
	f := newFixture(t, time.Hour, false)
	first, second := f.provisioned(), f.provisioned()

	typ, data := f.outcome(f.retire(spawner.KindRemove, map[string]any{"node_id": first}))
	if typ != bus.TypeCompleted || !slices.Equal(ids(data), []string{first}) {
		t.Fatalf("%s %v", typ, data)
	}
	var row struct {
		TriggerReason string                     `json:"trigger_reason"`
		ModelID       string                     `json:"model_id"`
		Snapshot      map[string]json.RawMessage `json:"snapshot"`
	}
	if _, ok, _ := f.cat.Record(f.Ctx, network, "floor:"+echo+":0.5", &row); !ok || row.TriggerReason != "operator_removal" || row.ModelID != f.driver.modelID {
		t.Errorf("floor row %v %+v", ok, row)
	}
	if string(row.Snapshot["n_observations"]) != "0" {
		t.Errorf("the floor row's snapshot: %v", row.Snapshot)
	}
	if !f.cat.has("presence:" + echo) {
		t.Error("the presence mark went with an instance still running")
	}

	if typ, data := f.outcome(f.retire(spawner.KindRemove, map[string]any{"node_id": second})); typ != bus.TypeCompleted {
		t.Fatalf("second: %s %v", typ, data)
	}
	if f.cat.has("presence:" + echo) {
		t.Error("the presence mark stayed without instances")
	}
	if !slices.Equal(f.driver.images, []string{echo}) {
		t.Errorf("images deleted %v", f.driver.images)
	}

	// A node it did not provision, or a target_topic other than the node's, is rejected.
	for _, fields := range []map[string]any{
		{"node_id": "USTRUCTURAL"},
		{"node_id": f.provisioned(), "target_topic": "other"},
	} {
		if typ, data := f.outcome(f.retire(spawner.KindRemove, fields)); typ != bus.TypeFailed || data["failure_code"] != "request_invalid" {
			t.Errorf("%v: %s %v", fields, typ, data)
		}
	}
}

// A replica competes with the floor of the instance it replicates
// (architecture §3.13.3).
func TestReplicaFloor(t *testing.T) {
	f := newFixture(t, time.Hour, false)
	f.provisioned()
	f.driver.mu.Lock()
	f.driver.fleet[0].SimilarityFloor = 0.7
	f.driver.mu.Unlock()
	f.provisioned()
	if fl := f.driver.acts[1].Floor; fl == nil || *fl != 0.7 {
		t.Errorf("the replica's floor: %v", fl)
	}
}

// veto archives every node of the artifact and leaves it out of node_provision
// until unveto (architecture §3.13.8).
func TestVeto(t *testing.T) {
	f := newFixture(t, time.Hour, false)
	a, b := f.provisioned(), f.provisioned()
	typ, data := f.outcome(f.retire(spawner.KindVeto, map[string]any{"artifact_ref": echo}))
	if got := ids(data); typ != bus.TypeCompleted || len(got) != 2 || !slices.Contains(got, a) || !slices.Contains(got, b) {
		t.Fatalf("%s %v", typ, data)
	}
	if typ, data := f.outcome(f.send(request{})); typ != bus.TypeFailed || data["failure_code"] != "artifact_vetoed" {
		t.Errorf("provisioning vetoed: %s %v", typ, data)
	}
	if typ, data := f.outcome(f.retire(spawner.KindUnveto, map[string]any{"artifact_ref": echo})); typ != bus.TypeCompleted {
		t.Fatalf("unveto: %s %v", typ, data)
	}
	if fleet, _ := f.driver.Fleet(f.Ctx); len(fleet) != 0 {
		t.Errorf("unveto brought up %v", fleet)
	}
	f.provisioned()

	// An entry the Catalog lacks, or a target_topic other than the entry's, is rejected.
	for _, fields := range []map[string]any{
		{"artifact_ref": "sha256:" + strings.Repeat("ab", 32)},
		{"artifact_ref": echo, "target_topic": "other"},
	} {
		if typ, data := f.outcome(f.retire(spawner.KindVeto, fields)); typ != bus.TypeFailed || data["failure_code"] != "request_invalid" {
			t.Errorf("%v: %s %v", fields, typ, data)
		}
	}
}

// purge deletes the image only when no network runs the artifact, and never a
// seeded one (architecture §3.13.8).
func TestPurge(t *testing.T) {
	f := newFixture(t, time.Hour, false)
	f.cat.PutRecord(f.Ctx, "other", "presence:"+echo, map[string]any{}, catalog.Write{Expires: time.Hour})
	typ, data := f.outcome(f.retire(spawner.KindPurge, map[string]any{"artifact_ref": echo}))
	if typ != bus.TypeFailed || data["failure_code"] != "request_invalid" || !strings.Contains(data["failure_reason"].(string), "other") {
		t.Fatalf("running in another network: %s %v", typ, data)
	}
	if e, _, _ := f.cat.Entry(f.Ctx, echo); e.DeletionMark {
		t.Error("the rejected purge left the deletion mark")
	}
	f.cat.mu.Lock()
	delete(f.cat.records, "other/presence:"+echo)
	f.cat.mu.Unlock()

	f.cat.PutRecord(f.Ctx, "other", "acquisition:"+echo, map[string]any{"provenance": "seeded"}, catalog.Write{})
	if typ, data := f.outcome(f.retire(spawner.KindPurge, map[string]any{"artifact_ref": echo})); typ != bus.TypeFailed || data["failure_code"] != "request_invalid" {
		t.Fatalf("seeded: %s %v", typ, data)
	}
	f.cat.PutRecord(f.Ctx, "other", "acquisition:"+echo, map[string]any{"provenance": "imported"}, catalog.Write{})

	if typ, data := f.outcome(f.retire(spawner.KindPurge, map[string]any{"artifact_ref": echo})); typ != bus.TypeCompleted {
		t.Fatalf("%s %v", typ, data)
	}
	if e, _, _ := f.cat.Entry(f.Ctx, echo); !e.DeletionMark || !slices.Equal(f.registry.deleted, []string{echo}) {
		t.Errorf("mark %v, deleted %v", e.DeletionMark, f.registry.deleted)
	}
}

// The renewal archives the nodes whose entry has a deletion mark: the image
// was deleted while their presence mark had expired (architecture §3.13.8).
// It holds no task, so it asks for the snapshot in a trace it opens, with
// operator as the origin audience, and closes its root once the query closes
// (architecture §3.5.1, implementation §1.2.4).
func TestRenewalArchivesDeleted(t *testing.T) {
	f := newFixture(t, 6*time.Second, false)
	announced := f.Tap(bus.SubjectAnnounced(spawner.TopicNodeQuery))
	spawnerID := f.ID("spawner")
	closes := f.Tap(bus.SubjectTerminal(bus.TypeCompleted, spawnerID, spawner.TopicNodeQuery))
	id := f.provisioned()
	f.cat.SetDeletionMark(f.Ctx, echo, true)
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		f.driver.mu.Lock()
		done := slices.Contains(f.driver.decommissioned, id)
		f.driver.mu.Unlock()
		if done && f.cat.has("floor:"+echo+":0.5") {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	var row struct {
		Snapshot map[string]json.RawMessage `json:"snapshot"`
	}
	if _, ok, _ := f.cat.Record(f.Ctx, network, "floor:"+echo+":0.5", &row); !ok || row.Snapshot["n_observations"] == nil {
		t.Fatalf("decommissioned %v, floor row %v %v", f.driver.decommissioned, ok, row.Snapshot)
	}

	var query bus.Event
	select {
	case m := <-announced:
		json.Unmarshal(m.Data, &query)
	case <-time.After(5 * time.Second):
		t.Fatal("no point query")
	}
	var d struct {
		OriginAudience string `json:"origin_audience"`
	}
	json.Unmarshal(query.Data, &d)
	trace := bus.TraceID(query.TraceParent)
	if d.OriginAudience != "operator" || trace == "" || query.ConfigVersion == "" {
		t.Errorf("the query's trace: %q %q %q", d.OriginAudience, query.TraceParent, query.ConfigVersion)
	}
	for {
		select {
		case m := <-closes:
			var e bus.Event
			json.Unmarshal(m.Data, &e)
			if e.Subject == trace {
				return
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("the root of %s did not close", trace)
		}
	}
}

func ids(data map[string]any) []string {
	var out []string
	list, _ := data["node_ids"].([]any)
	for _, v := range list {
		s, _ := v.(string)
		out = append(out, s)
	}
	return out
}
