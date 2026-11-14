package spawner_test

import (
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/globalconfig"
	"zobik.org/zobik/internal/role/catalog"
	"zobik.org/zobik/internal/role/spawner"
	"zobik.org/zobik/internal/signing"
	contextv1 "zobik.org/zobik/proto/zobik/context/v1"
	nriv1 "zobik.org/zobik/proto/zobik/nri/v1"
)

// park is a Catalog with a topic in each state: echo runs, spare has an
// eligible entry that never ran, and barred's only entry is vetoed and was
// archived and capped. The network is under capacity pressure and without a
// user channel; storage pressure is another network's (architecture §3.7.6,
// §3.13.3).
func park(t *testing.T) (f *fixture, spare, barred string) {
	f = newFixture(t, time.Hour, false)
	spare = "sha256:" + strings.Repeat("5a", 32)
	barred = "sha256:" + strings.Repeat("ba", 32)
	f.entry("spare", spare, "spares", nil)
	f.entry("barred", barred, "bars", nil)
	f.runs("UNODE9", "echo", echo, time.Now())
	f.cat.PutRecord(f.Ctx, network, "veto:"+barred, map[string]any{}, catalog.Write{})
	f.cat.PutRecord(f.Ctx, network, "floor:"+barred+":0.6", map[string]any{
		"trigger_reason": "operator_removal", "model_id": f.driver.modelID, "archived_at": time.Now().UTC(),
	}, catalog.Write{})
	f.cat.PutRecord(f.Ctx, network, "acquisition:"+echo, map[string]any{
		"provenance": "seeded", "cost": 0, "as_of": time.Now().UTC(),
	}, catalog.Write{})
	f.cat.PutRecord(f.Ctx, "other", "presence:"+spare, map[string]any{}, catalog.Write{Expires: time.Hour})
	f.cat.PutRecord(f.Ctx, network, "niche:barred", map[string]any{"successive_candidates": 3, "capped": true}, catalog.Write{})
	f.cat.PutRecord(f.Ctx, network, "capacity_pressure", map[string]any{"since": time.Now().UTC(), "evictions": 4}, catalog.Write{})
	f.cat.PutRecord(f.Ctx, network, "audience:user", map[string]any{"since": time.Now().UTC()}, catalog.Write{})
	f.cat.PutRecord(f.Ctx, "other", "storage_pressure", map[string]any{"since": time.Now().UTC(), "occupied": 9, "capacity": 10}, catalog.Write{})
	f.cat.mu.Lock()
	f.cat.occupied, f.cat.capacity = 100, 1000
	f.cat.bundles = []catalog.Bundle{{BundleID: "sha256:bundle", PurposeText: "both", Members: []string{echo, spare}}}
	f.cat.mu.Unlock()
	return f, spare, barred
}

// query opens a query on topic from the operator's channel, and returns its
// trace and its TaskID.
func (f *fixture) query(topic string) (traceID, taskID string) {
	f.T.Helper()
	traceID, taskID = strings.ReplaceAll(uuid.NewString(), "-", ""), uuid.NewString()
	ev, err := bus.NewEvent(bus.TypeAnnounced, f.channel, taskID, map[string]any{
		"topic": topic, "requester_identity": f.channel, "origin_audience": "operator",
	})
	if err != nil {
		f.T.Fatal(err)
	}
	ev.TraceParent, ev.ConfigVersion = "00-"+traceID+"-00f067aa0ba902b7-01", globalconfig.V0Version
	js := f.JetStream(f.Conn(bus.ChannelScope.Role, f.Identity("channel_operator")))
	if err := bus.Publish(f.Ctx, js, bus.SubjectAnnounced(topic), ev); err != nil {
		f.T.Fatal(err)
	}
	return traceID, taskID
}

// topic_catalog_query lists every topic of the Topic Registry, with its
// description, its state and its capped mark, in the payload of its close (architecture §3.7.6).
func TestTopicCatalogQuery(t *testing.T) {
	f, _, _ := park(t)
	_, taskID := f.query(spawner.TopicTopicQuery)
	typ, data := f.outcome(taskID)
	if typ != bus.TypeCompleted {
		t.Fatalf("%s %v", typ, data)
	}
	raw, _ := json.Marshal(data["topics"])
	var topics []spawner.TopicRow
	if err := json.Unmarshal(raw, &topics); err != nil {
		t.Fatal(err)
	}
	want := []spawner.TopicRow{
		{Topic: "barred", Description: "bars", State: spawner.TopicUncovered, Capped: true},
		{Topic: "echo", Description: "repeats", State: spawner.TopicActive},
		{Topic: "spare", Description: "spares", State: spawner.TopicAvailable},
	}
	if len(topics) != len(want) {
		t.Fatalf("topics %+v", topics)
	}
	for i := range want {
		if topics[i] != want[i] {
			t.Errorf("topic %+v, want %+v", topics[i], want[i])
		}
	}
}

// node_catalog_query writes the library to the Context Store, each entry with
// its state, its floors, its acquisition, the network's veto and the networks
// present, with the network's conditions, and the close carries the reference
// (architecture §3.13.3).
func TestNodeCatalogQuery(t *testing.T) {
	f, spare, barred := park(t)
	traceID, taskID := f.query(spawner.TopicCatalogQuery)
	typ, data := f.outcome(taskID)
	if typ != bus.TypeCompleted {
		t.Fatalf("%s %v", typ, data)
	}
	ref, _ := data["context_ref"].(map[string]any)
	if ref["trace_id"] != traceID || ref["task_id"] != taskID || ref["name"] != "library" {
		t.Fatalf("context_ref %v", data["context_ref"])
	}
	var lib spawner.Library
	if err := json.Unmarshal(f.readPortion(traceID, taskID, "library"), &lib); err != nil {
		t.Fatal(err)
	}
	if lib.StorageOccupied != 100 || lib.StorageCapacity != 1000 || len(lib.Bundles) != 1 || len(lib.Bundles[0].Members) != 2 {
		t.Errorf("storage %d/%d, bundles %+v", lib.StorageOccupied, lib.StorageCapacity, lib.Bundles)
	}
	c := lib.Conditions
	if c.CapacityPressure == nil || c.StoragePressure != nil || c.VectorSpace != nil ||
		len(c.Audiences) != 1 || c.Audiences[0].Audience != "user" {
		t.Errorf("conditions %+v", c)
	}
	got := map[string]spawner.LibraryEntry{}
	for _, e := range lib.Entries {
		got[e.ArtifactRef] = e
	}
	if e := got[echo]; e.State != spawner.EntryActive || len(e.ActiveFloors) != 1 || e.ActiveFloors[0] != 0.5 ||
		e.Acquisition == nil || e.Acquisition.Provenance != "seeded" || e.VetoMark || e.CapabilityText != "repeats" {
		t.Errorf("echo %+v", e)
	}
	if e := got[spare]; e.State != spawner.EntryRegistered || len(e.PresentIn) != 1 || e.PresentIn[0] != "other" || e.Acquisition != nil {
		t.Errorf("spare %+v", e)
	}
	if e := got[barred]; e.State != spawner.EntryArchived || !e.VetoMark || len(e.FloorHistory) != 1 ||
		e.FloorHistory[0].SimilarityFloor != 0.6 || len(e.ActiveFloors) != 0 {
		t.Errorf("barred %+v", e)
	}
}

// readPortion reads a portion of the trace as the channel that opened it,
// with the root's token (architecture §3.14.3).
func (f *fixture) readPortion(traceID, section, name string) []byte {
	f.T.Helper()
	ch := f.Identity("channel_operator")
	token := f.Sign(ch, signing.TypeScopeToken, signing.ScopeToken{Sub: f.channel, TraceID: traceID, Section: traceID}, f.Credential("channel_operator"))
	s, err := f.ContextClient(bus.ChannelScope.Role, ch).Read(f.Ctx, &contextv1.StoreRead{
		Token: token, Ref: &nriv1.ContextRef{TraceId: traceID, TaskId: section, Name: name},
	})
	if err != nil {
		f.T.Fatal(err)
	}
	var out []byte
	for {
		c, err := s.Recv()
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			f.T.Fatalf("reading %s/%s: %v", section, name, err)
		}
		out = append(out, c.GetData()...)
	}
}
