package networkmonitor_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/globalconfig"
	"zobik.org/zobik/internal/role"
	"zobik.org/zobik/internal/role/networkmonitor"
	"zobik.org/zobik/internal/role/taskbroker"
	"zobik.org/zobik/internal/signing"
	"zobik.org/zobik/internal/testnet"
	contextv1 "zobik.org/zobik/proto/zobik/context/v1"
	nriv1 "zobik.org/zobik/proto/zobik/nri/v1"
)

var fleet = []networkmonitor.Node{
	{NodeID: "UB", ArtifactRef: "sha256:b", Topic: "echo", ModelID: "m", SignerID: "S", SimilarityFloor: 0.6,
		EgressGrants: json.RawMessage(`[{"slot":"api","entry":"a","interface":"sha256:i"}]`), IngressGrants: json.RawMessage(`[]`), MemoryRequired: 128},
	{NodeID: "UA", ArtifactRef: "sha256:a", Topic: "spare", ModelID: "m", SignerID: "S", SimilarityFloor: 0.5,
		EgressGrants: json.RawMessage(`[]`), IngressGrants: json.RawMessage(`[]`), MemoryRequired: 64},
}

type fixture struct {
	*testnet.Net
	channel string
	closes  chan *nats.Msg
}

func newFixture(t *testing.T) *fixture {
	n := testnet.New(t, func(v globalconfig.Values) { testnet.Set(v, map[string]any{"auction_timeout": 200}) })
	f := &fixture{Net: n, channel: n.ID("channel_operator"), closes: n.Tap("task.*." + n.ID("channel_operator") + ".*")}
	n.Start("network_monitor", func(ctx context.Context, rt *role.Runtime, nc *nats.Conn) error {
		return networkmonitor.Run(ctx, rt, nc, networkmonitor.Options{
			Fleet: func(context.Context) ([]networkmonitor.Node, error) {
				return append([]networkmonitor.Node(nil), fleet...), nil
			},
			ContextAddr: n.ContextAddr,
		})
	})
	n.Start("task_broker", taskbroker.Run)
	return f
}

func (f *fixture) query(data map[string]any) (traceID, taskID string) {
	f.T.Helper()
	traceID, taskID = strings.ReplaceAll(uuid.NewString(), "-", ""), uuid.NewString()
	data["topic"], data["requester_identity"], data["origin_audience"] = networkmonitor.Topic, f.channel, "operator"
	ev, err := bus.NewEvent(bus.TypeAnnounced, f.channel, taskID, data)
	if err != nil {
		f.T.Fatal(err)
	}
	ev.TraceParent, ev.ConfigVersion = "00-"+traceID+"-00f067aa0ba902b7-01", globalconfig.V0Version
	js := f.JetStream(f.Conn(bus.ChannelScope.Role, f.Identity("channel_operator")))
	if err := bus.Publish(f.Ctx, js, bus.SubjectAnnounced(networkmonitor.Topic), ev); err != nil {
		f.T.Fatal(err)
	}
	return traceID, taskID
}

// The census lists the fleet with the fields of the Active Node Registry, in
// the portion census of the query's section, and the close carries the
// reference (architecture §3.12.3).
func TestCensus(t *testing.T) {
	f := newFixture(t)
	traceID, taskID := f.query(map[string]any{})
	var e bus.Event
	select {
	case m := <-f.closes:
		json.Unmarshal(m.Data, &e)
	case <-time.After(15 * time.Second):
		t.Fatal("the census did not close")
	}
	var d struct {
		ContextRef nriv1.ContextRef `json:"context_ref"`
	}
	json.Unmarshal(e.Data, &d)
	if e.Type != bus.TypeCompleted || e.Subject != taskID || d.ContextRef.TaskId != taskID {
		t.Fatalf("close %s %s", e.Type, e.Data)
	}
	var census struct {
		Nodes []networkmonitor.Node `json:"nodes"`
	}
	if err := json.Unmarshal(f.read(traceID, taskID, "census"), &census); err != nil {
		t.Fatal(err)
	}
	if len(census.Nodes) != 2 || census.Nodes[0].NodeID != "UA" || census.Nodes[1].NodeID != "UB" ||
		census.Nodes[1].SimilarityFloor != 0.6 || census.Nodes[1].MemoryRequired != 128 || census.Nodes[1].SignerID != "S" {
		t.Errorf("census %+v", census.Nodes)
	}
}

// awaitClose waits for the close of taskID.
func (f *fixture) awaitClose(taskID string) bus.Event {
	f.T.Helper()
	deadline := time.After(15 * time.Second)
	for {
		select {
		case m := <-f.closes:
			var e bus.Event
			json.Unmarshal(m.Data, &e)
			if e.Subject == taskID {
				return e
			}
		case <-deadline:
			f.T.Fatalf("%s did not close", taskID)
		}
	}
}

// The point form on a provisioned node closes with its vector in the close;
// a node without observations has every signal that needs evidence absent
// (architecture §3.12.2, §3.12.3, implementation §10.1).
func TestPointForm(t *testing.T) {
	f := newFixture(t)
	_, taskID := f.query(map[string]any{"node_id": "UA", "target_topic": "spare"})
	e := f.awaitClose(taskID)
	if e.Type != bus.TypeCompleted {
		t.Fatalf("close %s %s", e.Type, e.Data)
	}
	var d struct {
		NodeID      string                     `json:"node_id"`
		TargetTopic string                     `json:"target_topic"`
		Signals     map[string]json.RawMessage `json:"signals"`
	}
	json.Unmarshal(e.Data, &d)
	if d.NodeID != "UA" || d.TargetTopic != "spare" || string(d.Signals["n_observations"]) != "0" {
		t.Fatalf("close %s", e.Data)
	}
	if _, ok := d.Signals["score"]; ok {
		t.Errorf("a node without observations carries a score: %s", e.Data)
	}
}

// The point form is rejected on a structural node, which is not in the
// roster, and with a target_topic other than the node's (architecture
// §3.12.3, implementation §1.2.8).
func TestPointFormRejected(t *testing.T) {
	f := newFixture(t)
	for _, c := range []struct{ name, node, topic string }{
		{"structural", f.ID("config"), "config_change"},
		{"other topic", "UA", "echo"},
	} {
		_, taskID := f.query(map[string]any{"node_id": c.node, "target_topic": c.topic})
		e := f.awaitClose(taskID)
		var d struct {
			FailureCode string `json:"failure_code"`
		}
		json.Unmarshal(e.Data, &d)
		if e.Type != bus.TypeFailed || d.FailureCode != "request_invalid" {
			t.Errorf("%s: close %s %s", c.name, e.Type, e.Data)
		}
	}
}

func (f *fixture) read(traceID, section, name string) []byte {
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
