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

// The point form is not answered until El ciclo de vida: the role does not
// propose on it (architecture §3.12.3).
func TestPointFormUnanswered(t *testing.T) {
	f := newFixture(t)
	proposals := f.Tap(bus.SubjectProposed(f.ID("network_monitor"), networkmonitor.Topic))
	f.query(map[string]any{"node_id": "UA", "target_topic": "spare"})
	select {
	case <-proposals:
		t.Fatal("the role proposed on the point form")
	case <-time.After(2 * time.Second):
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
