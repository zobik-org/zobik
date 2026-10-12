package config_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/digest"
	"zobik.org/zobik/internal/globalconfig"
	"zobik.org/zobik/internal/role"
	"zobik.org/zobik/internal/role/config"
	"zobik.org/zobik/internal/role/taskbroker"
	"zobik.org/zobik/internal/signing"
	"zobik.org/zobik/internal/testnet"
	contextv1 "zobik.org/zobik/proto/zobik/context/v1"
)

// fixture is a network with the Task Broker and the Config Store running, and
// the operator's channel played by the test: it opens a trace, writes the edit
// in its section and announces config_change with the proof the console signs.
type fixture struct {
	*testnet.Net
	// channel is the operator channel's identity, which the test plays.
	channel string
	ctxc    contextv1.ContextStoreClient
	closes  chan *nats.Msg
}

func newFixture(t *testing.T) *fixture {
	n := testnet.New(t, func(v globalconfig.Values) {
		testnet.Set(v, map[string]any{"auction_timeout": 200})
	})
	n.Start("config", func(ctx context.Context, rt *role.Runtime, nc *nats.Conn) error {
		return config.Run(ctx, rt, nc, config.Options{ContextAddr: n.ContextAddr})
	})
	n.Start("task_broker", taskbroker.Run)
	return &fixture{
		Net:     n,
		channel: n.ID("channel_operator"),
		ctxc:    n.ContextClient(bus.ChannelScope.Role, n.Identity("channel_operator")),
		closes:  n.Tap("task.*." + n.ID("channel_operator") + ".config_change"),
	}
}

// edit is a request as the console assembles it; each field can be spoiled.
type edit struct {
	payload    []byte
	proofTask  string
	proofAct   string
	tamperData bool
	tamperBody bool
	noPayload  bool
}

func payload(base string, assignments map[string]any) []byte {
	b, _ := json.Marshal(map[string]any{"base_version": base, "assignments": assignments})
	return b
}

// send opens a trace, writes the edit in the root's section with the root's
// token, and announces the entry subtask on config_change (implementation §6,
// The service between the console and the channels).
func (f *fixture) send(e edit) (taskID string) {
	f.T.Helper()
	traceID := strings.ReplaceAll(uuid.NewString(), "-", "")
	taskID = uuid.NewString()
	ch := f.Identity("channel_operator")
	if !e.noPayload {
		token := f.Sign(ch, signing.TypeScopeToken, signing.ScopeToken{Sub: f.channel, TraceID: traceID, Section: traceID}, f.Credential("channel_operator"))
		w, err := f.ctxc.Write(f.Ctx)
		if err != nil {
			f.T.Fatal(err)
		}
		w.Send(&contextv1.StoreWrite{Body: &contextv1.StoreWrite_Header{Header: &contextv1.StoreWriteHeader{Token: token, Name: "edit"}}})
		w.Send(&contextv1.StoreWrite{Body: &contextv1.StoreWrite_Chunk{Chunk: e.payload}})
		if _, err := w.CloseAndRecv(); err != nil {
			f.T.Fatal(err)
		}
	}
	data := map[string]any{"topic": config.Topic, "requester_identity": f.channel}
	raw, _ := json.Marshal(data)
	dataDigest, err := digest.RequestData(raw)
	if err != nil {
		f.T.Fatal(err)
	}
	signed := e.payload
	if e.tamperBody {
		signed = append([]byte(" "), e.payload...)
	}
	act, proofTask := config.Topic, taskID
	if e.proofAct != "" {
		act = e.proofAct
	}
	if e.proofTask != "" {
		proofTask = e.proofTask
	}
	data["operator_proof"] = f.Sign(f.Root, signing.TypeOperatorProof, signing.OperatorProof{
		Act: act, TaskID: proofTask, DataDigest: dataDigest, PayloadDigest: digest.Bytes(signed),
	}, "")
	data["context_ref"] = map[string]any{"trace_id": traceID, "task_id": traceID, "name": "edit"}
	if e.tamperData {
		data["workspace"] = "another"
	}
	ev, err := bus.NewEvent(bus.TypeAnnounced, f.channel, taskID, data)
	if err != nil {
		f.T.Fatal(err)
	}
	ev.TraceParent, ev.ConfigVersion = "00-"+traceID+"-00f067aa0ba902b7-01", globalconfig.V0Version
	js := f.JetStream(f.Conn(bus.ChannelScope.Role, ch))
	if err := bus.Publish(f.Ctx, js, bus.SubjectAnnounced(config.Topic), ev); err != nil {
		f.T.Fatal(err)
	}
	return taskID
}

// outcome waits for the close of taskID.
func (f *fixture) outcome(taskID string) (typ string, data map[string]any) {
	f.T.Helper()
	timeout := time.After(10 * time.Second)
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

func (f *fixture) head() *globalconfig.Record {
	f.T.Helper()
	h, _, err := globalconfig.Head(f.Ctx, f.Console)
	if err != nil {
		f.T.Fatal(err)
	}
	return h
}

// A valid edit publishes the next version, whole and attributed, in the history
// and as the head, and closes with its config_version.
func TestPublish(t *testing.T) {
	f := newFixture(t)
	taskID := f.send(edit{payload: payload("v0", map[string]any{"reannounce_backoff_base": 6000})})
	typ, data := f.outcome(taskID)
	if typ != bus.TypeCompleted || data["config_version"] != "v1" {
		t.Fatalf("%s %v", typ, data)
	}
	head := f.head()
	if head.ConfigVersion != "v1" || head.TaskID != taskID || head.AuthorizedBy != "operator" ||
		string(head.Values["reannounce_backoff_base"]) != "6000" || head.Values["auction_timeout"] == nil {
		t.Errorf("head %+v", head)
	}
	v1, err := globalconfig.Version(f.Ctx, f.Console, "v1")
	if err != nil || v1.TaskID != taskID {
		t.Errorf("history v1: %+v %v", v1, err)
	}
	if v0, err := globalconfig.Version(f.Ctx, f.Console, "v0"); err != nil || string(v0.Values["reannounce_backoff_base"]) == "6000" {
		t.Errorf("v0 was modified: %+v %v", v0, err)
	}

	// An edit on v0 once v1 is the head is stale.
	typ, data = f.outcome(f.send(edit{payload: payload("v0", map[string]any{"reannounce_backoff_base": 7000})}))
	if typ != bus.TypeFailed || data["failure_code"] != "base_version_stale" {
		t.Errorf("stale: %s %v", typ, data)
	}
}

// Each layer rejects with its code, and nothing is published.
func TestRejections(t *testing.T) {
	f := newFixture(t)
	ok := payload("v0", map[string]any{"reannounce_backoff_base": 6000})
	for _, c := range []struct {
		name string
		edit edit
		code string
	}{
		{"proof of another task", edit{payload: ok, proofTask: "another"}, "proof_invalid"},
		{"proof of another act", edit{payload: ok, proofAct: "node_retire"}, "proof_invalid"},
		{"data the proof does not cover", edit{payload: ok, tamperData: true}, "proof_invalid"},
		{"edit the proof does not cover", edit{payload: ok, tamperBody: true}, "proof_invalid"},
		{"no edit in the Context Store", edit{payload: ok, noPayload: true}, "request_invalid"},
		{"edit without base_version", edit{payload: []byte(`{"assignments":{}}`)}, "request_invalid"},
		{"unknown key", edit{payload: payload("v0", map[string]any{"no_such_key": 1})}, "request_invalid"},
		{"out of range", edit{payload: payload("v0", map[string]any{"auction_timeout": 0})}, "request_invalid"},
		// The network opens only with embedding_model assigned (architecture §2.3).
		{"unsatisfied precondition", edit{payload: payload("v0", map[string]any{"network_admission": "open"})}, "request_invalid"},
	} {
		typ, data := f.outcome(f.send(c.edit))
		if typ != bus.TypeFailed || data["failure_code"] != c.code || data["failure_reason"] == "" {
			t.Errorf("%s: %s %v", c.name, typ, data)
		}
	}
	if head := f.head(); head.ConfigVersion != "v0" {
		t.Errorf("a rejected edit published %s", head.ConfigVersion)
	}
}

// A publication that stopped after the history and before the head is finished
// when the Store starts, and the same task closes with that version.
func TestRollForward(t *testing.T) {
	n := testnet.New(t, nil)
	head, rev, err := globalconfig.Head(n.Ctx, n.Console)
	if err != nil {
		t.Fatal(err)
	}
	v1 := *head
	v1.ConfigVersion, v1.TaskID = "v1", "T1"
	data, _ := json.Marshal(v1)
	if _, err := n.Console.Publish(n.Ctx, globalconfig.HistorySubject("v1"), data); err != nil {
		t.Fatal(err)
	}
	n.Start("config", func(ctx context.Context, rt *role.Runtime, nc *nats.Conn) error {
		return config.Run(ctx, rt, nc, config.Options{ContextAddr: n.ContextAddr})
	})
	got, newRev, err := globalconfig.Head(n.Ctx, n.Console)
	if err != nil || got.ConfigVersion != "v1" || newRev == rev {
		t.Errorf("head after starting: %+v %v", got, err)
	}
}
