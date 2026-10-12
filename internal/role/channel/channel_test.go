package channel_test

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/digest"
	"zobik.org/zobik/internal/globalconfig"
	"zobik.org/zobik/internal/role"
	"zobik.org/zobik/internal/role/channel"
	"zobik.org/zobik/internal/role/config"
	"zobik.org/zobik/internal/role/taskbroker"
	"zobik.org/zobik/internal/rpcauth"
	"zobik.org/zobik/internal/signing"
	"zobik.org/zobik/internal/testnet"
	channelv1 "zobik.org/zobik/proto/zobik/channel/v1"
	nriv1 "zobik.org/zobik/proto/zobik/nri/v1"
)

// fixture is a network with the Config Store, the Task Broker and the operator
// channel running, and the console played by the test.
type fixture struct {
	*testnet.Net
	addr    string
	channel string
	closes  chan *nats.Msg
}

func newFixture(t *testing.T) *fixture {
	n := testnet.New(t, func(v globalconfig.Values) {
		testnet.Set(v, map[string]any{"auction_timeout": 200})
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
	n.Start("channel_operator", func(ctx context.Context, rt *role.Runtime, nc *nats.Conn) error {
		return channel.Run(ctx, rt, nc, channel.Options{Audience: channel.AudienceOperator, Listen: addr, ContextAddr: n.ContextAddr})
	})
	id := n.ID("channel_operator")
	return &fixture{Net: n, addr: addr, channel: id, closes: n.Tap("task.*." + id + ".>")}
}

// client connects to the channel as kp, under the console's scope.
func (f *fixture) client(kp nkeys.KeyPair) channelv1.ChannelClient {
	f.T.Helper()
	pub, _ := kp.PublicKey()
	token, err := bus.IssueUser(f.Account.SigningKeys[bus.ConsoleScope.Role], f.Account.Public, pub, "console", time.Now().Add(time.Hour))
	if err != nil {
		f.T.Fatal(err)
	}
	conn, err := grpc.NewClient(f.addr, grpc.WithTransportCredentials(&rpcauth.Client{
		JWT: func() (string, error) { return token, nil }, Key: kp,
	}))
	if err != nil {
		f.T.Fatal(err)
	}
	f.T.Cleanup(func() { conn.Close() })
	return channelv1.NewChannelClient(conn)
}

// configSet opens a config_change as the console does: it signs the proof over
// the data and the edit, then opens the trace with them.
func (f *fixture) configSet(c channelv1.ChannelClient, base string, assignments map[string]any) string {
	f.T.Helper()
	edit, _ := json.Marshal(map[string]any{"base_version": base, "assignments": assignments})
	taskID := uuid.NewString()
	data := map[string]any{"topic": "config_change", "requester_identity": f.channel, "origin_audience": "operator"}
	raw, _ := json.Marshal(data)
	dataDigest, err := digest.RequestData(raw)
	if err != nil {
		f.T.Fatal(err)
	}
	data["operator_proof"] = f.Sign(f.Root, signing.TypeOperatorProof, signing.OperatorProof{
		Act: "config_change", TaskID: taskID, DataDigest: dataDigest, PayloadDigest: digest.Bytes(edit),
	}, "")
	raw, _ = json.Marshal(data)
	stream, err := c.Open(f.Ctx)
	if err != nil {
		f.T.Fatal(err)
	}
	stream.Send(&channelv1.OpenTrace{Part: &channelv1.OpenTrace_Header{Header: &channelv1.OpenHeader{Topic: "config_change", TaskId: taskID, Data: raw}}})
	stream.Send(&channelv1.OpenTrace{Part: &channelv1.OpenTrace_Chunk{Chunk: edit}})
	ack, err := stream.CloseAndRecv()
	if err != nil {
		f.T.Fatal(err)
	}
	if ack.GetEntryTaskId() != taskID || len(ack.GetTraceId()) != 32 {
		f.T.Fatalf("ack %+v", ack)
	}
	return ack.GetTraceId()
}

// rootClose waits for the terminal event of the root traceID.
func (f *fixture) rootClose(traceID string, within time.Duration) *bus.Event {
	f.T.Helper()
	timeout := time.After(within)
	for {
		select {
		case m := <-f.closes:
			var e bus.Event
			json.Unmarshal(m.Data, &e)
			if e.Subject == traceID {
				return &e
			}
		case <-timeout:
			return nil
		}
	}
}

type consultation struct {
	TaskID string
	Data   struct {
		Kind    string         `json:"kind"`
		Options []string       `json:"options"`
		Details map[string]any `json:"details"`
	}
}

func receive(t *testing.T, s channelv1.Channel_AttachClient) *channelv1.ChannelMessage {
	t.Helper()
	got := make(chan *channelv1.ChannelMessage, 1)
	go func() {
		m, err := s.Recv()
		if err == nil {
			got <- m
		}
	}()
	select {
	case m := <-got:
		return m
	case <-time.After(10 * time.Second):
		t.Fatal("nothing arrived over Attach")
	}
	return nil
}

func asConsultation(t *testing.T, m *channelv1.ChannelMessage) consultation {
	t.Helper()
	d := m.GetConsultation()
	if d == nil {
		t.Fatalf("not a consultation: %v", m)
	}
	var e bus.Event
	json.Unmarshal(d.GetEvent(), &e)
	var c consultation
	c.TaskID = e.Subject
	json.Unmarshal(e.Data, &c.Data)
	if _, ok := c.Data.Details["scope_token"]; ok {
		t.Error("the consultation carries a scope token")
	}
	return c
}

// The demo of the stage: the console reads the head, opens a config_change,
// receives the delivery of its outcome, confirms it, and the root closes.
func TestConfigSet(t *testing.T) {
	f := newFixture(t)
	c := f.client(f.ConsoleKey)
	snap, err := c.ReadConfig(f.Ctx, &channelv1.ReadConfigRequest{})
	if err != nil || snap.GetConfigVersion() != "v0" {
		t.Fatalf("ReadConfig: %v %v", snap, err)
	}
	session, err := c.Attach(f.Ctx)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	traceID := f.configSet(c, "v0", map[string]any{"reannounce_backoff_base": 6000})

	q := asConsultation(t, receive(t, session))
	if q.Data.Kind != "result_delivery" || len(q.Data.Options) != 1 || q.Data.Options[0] != "received" {
		t.Errorf("delivery %+v", q.Data)
	}
	if q.Data.Details["type"] != bus.TypeCompleted || q.Data.Details["data"].(map[string]any)["config_version"] != "v1" {
		t.Errorf("details %v", q.Data.Details)
	}
	session.Send(&channelv1.ConsoleMessage{Body: &channelv1.ConsoleMessage_Shown{Shown: &channelv1.Shown{TaskId: q.TaskID}}})
	if _, err := c.Answer(f.Ctx, &channelv1.AnswerRequest{TaskId: q.TaskID, Data: []byte(`{"decision":"received"}`)}); err != nil {
		t.Fatal(err)
	}
	if e := f.rootClose(traceID, 5*time.Second); e == nil || e.Type != bus.TypeCompleted {
		t.Errorf("root close %+v", e)
	}
	if snap, _ := c.ReadConfig(f.Ctx, &channelv1.ReadConfigRequest{}); snap.GetConfigVersion() != "v1" {
		t.Errorf("head after: %v", snap)
	}
	if _, err := c.Answer(f.Ctx, &channelv1.AnswerRequest{TaskId: q.TaskID, Data: []byte(`{}`)}); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("answering a closed query: %v", err)
	}
}

// A query shown and left without an answer when the session closes is closed
// unanswered; the next session receives its expired, and the delivery applies
// its outcome_if_silent and closes the root (architecture §3.5.4).
func TestSessionCloses(t *testing.T) {
	f := newFixture(t)
	c := f.client(f.ConsoleKey)
	ctx, cancel := context.WithCancel(f.Ctx)
	session, err := c.Attach(ctx)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	traceID := f.configSet(c, "v0", map[string]any{"reannounce_backoff_base": 6000})
	q := asConsultation(t, receive(t, session))
	session.Send(&channelv1.ConsoleMessage{Body: &channelv1.ConsoleMessage_Shown{Shown: &channelv1.Shown{TaskId: q.TaskID}}})
	time.Sleep(200 * time.Millisecond)
	cancel()

	if e := f.rootClose(traceID, 5*time.Second); e == nil || e.Type != bus.TypeCompleted {
		t.Errorf("root close %+v", e)
	}
	again, err := c.Attach(f.Ctx)
	if err != nil {
		t.Fatal(err)
	}
	ex := receive(t, again).GetExpired()
	if ex == nil || ex.GetTaskId() != q.TaskID || ex.GetFailureCode() != "unanswered" {
		t.Fatalf("expired %v", ex)
	}
	again.Send(&channelv1.ConsoleMessage{Body: &channelv1.ConsoleMessage_Received{Received: &nriv1.Received{DeliveryId: ex.GetDeliveryId()}}})
}

// Only the console's identity reaches the service, and Open admits only the
// channel's entries.
func TestAccess(t *testing.T) {
	f := newFixture(t)
	other, _ := nkeys.CreateUser()
	if _, err := f.client(other).ReadConfig(f.Ctx, &channelv1.ReadConfigRequest{}); status.Code(err) != codes.PermissionDenied {
		t.Errorf("another identity under the console's scope: %v", err)
	}
	c := f.client(f.ConsoleKey)
	for _, h := range []*channelv1.OpenHeader{
		{Topic: "intake", Data: []byte(`{}`)},
		{Topic: "config_change", Data: []byte(`{}`)},
	} {
		s, err := c.Open(f.Ctx)
		if err != nil {
			t.Fatal(err)
		}
		s.Send(&channelv1.OpenTrace{Part: &channelv1.OpenTrace_Header{Header: h}})
		if _, err := s.CloseAndRecv(); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: %v", h.Topic, err)
		}
	}
}
