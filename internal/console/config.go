package console

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nkeys"
	"google.golang.org/grpc"

	"zobik.org/zobik/internal/configschema"
	"zobik.org/zobik/internal/digest"
	"zobik.org/zobik/internal/globalconfig"
	"zobik.org/zobik/internal/rpcauth"
	"zobik.org/zobik/internal/signing"
	channelv1 "zobik.org/zobik/proto/zobik/channel/v1"
)

// The administration affordance of architecture §3.17.1: present the current
// snapshot, accept assignments, emit config_change with the operator's proof,
// and return the outcome. The console carries; it derives no value.

// Dial connects to a channel's service as the console's platform identity
// (implementation §6, The service between the console and the channels).
func Dial(addr, consoleJWT string, key nkeys.KeyPair) (channelv1.ChannelClient, func() error, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(&rpcauth.Client{
		JWT: func() (string, error) { return consoleJWT, nil }, Key: key,
	}))
	if err != nil {
		return nil, nil, err
	}
	return channelv1.NewChannelClient(conn), conn.Close, nil
}

// Operator is the console's half of the Operator Channel: it signs with the
// root, and the channel it names carries the act (implementation §6, The root
// and the split Operator Channel).
type Operator struct {
	Root nkeys.KeyPair
	// ChannelID is the channel's platform identity, which the act names as its requester.
	ChannelID string
	Client    channelv1.ChannelClient
}

// Snapshot is the head of the Global Configuration.
type Snapshot struct {
	ConfigVersion string
	PublishedAt   string
	Values        globalconfig.Values
}

// Snapshot reads the head through the channel.
func (o *Operator) Snapshot(ctx context.Context) (*Snapshot, error) {
	s, err := o.Client.ReadConfig(ctx, &channelv1.ReadConfigRequest{})
	if err != nil {
		return nil, err
	}
	out := &Snapshot{ConfigVersion: s.GetConfigVersion(), PublishedAt: s.GetPublishedAt()}
	if err := json.Unmarshal(s.GetValues(), &out.Values); err != nil {
		return nil, err
	}
	return out, nil
}

// Outcome is how a config_change closed, as its delivery brought it.
type Outcome struct {
	TraceID string
	// ConfigVersion is the published version, on success.
	ConfigVersion string
	// FailureCode and FailureReason are the rejection's (architecture §3.14.4).
	FailureCode, FailureReason string
}

// ConfigSet applies assignments on the snapshot of version base: it validates
// them locally against the schema and its preconditions, signs the proof, opens
// the config_change and waits for the delivery of its outcome, which it
// confirms with received (architecture §3.14.4, §3.5.4).
func (o *Operator) ConfigSet(ctx context.Context, base *Snapshot, assignments map[string]json.RawMessage) (*Outcome, error) {
	schema, err := configschema.Load()
	if err != nil {
		return nil, err
	}
	if _, err := schema.ApplyVersioned(base.Values, assignments); err != nil {
		return nil, err
	}
	edit, err := json.Marshal(map[string]any{"base_version": base.ConfigVersion, "assignments": assignments})
	if err != nil {
		return nil, err
	}
	// The data the proof covers carries what the channel would otherwise stamp,
	// so it does not change after the signature (implementation §9.2).
	taskID := uuid.NewString()
	data := map[string]any{"topic": topicConfigChange, "requester_identity": o.ChannelID, "origin_audience": "operator"}
	raw, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	dataDigest, err := digest.RequestData(raw)
	if err != nil {
		return nil, err
	}
	proof, err := signing.Sign(o.Root, signing.TypeOperatorProof, signing.OperatorProof{
		Iat: time.Now().Unix(), Act: topicConfigChange, TaskID: taskID,
		DataDigest: dataDigest, PayloadDigest: digest.Bytes(edit),
	}, "")
	if err != nil {
		return nil, err
	}
	data["operator_proof"] = proof
	if raw, err = json.Marshal(data); err != nil {
		return nil, err
	}

	// The session goes first: with the console attached, the channel takes the
	// delivery in round 1 (implementation §6, The operator console).
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	session, err := o.Client.Attach(ctx)
	if err != nil {
		return nil, err
	}
	deliveries := make(chan delivery, 16)
	go receiveDeliveries(session, deliveries)

	open, err := o.Client.Open(ctx)
	if err != nil {
		return nil, err
	}
	open.Send(&channelv1.OpenTrace{Part: &channelv1.OpenTrace_Header{Header: &channelv1.OpenHeader{Topic: topicConfigChange, TaskId: taskID, Data: raw}}})
	open.Send(&channelv1.OpenTrace{Part: &channelv1.OpenTrace_Chunk{Chunk: edit}})
	ack, err := open.CloseAndRecv()
	if err != nil {
		return nil, err
	}
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case d, ok := <-deliveries:
			if !ok {
				return nil, errors.New("console: the channel closed the session")
			}
			if d.Evidence.TaskID != ack.GetEntryTaskId() {
				continue
			}
			// Showing the outcome is delivering it (implementation §6, The service
			// between the console and the channels).
			session.Send(&channelv1.ConsoleMessage{Body: &channelv1.ConsoleMessage_Shown{Shown: &channelv1.Shown{TaskId: d.taskID}}})
			if _, err := o.Client.Answer(ctx, &channelv1.AnswerRequest{TaskId: d.taskID, Data: []byte(`{"decision":"received"}`)}); err != nil {
				return nil, fmt.Errorf("console: confirming the delivery: %w", err)
			}
			return &Outcome{
				TraceID: ack.GetTraceId(), ConfigVersion: d.Details.Data.ConfigVersion,
				FailureCode: d.Details.FailureCode, FailureReason: d.Details.FailureReason,
			}, nil
		}
	}
}

const topicConfigChange = "config_change"

// delivery is a result_delivery query as the console reads it (architecture §3.5.4).
type delivery struct {
	taskID   string
	Kind     string `json:"kind"`
	Evidence struct {
		TaskID string `json:"task_id"`
	} `json:"evidence"`
	Details struct {
		Type          string `json:"type"`
		FailureCode   string `json:"failure_code"`
		FailureReason string `json:"failure_reason"`
		Data          struct {
			ConfigVersion string `json:"config_version"`
		} `json:"data"`
	} `json:"details"`
}

// receiveDeliveries passes on the result deliveries the session brings.
func receiveDeliveries(session channelv1.Channel_AttachClient, out chan<- delivery) {
	defer close(out)
	for {
		m, err := session.Recv()
		if err == io.EOF || err != nil {
			return
		}
		c := m.GetConsultation()
		if c == nil {
			continue
		}
		var e struct {
			Subject string          `json:"subject"`
			Data    json.RawMessage `json:"data"`
		}
		if json.Unmarshal(c.GetEvent(), &e) != nil {
			continue
		}
		var d delivery
		if json.Unmarshal(e.Data, &d) != nil || d.Kind != "result_delivery" {
			continue
		}
		d.taskID = e.Subject
		out <- d
	}
}
