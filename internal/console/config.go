package console

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nkeys"

	"zobik.org/zobik/internal/configschema"
	"zobik.org/zobik/internal/digest"
	"zobik.org/zobik/internal/globalconfig"
	"zobik.org/zobik/internal/signing"
	channelv1 "zobik.org/zobik/proto/zobik/channel/v1"
)

// The administration affordance of architecture §3.17.1: present the current
// snapshot, accept assignments and emit config_change with the operator's
// proof. The console carries; it derives no value.

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

// Opened is a trace the console opened through a channel.
type Opened struct {
	TraceID     string `json:"trace_id"`
	EntryTaskID string `json:"entry_task_id"`
}

// ConfigSet applies assignments on the snapshot of version base: it validates
// them locally against the schema and its preconditions, signs the proof and
// opens the config_change. Its outcome arrives as a delivery over the session
// (architecture §3.14.4, §3.5.4).
func (o *Operator) ConfigSet(ctx context.Context, base *Snapshot, assignments map[string]json.RawMessage) (*Opened, error) {
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
	return o.open(ctx, topicConfigChange, map[string]any{}, signing.OperatorProof{PayloadDigest: digest.Bytes(edit)}, edit)
}

// open signs the act on topic over data and proof's clear fields, and opens it
// through the channel with payload, if any, by Claim-Check. The data the proof
// covers carries what the channel would otherwise stamp, so it does not change
// after the signature (implementation §9.2).
func (o *Operator) open(ctx context.Context, topic string, data map[string]any, proof signing.OperatorProof, payload []byte) (*Opened, error) {
	taskID := uuid.NewString()
	data["topic"], data["requester_identity"], data["origin_audience"] = topic, o.ChannelID, "operator"
	raw, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	if proof.DataDigest, err = digest.RequestData(raw); err != nil {
		return nil, err
	}
	proof.Iat, proof.Act, proof.TaskID = time.Now().Unix(), topic, taskID
	signed, err := signing.Sign(o.Root, signing.TypeOperatorProof, proof, "")
	if err != nil {
		return nil, err
	}
	data["operator_proof"] = signed
	if raw, err = json.Marshal(data); err != nil {
		return nil, err
	}
	return o.send(ctx, topic, taskID, raw, payload)
}

// Query opens a query on topic, which carries no proof: what it asks changes
// nothing (architecture §3.7.6, §3.12.3, §3.13.3). Its outcome arrives as a
// delivery over the session.
func (o *Operator) Query(ctx context.Context, topic string) (*Opened, error) {
	return o.send(ctx, topic, "", []byte("{}"), nil)
}

// send opens the trace through the channel, with the entry subtask's TaskID
// if the console mints it.
func (o *Operator) send(ctx context.Context, topic, taskID string, raw, payload []byte) (*Opened, error) {
	open, err := o.Client.Open(ctx)
	if err != nil {
		return nil, err
	}
	parts := []*channelv1.OpenTrace{{Part: &channelv1.OpenTrace_Header{Header: &channelv1.OpenHeader{Topic: topic, TaskId: taskID, Data: raw}}}}
	if payload != nil {
		parts = append(parts, &channelv1.OpenTrace{Part: &channelv1.OpenTrace_Chunk{Chunk: payload}})
	}
	// A Send the channel ended returns io.EOF, and CloseAndRecv carries its status.
	for _, part := range parts {
		if err := open.Send(part); err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
	}
	ack, err := open.CloseAndRecv()
	if err != nil {
		return nil, err
	}
	return &Opened{TraceID: ack.GetTraceId(), EntryTaskID: ack.GetEntryTaskId()}, nil
}

const topicConfigChange = "config_change"
