package claim

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"

	"zobik.org/zobik/internal/bus"
)

// AudienceOperator is the origin audience of a trace a structural role opens
// for its own work: the tenant that operates the network answers for it
// (architecture §3.5.1, §3.5.3).
const AudienceOperator = "operator"

// Root is a trace a structural role opens for work it originates on its own,
// with no task to announce it from (architecture §3.5.1). The role holds the
// root and announces the work as a subtask of it, on Topic; when that subtask
// closes, it closes the root itself, without a delivery.
type Root struct {
	TraceID       string    `json:"trace_id"`
	TraceParent   string    `json:"traceparent"`
	ConfigVersion string    `json:"config_version"`
	Topic         string    `json:"topic"`
	OpenedAt      time.Time `json:"opened_at"`
}

// OpenRoot generates the trace, pinned to configVersion, whose subtask goes on
// topic: opening the trace pins the configuration (architecture §3.5.1).
func OpenRoot(configVersion, topic string) Root {
	trace, span := make([]byte, 16), make([]byte, 8)
	rand.Read(trace)
	rand.Read(span)
	id := hex.EncodeToString(trace)
	return Root{
		TraceID: id, TraceParent: "00-" + id + "-" + hex.EncodeToString(span) + "-01",
		ConfigVersion: configVersion, Topic: topic, OpenedAt: time.Now().UTC(),
	}
}

// Inherited is what the subtask's announcement carries from the trace: the
// origin audience, and no workspace (architecture §3.5.1).
func (r Root) Inherited() map[string]any {
	return map[string]any{"origin_audience": AudienceOperator}
}

// Announce stamps the trace on the announcement of the subtask.
func (r Root) Announce(e *bus.Event) {
	e.TraceParent, e.ConfigVersion = r.TraceParent, r.ConfigVersion
}

// Close publishes the task.completed of the root, addressed to the role itself,
// which is its requester (architecture §3.5.1, §3.9.1). Its id comes from the
// TraceID, so the stream deduplicates a close repeated after a restart.
func (r Root) Close(ctx context.Context, js jetstream.JetStream, identity string) error {
	data := map[string]any{
		"topic":   r.Topic,
		"metrics": map[string]any{"tokens_in": 0, "tokens_out": 0, "cost": 0, "duration_ms": time.Since(r.OpenedAt).Milliseconds()},
	}
	e, err := bus.NewEvent(bus.TypeCompleted, identity, r.TraceID, data)
	if err != nil {
		return err
	}
	e.ID = uuid.NewSHA1(uuid.NameSpaceOID, []byte("root-close:"+r.TraceID)).String()
	e.TraceParent, e.ConfigVersion = r.TraceParent, r.ConfigVersion
	return bus.Publish(ctx, js, bus.SubjectTerminal(bus.TypeCompleted, identity, r.Topic), e)
}
