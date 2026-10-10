package bus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"

	"zobik.org/zobik/internal/schema"
	"zobik.org/zobik/schemas"
)

// The closed set of types (architecture §3.8.2).
const (
	TypeAnnounced = "task.announced"
	TypeProposed  = "task.proposed"
	TypeAssigned  = "task.assigned"
	TypeRejected  = "task.rejected"
	TypeCompleted = "task.completed"
	TypeFailed    = "task.failed"
	TypeAborted   = "task.aborted"
	TypeNotice    = "notice"
)

// Event is the envelope: CloudEvents in structured mode, JSON (implementation §1.2.1).
type Event struct {
	SpecVersion     string          `json:"specversion"`
	ID              string          `json:"id"`
	Type            string          `json:"type"`
	Source          string          `json:"source"`
	Subject         string          `json:"subject"`
	Time            time.Time       `json:"time"`
	TraceParent     string          `json:"traceparent,omitempty"`
	ConfigVersion   string          `json:"configversion,omitempty"`
	DataContentType string          `json:"datacontenttype"`
	Data            json.RawMessage `json:"data"`
}

// NewEvent fills the envelope's fixed attributes, a fresh id and the time.
func NewEvent(typ, source, subject string, data any) (*Event, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	return &Event{
		SpecVersion:     "1.0",
		ID:              uuid.NewString(),
		Type:            typ,
		Source:          source,
		Subject:         subject,
		Time:            time.Now().UTC(),
		DataContentType: "application/json",
		Data:            raw,
	}, nil
}

var compiled = sync.OnceValues(func() (map[string]*schema.Schema, error) {
	out := map[string]*schema.Schema{}
	for _, name := range []string{"envelope.schema.json", "data.schema.json"} {
		doc, err := schemas.FS.ReadFile(name)
		if err != nil {
			return nil, err
		}
		if out[name], err = schema.Compile(doc); err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
	}
	return out, nil
})

// commonData is what the envelope rules read from data.
type commonData struct {
	Topic         *string         `json:"topic"`
	TaskEmbedding *string         `json:"task_embedding"`
	FailureReason *string         `json:"failure_reason"`
	Metrics       json.RawMessage `json:"metrics"`
}

// Validate checks the event against the envelope's schema and its data against the
// schema of the common fields (implementation §10.1), then the rules of
// architecture §3.8 the schemas cannot state, which depend on the type.
func (e *Event) Validate() error {
	s, err := compiled()
	if err != nil {
		return err
	}
	doc, err := json.Marshal(e)
	if err != nil {
		return err
	}
	if err := s["envelope.schema.json"].ValidateJSON(doc); err != nil {
		return fmt.Errorf("envelope: %w", err)
	}
	if err := s["data.schema.json"].ValidateJSON(e.Data); err != nil {
		return fmt.Errorf("data: %w", err)
	}
	var d commonData
	if err := json.Unmarshal(e.Data, &d); err != nil {
		return fmt.Errorf("data: %w", err)
	}

	isTask := strings.HasPrefix(e.Type, "task.")
	switch {
	// architecture §3.8.1: traceparent in every task.* event; config_version follows it.
	case isTask && e.TraceParent == "":
		return errors.New("envelope: a task event carries traceparent")
	case (e.TraceParent == "") != (e.ConfigVersion == ""):
		return errors.New("envelope: configversion goes exactly where traceparent goes")
	// architecture §3.7.4: only the task.announced carries the task_embedding.
	case d.TaskEmbedding != nil && e.Type != TypeAnnounced:
		return errors.New("data: only task.announced carries task_embedding")
	// architecture §3.3, §3.10.
	case e.Type == TypeFailed && d.FailureReason == nil:
		return errors.New("data.failure_reason: required in task.failed")
	case (e.Type == TypeCompleted || e.Type == TypeFailed) && d.Metrics == nil:
		return errors.New("data.metrics: required in a terminal event")
	// architecture §3.8.2: a notice carries no metrics.
	case e.Type == TypeNotice && d.Metrics != nil:
		return errors.New("data: a notice carries no metrics")
	}
	return nil
}

// Publish validates the event and publishes it on subject, which has to be the
// shape its type travels on (implementation §1.2.1). The stream deduplicates by
// the event's id. Whoever publishes validates, before publishing (implementation §10.2).
func Publish(ctx context.Context, js jetstream.JetStream, subject string, e *Event) error {
	if err := e.Validate(); err != nil {
		return err
	}
	if err := checkSubject(subject, e); err != nil {
		return err
	}
	payload, err := json.Marshal(e)
	if err != nil {
		return err
	}
	_, err = js.Publish(ctx, subject, payload, jetstream.WithMsgID(e.ID))
	return err
}
