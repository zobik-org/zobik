package bus

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
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

// compiled holds every schema the binary brings, by file name (implementation §10.2).
var compiled = sync.OnceValues(func() (map[string]*schema.Schema, error) {
	out := map[string]*schema.Schema{}
	names, err := fs.Glob(schemas.FS, "*.schema.json")
	if err != nil {
		return nil, err
	}
	for _, name := range names {
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

// Schema returns a schema the binary brings, by file name.
func Schema(name string) (*schema.Schema, error) {
	s, err := compiled()
	if err != nil {
		return nil, err
	}
	if s[name] == nil {
		return nil, fmt.Errorf("bus: no schema %s", name)
	}
	return s[name], nil
}

// topicSchemas names the schemas of the data a fixed-convention topic fixes for
// typ, chosen by the topic and, in the hitl_contact_* family, by kind
// (implementation §10.1): <topic>.schema.json for the announcement and
// <topic>.completed.schema.json for its answer. A query whose kind has no schema
// of its own validates against the family's common part.
func topicSchemas(typ, topic, kind string) []string {
	if strings.HasPrefix(topic, hitlPrefix) {
		topic = "hitl_contact"
		if typ == TypeAnnounced {
			return []string{topic + "." + kind + ".schema.json", topic + ".schema.json"}
		}
	}
	switch typ {
	case TypeAnnounced:
		return []string{topic + ".schema.json"}
	case TypeCompleted:
		return []string{topic + ".completed.schema.json"}
	}
	return nil
}

// hitlPrefix is the prefix of the hitl_contact_<audience> family (architecture §3.5.3).
const hitlPrefix = "hitl_contact_"

// commonData is what the envelope rules read from data.
type commonData struct {
	Topic         *string         `json:"topic"`
	TaskEmbedding *string         `json:"task_embedding"`
	TaskText      *string         `json:"task_text"`
	Requester     *string         `json:"requester_identity"`
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
	// architecture §3.7.4: only the task.announced carries the task_embedding and
	// the text it comes from.
	case d.TaskEmbedding != nil && e.Type != TypeAnnounced:
		return errors.New("data: only task.announced carries task_embedding")
	case d.TaskText != nil && e.Type != TypeAnnounced:
		return errors.New("data: only task.announced carries task_text")
	// architecture §3.6.1: the terminal event of a task travels toward the
	// identity its announcement declares, as a token of its subject.
	case e.Type == TypeAnnounced && (d.Requester == nil || !singleTerm(*d.Requester)):
		return errors.New("data.requester_identity: required in task.announced, as a single term")
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

// ValidateTopic checks the data against the schema its topic fixes for its type,
// if it fixes one (implementation §10.1). Whoever assembles a payload applies it
// before publishing, and the claimant on receiving it; the Task Broker, which
// carries the request without reading it, does not (implementation §10.2).
func (e *Event) ValidateTopic() error {
	s, err := compiled()
	if err != nil {
		return err
	}
	var d struct {
		Topic *string `json:"topic"`
		Kind  string  `json:"kind"`
	}
	if err := json.Unmarshal(e.Data, &d); err != nil {
		return fmt.Errorf("data: %w", err)
	}
	if d.Topic == nil {
		return nil
	}
	for _, name := range topicSchemas(e.Type, *d.Topic, d.Kind) {
		if ts := s[name]; ts != nil {
			if err := ts.ValidateJSON(e.Data); err != nil {
				return fmt.Errorf("data of %s: %w", *d.Topic, err)
			}
			return nil
		}
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

// TraceID returns the trace a W3C traceparent names, or "" if it is not one.
func TraceID(traceparent string) string {
	parts := strings.Split(traceparent, "-")
	if len(parts) != 4 || len(parts[1]) != 32 {
		return ""
	}
	return parts[1]
}

// SpanID is the span of the task a task.assigned awards, from that event's id:
// the first 8 bytes of its SHA-256, or the next 8 if those are all zero, which
// W3C does not admit (implementation §1.2.6).
func SpanID(eventID string) string {
	sum := sha256.Sum256([]byte(eventID))
	span := sum[:8]
	if binary.BigEndian.Uint64(span) == 0 {
		span = sum[8:16]
	}
	return hex.EncodeToString(span)
}

// WithParent returns traceparent naming span as the parent, on the same trace
// and with the same flags, or "" if traceparent is not one.
func WithParent(traceparent, span string) string {
	parts := strings.Split(traceparent, "-")
	if len(parts) != 4 || len(parts[1]) != 32 {
		return ""
	}
	parts[2] = span
	return strings.Join(parts, "-")
}
