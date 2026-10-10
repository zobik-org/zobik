package globalconfig

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"zobik.org/zobik/internal/bus"
)

// The Config Store keeps the head in a KV bucket and every published version in
// an append-only stream (implementation §1.2.5). Each is the whole snapshot.

// HeadKey is the head's key in the config bucket.
const HeadKey = "head"

// Record is a published version: the whole snapshot, attributed and with its
// publication timestamp (architecture §2.3).
type Record struct {
	ConfigVersion string    `json:"config_version"`
	PublishedAt   time.Time `json:"published_at"`
	AuthorizedBy  string    `json:"authorized_by"`
	Values        Values    `json:"values"`
}

// V0Version is the version zobik init writes.
const V0Version = "v0"

// AuthorizedByDeployment attributes v0, which comes with the deployment.
const AuthorizedByDeployment = "deployment"

// HistorySubject is the subject a version is kept under.
func HistorySubject(version string) string { return bus.SubjectHistory + "." + version }

// WriteV0 writes rec as the first version, in the history and as the head, each
// only if absent: running it again converges, and never replaces a published version.
func WriteV0(ctx context.Context, js jetstream.JetStream, rec Record) error {
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	history, err := js.Stream(ctx, bus.StreamHistory)
	if err != nil {
		return err
	}
	subject := HistorySubject(rec.ConfigVersion)
	if _, err := history.GetLastMsgForSubject(ctx, subject); errors.Is(err, jetstream.ErrMsgNotFound) {
		if _, err := js.Publish(ctx, subject, data, jetstream.WithExpectLastSequencePerSubject(0)); err != nil {
			return fmt.Errorf("globalconfig: history %s: %w", rec.ConfigVersion, err)
		}
	} else if err != nil {
		return err
	}
	kv, err := js.KeyValue(ctx, bus.BucketConfig)
	if err != nil {
		return err
	}
	if _, err := kv.Create(ctx, HeadKey, data); err != nil && !errors.Is(err, jetstream.ErrKeyExists) {
		return fmt.Errorf("globalconfig: head: %w", err)
	}
	return nil
}

// Head reads the head and the revision a compare-and-swap writes against.
func Head(ctx context.Context, js jetstream.JetStream) (*Record, uint64, error) {
	kv, err := js.KeyValue(ctx, bus.BucketConfig)
	if err != nil {
		return nil, 0, err
	}
	e, err := kv.Get(ctx, HeadKey)
	if err != nil {
		return nil, 0, fmt.Errorf("globalconfig: head: %w", err)
	}
	var rec Record
	if err := json.Unmarshal(e.Value(), &rec); err != nil {
		return nil, 0, err
	}
	return &rec, e.Revision(), nil
}

// Version reads a published version from the history: what a trace that pinned
// it reads (architecture §2.3).
func Version(ctx context.Context, js jetstream.JetStream, version string) (*Record, error) {
	history, err := js.Stream(ctx, bus.StreamHistory)
	if err != nil {
		return nil, err
	}
	m, err := history.GetLastMsgForSubject(ctx, HistorySubject(version))
	if err != nil {
		return nil, fmt.Errorf("globalconfig: version %s: %w", version, err)
	}
	var rec Record
	if err := json.Unmarshal(m.Data, &rec); err != nil {
		return nil, err
	}
	return &rec, nil
}
