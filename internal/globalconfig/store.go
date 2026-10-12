package globalconfig

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
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
	// TaskID is the config_change that published the version; v0 has none.
	TaskID string `json:"task_id,omitempty"`
	Values Values `json:"values"`
}

// V0Version is the version zobik init writes.
const V0Version = "v0"

// AuthorizedByDeployment attributes v0, which comes with the deployment, and
// AuthorizedByOperator every version a config_change publishes with the
// operator's proof (architecture §3.14.4).
const (
	AuthorizedByDeployment = "deployment"
	AuthorizedByOperator   = "operator"
)

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

// NextVersion is the version that follows v in the sequence: v0, v1, v2…
func NextVersion(v string) (string, error) {
	n, err := versionNumber(v)
	if err != nil {
		return "", err
	}
	return "v" + strconv.Itoa(n+1), nil
}

func versionNumber(v string) (int, error) {
	n, err := strconv.Atoi(strings.TrimPrefix(v, "v"))
	if err != nil || !strings.HasPrefix(v, "v") || n < 0 {
		return 0, fmt.Errorf("globalconfig: %q is not a config_version", v)
	}
	return n, nil
}

// ErrHeadMoved is a head that is no longer at the revision a publication read.
var ErrHeadMoved = errors.New("globalconfig: the head moved")

// Publish publishes rec as the version that follows the head read at headRev:
// first in the history, then as the head with a CAS on that revision
// (implementation §1.2.5). The history is the record, so a version that is
// already there under the same TaskID is the one a previous attempt left, and
// the head catches up with it.
func Publish(ctx context.Context, js jetstream.JetStream, headRev uint64, rec Record) error {
	data, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	subject := HistorySubject(rec.ConfigVersion)
	_, err = js.Publish(ctx, subject, data, jetstream.WithExpectLastSequencePerSubject(0))
	if err != nil {
		prev, gerr := Version(ctx, js, rec.ConfigVersion)
		if gerr != nil || prev.TaskID != rec.TaskID || rec.TaskID == "" {
			return fmt.Errorf("globalconfig: history %s: %w", rec.ConfigVersion, err)
		}
		if data, err = json.Marshal(prev); err != nil {
			return err
		}
	}
	kv, err := js.KeyValue(ctx, bus.BucketConfig)
	if err != nil {
		return err
	}
	if _, err := kv.Update(ctx, HeadKey, data, headRev); err != nil {
		if errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
			return ErrHeadMoved
		}
		return fmt.Errorf("globalconfig: head: %w", err)
	}
	return nil
}

// RollForward brings the head to the last version of the history when a
// publication stopped between the two writes, and returns the head as it ends.
func RollForward(ctx context.Context, js jetstream.JetStream) (*Record, uint64, error) {
	head, rev, err := Head(ctx, js)
	if err != nil {
		return nil, 0, err
	}
	history, err := js.Stream(ctx, bus.StreamHistory)
	if err != nil {
		return nil, 0, err
	}
	m, err := history.GetMsg(ctx, history.CachedInfo().State.LastSeq)
	if err != nil {
		return nil, 0, err
	}
	var last Record
	if err := json.Unmarshal(m.Data, &last); err != nil {
		return nil, 0, err
	}
	headN, err := versionNumber(head.ConfigVersion)
	if err != nil {
		return nil, 0, err
	}
	lastN, err := versionNumber(last.ConfigVersion)
	if err != nil || lastN <= headN {
		return head, rev, err
	}
	kv, err := js.KeyValue(ctx, bus.BucketConfig)
	if err != nil {
		return nil, 0, err
	}
	rev, err = kv.Update(ctx, HeadKey, m.Data, rev)
	if err != nil {
		return nil, 0, fmt.Errorf("globalconfig: rolling the head forward: %w", err)
	}
	return &last, rev, nil
}
