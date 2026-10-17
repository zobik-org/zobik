package tracingcollector

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"zobik.org/zobik/internal/bus"
)

// fact is what the role keeps of an observed event: the fields the spans are
// built from, without the credential, the scope token or the embedding.
type fact struct {
	ID   string `json:"id"`
	Type string `json:"type"`
	// Stored is when the stream stored the event, by the NATS server's clock,
	// which is the same for every family: it orders the events of a trace
	// and is what the readers' progress is measured in.
	Stored time.Time `json:"stored"`
	// Time is the envelope's: when the event happened, which the span shows.
	Time time.Time `json:"time"`
	// Task is the envelope's subject: the task the event is about.
	Task   string `json:"task"`
	Source string `json:"source"`
	// Node is the identity the subject names: the proposer, or the recipient of
	// an award or a rejection (implementation §1.2.1).
	Node string `json:"node,omitempty"`
	// Parent is the span the traceparent names (architecture §3.2).
	Parent        string   `json:"parent"`
	ConfigVersion string   `json:"config_version,omitempty"`
	Data          factData `json:"data"`
}

// factData are the fields of data the spans carry (implementation §1.2.6).
type factData struct {
	Topic              *string  `json:"topic,omitempty"`
	TaskText           *string  `json:"task_text,omitempty"`
	Workspace          *string  `json:"workspace,omitempty"`
	CurrentAttempts    *int64   `json:"current_attempts,omitempty"`
	PreviousAttempt    *string  `json:"previous_attempt,omitempty"`
	Similarity         *float64 `json:"similarity,omitempty"`
	DeclaredSimilarity *float64 `json:"declared_similarity,omitempty"`
	FailureCode        *string  `json:"failure_code,omitempty"`
	FailureReason      *string  `json:"failure_reason,omitempty"`
	Metrics            *metrics `json:"metrics,omitempty"`
	Kind               *string  `json:"kind,omitempty"`
}

type metrics struct {
	TokensIn   int64   `json:"tokens_in"`
	TokensOut  int64   `json:"tokens_out"`
	Cost       float64 `json:"cost"`
	DurationMs int64   `json:"duration_ms"`
}

// newFact reads an event as the role keeps it, or returns false if it carries
// no traceparent: the subscription is defined by carrying it (architecture §3.2).
func newFact(e *bus.Event, subject string, stored time.Time) (fact, string, bool) {
	trace := bus.TraceID(e.TraceParent)
	if trace == "" {
		return fact{}, "", false
	}
	f := fact{
		ID: e.ID, Type: e.Type, Stored: stored, Time: e.Time, Task: e.Subject, Source: e.Source,
		Parent: strings.Split(e.TraceParent, "-")[2], ConfigVersion: e.ConfigVersion,
	}
	_ = json.Unmarshal(e.Data, &f.Data) // a field of another type reads as absent
	// prop.<identity>.<topic>, task.assigned.<identity>, task.rejected.<identity>.
	tokens := strings.Split(subject, ".")
	switch {
	case e.Type == bus.TypeProposed && len(tokens) == 3:
		f.Node = tokens[1]
	case (e.Type == bus.TypeAssigned || e.Type == bus.TypeRejected) && len(tokens) == 3:
		f.Node = tokens[2]
	}
	return f, trace, true
}

// trace is what the role keeps of a live trace.
type trace struct {
	ID    string `json:"trace_id"`
	Facts []fact `json:"facts"`
	// Exported are the spans the backend accepted, by span ID.
	Exported map[string]bool `json:"exported,omitempty"`
	// Last is when the role applied the trace's last event.
	Last time.Time `json:"last"`
}

// add keeps f, unless the trace already has it: a redelivery.
func (t *trace) add(f fact) bool {
	for _, have := range t.Facts {
		if have.ID == f.ID {
			return false
		}
	}
	t.Facts = append(t.Facts, f)
	return true
}

// traces keeps each live trace in a file of its own, traces/<TraceID>.json.
type traces struct{ dir string }

func openTraces(volume string) (*traces, error) {
	dir := filepath.Join(volume, "traces")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	return &traces{dir: dir}, nil
}

func (s *traces) path(traceID string) string { return filepath.Join(s.dir, traceID+".json") }

// save writes the trace through a temporary file, with fsync on the file and on
// the directory: the event is acknowledged only once it is on disk.
func (s *traces) save(t *trace) error {
	b, err := json.Marshal(t)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(s.dir, ".tmp-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), s.path(t.ID)); err != nil {
		return err
	}
	d, err := os.Open(s.dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func (s *traces) remove(traceID string) error {
	err := os.Remove(s.path(traceID))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

// load reads every trace.
func (s *traces) load() (map[string]*trace, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}
	out := map[string]*trace{}
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(s.dir, e.Name()))
		if err != nil {
			return nil, err
		}
		var t trace
		if err := json.Unmarshal(b, &t); err != nil {
			return nil, err
		}
		out[t.ID] = &t
	}
	return out, nil
}
