package channel

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

// root is a trace the channel opened. The channel holds it as assigned from the
// moment it opens it, and closes it once the outcome is delivered
// (architecture §3.5.1, §3.5.4). It is kept in the role's volume, so a restart
// does not lose it.
type root struct {
	TraceID     string    `json:"trace_id"`
	EntryTaskID string    `json:"entry_task_id"`
	EntryTopic  string    `json:"entry_topic"`
	Token       string    `json:"token"`
	TraceParent string    `json:"traceparent"`
	Version     string    `json:"config_version"`
	OpenedAt    time.Time `json:"opened_at"`
	// Entry is the announcement of the entry subtask's first attempt, which a
	// reopen repeats; EntryAttempts, the attempts of its lineage
	// (architecture §3.3).
	Entry         *bus.Event `json:"entry"`
	EntryAttempts int        `json:"entry_attempts"`
	// Pending is the attempt not yet published, with its task_embedding still
	// to stamp; Vector is the last one generated for the entry's text, under
	// ModelID, which a reopen in the same space reuses (architecture §3.7.4).
	Pending *bus.Event `json:"pending,omitempty"`
	Vector  string     `json:"vector,omitempty"`
	ModelID string     `json:"model_id,omitempty"`
	// Outcome is the terminal event of the entry subtask, once it arrives.
	Outcome *bus.Event `json:"outcome,omitempty"`
	// Delivery is the TaskID of the outcome delivery's last attempt, its
	// attempt number and whether it is the final round (architecture §3.5.2).
	Delivery   string `json:"delivery,omitempty"`
	Attempts   int    `json:"attempts,omitempty"`
	FinalRound bool   `json:"final_round,omitempty"`
	// Options are those of the delivery's last attempt.
	Options []string `json:"options,omitempty"`
	// RetainUntil is, once the root closes, the end of context_retention_grace:
	// until then the role keeps its token, for the purge (architecture §3.6.1, §3.4).
	RetainUntil time.Time `json:"retain_until,omitzero"`
}

// roots keeps each root in a file of its own, roots/<TraceID>.json.
type roots struct{ dir string }

func openRoots(volume string) (*roots, error) {
	dir := filepath.Join(volume, "roots")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	return &roots{dir: dir}, nil
}

func (r *roots) path(traceID string) string { return filepath.Join(r.dir, traceID+".json") }

// save writes the root through a temporary file, so a crash leaves the old
// version or the new one.
func (r *roots) save(rt *root) error {
	b, err := json.Marshal(rt)
	if err != nil {
		return err
	}
	tmp := r.path(rt.TraceID) + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, r.path(rt.TraceID))
}

func (r *roots) remove(traceID string) error {
	err := os.Remove(r.path(traceID))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

// load reads every root.
func (r *roots) load() (map[string]*root, error) {
	entries, err := os.ReadDir(r.dir)
	if err != nil {
		return nil, err
	}
	out := map[string]*root{}
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(r.dir, e.Name()))
		if err != nil {
			return nil, err
		}
		var rt root
		if err := json.Unmarshal(b, &rt); err != nil {
			return nil, err
		}
		out[rt.TraceID] = &rt
	}
	return out, nil
}
