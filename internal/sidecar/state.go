package sidecar

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"zobik.org/zobik/internal/bus"
)

// state is what the Integration Sidecar keeps in its private volume: the
// outbox and the ids of the events it delivered (architecture §3.6.4). Each is
// a directory of files written with fsync, so a crash leaves a file whole or
// absent (implementation §1.2.12).
type state struct {
	outbox, seen string
}

func openState(dir string) (*state, error) {
	s := &state{outbox: filepath.Join(dir, "outbox"), seen: filepath.Join(dir, "seen")}
	for _, d := range []string{s.outbox, s.seen} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// pending is an event in the outbox: on its subject, and, if Embed, waiting
// for the task_embedding of its task_text (architecture §3.7.4).
type pending struct {
	Subject string     `json:"subject"`
	Event   *bus.Event `json:"event"`
	Embed   bool       `json:"embed,omitempty"`
}

// fileName is a name the event's id cannot take out of its directory: ids come
// from other nodes.
func fileName(id string) string {
	sum := sha256.Sum256([]byte(id))
	return hex.EncodeToString(sum[:])
}

// put persists p before anything tries to publish it.
func (s *state) put(p *pending) error {
	b, err := json.Marshal(p)
	if err != nil {
		return err
	}
	return writeSynced(s.outbox, fileName(p.Event.ID)+".json", b)
}

// sent removes an event the Bus confirmed.
func (s *state) sent(id string) error {
	err := os.Remove(filepath.Join(s.outbox, fileName(id)+".json"))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

// unsent reads the outbox: what a previous run left without confirming.
func (s *state) unsent() ([]*pending, error) {
	entries, err := os.ReadDir(s.outbox)
	if err != nil {
		return nil, err
	}
	var out []*pending
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(s.outbox, e.Name()))
		if err != nil {
			return nil, err
		}
		var p pending
		if err := json.Unmarshal(b, &p); err != nil || p.Event == nil {
			// Set aside, so one damaged file holds back neither the rest nor
			// the startup.
			log.Printf("sidecar: the outbox holds %s, which is not an event: set aside", e.Name())
			_ = os.Rename(filepath.Join(s.outbox, e.Name()), filepath.Join(s.outbox, e.Name()+".bad"))
			continue
		}
		out = append(out, &p)
	}
	return out, nil
}

// mark records the id of an event the Logic Container confirmed.
func (s *state) mark(id string) error {
	return writeSynced(s.seen, fileName(id), nil)
}

// delivered reports whether the event with id was already confirmed.
func (s *state) delivered(id string) bool {
	_, err := os.Stat(filepath.Join(s.seen, fileName(id)))
	return err == nil
}

// prune forgets the ids older than the Bus's retention, beyond which the Bus
// no longer holds the event to deliver it again (implementation §1.2.1).
func (s *state) prune() error {
	entries, err := os.ReadDir(s.seen)
	if err != nil {
		return err
	}
	limit := time.Now().Add(-bus.Retention)
	for _, e := range entries {
		info, err := e.Info()
		if err == nil && info.ModTime().Before(limit) {
			_ = os.Remove(filepath.Join(s.seen, e.Name()))
		}
	}
	return nil
}

// writeSynced writes name in dir through a temporary file, with fsync on the
// file and on the directory.
func writeSynced(dir, name string, b []byte) error {
	tmp, err := os.CreateTemp(dir, ".tmp-*")
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
	if err := os.Rename(tmp.Name(), filepath.Join(dir, name)); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
