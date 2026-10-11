package contextstore

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

// The storage of implementation §1.2.9: one file per portion under
// <TraceID>/<TaskID>/, named by the portion in hexadecimal, so the path is built
// only from the token's identifiers and hexadecimal characters.

// MaxName is the ceiling of a portion's name, which keeps the hexadecimal file
// name within the 255 bytes a filesystem admits.
const MaxName = 120

// closedFile marks a trace's close with its time. A section is named by a
// TaskID, which never starts with a dot.
const closedFile = ".closed"

var (
	ErrNotFound  = errors.New("contextstore: no such portion")
	ErrExhausted = errors.New("contextstore: the write does not fit")
	ErrInvalid   = errors.New("contextstore: invalid identifier or name")
)

// validID admits the identifiers a path is built from: a TraceID or a TaskID.
var validID = regexp.MustCompile(`^[A-Za-z0-9-]{1,64}$`).MatchString

// Store is the content volume. It counts what it holds, as the only writer, so
// the ceilings hold on any filesystem.
type Store struct {
	dir                    string
	storeLimit, traceLimit int64

	mu     sync.Mutex
	total  int64
	traces map[string]int64
}

// Open opens the volume at dir and counts what it already holds.
func Open(dir string, storeLimit, traceLimit int64) (*Store, error) {
	s := &Store{dir: dir, storeLimit: storeLimit, traceLimit: traceLimit, traces: map[string]int64{}}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	traces, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, t := range traces {
		if !t.IsDir() {
			continue
		}
		var size int64
		err := filepath.WalkDir(filepath.Join(dir, t.Name()), func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() {
				return err
			}
			if strings.HasPrefix(d.Name(), ".tmp-") {
				return os.Remove(path) // a write a crash interrupted
			}
			if d.Name() == closedFile {
				return nil
			}
			info, err := d.Info()
			if err == nil {
				size += info.Size()
			}
			return err
		})
		if err != nil {
			return nil, err
		}
		s.traces[t.Name()] = size
		s.total += size
	}
	return s, nil
}

func (s *Store) portion(trace, task, name string) (string, error) {
	if !validID(trace) || !validID(task) || name == "" || len(name) > MaxName {
		return "", ErrInvalid
	}
	return filepath.Join(s.dir, trace, task, hex.EncodeToString([]byte(name))), nil
}

// Read opens a portion.
func (s *Store) Read(trace, task, name string) (*os.File, error) {
	path, err := s.portion(trace, task, name)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, ErrNotFound
	}
	return f, err
}

// List returns the names of a section's portions.
func (s *Store) List(trace, task string) ([]string, error) {
	if !validID(trace) || !validID(task) {
		return nil, ErrInvalid
	}
	entries, err := os.ReadDir(filepath.Join(s.dir, trace, task))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var names []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".") {
			continue
		}
		name, err := hex.DecodeString(e.Name())
		if err != nil {
			continue
		}
		names = append(names, string(name))
	}
	return names, nil
}

// Writer writes a portion: it is written to a temporary file and renamed when
// done, so a read never sees a half-written one, and rewriting a portion replaces
// its entire content.
type Writer struct {
	s            *Store
	trace, final string
	f            *os.File
	n, old       int64
	charged      int64
}

// Create starts the write of a portion.
func (s *Store) Create(trace, task, name string) (*Writer, error) {
	final, err := s.portion(trace, task, name)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(final), 0o700); err != nil {
		return nil, err
	}
	w := &Writer{s: s, trace: trace, final: final}
	if info, err := os.Stat(final); err == nil {
		w.old = info.Size()
	}
	if w.f, err = os.CreateTemp(filepath.Dir(final), ".tmp-"); err != nil {
		return nil, err
	}
	return w, nil
}

// Write appends p, rejecting it whole if the portion stops fitting in either ceiling.
func (w *Writer) Write(p []byte) error {
	// What a write adds is what the new content exceeds the one it replaces.
	charge := max(0, w.n+int64(len(p))-w.old)
	delta := charge - w.charged
	w.s.mu.Lock()
	if w.s.total+delta > w.s.storeLimit || w.s.traces[w.trace]+delta > w.s.traceLimit {
		w.s.mu.Unlock()
		return ErrExhausted
	}
	w.s.total += delta
	w.s.traces[w.trace] += delta
	w.s.mu.Unlock()
	w.charged = charge
	if _, err := w.f.Write(p); err != nil {
		return err
	}
	w.n += int64(len(p))
	return nil
}

// Commit makes the portion visible.
func (w *Writer) Commit() error {
	if err := w.f.Close(); err != nil {
		w.Abort()
		return err
	}
	w.s.mu.Lock()
	defer w.s.mu.Unlock()
	// The content replaced is the one there now, which a concurrent write may have changed.
	var cur int64
	if info, err := os.Stat(w.final); err == nil {
		cur = info.Size()
	}
	if err := os.Rename(w.f.Name(), w.final); err != nil {
		os.Remove(w.f.Name())
		w.release()
		return err
	}
	correction := (w.n - cur) - w.charged
	w.s.total += correction
	w.s.traces[w.trace] += correction
	return nil
}

// Abort discards the write.
func (w *Writer) Abort() {
	w.f.Close()
	os.Remove(w.f.Name())
	w.s.mu.Lock()
	w.release()
	w.s.mu.Unlock()
}

func (w *Writer) release() {
	w.s.total -= w.charged
	w.s.traces[w.trace] -= w.charged
	w.charged = 0
}

// Purge deletes a trace whole.
func (s *Store) Purge(trace string) error {
	if !validID(trace) {
		return ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.RemoveAll(filepath.Join(s.dir, trace)); err != nil {
		return err
	}
	s.total -= s.traces[trace]
	delete(s.traces, trace)
	return nil
}

// MarkClosed records the close of a trace that holds content; one with no
// content has nothing to purge.
func (s *Store) MarkClosed(trace string, at time.Time) error {
	if !validID(trace) {
		return ErrInvalid
	}
	dir := filepath.Join(s.dir, trace)
	if _, err := os.Stat(dir); errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return os.WriteFile(filepath.Join(dir, closedFile), []byte(at.UTC().Format(time.RFC3339Nano)), 0o600)
}

// Sweep purges the traces closed for longer than grace (architecture §3.4).
func (s *Store) Sweep(grace time.Duration, now time.Time) error {
	traces, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}
	for _, t := range traces {
		raw, err := os.ReadFile(filepath.Join(s.dir, t.Name(), closedFile))
		if err != nil {
			continue
		}
		closed, err := time.Parse(time.RFC3339Nano, string(raw))
		if err != nil {
			return fmt.Errorf("contextstore: %s: %w", t.Name(), err)
		}
		if !now.Before(closed.Add(grace)) {
			if err := s.Purge(t.Name()); err != nil {
				return err
			}
		}
	}
	return nil
}

// Usage returns what the volume and a trace hold.
func (s *Store) Usage(trace string) (total, ofTrace int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.total, s.traces[trace]
}
