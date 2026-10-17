package tracingcollector_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/globalconfig"
	"zobik.org/zobik/internal/role"
	"zobik.org/zobik/internal/role/tracingcollector"
	"zobik.org/zobik/internal/testnet"
)

// backend keeps what it accepts, and refuses the first fail exports: the
// tracing backend down.
type backend struct {
	mu    sync.Mutex
	fail  int
	spans []sdktrace.ReadOnlySpan
}

func (b *backend) Reach(context.Context) error { return nil }

func (b *backend) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.fail > 0 {
		b.fail--
		return errors.New("backend down")
	}
	b.spans = append(b.spans, spans...)
	return nil
}

func (b *backend) exported() []sdktrace.ReadOnlySpan {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]sdktrace.ReadOnlySpan(nil), b.spans...)
}

// fixture is a network with the Tracing Collector and the events of a
// config_change written by the test, as the channel, the Task Broker and the
// Config Store publish them.
type fixture struct {
	*testnet.Net
	js      jetstream.JetStream
	backend *backend
	data    string
	trace   string
	root    string
	entry   string
}

func newFixture(t *testing.T, b *backend, edit func(globalconfig.Values)) *fixture {
	n := testnet.New(t, edit)
	span := make([]byte, 8)
	rand.Read(span)
	return &fixture{
		Net: n, js: n.JetStream(n.Conn(bus.PublishScope.Role, nil)), backend: b, data: t.TempDir(),
		trace: strings.ReplaceAll(uuid.NewString(), "-", ""), root: hex.EncodeToString(span), entry: uuid.NewString(),
	}
}

func (f *fixture) start() func() {
	_, stop := f.StartStoppable("tracing_collector", func(ctx context.Context, rt *role.Runtime, nc *nats.Conn) error {
		return tracingcollector.Run(ctx, rt, nc, tracingcollector.Options{Network: "prueba", Backend: f.backend, DataDir: f.data})
	})
	return stop
}

// emit publishes an event of the trace whose traceparent names parent.
func (f *fixture) emit(typ, source, task, subject, parent string, data map[string]any) *bus.Event {
	f.T.Helper()
	e, err := bus.NewEvent(typ, source, task, data)
	if err != nil {
		f.T.Fatal(err)
	}
	e.TraceParent, e.ConfigVersion = "00-"+f.trace+"-"+parent+"-01", "v0"
	if err := bus.Publish(f.Ctx, f.js, subject, e); err != nil {
		f.T.Fatal(err)
	}
	return e
}

const (
	channel = "UCHANNEL"
	broker  = "UBROKER"
	store   = "UCONFIG"
	other   = "UOTHER"
)

var metrics = map[string]any{"tokens_in": 0, "tokens_out": 0, "cost": 0, "duration_ms": 12}

// The opening and the award of the config_change.
func (f *fixture) opening() (assigned *bus.Event) {
	f.emit(bus.TypeAnnounced, channel, f.entry, bus.SubjectAnnounced("config_change"), f.root,
		map[string]any{"topic": "config_change", "task_text": "set auction_timeout", "requester_identity": channel, "workspace": "zobik"})
	f.emit(bus.TypeProposed, store, f.entry, bus.SubjectProposed(store, "config_change"), f.root,
		map[string]any{"topic": "config_change"})
	f.emit(bus.TypeProposed, other, f.entry, bus.SubjectProposed(other, "config_change"), f.root,
		map[string]any{"topic": "config_change", "declared_similarity": 0.5})
	return f.emit(bus.TypeAssigned, broker, f.entry, bus.SubjectAddressed(bus.TypeAssigned, store), f.root,
		map[string]any{"topic": "config_change", "similarity": 0.9, "requester_identity": channel})
}

// The close of the entry subtask and of the root, with the rejection the Task
// Broker stores after the fast close and a notice of the entry's span.
func (f *fixture) closing(assigned *bus.Event) {
	f.emit(bus.TypeCompleted, store, f.entry, bus.SubjectTerminal(bus.TypeCompleted, channel, "config_change"), f.root,
		map[string]any{"topic": "config_change", "metrics": metrics})
	f.emit(bus.TypeRejected, broker, f.entry, bus.SubjectAddressed(bus.TypeRejected, other), f.root,
		map[string]any{"topic": "config_change", "similarity": 0.4})
	f.emit(bus.TypeNotice, store, uuid.NewString(), bus.SubjectNotice("config_change"), bus.SpanID(assigned.ID),
		map[string]any{"topic": "config_change", "kind": "progress"})
	f.emit(bus.TypeCompleted, channel, f.trace, bus.SubjectTerminal(bus.TypeCompleted, channel, "config_change"), f.root,
		map[string]any{"topic": "config_change", "metrics": metrics})
}

// await waits until the backend holds want spans, and a little longer, to see
// none is exported twice.
func (f *fixture) await(want int) map[string]sdktrace.ReadOnlySpan {
	f.T.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for len(f.backend.exported()) < want {
		if time.Now().After(deadline) {
			f.T.Fatalf("the backend has %d spans, want %d", len(f.backend.exported()), want)
		}
		time.Sleep(100 * time.Millisecond)
	}
	time.Sleep(3 * time.Second)
	got := f.backend.exported()
	if len(got) != want {
		f.T.Fatalf("the backend has %d spans, want %d", len(got), want)
	}
	byName := map[string]sdktrace.ReadOnlySpan{}
	for _, s := range got {
		byName[s.Name()] = s
	}
	return byName
}

func attrs(s sdktrace.ReadOnlySpan) map[string]attribute.Value {
	out := map[string]attribute.Value{}
	for _, kv := range s.Attributes() {
		out[string(kv.Key)] = kv.Value
	}
	return out
}

// check verifies the trace of the config_change: the entry's span under the
// root, with its auction and its notice as span events (implementation §1.2.6).
func (f *fixture) check(spans map[string]sdktrace.ReadOnlySpan, assigned *bus.Event) {
	f.T.Helper()
	root, entry := spans["root"], spans["config_change"]
	if root == nil || entry == nil {
		f.T.Fatalf("spans %v", spans)
	}
	if root.SpanContext().TraceID().String() != f.trace || root.SpanContext().SpanID().String() != f.root || root.Parent().IsValid() {
		f.T.Errorf("root %v, parent %v", root.SpanContext(), root.Parent())
	}
	if entry.SpanContext().SpanID().String() != bus.SpanID(assigned.ID) || entry.Parent().SpanID().String() != f.root {
		f.T.Errorf("entry %v, parent %v", entry.SpanContext(), entry.Parent())
	}
	a := attrs(entry)
	if a["zobik.task_id"].AsString() != f.entry || a["zobik.node_id"].AsString() != store || a["zobik.similarity"].AsFloat64() != 0.9 ||
		a["zobik.outcome"].AsString() != "completed" || a["zobik.task_text"].AsString() != "set auction_timeout" ||
		a["zobik.workspace"].AsString() != "zobik" || a["zobik.metrics.duration_ms"].AsInt64() != 12 {
		f.T.Errorf("entry attributes %v", a)
	}
	if r := attrs(root); r["zobik.task_id"].AsString() != f.trace || r["zobik.outcome"].AsString() != "completed" || r["zobik.config_version"].AsString() != "v0" {
		f.T.Errorf("root attributes %v", r)
	}
	var events []string
	for _, e := range entry.Events() {
		events = append(events, e.Name)
	}
	if strings.Join(events, ",") != "task.announced,task.proposed,task.proposed,task.rejected,notice" {
		f.T.Errorf("entry events %v", events)
	}
	for _, s := range spans {
		if s.Resource().String() != "service.name=prueba" {
			f.T.Errorf("resource %s", s.Resource())
		}
	}
}

// A restart in the middle of the trace loses nothing: the events applied
// before it are in the volume, and the rest is read on coming back.
func TestRestart(t *testing.T) {
	f := newFixture(t, &backend{}, nil)
	stop := f.start()
	assigned := f.opening()
	f.awaitFacts(4)
	stop()

	f.closing(assigned)
	f.start()
	f.check(f.await(2), assigned)
}

// With the backend down, the spans stay and go once it comes back.
func TestBackendDown(t *testing.T) {
	f := newFixture(t, &backend{fail: 3}, nil)
	f.start()
	assigned := f.opening()
	f.closing(assigned)
	f.check(f.await(2), assigned)
}

// A trace whose root never closes, like one zobik publish opens, is discarded
// context_retention_grace after its last event, once no award is open; the
// spans it closed were exported.
func TestRootlessTraceIsDiscarded(t *testing.T) {
	f := newFixture(t, &backend{}, func(v globalconfig.Values) {
		testnet.Set(v, map[string]any{"context_retention_grace": 1000})
	})
	stop := f.start()
	assigned := f.opening()
	f.emit(bus.TypeCompleted, store, f.entry, bus.SubjectTerminal(bus.TypeCompleted, channel, "config_change"), f.root,
		map[string]any{"topic": "config_change", "metrics": metrics})
	if spans := f.await(1); spans["config_change"] == nil || spans["config_change"].SpanContext().SpanID().String() != bus.SpanID(assigned.ID) {
		t.Fatalf("spans %v", spans)
	}
	// The sweep runs on starting, and then once a minute.
	stop()
	f.start()
	path := filepath.Join(f.data, "traces", f.trace+".json")
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the trace was not discarded")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// awaitFacts waits until the trace's file holds n events.
func (f *fixture) awaitFacts(n int) {
	f.T.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var tr struct {
			Facts []json.RawMessage `json:"facts"`
		}
		if b, err := os.ReadFile(filepath.Join(f.data, "traces", f.trace+".json")); err == nil {
			_ = json.Unmarshal(b, &tr)
		}
		if len(tr.Facts) >= n {
			return
		}
		if time.Now().After(deadline) {
			f.T.Fatalf("the trace holds %d events, want %d", len(tr.Facts), n)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
