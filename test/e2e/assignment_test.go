//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"regexp"
	"strings"
	"testing"
	"time"

	"zobik.org/zobik/internal/bus"
)

const credentials = "operator\npw-e2e\n"

// La asignación: zobik config set walks in zobik tap the cycle announced →
// proposed → assigned → completed on config_change, the same cycle of the
// delivery on hitl_contact_operator and the task.completed that closes the root.
func TestAssignment(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	z := newNetwork(t, ctx)
	z.run(t, ctx, credentials, "init")

	lines := z.start(t, ctx, "tap")
	waitFor(t, lines, regexp.MustCompile(`^tap: listening`))
	events := tapEvents(lines)

	if out := z.run(t, ctx, credentials, "config", "get"); !strings.HasPrefix(out, "v0,") {
		t.Fatalf("config get on a new network said: %s", out)
	}

	out := z.run(t, ctx, credentials, "config", "set", "auction_timeout=3000")
	if !strings.Contains(out, "published v1") {
		t.Fatalf("config set said: %s", out)
	}
	trace := regexp.MustCompile(`trace (\S+)`).FindStringSubmatch(out)
	if trace == nil {
		t.Fatalf("config set named no trace: %s", out)
	}
	cycle(t, events, trace[1])

	out = z.run(t, ctx, credentials, "config", "get")
	if !strings.HasPrefix(out, "v1,") || !regexp.MustCompile(`auction_timeout\s+3000\n`).MatchString(out) {
		t.Errorf("config get after the edit said: %s", out)
	}

	// A precondition between keys fails in the console, before anything is sent.
	if out := z.fail(t, ctx, credentials, "config", "set", "network_admission=open"); !strings.Contains(out, "not sent") {
		t.Errorf("opening the network without embedding_model said: %s", out)
	}
	// An edit made on an old version reaches the Config Store and comes back rejected.
	if out := z.fail(t, ctx, credentials, "config", "set", "--base", "v0", "auction_timeout=4000"); !strings.Contains(out, "base_version_stale") {
		t.Errorf("an edit on v0 said: %s", out)
	}
	z.fail(t, ctx, "operator\nwrong\n", "config", "get")
}

// fail runs a subcommand that has to fail and returns what it said.
func (z *network) fail(t *testing.T, ctx context.Context, stdin string, args ...string) string {
	t.Helper()
	out, err := z.cmd(ctx, stdin, args...).CombinedOutput()
	if err == nil {
		t.Errorf("zobik %s succeeded: %s", strings.Join(args, " "), out)
	}
	return string(out)
}

// tapEvent is an event as zobik tap prints it: the subject on the header line,
// then the envelope.
type tapEvent struct {
	subject string
	bus.Event
}

// topic returns data.topic, or "" if the event carries none.
func (e tapEvent) topic() string {
	var d struct {
		Topic string `json:"topic"`
	}
	json.Unmarshal(e.Data, &d)
	return d.Topic
}

// tapEvents parses the lines of zobik tap into events: each one starts on a
// header line "── <time>  <subject>" and ends with the envelope's closing brace.
func tapEvents(lines <-chan string) <-chan tapEvent {
	out := make(chan tapEvent, 256)
	go func() {
		defer close(out)
		var subject string
		var body strings.Builder
		for l := range lines {
			switch {
			case strings.HasPrefix(l, "── "):
				f := strings.Fields(l)
				subject = f[len(f)-1]
				body.Reset()
			case l == "}" && subject != "":
				body.WriteString(l)
				var e tapEvent
				if json.Unmarshal([]byte(body.String()), &e.Event) == nil {
					e.subject = subject
					out <- e
				}
				subject = ""
			case subject != "":
				body.WriteString(l)
			}
		}
	}()
	return out
}

// cycle waits for the events of trace: each subtask of the trace walks
// announced → proposed → assigned → completed, and the root closes.
func cycle(t *testing.T, events <-chan tapEvent, trace string) {
	t.Helper()
	type step struct{ topic, typ string }
	want := map[step]bool{}
	for _, topic := range []string{"config_change", "hitl_contact_operator"} {
		for _, typ := range []string{bus.TypeAnnounced, bus.TypeProposed, bus.TypeAssigned, bus.TypeCompleted} {
			want[step{topic, typ}] = true
		}
	}
	tasks := map[string]string{} // TaskID → topic, from its announcement
	closed := false
	timeout := time.After(time.Minute)
	for len(want) > 0 || !closed {
		var e tapEvent
		var ok bool
		select {
		case e, ok = <-events:
			if !ok {
				t.Fatalf("trace %s: zobik tap ended, missing %v, root closed: %v", trace, want, closed)
			}
		case <-timeout:
			t.Fatalf("trace %s: missing %v, root closed: %v", trace, want, closed)
		}
		if bus.TraceID(e.TraceParent) != trace {
			continue
		}
		if e.Type == bus.TypeCompleted && e.Subject == trace {
			closed = true
			continue
		}
		topic := e.topic()
		if e.Type == bus.TypeAnnounced {
			tasks[e.Subject] = topic
		}
		if topic == "" { // task.assigned carries the topic only in the task it names
			topic = tasks[e.Subject]
		}
		delete(want, step{topic, e.Type})
	}
}
