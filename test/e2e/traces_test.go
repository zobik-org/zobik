//go:build e2e

package e2e

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"zobik.org/zobik/internal/bus"
)

// Las trazas: the trace of a config_change in Jaeger, with a span per task and
// the tree of its subtasks. historian asks trace_query about its own trace: the
// reconstruction it gets names its task and the query, and in Jaeger the query's
// span hangs from historian's, derived from its task.assigned.
func TestTraces(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	z := newNetwork(t, ctx)
	z.build(t, ctx, "images/zobik-sidecar.Dockerfile", "zobik-sidecar:dev")
	z.build(t, ctx, "images/zobik-python.Dockerfile", "zobik-python:dev")
	z.build(t, ctx, "examples/blueprints/historian/Dockerfile", "zobik-historian:dev")
	z.build(t, ctx, "test/e2e/embeddings/Dockerfile", embeddingsImage)
	z.tracingUI = freePort(t)
	z.init(t, ctx)
	jaeger := "http://127.0.0.1:" + z.tracingUI

	blueprint := filepath.Join(z.root, "examples/blueprints/historian/blueprint.json")
	raw, err := os.ReadFile(blueprint)
	if err != nil {
		t.Fatal(err)
	}
	var b struct {
		CapabilityText string `json:"capability_text"`
	}
	if err := json.Unmarshal(raw, &b); err != nil {
		t.Fatal(err)
	}
	text := "tell me what happened"
	same := []float32{1, 0}
	z.embeddings(t, ctx, map[string][]float32{documentPrefix + b.CapabilityText: same, queryPrefix + text: same})

	out := z.run(t, ctx, credentials, "config", "set",
		`egress_registry={"embeddings":{"target":"http://embeddings:8080"}}`,
		`embedding_model={"entry":"embeddings","model":"fake","document_prefix":"`+documentPrefix+`","query_prefix":"`+queryPrefix+`","max_text_size":8192}`)
	trace := regexp.MustCompile(`trace (\S+)`).FindStringSubmatch(out)
	if trace == nil {
		t.Fatalf("config set named no trace: %s", out)
	}
	z.run(t, ctx, credentials, "config", "set", "network_admission=open")

	// The config_change's trace: the root and, hanging from it, the inbound
	// subtask and the delivery.
	spans := jaegerTrace(t, ctx, jaeger, trace[1], "root", "config_change", "hitl_contact_operator")
	root := spans["root"]
	if root.ParentSpanID != "" {
		t.Errorf("the root span has a parent: %+v", root)
	}
	for _, name := range []string{"config_change", "hitl_contact_operator"} {
		if s := spans[name]; s.ParentSpanID != root.SpanID {
			t.Errorf("the span %s hangs from %s, not from the root %s", name, s.ParentSpanID, root.SpanID)
		}
	}
	if root.service != z.name {
		t.Errorf("the spans' service.name is %q, not the network %q", root.service, z.name)
	}

	lines := z.start(t, ctx, "tap")
	waitFor(t, lines, regexp.MustCompile(`^tap: listening`))
	events := tapEvents(lines)

	out = z.run(t, ctx, "", "node", "up", "--topic", "historian", blueprint)
	node := regexp.MustCompile(`provisioned node \S+ on historian: (\S+) and`).FindStringSubmatch(out)
	if node == nil {
		t.Fatalf("node up said: %s", out)
	}
	// zobik publish opens the trace without a root task: the root span never
	// closes, so it never reaches Jaeger.
	traceID, rootSpan := randomHex(16), randomHex(8)
	task := uuid.NewString()
	z.run(t, ctx, "", "publish", "--topic", "historian", "--task", task, "--text", text,
		"--traceparent", "00-"+traceID+"-"+rootSpan+"-01")
	historianSpan := bus.SpanID(lifecycle(t, events, task))

	// The reconstruction historian printed: its task, open while it asked, and
	// the query hanging from it.
	r := reconstruction(t, ctx, node[1])
	if r.TraceID != traceID {
		t.Errorf("the reconstruction is of the trace %s, not %s", r.TraceID, traceID)
	}
	byTopic := map[string]reconstructed{}
	for _, task := range r.Tasks {
		byTopic[task.Topic] = task
	}
	if h := byTopic["historian"]; h.TaskID != task || h.SpanID != historianSpan || h.ParentSpanID != rootSpan || h.TaskText != text || h.Outcome != "open" {
		t.Errorf("historian's task in the reconstruction: %+v", h)
	}
	if q := byTopic["trace_query"]; q.ParentSpanID != historianSpan || q.Outcome != "open" {
		t.Errorf("the query in the reconstruction: %+v", q)
	}
	if root := byTopic[""]; root.TaskID != traceID || root.SpanID != rootSpan || root.Outcome != "open" {
		t.Errorf("the root in the reconstruction: %+v", root)
	}

	// In Jaeger, once both close: historian's span, derived from its
	// task.assigned, and the query's hanging from it.
	spans = jaegerTrace(t, ctx, jaeger, traceID, "historian", "trace_query")
	if h := spans["historian"]; h.SpanID != historianSpan || h.ParentSpanID != rootSpan {
		t.Errorf("historian's span in Jaeger: %+v", h)
	}
	if q := spans["trace_query"]; q.ParentSpanID != historianSpan {
		t.Errorf("the query's span in Jaeger hangs from %s, not from historian's %s", q.ParentSpanID, historianSpan)
	}
}

// freePort returns a port on 127.0.0.1 that nothing listens on.
func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return strconv.Itoa(l.Addr().(*net.TCPAddr).Port)
}

func randomHex(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// reconstructed is a task of the reconstruction, with the fields the test checks
// (trace_query.reconstruction.schema.json).
type reconstructed struct {
	TaskID       string `json:"task_id"`
	SpanID       string `json:"span_id"`
	ParentSpanID string `json:"parent_span_id"`
	Topic        string `json:"topic"`
	TaskText     string `json:"task_text"`
	Outcome      string `json:"outcome"`
}

// reconstruction reads from the log of historian's Logic Container the
// reconstruction it printed.
func reconstruction(t *testing.T, ctx context.Context, container string) (r struct {
	TraceID string          `json:"trace_id"`
	Tasks   []reconstructed `json:"tasks"`
}) {
	t.Helper()
	logs, err := exec.CommandContext(ctx, "docker", "logs", container).CombinedOutput()
	if err != nil {
		t.Fatalf("reading %s's log: %v\n%s", container, err, logs)
	}
	for _, l := range strings.Split(string(logs), "\n") {
		if strings.HasPrefix(l, "{") && json.Unmarshal([]byte(l), &r) == nil && r.TraceID != "" {
			return r
		}
	}
	t.Fatalf("%s printed no reconstruction:\n%s", container, logs)
	return r
}

// jaegerSpan is a span as Jaeger's API v3 returns it, in OTLP's JSON.
type jaegerSpan struct {
	SpanID       string `json:"spanId"`
	ParentSpanID string `json:"parentSpanId"`
	Name         string `json:"name"`
	service      string
}

// jaegerTrace waits until Jaeger holds a span of trace for each of names, and
// returns them by name.
func jaegerTrace(t *testing.T, ctx context.Context, jaeger, trace string, names ...string) map[string]jaegerSpan {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	var last string
	for {
		spans, body, err := getTrace(ctx, jaeger, trace)
		if err == nil {
			missing := false
			for _, n := range names {
				if _, ok := spans[n]; !ok {
					missing = true
				}
			}
			if !missing {
				return spans
			}
			last = body
		} else {
			last = err.Error()
		}
		if time.Now().After(deadline) {
			t.Fatalf("Jaeger has no spans %v of the trace %s; last answer: %s", names, trace, last)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for the trace %s in Jaeger: %v", trace, ctx.Err())
		case <-time.After(2 * time.Second):
		}
	}
}

func getTrace(ctx context.Context, jaeger, trace string) (map[string]jaegerSpan, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, jaeger+"/api/v3/traces/"+trace, nil)
	if err != nil {
		return nil, "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("%s: %s", resp.Status, raw)
	}
	var d struct {
		Result struct {
			ResourceSpans []struct {
				Resource struct {
					Attributes []struct {
						Key   string `json:"key"`
						Value struct {
							StringValue string `json:"stringValue"`
						} `json:"value"`
					} `json:"attributes"`
				} `json:"resource"`
				ScopeSpans []struct {
					Spans []jaegerSpan `json:"spans"`
				} `json:"scopeSpans"`
			} `json:"resourceSpans"`
		} `json:"result"`
	}
	if err := json.Unmarshal(raw, &d); err != nil {
		return nil, string(raw), err
	}
	spans := map[string]jaegerSpan{}
	for _, rs := range d.Result.ResourceSpans {
		var service string
		for _, a := range rs.Resource.Attributes {
			if a.Key == "service.name" {
				service = a.Value.StringValue
			}
		}
		for _, ss := range rs.ScopeSpans {
			for _, s := range ss.Spans {
				s.service = service
				spans[s.Name] = s
			}
		}
	}
	return spans, string(raw), nil
}
