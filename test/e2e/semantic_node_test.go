//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"zobik.org/zobik/bundles/forge/openai"
	"zobik.org/zobik/internal/console"
	"zobik.org/zobik/internal/egress"
)

const (
	openaiImage = "zobik-openai:e2e"
	openaiKey   = "sk-e2e"
)

// Un nodo semántico: a request written in the panel is answered by the
// assistant blueprint, whose call to the model asks the person first. The
// call goes to the fake OpenAI endpoint with the key from the Secret Store,
// and its cost, from the rate table, is in the trace.
func TestSemanticNode(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	z := newNetwork(t, ctx)
	z.build(t, ctx, "images/zobik-sidecar.Dockerfile", "zobik-sidecar:dev")
	z.build(t, ctx, "test/e2e/embeddings/Dockerfile", embeddingsImage)
	z.build(t, ctx, "test/e2e/openai/Dockerfile", openaiImage)
	link, _ := z.init(t, ctx)
	z.blueprint(t, ctx, "examples/blueprints/assistant/Dockerfile", "zobik-assistant:dev")
	p := signIn(t, ctx, link)

	blueprint := filepath.Join(z.root, "examples/blueprints/assistant/blueprint.json")
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
	text := "what is the capital of France?"
	same := []float32{1, 0}
	z.embeddings(t, ctx, map[string][]float32{documentPrefix + b.CapabilityText: same, queryPrefix + text: same})
	z.openai(t, ctx)

	// The key and the table are acts of deployment; the entry asks requester
	// before each call, as in the demo.
	z.run(t, ctx, credentials+openaiKey+"\n", "secret", "set", "openai")
	z.run(t, ctx, credentials, "rates", "set", openai.RateSource, filepath.Join(z.root, "bundles/forge/openai/rates.json"))
	entry, err := exec.CommandContext(ctx, z.bin, "model-entry", "--target", "http://openai:8080", "--handle", "openai", "--then", "requester").Output()
	if err != nil {
		t.Fatalf("zobik model-entry: %v", err)
	}
	z.run(t, ctx, credentials, "config", "set",
		`egress_registry={"embeddings":`+string(egress.EmbeddingsEntry("http://embeddings:8080"))+`,"openai":`+strings.TrimSpace(string(entry))+`}`,
		`embedding_model={"entry":"embeddings","model":"fake","document_prefix":"`+documentPrefix+`","query_prefix":"`+queryPrefix+`","max_text_size":8192}`)
	z.run(t, ctx, credentials, "config", "set", "network_admission=open")
	if out := z.run(t, ctx, "", "node", "up", "--topic", "intake", blueprint); !strings.Contains(out, "provisioned node") {
		t.Fatalf("node up said: %s", out)
	}

	changed := p.events(t, ctx)
	var conv console.Conversation
	p.call(t, ctx, http.MethodPost, console.PathConversations, nil, http.StatusCreated, &conv)
	request := p.send(t, ctx, conv.ID, text)

	// The gate asks the person, with the call as it will go out.
	gate := p.wait(t, ctx, changed, conv.ID, func(m *console.Message) bool {
		return m.Kind == console.KindQuery && m.TraceID == request.TraceID && m.State == console.StateAwaiting
	})
	var d struct {
		Kind    string `json:"kind"`
		Details struct {
			Entry string `json:"entry"`
			Call  struct {
				Operation string `json:"operation"`
				Fields    struct {
					Model string `json:"model"`
					Store *bool  `json:"store"`
				} `json:"fields"`
			} `json:"call"`
		} `json:"details"`
	}
	if err := json.Unmarshal(gate.Data, &d); err != nil {
		t.Fatalf("the gate's data: %v: %s", err, gate.Data)
	}
	if d.Kind != "gate_approval" || d.Details.Entry != "openai" || d.Details.Call.Operation != openai.Operation ||
		d.Details.Call.Fields.Model == "" || d.Details.Call.Fields.Store == nil || *d.Details.Call.Fields.Store {
		t.Errorf("the gate: %s", gate.Data)
	}
	if !slices.Contains(gate.Options, "approve") {
		t.Errorf("the gate's options: %v", gate.Options)
	}
	p.shown(t, ctx, gate.TaskID)
	p.call(t, ctx, http.MethodPost, console.PathAnswers, console.Answer{TaskID: gate.TaskID, Decision: "approve"}, http.StatusNoContent, nil)

	outcome := p.wait(t, ctx, changed, conv.ID, func(m *console.Message) bool {
		return m.Kind == console.KindOutcome && m.TraceID == request.TraceID && m.State == console.StateAwaiting
	})
	if want := "echo: " + text; outcome.Text != want {
		t.Errorf("the outcome is %q, want %q", outcome.Text, want)
	}
	p.accept(t, ctx, conv.ID, outcome)

	// The fake's usage, priced by the table: (100 − 20) input, 20 cached and
	// 50 output tokens of the model the response names.
	var table struct {
		Rates map[string]map[string]float64 `json:"rates"`
	}
	if err := json.Unmarshal(openai.Rates, &table); err != nil {
		t.Fatal(err)
	}
	r, ok := table.Rates[d.Details.Call.Fields.Model]
	if !ok {
		t.Fatalf("the rate table has no %s", d.Details.Call.Fields.Model)
	}
	want := 80*r["input"] + 20*r["cached_input"] + 50*r["output"]
	var session console.SessionJSON
	p.call(t, ctx, http.MethodGet, console.PathSession, nil, http.StatusOK, &session)
	spans := jaegerTrace(t, ctx, &jaegerClient{base: session.TraceUI, c: p.c}, request.TraceID, "root", "intake")
	cost, ok := spans["intake"].double("zobik.metrics.cost")
	if !ok || math.Abs(cost-want) > 1e-12 {
		t.Errorf("the intake span's cost is %v (present %v), want %v", cost, ok, want)
	}
}

// openai runs the fake OpenAI endpoint on the network's Docker network,
// reached at http://openai:8080, requiring openaiKey.
func (z *network) openai(t *testing.T, ctx context.Context) {
	t.Helper()
	name := "zobik-" + z.name + "-openai"
	// The network's label lets remove find it if the cleanup never runs.
	run := exec.CommandContext(ctx, "docker", "run", "-d", "--name", name,
		"--label", "org.zobik.network="+z.name,
		"--network", "zobik-"+z.name, "--network-alias", "openai",
		openaiImage, "-key", openaiKey)
	if out, err := run.CombinedOutput(); err != nil {
		t.Fatalf("running the OpenAI endpoint: %v\n%s", err, out)
	}
	t.Cleanup(func() { exec.Command("docker", "rm", "-f", name).Run() })
}
