//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// El acto del Spawner: zobik node provision emits node_provision from the
// console, and the Spawner brings up echo from its Catalog entry with the
// Active Node Registry's labels. A node that does not fit in fleet_capacity
// closes with capacity_exhausted, and a node keeps winning tasks after its
// identity's life, because the Spawner renews it.
func TestSpawner(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	z := newNetwork(t, ctx)
	z.build(t, ctx, "images/zobik-sidecar.Dockerfile", "zobik-sidecar:dev")
	z.build(t, ctx, "test/e2e/embeddings/Dockerfile", embeddingsImage)
	// Two nodes of echo fit, 160 MB each with the Integration Sidecar, and a
	// third does not.
	const lifetime = 30 * time.Second
	lines, _ := z.startWith(t, ctx, credentials, "init", "--fleet-capacity", "330", "--node-identity-lifetime", lifetime.String())
	waitWithin(t, lines, regexp.MustCompile(`the panel is at (\S+)`), 8*time.Minute)
	z.blueprint(t, ctx, "examples/blueprints/echo/Dockerfile", "zobik-echo:dev")

	blueprint := filepath.Join(z.root, "examples/blueprints/echo/blueprint.json")
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
	text := "repeat this"
	same := []float32{1, 0}
	z.embeddings(t, ctx, map[string][]float32{documentPrefix + b.CapabilityText: same, queryPrefix + text: same})
	z.run(t, ctx, credentials, "config", "set",
		embeddingsRegistry,
		`embedding_model={"entry":"embeddings","model":"fake","document_prefix":"`+documentPrefix+`","query_prefix":"`+queryPrefix+`","max_text_size":8192}`)
	z.run(t, ctx, credentials, "config", "set", "network_admission=open")

	node, logic := z.provision(t, ctx, "echo", blueprint)
	labels := containerLabels(t, ctx, logic)
	for k, want := range map[string]string{
		"org.zobik.node": node, "org.zobik.topic": "echo", "org.zobik.model_id": "",
		"org.zobik.signer_id": "", "org.zobik.similarity_floor": "0.6",
		"org.zobik.egress_grants": "[]", "org.zobik.ingress_grants": "[]", "org.zobik.memory_required": "128",
	} {
		if got, ok := labels[k]; !ok || (want != "" && got != want) {
			t.Errorf("label %s = %q, want %q", k, got, want)
		}
	}

	// A second instance of the entry fits; a third does not.
	ref := z.register(t, ctx, "echo", blueprint)
	z.run(t, ctx, credentials, "node", "provision", ref)
	if out := z.fail(t, ctx, credentials, "node", "provision", ref); !strings.Contains(out, "capacity_exhausted") {
		t.Errorf("a third node: %s", out)
	}
	// A request names an entry of the Catalog.
	if out := z.fail(t, ctx, credentials, "node", "provision", "sha256:"+strings.Repeat("0", 64)); !strings.Contains(out, "no such entry") {
		t.Errorf("an entry the Catalog lacks: %s", out)
	}

	// Past several lives of its first identity, echo still wins a task.
	time.Sleep(3 * lifetime)
	tap := z.start(t, ctx, "tap")
	waitFor(t, tap, regexp.MustCompile(`^tap: listening`))
	events := tapEvents(tap)
	task := uuid.NewString()
	z.run(t, ctx, "", "publish", "--topic", "echo", "--task", task, "--text", text)
	lifecycle(t, events, task)
}

// containerLabels reads a container's labels.
func containerLabels(t *testing.T, ctx context.Context, name string) map[string]string {
	t.Helper()
	out, err := exec.CommandContext(ctx, "docker", "inspect", "--format", "{{json .Config.Labels}}", name).Output()
	if err != nil {
		t.Fatalf("inspecting %s: %v", name, err)
	}
	var labels map[string]string
	if err := json.Unmarshal(out, &labels); err != nil {
		t.Fatal(err)
	}
	return labels
}
