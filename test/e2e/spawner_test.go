//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"net/http"
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
// evicts the oldest free one, and a node keeps winning tasks after its
// identity's life, because the Spawner renews it. Then node_retire removes,
// vetoes and purges.
func TestSpawner(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	z := newNetwork(t, ctx)
	z.build(t, ctx, "images/zobik-sidecar.Dockerfile", "zobik-sidecar:dev")
	z.build(t, ctx, "test/e2e/embeddings/Dockerfile", embeddingsImage)
	// Two nodes of echo fit, 160 MB each with the Integration Sidecar, and a
	// third evicts one.
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

	// A second instance of the entry fits; a third evicts the free node with
	// the oldest assignment, the first, which never had one (architecture
	// §3.13.10, implementation §1.2.4).
	ref := z.register(t, ctx, "echo", blueprint)
	provisioned := regexp.MustCompile(`provisioned node (U[A-Z0-9]+)`)
	m := provisioned.FindStringSubmatch(z.run(t, ctx, credentials, "node", "provision", ref))
	if m == nil {
		t.Fatal("the second node was not provisioned")
	}
	second := m[1]
	if provisioned.FindStringSubmatch(z.run(t, ctx, credentials, "node", "provision", ref)) == nil {
		t.Fatal("the third node was not provisioned")
	}
	if exec.CommandContext(ctx, "docker", "container", "inspect", logic).Run() == nil {
		t.Errorf("the first node was not evicted")
	}
	node, logic = second, z.logic(second)
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

	z.retire(t, ctx, node, logic, ref, blueprint)
}

// Retirar y vetar: node remove archives a node with its containers and
// volumes; node veto archives the other instance and keeps the artifact from
// coming up until node unveto; node purge rejects a seeded artifact and one a
// network runs, and deletes the image of one nobody runs.
func (z *network) retire(t *testing.T, ctx context.Context, node, logic, ref, blueprint string) {
	t.Helper()
	if out := z.run(t, ctx, credentials, "node", "remove", node); !strings.Contains(out, "archived nodes "+node) {
		t.Errorf("node remove said: %s", out)
	}
	for _, kind := range []string{"container", "volume"} {
		name := logic
		if kind == "volume" {
			name = strings.TrimSuffix(logic, "-logic") + "-private"
		}
		if exec.CommandContext(ctx, "docker", kind, "inspect", name).Run() == nil {
			t.Errorf("the %s %s outlived the node", kind, name)
		}
	}

	if out := z.run(t, ctx, credentials, "node", "veto", ref); !regexp.MustCompile(`archived nodes U[A-Z0-9]+`).MatchString(out) {
		t.Errorf("node veto said: %s", out)
	}
	if out := z.fail(t, ctx, credentials, "node", "provision", ref); !strings.Contains(out, "artifact_vetoed") {
		t.Errorf("provisioning a vetoed artifact: %s", out)
	}
	z.run(t, ctx, credentials, "node", "unveto", ref)
	if out := z.fail(t, ctx, credentials, "node", "purge", ref); !strings.Contains(out, "seeded") {
		t.Errorf("purging a seeded artifact: %s", out)
	}

	// Another artifact, imported, is purged once nothing runs it.
	z.blueprint(t, ctx, "examples/blueprints/echo/Dockerfile", "zobik-echo:purge", "--label", "org.zobik.test=purge")
	raw, err := os.ReadFile(blueprint)
	if err != nil {
		t.Fatal(err)
	}
	other := filepath.Join(t.TempDir(), "blueprint.json")
	if err := os.WriteFile(other, []byte(strings.Replace(string(raw), "zobik-echo:dev", "zobik-echo:purge", 1)), 0o644); err != nil {
		t.Fatal(err)
	}
	var e struct {
		ArtifactRef string `json:"artifact_ref"`
	}
	out := z.run(t, ctx, "", "register", "--topic", "echo", "--provenance", "imported", other)
	if err := json.Unmarshal([]byte(lastLine(out)), &e); err != nil || e.ArtifactRef == "" || e.ArtifactRef == ref {
		t.Fatalf("register said: %s", out)
	}
	m := regexp.MustCompile(`provisioned node (U[A-Z0-9]+)`).FindStringSubmatch(z.run(t, ctx, credentials, "node", "provision", e.ArtifactRef))
	if m == nil {
		t.Fatal("the imported artifact was not provisioned")
	}
	if out := z.fail(t, ctx, credentials, "node", "purge", e.ArtifactRef); !strings.Contains(out, "runs in "+z.name) {
		t.Errorf("purging a running artifact: %s", out)
	}
	z.run(t, ctx, credentials, "node", "remove", m[1])
	z.run(t, ctx, credentials, "node", "purge", e.ArtifactRef)
	if out := z.fail(t, ctx, credentials, "node", "provision", e.ArtifactRef); !strings.Contains(out, "being deleted") {
		t.Errorf("provisioning a purged artifact: %s", out)
	}
	base := strings.TrimSpace(z.run(t, ctx, "", "base-image"))
	addr, _, _ := strings.Cut(base, "/")
	resp, err := http.Get("http://" + addr + "/v2/artifacts/manifests/" + e.ArtifactRef)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("the purged manifest in the registry: %s", resp.Status)
	}
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
