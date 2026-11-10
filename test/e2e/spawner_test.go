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
	up := time.Now()
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
	// After starting, the Spawner holds every node busy for a task_lease_ttl
	// (implementation §1.2.4): only past it is a node free to evict.
	time.Sleep(time.Until(up.Add(35 * time.Second)))
	if provisioned.FindStringSubmatch(z.run(t, ctx, credentials, "node", "provision", ref)) == nil {
		t.Fatal("the third node was not provisioned")
	}
	if exec.CommandContext(ctx, "docker", "container", "inspect", logic).Run() == nil {
		t.Errorf("the first node was not evicted")
	}
	node, logic = second, z.logic(second)
	z.parkQueries(t, ctx, blueprint, ref)
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
	// With no instance left, the entry is archived, and the network's veto shows.
	var lib library
	z.query(t, ctx, &lib, "catalog", "list")
	if e := lib.entry(ref); e == nil || e.State != "archived" || !e.VetoMark || len(e.FloorHistory) == 0 || len(e.ActiveFloors) != 0 {
		t.Errorf("the vetoed echo in the library: %+v", e)
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

// Las consultas del parque: the census lists the fleet with the fields of the
// Active Node Registry; node_catalog_query returns each entry with its state;
// topic_catalog_query returns a topic active, one available and one uncovered
// (architecture §3.7.6, §3.12.3, §3.13.3).
func (z *network) parkQueries(t *testing.T, ctx context.Context, blueprint, echo string) {
	t.Helper()
	// spare has an entry the network can bring up; barred's only entry is vetoed.
	refs := map[string]string{}
	for _, topic := range []string{"spare", "barred"} {
		tag := "zobik-echo:" + topic
		z.blueprint(t, ctx, "examples/blueprints/echo/Dockerfile", tag, "--label", "org.zobik.test="+topic)
		raw, err := os.ReadFile(blueprint)
		if err != nil {
			t.Fatal(err)
		}
		other := filepath.Join(t.TempDir(), "blueprint.json")
		if err := os.WriteFile(other, []byte(strings.Replace(string(raw), "zobik-echo:dev", tag, 1)), 0o644); err != nil {
			t.Fatal(err)
		}
		refs[topic] = z.register(t, ctx, topic, other)
	}
	z.run(t, ctx, credentials, "node", "veto", refs["barred"])

	var registry struct {
		Topics []struct{ Topic, Description, State string }
	}
	z.query(t, ctx, &registry, "topic", "list")
	states := map[string]string{}
	for _, tp := range registry.Topics {
		states[tp.Topic] = tp.State
		if tp.Description != topicDescription(tp.Topic) {
			t.Errorf("topic %s described as %q", tp.Topic, tp.Description)
		}
	}
	if states["echo"] != "active" || states["spare"] != "available" || states["barred"] != "uncovered" {
		t.Errorf("topic states %v", states)
	}

	var census struct {
		Nodes []struct {
			NodeID          string          `json:"node_id"`
			ArtifactRef     string          `json:"artifact_ref"`
			Topic           string          `json:"topic"`
			ModelID         string          `json:"model_id"`
			SignerID        string          `json:"signer_id"`
			SimilarityFloor float64         `json:"similarity_floor"`
			EgressGrants    json.RawMessage `json:"egress_grants"`
			MemoryRequired  int64           `json:"memory_required"`
		} `json:"nodes"`
	}
	z.query(t, ctx, &census, "node", "list")
	if len(census.Nodes) != 2 {
		t.Fatalf("census %+v", census.Nodes)
	}
	for _, n := range census.Nodes {
		labels := containerLabels(t, ctx, z.logic(n.NodeID))
		if n.ArtifactRef != echo || n.Topic != "echo" || n.ModelID != labels["org.zobik.model_id"] ||
			n.SignerID != labels["org.zobik.signer_id"] || n.SimilarityFloor != 0.6 || n.MemoryRequired != 128 || string(n.EgressGrants) != "[]" {
			t.Errorf("census row %+v", n)
		}
	}

	var lib library
	z.query(t, ctx, &lib, "catalog", "list")
	for ref, want := range map[string]string{echo: "active", refs["spare"]: "registered", refs["barred"]: "registered"} {
		if e := lib.entry(ref); e == nil || e.State != want {
			t.Errorf("entry %s: %+v, want %s", ref, e, want)
		}
	}
	if e := lib.entry(echo); e != nil && (len(e.ActiveFloors) != 2 || len(e.PresentIn) != 1 || e.PresentIn[0] != z.name || e.VetoMark) {
		t.Errorf("echo in the library: %+v", e)
	}
	if e := lib.entry(refs["barred"]); e != nil && !e.VetoMark {
		t.Errorf("barred in the library: %+v", e)
	}
}

// library is what the tests read of node_catalog_query's answer.
type library struct {
	Entries []libraryEntry `json:"entries"`
}

type libraryEntry struct {
	ArtifactRef  string            `json:"artifact_ref"`
	State        string            `json:"state"`
	ActiveFloors []float64         `json:"active_floors"`
	FloorHistory []json.RawMessage `json:"floor_history"`
	VetoMark     bool              `json:"veto_mark"`
	PresentIn    []string          `json:"present_in"`
}

func (l *library) entry(ref string) *libraryEntry {
	for i := range l.Entries {
		if l.Entries[i].ArtifactRef == ref {
			return &l.Entries[i]
		}
	}
	return nil
}

// query runs a console query command and decodes the answer it prints after
// its trace line.
func (z *network) query(t *testing.T, ctx context.Context, out any, args ...string) {
	t.Helper()
	printed := z.run(t, ctx, credentials, args...)
	i := strings.Index(printed, "\n{")
	if i < 0 || json.Unmarshal([]byte(printed[i+1:]), out) != nil {
		t.Fatalf("zobik %s printed: %s", strings.Join(args, " "), printed)
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
