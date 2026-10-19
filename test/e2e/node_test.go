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

	"zobik.org/zobik/internal/bus"
)

const (
	embeddingsImage = "zobik-embeddings:e2e"
	documentPrefix  = "search_document: "
	queryPrefix     = "search_query: "
)

// Un nodo: a task.announced published by hand is won by echo, which completes it.
func TestNode(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	z := newNetwork(t, ctx)
	z.build(t, ctx, "images/zobik-sidecar.Dockerfile", "zobik-sidecar:dev")
	z.build(t, ctx, "images/zobik-python.Dockerfile", "zobik-python:dev")
	z.build(t, ctx, "examples/blueprints/echo/Dockerfile", "zobik-echo:dev")
	z.build(t, ctx, "test/e2e/embeddings/Dockerfile", embeddingsImage)
	z.run(t, ctx, credentials, "init")

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
	// The task's text and echo's capability share a vector: similarity 1, above
	// the default task_similarity_floor.
	text := "repeat this"
	same := []float32{1, 0}
	z.embeddings(t, ctx, map[string][]float32{documentPrefix + b.CapabilityText: same, queryPrefix + text: same})

	z.run(t, ctx, credentials, "config", "set",
		`egress_registry={"embeddings":{"target":"http://embeddings:8080"}}`,
		`embedding_model={"entry":"embeddings","model":"fake","document_prefix":"`+documentPrefix+`","query_prefix":"`+queryPrefix+`","max_text_size":8192}`)
	z.run(t, ctx, credentials, "config", "set", "network_admission=open")

	lines := z.start(t, ctx, "tap")
	waitFor(t, lines, regexp.MustCompile(`^tap: listening`))
	events := tapEvents(lines)

	if out := z.run(t, ctx, "", "node", "up", "--topic", "echo", blueprint); !strings.Contains(out, "provisioned node") {
		t.Fatalf("node up said: %s", out)
	}
	task := uuid.NewString()
	z.run(t, ctx, "", "publish", "--topic", "echo", "--task", task, "--text", text)
	lifecycle(t, events, task)
}

// embeddings runs the fake embeddings endpoint on the network's Docker network,
// reached at http://embeddings:8080, answering the inputs of fixed with their
// vectors.
func (z *network) embeddings(t *testing.T, ctx context.Context, fixed map[string][]float32) {
	t.Helper()
	vectors, err := json.Marshal(fixed)
	if err != nil {
		t.Fatal(err)
	}
	name := "zobik-" + z.name + "-embeddings"
	// The network's label lets remove find it if the cleanup never runs.
	run := exec.CommandContext(ctx, "docker", "run", "-d", "--name", name,
		"--label", "org.zobik.network="+z.name,
		"--network", "zobik-"+z.name, "--network-alias", "embeddings",
		embeddingsImage, "-vectors", string(vectors))
	if out, err := run.CombinedOutput(); err != nil {
		t.Fatalf("running the embeddings endpoint: %v\n%s", err, out)
	}
	t.Cleanup(func() { exec.Command("docker", "rm", "-f", name).Run() })
}

// lifecycle waits for task to walk announced → proposed → assigned → completed,
// and for its completion to carry the context_ref of the portion result of its
// section. It returns the id of the task.assigned.
func lifecycle(t *testing.T, events <-chan tapEvent, task string) (assigned string) {
	t.Helper()
	want := map[string]bool{bus.TypeAnnounced: true, bus.TypeProposed: true, bus.TypeAssigned: true, bus.TypeCompleted: true}
	timeout := time.After(2 * time.Minute)
	for len(want) > 0 {
		var e tapEvent
		var ok bool
		select {
		case e, ok = <-events:
			if !ok {
				t.Fatalf("task %s: zobik tap ended, missing %v", task, want)
			}
		case <-timeout:
			t.Fatalf("task %s: missing %v", task, want)
		}
		if e.Subject != task {
			continue
		}
		switch e.Type {
		case bus.TypeAssigned:
			assigned = e.ID
		case bus.TypeFailed:
			t.Fatalf("task %s failed: %s", task, e.Data)
		case bus.TypeCompleted:
			var d struct {
				ContextRef struct {
					TaskID string `json:"task_id"`
					Name   string `json:"name"`
				} `json:"context_ref"`
			}
			if json.Unmarshal(e.Data, &d); d.ContextRef.TaskID != task || d.ContextRef.Name != "result" {
				t.Errorf("task %s completed without the context_ref of its result: %s", task, e.Data)
			}
		}
		delete(want, e.Type)
	}
	return assigned
}
