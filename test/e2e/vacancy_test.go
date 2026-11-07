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
	"zobik.org/zobik/internal/embedding"
)

// La vacancia: an announcement on intake with no node makes the Spawner
// provision echo from the Catalog, and the following announcement goes to
// echo. A task whose model_id is not the current one fails with
// stale_vector_space (architecture §3.13.2).
func TestVacancy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	z := newNetwork(t, ctx)
	z.build(t, ctx, "images/zobik-sidecar.Dockerfile", "zobik-sidecar:dev")
	z.build(t, ctx, "test/e2e/embeddings/Dockerfile", embeddingsImage)
	lines, _ := z.startWith(t, ctx, credentials, "init")
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
	// One vacant round is enough, and a task the Spawner lets go comes back soon.
	z.run(t, ctx, credentials, "config", "set", "network_admission=open", "vacancy_rounds_threshold=1",
		"reannounce_backoff_base=1000", "max_reannounce_backoff=4000", "task_lease_ttl=10000")
	z.register(t, ctx, bus.TopicIntake, blueprint)

	tap := z.start(t, ctx, "tap")
	waitFor(t, tap, regexp.MustCompile(`^tap: listening`))
	events := tapEvents(tap)

	// No node claims intake: the Spawner provisions echo, which wins the task
	// on its re-announcement.
	task := uuid.NewString()
	z.run(t, ctx, "", "publish", "--topic", bus.TopicIntake, "--task", task, "--text", text)
	lifecycle(t, events, task)
	out, err := exec.CommandContext(ctx, "docker", "ps", "-q", "--filter", "label=org.zobik.topic="+bus.TopicIntake).Output()
	if err != nil || len(strings.Fields(string(out))) != 1 {
		t.Errorf("the nodes on intake: %q %v", out, err)
	}

	// A task left in another vector space: echo does not propose, and the
	// Spawner fails it.
	stale := uuid.NewString()
	data, _ := json.Marshal(map[string]any{"task_embedding": embedding.Encode(same), "model_id": "sha256:old"})
	z.run(t, ctx, "", "publish", "--topic", bus.TopicIntake, "--task", stale, "--data", string(data))
	failed := func() string {
		timeout := time.After(2 * time.Minute)
		for {
			select {
			case e, ok := <-events:
				if !ok {
					t.Fatal("zobik tap ended")
				}
				if e.Subject != stale || e.Type != bus.TypeFailed {
					continue
				}
				var d struct {
					FailureCode string `json:"failure_code"`
				}
				json.Unmarshal(e.Data, &d)
				return d.FailureCode
			case <-timeout:
				return ""
			}
		}
	}()
	if failed != "stale_vector_space" {
		t.Errorf("the stale task: %q", failed)
	}
}
