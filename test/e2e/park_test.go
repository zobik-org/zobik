//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/console"
	"zobik.org/zobik/internal/embedding"
)

// El parque: a request written in the panel, on intake with no node, makes the
// Spawner provision echo from the Catalog, and echo's answer comes back. The
// park queries, opened through the console's API as the operation area opens
// them, show echo in the fleet and its entry active. A task whose model_id is
// not the current one fails with stale_vector_space (architecture §3.13.2).
func TestPark(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	z := newNetwork(t, ctx)
	z.build(t, ctx, "images/zobik-sidecar.Dockerfile", "zobik-sidecar:dev")
	z.build(t, ctx, "test/e2e/embeddings/Dockerfile", embeddingsImage)
	link, _ := z.init(t, ctx)
	z.blueprint(t, ctx, "examples/blueprints/echo/Dockerfile", "zobik-echo:dev")
	p := signIn(t, ctx, link)

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
	ref := z.register(t, ctx, bus.TopicIntake, blueprint)

	// No node claims intake: the Spawner provisions echo, which wins the
	// request's entry subtask on its re-announcement.
	changed := p.events(t, ctx)
	var conv console.Conversation
	p.call(t, ctx, http.MethodPost, console.PathConversations, nil, http.StatusCreated, &conv)
	request := p.send(t, ctx, conv.ID, text)
	outcome := p.wait(t, ctx, changed, conv.ID, func(m *console.Message) bool {
		return m.Kind == console.KindOutcome && m.TraceID == request.TraceID && m.State == console.StateAwaiting
	})
	if outcome.Text != text || !slices.Equal(outcome.Options, []string{"accept", "reject"}) {
		t.Errorf("the outcome of %q: %+v", text, outcome)
	}
	p.accept(t, ctx, conv.ID, outcome)

	// The park shows the node the Spawner brought up.
	var census struct {
		Nodes []struct {
			NodeID      string `json:"node_id"`
			ArtifactRef string `json:"artifact_ref"`
			Topic       string `json:"topic"`
		} `json:"nodes"`
	}
	z.query(t, ctx, &census, "node", "list")
	if len(census.Nodes) != 1 || census.Nodes[0].ArtifactRef != ref || census.Nodes[0].Topic != bus.TopicIntake {
		t.Errorf("census %+v", census.Nodes)
	} else if labels := containerLabels(t, ctx, z.logic(census.Nodes[0].NodeID)); labels["org.zobik.topic"] != bus.TopicIntake {
		t.Errorf("the census's node has the labels %v", labels)
	}
	var lib library
	z.query(t, ctx, &lib, "catalog", "list")
	if e := lib.entry(ref); e == nil || e.State != "active" || len(e.PresentIn) != 1 || e.PresentIn[0] != z.name {
		t.Errorf("echo in the library: %+v", e)
	}

	// A task left in another vector space: echo does not propose, and the
	// Spawner fails it.
	tap := z.start(t, ctx, "tap")
	waitFor(t, tap, regexp.MustCompile(`^tap: listening`))
	events := tapEvents(tap)
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
