package egress

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"slices"
	"sync"
)

// EmbeddingsInterface is the interface of the OpenAI embeddings API, which the
// distribution brings with the Integration Sidecar (implementation §1.2.15).
//
//go:embed embeddings.interface.json
var EmbeddingsInterface []byte

// EmbeddingsOperation is its operation, POST /v1/embeddings.
const EmbeddingsOperation = "create_embeddings"

var embeddings = sync.OnceValues(func() (*Interface, error) {
	return ParseInterface(EmbeddingsInterface)
})

// EmbeddingsEntry is the entry of an embeddings endpoint on a local runtime: no
// credential and no rate (implementation §1.2.15, §8.5).
func EmbeddingsEntry(target string) json.RawMessage {
	entry, err := json.Marshal(map[string]any{
		"interface":    json.RawMessage(EmbeddingsInterface),
		"target":       target,
		"operations":   []string{EmbeddingsOperation},
		"rules":        []map[string]string{{"then": Allow}},
		"billing_rule": map[string]any{EmbeddingsOperation: map[string]string{"cost": free, "tokens_in": "response.status >= 400 ? 0 : response.usage.prompt_tokens"}},
	})
	if err != nil {
		panic(err)
	}
	return entry
}

// CheckRegistry checks every entry of a snapshot's egress_registry
// (architecture §3.14.4).
func CheckRegistry(values map[string]json.RawMessage) error {
	raw, ok := values["egress_registry"]
	if !ok {
		return nil
	}
	_, err := ParseRegistry(raw)
	return err
}

// CheckSpace checks that the entry embedding_model names implements the
// embeddings interface and admits its operation (implementation §1.2.15). That
// the entry exists is a precondition of the schema.
func CheckSpace(values map[string]json.RawMessage) error {
	raw, ok := values["embedding_model"]
	if !ok {
		return nil
	}
	var m struct {
		Entry string `json:"entry"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return err
	}
	var registry map[string]json.RawMessage
	if r, ok := values["egress_registry"]; ok {
		if err := json.Unmarshal(r, &registry); err != nil {
			return err
		}
	}
	entryRaw, ok := registry[m.Entry]
	if !ok {
		return nil
	}
	e, err := ParseEntry(entryRaw)
	if err != nil {
		return fmt.Errorf("embedding_model.entry: %w", err)
	}
	want, err := embeddings()
	if err != nil {
		return err
	}
	if e.Interface.Digest != want.Digest || !slices.Contains(e.Operations, EmbeddingsOperation) {
		return fmt.Errorf("embedding_model.entry %q does not implement the embeddings interface with %s", m.Entry, EmbeddingsOperation)
	}
	return nil
}
