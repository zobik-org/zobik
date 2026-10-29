package embedding

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"sync"
	"time"

	"zobik.org/zobik/internal/digest"
	"zobik.org/zobik/internal/egress"
)

// Space is the network's vector space: the value of embedding_model with the
// egress_registry entry it names (architecture §2.3, §3.7.3).
type Space struct {
	// ModelID identifies the space: the digest of the fields of
	// embedding_model that define it (implementation §1.2.15).
	ModelID        string
	Target         string
	Model          string
	DocumentPrefix string
	QueryPrefix    string
	// MaxTextSize is the longest text the model accepts, prefix included, in
	// bytes of UTF-8.
	MaxTextSize int

	// entry is the entry the embeddings go through, with its adapter, its rules
	// and its metering (architecture §3.16.3).
	entry *egress.Entry
}

// ErrNoSpace is a configuration without embedding_model: the network is born
// without a vector space (architecture §2.3).
var ErrNoSpace = errors.New("embedding: the network has no vector space: embedding_model is not assigned")

// entries caches the compiled entries by their document: every announcement
// resolves the space, and compiling an entry's rules is the costly part.
var entries = struct {
	sync.Mutex
	m map[string]*egress.Entry
}{m: map[string]*egress.Entry{}}

// maxEntries bounds the entries kept compiled.
const maxEntries = 16

func compiled(raw json.RawMessage) (*egress.Entry, error) {
	entries.Lock()
	e, ok := entries.m[string(raw)]
	entries.Unlock()
	if ok {
		return e, nil
	}
	e, err := egress.ParseEntry(raw)
	if err != nil {
		return nil, err
	}
	entries.Lock()
	if len(entries.m) >= maxEntries {
		clear(entries.m)
	}
	entries.m[string(raw)] = e
	entries.Unlock()
	return e, nil
}

// Resolve reads the space from a snapshot of the Global Configuration.
func Resolve(values map[string]json.RawMessage) (*Space, error) {
	raw, ok := values["embedding_model"]
	if !ok {
		return nil, ErrNoSpace
	}
	var m struct {
		Entry          string `json:"entry"`
		Model          string `json:"model"`
		DocumentPrefix string `json:"document_prefix"`
		QueryPrefix    string `json:"query_prefix"`
		MaxTextSize    int    `json:"max_text_size"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("embedding: embedding_model: %w", err)
	}
	if m.MaxTextSize <= 0 {
		return nil, errors.New("embedding: embedding_model carries no max_text_size")
	}
	var registry map[string]json.RawMessage
	if r, ok := values["egress_registry"]; ok {
		if err := json.Unmarshal(r, &registry); err != nil {
			return nil, fmt.Errorf("embedding: egress_registry: %w", err)
		}
	}
	entryRaw, ok := registry[m.Entry]
	if !ok {
		return nil, fmt.Errorf("embedding: embedding_model.entry %q names no egress_registry entry", m.Entry)
	}
	var target struct {
		Target string `json:"target"`
	}
	_ = json.Unmarshal(entryRaw, &target)
	base, err := url.Parse(target.Target)
	if err != nil || (base.Scheme != "http" && base.Scheme != "https") || base.Host == "" {
		return nil, fmt.Errorf("embedding: target %q is not an http or https URL", target.Target)
	}
	entry, err := compiled(entryRaw)
	if err != nil {
		return nil, fmt.Errorf("embedding: embedding_model.entry %q: %w", m.Entry, err)
	}
	// max_text_size bounds what is embedded without changing the space, so it
	// stays out of the model_id (implementation §1.2.15).
	space, err := json.Marshal(map[string]string{
		"entry": m.Entry, "model": m.Model, "document_prefix": m.DocumentPrefix, "query_prefix": m.QueryPrefix,
	})
	if err != nil {
		return nil, err
	}
	id, err := digest.JSON(space)
	if err != nil {
		return nil, fmt.Errorf("embedding: model_id: %w", err)
	}
	return &Space{
		ModelID:        id,
		Target:         target.Target,
		Model:          m.Model,
		DocumentPrefix: m.DocumentPrefix,
		QueryPrefix:    m.QueryPrefix,
		MaxTextSize:    m.MaxTextSize,
		entry:          entry,
	}, nil
}

// ErrTooLong is a text the model does not accept: the endpoint would reject it
// on every retry (implementation §1.2.15).
var ErrTooLong = errors.New("embedding: the text exceeds the max_text_size of embedding_model")

// FitsQuery reports whether a task's text, with its prefix, fits in max_text_size.
func (s *Space) FitsQuery(text string) bool { return len(s.QueryPrefix)+len(text) <= s.MaxTextSize }

// FitsDocument reports whether a capability_text, with its prefix, fits in max_text_size.
func (s *Space) FitsDocument(text string) bool {
	return len(s.DocumentPrefix)+len(text) <= s.MaxTextSize
}

// Query generates the embedding of a task's text, the querying side of the
// matching (architecture §3.7.4).
func (s *Space) Query(ctx context.Context, x *egress.Executor, text string) ([]float32, error) {
	if !s.FitsQuery(text) {
		return nil, ErrTooLong
	}
	return s.generate(ctx, x, s.QueryPrefix+text)
}

// Document generates the embedding of a capability_text (architecture §3.7.3).
func (s *Space) Document(ctx context.Context, x *egress.Executor, text string) ([]float32, error) {
	if !s.FitsDocument(text) {
		return nil, ErrTooLong
	}
	return s.generate(ctx, x, s.DocumentPrefix+text)
}

// generate calls the operation of the OpenAI embeddings API through the entry,
// with its rules and its adapter (implementation §1.2.15), and returns the
// vector normalized (architecture §3.7.5). Its cost does not enter any task's
// metrics: the embedding is integration, not domain work (architecture §3.6.1).
func (s *Space) generate(ctx context.Context, x *egress.Executor, input string) ([]float32, error) {
	fields, err := json.Marshal(map[string]any{
		"model":           s.Model,
		"input":           []string{input},
		"encoding_format": "float",
	})
	if err != nil {
		return nil, err
	}
	call, err := s.entry.NewCall(egress.EmbeddingsOperation, fields, false)
	if err != nil {
		return nil, fmt.Errorf("embedding: %w", err)
	}
	if o := s.entry.Decide(call, egress.Lists, time.Now()); o.Then != egress.Allow {
		return nil, fmt.Errorf("embedding: %s", o)
	}
	res, err := x.Execute(ctx, s.entry, call, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("embedding: %w", err)
	}
	if status := res.Response["status"]; status != json.Number("200") {
		return nil, fmt.Errorf("embedding: the endpoint answered %v", status)
	}
	data, _ := res.Response["data"].([]any)
	if len(data) != 1 {
		return nil, errors.New("embedding: the response carries no single embedding")
	}
	item, _ := data[0].(map[string]any)
	values, _ := item["embedding"].([]any)
	if len(values) == 0 {
		return nil, errors.New("embedding: the response carries no single embedding")
	}
	v := make([]float64, len(values))
	for i, x := range values {
		n, ok := x.(json.Number)
		if !ok {
			return nil, fmt.Errorf("embedding: response: element %d is not a number", i)
		}
		if v[i], err = n.Float64(); err != nil {
			return nil, fmt.Errorf("embedding: response: %w", err)
		}
	}
	return normalize(v)
}

// normalize scales v to unit length, the form embeddings are stored and signed
// in (architecture §3.7.5).
func normalize(v []float64) ([]float32, error) {
	var sum float64
	for _, x := range v {
		sum += x * x
	}
	norm := math.Sqrt(sum)
	if norm == 0 || math.IsNaN(norm) || math.IsInf(norm, 0) {
		return nil, errors.New("embedding: the endpoint returned a vector with no direction")
	}
	out := make([]float32, len(v))
	for i, x := range v {
		out[i] = float32(x / norm)
	}
	return out, nil
}
