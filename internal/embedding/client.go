package embedding

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"

	"zobik.org/zobik/internal/digest"
)

// Space is the network's vector space: the value of embedding_model with the
// target of the egress_registry entry it names (architecture §2.3, §3.7.3).
type Space struct {
	// ModelID identifies the space: the digest of embedding_model
	// (implementation §1.2.15).
	ModelID        string
	Target         string
	Model          string
	DocumentPrefix string
	QueryPrefix    string
}

// ErrNoSpace is a configuration without embedding_model: the network is born
// without a vector space (architecture §2.3).
var ErrNoSpace = errors.New("embedding: the network has no vector space: embedding_model is not assigned")

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
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("embedding: embedding_model: %w", err)
	}
	var registry map[string]struct {
		Target string `json:"target"`
	}
	if r, ok := values["egress_registry"]; ok {
		if err := json.Unmarshal(r, &registry); err != nil {
			return nil, fmt.Errorf("embedding: egress_registry: %w", err)
		}
	}
	entry, ok := registry[m.Entry]
	if !ok {
		return nil, fmt.Errorf("embedding: embedding_model.entry %q names no egress_registry entry", m.Entry)
	}
	id, err := digest.JSON(raw)
	if err != nil {
		return nil, fmt.Errorf("embedding: model_id: %w", err)
	}
	return &Space{
		ModelID:        id,
		Target:         entry.Target,
		Model:          m.Model,
		DocumentPrefix: m.DocumentPrefix,
		QueryPrefix:    m.QueryPrefix,
	}, nil
}

// Query generates the embedding of a task's text, the querying side of the
// matching (architecture §3.7.4).
func (s *Space) Query(ctx context.Context, c *http.Client, text string) ([]float32, error) {
	return s.generate(ctx, c, s.QueryPrefix+text)
}

// Document generates the embedding of a capability_text (architecture §3.7.3).
func (s *Space) Document(ctx context.Context, c *http.Client, text string) ([]float32, error) {
	return s.generate(ctx, c, s.DocumentPrefix+text)
}

// maxResponse bounds the response read: a 4096-dimension vector in JSON is
// under 100 KB.
const maxResponse = 4 << 20

// Client is the HTTP client of the embeddings endpoint: it follows no redirect,
// which would take the request out of the entry's target (implementation §8.3).
func Client() *http.Client {
	return &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
}

// generate calls POST /v1/embeddings under the target, the operation of the
// OpenAI embeddings API (implementation §1.2.15), and returns the vector
// normalized (architecture §3.7.5).
func (s *Space) generate(ctx context.Context, c *http.Client, input string) ([]float32, error) {
	base, err := url.Parse(s.Target)
	if err != nil || (base.Scheme != "http" && base.Scheme != "https") || base.Host == "" {
		return nil, fmt.Errorf("embedding: target %q is not an http or https URL", s.Target)
	}
	body, err := json.Marshal(map[string]any{
		"model":           s.Model,
		"input":           []string{input},
		"encoding_format": "float",
	})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base.JoinPath("v1", "embeddings").String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.Do(req)
	if err != nil {
		return nil, fmt.Errorf("embedding: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponse+1))
	if err != nil {
		return nil, fmt.Errorf("embedding: %w", err)
	}
	if len(raw) > maxResponse {
		return nil, errors.New("embedding: the response exceeds its bound")
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("embedding: the endpoint answered %s: %s", resp.Status, truncate(raw, 200))
	}
	var out struct {
		Data []struct {
			Embedding []float64 `json:"embedding"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("embedding: response: %w", err)
	}
	if len(out.Data) != 1 || len(out.Data[0].Embedding) == 0 {
		return nil, errors.New("embedding: the response carries no single embedding")
	}
	return normalize(out.Data[0].Embedding)
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

func truncate(b []byte, n int) []byte {
	if len(b) <= n {
		return b
	}
	return append(b[:n:n], "…"...)
}
