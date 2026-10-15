// Package embeddingtest is the fake embeddings endpoint of the test harness: it
// serves POST /v1/embeddings with deterministic vectors, and each test fixes the
// ones its similarity floors are measured against.
package embeddingtest

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

// Dimensions is the length of the vectors the endpoint derives by itself.
const Dimensions = 8

// Request is a call the endpoint received.
type Request struct {
	Model string
	Input string
}

// Handler is the fake endpoint. An input with a vector set answers with it; any
// other, with one derived from its hash.
type Handler struct {
	mu       sync.Mutex
	vectors  map[string][]float32
	requests []Request
}

// NewHandler returns the endpoint with no vector set.
func NewHandler() *Handler {
	return &Handler{vectors: map[string][]float32{}}
}

// Server is the endpoint served on a local port.
type Server struct {
	*httptest.Server
	*Handler
}

// NewServer starts the endpoint and stops it when the test ends.
func NewServer(t testing.TB) *Server {
	h := NewHandler()
	s := &Server{Server: httptest.NewServer(h), Handler: h}
	t.Cleanup(s.Close)
	return s
}

// Set fixes the vector the endpoint answers input with, prefix included.
func (h *Handler) Set(input string, v []float32) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.vectors[input] = v
}

// Requests returns the calls received so far.
func (h *Handler) Requests() []Request {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]Request(nil), h.requests...)
}

// ServeHTTP answers POST /v1/embeddings.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || r.URL.Path != "/v1/embeddings" {
		http.NotFound(w, r)
		return
	}
	var req struct {
		Model string   `json:"model"`
		Input []string `json:"input"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Model == "" || len(req.Input) == 0 {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	type datum struct {
		Object    string    `json:"object"`
		Index     int       `json:"index"`
		Embedding []float32 `json:"embedding"`
	}
	data := make([]datum, len(req.Input))
	h.mu.Lock()
	for i, in := range req.Input {
		h.requests = append(h.requests, Request{Model: req.Model, Input: in})
		v, ok := h.vectors[in]
		if !ok {
			v = derive(in)
		}
		data[i] = datum{Object: "embedding", Index: i, Embedding: v}
	}
	h.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"object": "list",
		"model":  req.Model,
		"data":   data,
		"usage":  map[string]int{"prompt_tokens": 0, "total_tokens": 0},
	})
}

// derive is the vector of an input without one set: its hash, read as
// Dimensions components in [-1, 1).
func derive(input string) []float32 {
	sum := sha256.Sum256([]byte(input))
	v := make([]float32, Dimensions)
	for i := range v {
		v[i] = float32(binary.LittleEndian.Uint32(sum[4*i:]))/(1<<31) - 1
	}
	return v
}
