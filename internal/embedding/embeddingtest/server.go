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

// Server is the fake endpoint. An input with a vector set answers with it; any
// other, with one derived from its hash.
type Server struct {
	*httptest.Server

	mu       sync.Mutex
	vectors  map[string][]float32
	requests []Request
}

// NewServer starts the endpoint and stops it when the test ends.
func NewServer(t testing.TB) *Server {
	s := &Server{vectors: map[string][]float32{}}
	s.Server = httptest.NewServer(http.HandlerFunc(s.serve))
	t.Cleanup(s.Close)
	return s
}

// Set fixes the vector the endpoint answers input with, prefix included.
func (s *Server) Set(input string, v []float32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.vectors[input] = v
}

// Requests returns the calls received so far.
func (s *Server) Requests() []Request {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Request(nil), s.requests...)
}

func (s *Server) serve(w http.ResponseWriter, r *http.Request) {
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
	s.mu.Lock()
	for i, in := range req.Input {
		s.requests = append(s.requests, Request{Model: req.Model, Input: in})
		v, ok := s.vectors[in]
		if !ok {
			v = derive(in)
		}
		data[i] = datum{Object: "embedding", Index: i, Embedding: v}
	}
	s.mu.Unlock()
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
