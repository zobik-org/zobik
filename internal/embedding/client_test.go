package embedding_test

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"zobik.org/zobik/internal/digest"
	"zobik.org/zobik/internal/embedding"
	"zobik.org/zobik/internal/embedding/embeddingtest"
)

const model = `{"entry":"embeddings","model":"nomic-embed-text","document_prefix":"search_document: ","query_prefix":"search_query: "}`

func values(target string) map[string]json.RawMessage {
	return map[string]json.RawMessage{
		"embedding_model": json.RawMessage(model),
		"egress_registry": json.RawMessage(`{"embeddings":{"target":"` + target + `"}}`),
	}
}

func TestResolve(t *testing.T) {
	s, err := embedding.Resolve(values("http://llama:8080"))
	if err != nil {
		t.Fatal(err)
	}
	if s.Target != "http://llama:8080" || s.Model != "nomic-embed-text" || s.QueryPrefix != "search_query: " || s.DocumentPrefix != "search_document: " {
		t.Errorf("space: %+v", s)
	}

	// The model_id is the digest of embedding_model in its canonical form, so
	// another spelling of the same value gives the same one (implementation §1.2.15).
	spelled := values("http://llama:8080")
	spelled["embedding_model"] = json.RawMessage(`{ "query_prefix":"search_query: ", "model":"nomic-embed-text", "entry":"embeddings", "document_prefix":"search_document: " }`)
	other, err := embedding.Resolve(spelled)
	if err != nil {
		t.Fatal(err)
	}
	want, _ := digest.JSON([]byte(model))
	if s.ModelID != want || other.ModelID != want {
		t.Errorf("model_id %s and %s, want %s", s.ModelID, other.ModelID, want)
	}

	// The target is not part of the space.
	moved, _ := embedding.Resolve(values("http://elsewhere:8080"))
	if moved.ModelID != s.ModelID {
		t.Error("moving the entry's target changed the model_id")
	}

	if _, err := embedding.Resolve(map[string]json.RawMessage{}); !errors.Is(err, embedding.ErrNoSpace) {
		t.Errorf("without embedding_model: %v", err)
	}
	orphan := values("http://llama:8080")
	orphan["egress_registry"] = json.RawMessage(`{}`)
	if _, err := embedding.Resolve(orphan); err == nil || !strings.Contains(err.Error(), "names no egress_registry entry") {
		t.Errorf("an entry missing: %v", err)
	}
}

func TestGenerate(t *testing.T) {
	fake := embeddingtest.NewServer(t)
	fake.Set("search_query: echo this", []float32{3, 4})
	fake.Set("search_document: echoes text", []float32{0, 2})
	s, err := embedding.Resolve(values(fake.URL))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	q, err := s.Query(ctx, embedding.Client(), "echo this")
	if err != nil {
		t.Fatal(err)
	}
	if len(q) != 2 || q[0] != 0.6 || q[1] != 0.8 {
		t.Errorf("query vector %v, want it normalized to [0.6 0.8]", q)
	}
	d, err := s.Document(ctx, embedding.Client(), "echoes text")
	if err != nil {
		t.Fatal(err)
	}
	if sim, _ := embedding.Similarity(q, d); math.Abs(sim-0.8) > 1e-6 {
		t.Errorf("similarity %v, want 0.8", sim)
	}

	// A text without a vector set gets a deterministic one.
	a, _ := s.Query(ctx, embedding.Client(), "anything")
	b, _ := s.Query(ctx, embedding.Client(), "anything")
	if sim, _ := embedding.Similarity(a, b); math.Abs(sim-1) > 1e-6 {
		t.Errorf("the same text gave vectors at similarity %v", sim)
	}

	got := fake.Requests()
	if len(got) != 4 || got[0] != (embeddingtest.Request{Model: "nomic-embed-text", Input: "search_query: echo this"}) {
		t.Errorf("requests: %+v", got)
	}
}

func TestGenerateUnderPath(t *testing.T) {
	var path string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		w.Write([]byte(`{"data":[{"embedding":[1,0]}]}`))
	}))
	defer srv.Close()
	s, _ := embedding.Resolve(values(srv.URL + "/openai"))
	if _, err := s.Query(context.Background(), embedding.Client(), "x"); err != nil {
		t.Fatal(err)
	}
	if path != "/openai/v1/embeddings" {
		t.Errorf("path %s, want the operation under the target's root", path)
	}
}

func TestGenerateFailures(t *testing.T) {
	cases := map[string]struct {
		status int
		body   string
		want   string
	}{
		"an error status":  {http.StatusServiceUnavailable, `loading model`, "503"},
		"not JSON":         {http.StatusOK, `<html>`, "response"},
		"no embedding":     {http.StatusOK, `{"data":[]}`, "no single embedding"},
		"two embeddings":   {http.StatusOK, `{"data":[{"embedding":[1]},{"embedding":[1]}]}`, "no single embedding"},
		"a zero vector":    {http.StatusOK, `{"data":[{"embedding":[0,0]}]}`, "no direction"},
		"a redirect":       {http.StatusFound, ``, "302"},
		"an empty vector":  {http.StatusOK, `{"data":[{"embedding":[]}]}`, "no single embedding"},
		"a string element": {http.StatusOK, `{"data":[{"embedding":["1"]}]}`, "response"},
	}
	for name, c := range cases {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if c.status == http.StatusFound {
				w.Header().Set("Location", "http://elsewhere.example/")
			}
			w.WriteHeader(c.status)
			w.Write([]byte(c.body))
		}))
		s, _ := embedding.Resolve(values(srv.URL))
		_, err := s.Query(context.Background(), embedding.Client(), "x")
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: got %v, want %q", name, err, c.want)
		}
		srv.Close()
	}

	bad, _ := embedding.Resolve(values("ftp://llama"))
	if _, err := bad.Query(context.Background(), embedding.Client(), "x"); err == nil || !strings.Contains(err.Error(), "http or https") {
		t.Errorf("an ftp target: %v", err)
	}
}
