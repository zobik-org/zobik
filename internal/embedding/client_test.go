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
	"zobik.org/zobik/internal/egress"
	"zobik.org/zobik/internal/embedding"
	"zobik.org/zobik/internal/embedding/embeddingtest"
)

const model = `{"entry":"embeddings","model":"nomic-embed-text","document_prefix":"search_document: ","query_prefix":"search_query: ","max_text_size":8192}`

// executor reaches an entry without credential nor rate.
var executor = &egress.Executor{HTTP: egress.HTTPClient(nil), Rates: func(string) (*egress.Table, bool) { return nil, false }}

func values(target string) map[string]json.RawMessage {
	return map[string]json.RawMessage{
		"embedding_model": json.RawMessage(model),
		"egress_registry": json.RawMessage(`{"embeddings":` + string(egress.EmbeddingsEntry(target)) + `}`),
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

	// The model_id is the digest of the fields that define the space in their
	// canonical form, so another spelling of the same value gives the same one
	// (implementation §1.2.15).
	spelled := values("http://llama:8080")
	spelled["embedding_model"] = json.RawMessage(`{ "query_prefix":"search_query: ", "model":"nomic-embed-text", "entry":"embeddings", "document_prefix":"search_document: ", "max_text_size":8192 }`)
	other, err := embedding.Resolve(spelled)
	if err != nil {
		t.Fatal(err)
	}
	want, _ := digest.JSON([]byte(`{"entry":"embeddings","model":"nomic-embed-text","document_prefix":"search_document: ","query_prefix":"search_query: "}`))
	if s.ModelID != want || other.ModelID != want {
		t.Errorf("model_id %s and %s, want %s", s.ModelID, other.ModelID, want)
	}

	// max_text_size bounds what is embedded without changing the space.
	bounded := values("http://llama:8080")
	bounded["embedding_model"] = json.RawMessage(strings.Replace(model, "8192", "2048", 1))
	if b, err := embedding.Resolve(bounded); err != nil || b.ModelID != want || b.MaxTextSize != 2048 {
		t.Errorf("another max_text_size: %+v %v", b, err)
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

	q, err := s.Query(ctx, executor, "echo this")
	if err != nil {
		t.Fatal(err)
	}
	if len(q) != 2 || q[0] != 0.6 || q[1] != 0.8 {
		t.Errorf("query vector %v, want it normalized to [0.6 0.8]", q)
	}
	d, err := s.Document(ctx, executor, "echoes text")
	if err != nil {
		t.Fatal(err)
	}
	if sim, _ := embedding.Similarity(q, d); math.Abs(sim-0.8) > 1e-6 {
		t.Errorf("similarity %v, want 0.8", sim)
	}

	// A text without a vector set gets a deterministic one.
	a, _ := s.Query(ctx, executor, "anything")
	b, _ := s.Query(ctx, executor, "anything")
	if sim, _ := embedding.Similarity(a, b); math.Abs(sim-1) > 1e-6 {
		t.Errorf("the same text gave vectors at similarity %v", sim)
	}

	// A text longer than max_text_size, prefix included, never reaches the
	// endpoint (implementation §1.2.15).
	long := strings.Repeat("x", 8192-len("search_query: ")+1)
	if s.FitsQuery(long) || !s.FitsQuery(long[1:]) {
		t.Error("FitsQuery does not count the prefix")
	}
	if _, err := s.Query(ctx, executor, long); !errors.Is(err, embedding.ErrTooLong) {
		t.Errorf("a text too long: %v", err)
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
	if _, err := s.Query(context.Background(), executor, "x"); err != nil {
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
		"not JSON":         {http.StatusOK, `<html>`, "no single embedding"},
		"no embedding":     {http.StatusOK, `{"data":[]}`, "no single embedding"},
		"two embeddings":   {http.StatusOK, `{"data":[{"embedding":[1]},{"embedding":[1]}]}`, "no single embedding"},
		"a zero vector":    {http.StatusOK, `{"data":[{"embedding":[0,0]}]}`, "no direction"},
		"a redirect":       {http.StatusFound, ``, "302"},
		"an empty vector":  {http.StatusOK, `{"data":[{"embedding":[]}]}`, "no single embedding"},
		"a string element": {http.StatusOK, `{"data":[{"embedding":["1"]}]}`, "not a number"},
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
		_, err := s.Query(context.Background(), executor, "x")
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: got %v, want %q", name, err, c.want)
		}
		srv.Close()
	}

	if _, err := embedding.Resolve(values("ftp://llama")); err == nil || !strings.Contains(err.Error(), "http or https") {
		t.Errorf("an ftp target: %v", err)
	}
}

// The embeddings go through the entry's rules and with its credential
// (architecture §3.16.3).
func TestGenerateThroughTheEntry(t *testing.T) {
	var auth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth = r.Header.Get("Authorization")
		w.Write([]byte(`{"data":[{"embedding":[1,0]}]}`))
	}))
	defer srv.Close()
	entry := func(rules string) map[string]json.RawMessage {
		var e map[string]any
		_ = json.Unmarshal(egress.EmbeddingsEntry(srv.URL), &e)
		e["credential_handle"] = "embeddings_key"
		e["credential_placement"] = map[string]string{"header": "Authorization", "scheme": "Bearer"}
		e["rules"] = json.RawMessage(rules)
		raw, _ := json.Marshal(map[string]any{"embeddings": e})
		return map[string]json.RawMessage{"embedding_model": json.RawMessage(model), "egress_registry": raw}
	}
	x := &egress.Executor{
		HTTP:       egress.HTTPClient(nil),
		Credential: func(context.Context, string) ([]byte, error) { return []byte("k"), nil },
		Rates:      func(string) (*egress.Table, bool) { return nil, false },
	}
	s, err := embedding.Resolve(entry(`[{"then":"allow"}]`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Query(context.Background(), x, "x"); err != nil || auth != "Bearer k" {
		t.Errorf("allowed: %v, with %q", err, auth)
	}
	s, _ = embedding.Resolve(entry(`[{"when":"model == \"nomic-embed-text\"","then":"deny"},{"then":"allow"}]`))
	if _, err := s.Query(context.Background(), x, "x"); err == nil || !strings.Contains(err.Error(), "deny") {
		t.Errorf("denied: %v", err)
	}
}
