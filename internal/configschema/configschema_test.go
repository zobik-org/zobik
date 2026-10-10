package configschema

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"zobik.org/zobik/internal/globalconfig"
)

// The v0 of implementation §6, The initial Global Configuration, for the keys
// this binary declares.
func TestV0(t *testing.T) {
	s, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"auction_timeout":               `2000`,
		"reannounce_backoff_base":       `5000`,
		"max_reannounce_backoff":        `300000`,
		"task_lease_ttl":                `30000`,
		"similarity_precision_decimals": `3`,
		"network_admission":             `"frozen"`,
		"context_retention_grace":       `604800000`,
		"hitl_response_window":          `86400000`,
		"result_delivery":               `{"audiences":{"user":"acceptance"},"default":"ack"}`,
	}
	v0 := s.V0()
	if len(v0) != len(want) {
		t.Errorf("v0 has %d keys, want %d", len(v0), len(want))
	}
	for k, w := range want {
		if got := compact(t, v0[k]); got != w {
			t.Errorf("%s = %s, want %s", k, got, w)
		}
	}
	if _, ok := v0["embedding_model"]; ok {
		t.Error("embedding_model has no default")
	}
}

var model = `{"entry":"embeddings","model":"nomic-embed-text","document_prefix":"search_document: ","query_prefix":"search_query: "}`

func TestApplyVersioned(t *testing.T) {
	s, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	v0 := s.V0()
	cases := []struct {
		name   string
		head   globalconfig.Values
		assign map[string]string
		reject string // a substring of the reason; empty when it applies
	}{
		{"a valid edit", v0, map[string]string{"auction_timeout": `3000`}, ""},
		{"no key", v0, map[string]string{}, "assigns no key"},
		{"unknown key", v0, map[string]string{"auction_timeut": `3000`}, "no such key"},
		{"wrong type", v0, map[string]string{"auction_timeout": `"3s"`}, "expected integer"},
		{"out of range", v0, map[string]string{"similarity_precision_decimals": `9`}, "maximum"},
		{"enum", v0, map[string]string{"network_admission": `"paused"`}, "not one of"},
		{"open without a vector space", v0, map[string]string{"network_admission": `"open"`}, "embedding_model assigned"},
		{"auction not below the window", v0, map[string]string{"auction_timeout": `86400000`}, "less than hitl_response_window"},
		{"window not above the auction", v0, map[string]string{"hitl_response_window": `1000`}, "less than hitl_response_window"},
		{"assign the vector space frozen", v0, map[string]string{"embedding_model": model}, ""},
		{"assign the vector space and open together", v0, map[string]string{"embedding_model": model, "network_admission": `"open"`}, "requires network_admission = frozen"},
		{"open once it has one", with(v0, "embedding_model", model), map[string]string{"network_admission": `"open"`}, ""},
		{"edit the vector space open", with(with(v0, "embedding_model", model), "network_admission", `"open"`), map[string]string{"embedding_model": model}, "requires network_admission = frozen"},
		{"a composite field missing", v0, map[string]string{"embedding_model": `{"entry":"e","model":"m"}`}, "required"},
	}
	for _, c := range cases {
		assign := map[string]json.RawMessage{}
		for k, v := range c.assign {
			assign[k] = json.RawMessage(v)
		}
		got, err := s.ApplyVersioned(c.head, assign)
		if c.reject == "" {
			if err != nil {
				t.Errorf("%s: %v", c.name, err)
				continue
			}
			for k, v := range c.assign {
				if compact(t, got[k]) != compact(t, json.RawMessage(v)) {
					t.Errorf("%s: %s = %s", c.name, k, got[k])
				}
			}
			continue
		}
		var invalid *globalconfig.InvalidError
		if !errors.As(err, &invalid) || !strings.Contains(err.Error(), c.reject) {
			t.Errorf("%s: got %v, want a rejection with %q", c.name, err, c.reject)
		}
	}
}

// The head is never modified: each edit produces a new snapshot (architecture §2.3).
func TestApplyLeavesHead(t *testing.T) {
	s, _ := Load()
	head := s.V0()
	if _, err := s.ApplyVersioned(head, map[string]json.RawMessage{"auction_timeout": json.RawMessage(`3000`)}); err != nil {
		t.Fatal(err)
	}
	if string(head["auction_timeout"]) != "2000" {
		t.Errorf("the head changed: %s", head["auction_timeout"])
	}
}

func with(v globalconfig.Values, key, value string) globalconfig.Values {
	out := globalconfig.Values{}
	for k, x := range v {
		out[k] = x
	}
	out[key] = json.RawMessage(value)
	return out
}

func compact(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("%s: %v", raw, err)
	}
	b, _ := json.Marshal(v)
	return string(b)
}
