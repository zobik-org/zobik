package egress

import (
	"encoding/json"
	"strings"
	"testing"
)

// openAI is the interface of the OpenAI model slot (implementation §7.2).
const openAI = `{
  "protocol": "https",
  "direction": "egress",
  "operations": {
    "create_response": {
      "fields": {
        "type": "object",
        "required": ["model", "input"],
        "properties": {
          "model": {"type": "string"},
          "instructions": {"type": "string"},
          "input": {"type": "array", "items": {"opaque": true}},
          "tools": {"type": "array", "items": {"opaque": true}},
          "text_format": {"opaque": true},
          "max_output_tokens": {"type": "integer"}
        }
      },
      "response": {
        "type": "object",
        "properties": {
          "output": {"opaque": true},
          "model": {"type": "string"},
          "usage": {
            "type": "object",
            "properties": {
              "input_tokens": {"type": "integer"},
              "input_tokens_details": {"type": "object", "properties": {"cached_tokens": {"type": "integer"}}},
              "output_tokens": {"type": "integer"}
            }
          }
        }
      },
      "https": {
        "method": "POST",
        "path": "/v1/responses",
        "headers": {"Content-Type": "application/json"},
        "fields": {
          "model": {"in": "json", "at": "/model"},
          "instructions": {"in": "json", "at": "/instructions"},
          "input": {"in": "json", "at": "/input"},
          "tools": {"in": "json", "at": "/tools"},
          "text_format": {"in": "json", "at": "/text/format"},
          "max_output_tokens": {"in": "json", "at": "/max_output_tokens"}
        },
        "response": {
          "output": {"from": "json", "at": "/output"},
          "model": {"from": "json", "at": "/model"},
          "usage": {"from": "json", "at": "/usage"}
        }
      }
    }
  }
}`

// cost is the cost of the OpenAI model slot (implementation §7.2).
const cost = `response.status >= 400
  ? 0.0
  : double(response.usage.input_tokens - response.usage.input_tokens_details.cached_tokens) * rate[response.model].input
    + double(response.usage.input_tokens_details.cached_tokens) * rate[response.model].cached_input
    + double(response.usage.output_tokens) * rate[response.model].output`

// openAIEntry is the proposed entry of implementation §7.2, with the rules and
// the edits of each case applied over it.
func openAIEntry(t *testing.T, edit func(map[string]any)) json.RawMessage {
	t.Helper()
	e := map[string]any{
		"interface":            json.RawMessage(openAI),
		"target":               "https://api.openai.com",
		"operations":           []string{"create_response"},
		"credential_handle":    "openai",
		"credential_placement": map[string]string{"header": "Authorization", "scheme": "Bearer"},
		"rules": []map[string]string{
			{"when": `model.startsWith("gpt-5-pro")`, "then": "requester"},
			{"then": "allow"},
		},
		"billing_rule": map[string]any{"create_response": map[string]string{
			"cost":       cost,
			"tokens_in":  "response.status >= 400 ? 0 : response.usage.input_tokens",
			"tokens_out": "response.status >= 400 ? 0 : response.usage.output_tokens",
		}},
		"rate_source": "openai",
	}
	if edit != nil {
		edit(e)
	}
	raw, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestParseEntry(t *testing.T) {
	e, err := ParseEntry(openAIEntry(t, nil))
	if err != nil {
		t.Fatal(err)
	}
	if e.GrantScope != ScopeOpen || len(e.Rules) != 2 || e.Rules[0].Then != "requester" || e.Rules[1].When != nil {
		t.Errorf("entry: %+v", e)
	}
	b := e.Billing["create_response"]
	if b == nil || b.Cost == nil || b.TokensIn == nil || b.TokensOut == nil {
		t.Fatalf("billing: %+v", b)
	}
	if !strings.HasPrefix(e.Interface.Digest, "sha256:") {
		t.Errorf("digest %q", e.Interface.Digest)
	}

	// The digest is that of the canonical form: another serialization of the
	// same interface gives the same one (implementation §8.1).
	var v any
	if err := json.Unmarshal([]byte(openAI), &v); err != nil {
		t.Fatal(err)
	}
	compact, _ := json.Marshal(v)
	again, err := ParseInterface(compact)
	if err != nil || again.Digest != e.Interface.Digest {
		t.Errorf("the digest changed with the serialization: %v", err)
	}
}

func TestEmbeddingsEntry(t *testing.T) {
	if _, err := ParseEntry(EmbeddingsEntry("http://llama:8080")); err != nil {
		t.Fatal(err)
	}
}

func TestParseEntryRejects(t *testing.T) {
	// op decodes the interface the first time, so each case edits it in place.
	op := func(e map[string]any) map[string]any {
		in, ok := e["interface"].(map[string]any)
		if !ok {
			_ = json.Unmarshal(e["interface"].(json.RawMessage), &in)
			e["interface"] = in
		}
		return in["operations"].(map[string]any)["create_response"].(map[string]any)
	}
	mapping := func(e map[string]any) map[string]any { return op(e)["https"].(map[string]any) }
	fields := func(e map[string]any) map[string]any {
		return op(e)["fields"].(map[string]any)["properties"].(map[string]any)
	}
	cases := []struct {
		name   string
		edit   func(map[string]any)
		reject string
	}{
		{"an operation the interface lacks", func(e map[string]any) { e["operations"] = []string{"create_response", "delete_all"} }, "no operation"},
		{"a wildcard operation", func(e map[string]any) { e["operations"] = []string{"*"} }, "no operation"},
		{"a condition on a field the interface lacks", func(e map[string]any) {
			e["rules"] = []map[string]string{{"when": `price > 5`, "then": "deny"}, {"then": "allow"}}
		}, "undeclared reference"},
		{"a condition of the wrong type", func(e map[string]any) {
			e["rules"] = []map[string]string{{"when": `model > 5`, "then": "deny"}, {"then": "allow"}}
		}, "no matching overload"},
		{"a last rule with when", func(e map[string]any) {
			e["rules"] = []map[string]string{{"when": `model == "x"`, "then": "deny"}}
		}, "last rule carries no when"},
		{"a rule without when before the last", func(e map[string]any) {
			e["rules"] = []map[string]string{{"then": "allow"}, {"then": "deny"}}
		}, "only the last rule"},
		{"an outcome that is no audience", func(e map[string]any) { e["rules"] = []map[string]string{{"then": "Finance Team"}} }, "not allow, deny or an audience"},
		{"a billing_rule missing an operation", func(e map[string]any) { e["billing_rule"] = map[string]any{} }, "has no expression"},
		{"a cost that is no double", func(e map[string]any) {
			e["billing_rule"] = map[string]any{"create_response": map[string]string{"cost": "response.usage.input_tokens"}}
		}, "not double"},
		{"tokens that are no int", func(e map[string]any) {
			e["billing_rule"] = map[string]any{"create_response": map[string]string{"cost": "0.0", "tokens_in": "1.5"}}
		}, "not int"},
		{"a cost without rate_source", func(e map[string]any) { delete(e, "rate_source") }, "costs 0.0"},
		{"a platform handle", func(e map[string]any) { e["credential_handle"] = "zobik/catalog" }, "platform's"},
		{"a credential without placement", func(e map[string]any) { delete(e, "credential_placement") }, "says where it goes"},
		{"a placement without credential", func(e map[string]any) { delete(e, "credential_handle") }, "no credential_handle"},
		{"a placement in two places", func(e map[string]any) {
			e["credential_placement"] = map[string]string{"header": "Authorization", "query": "key"}
		}, "a header or in a query"},
		{"a field where the credential goes", func(e map[string]any) {
			mapping(e)["headers"] = map[string]string{"authorization": "x"}
		}, "writes the header"},
		{"grantee_topics outside topics", func(e map[string]any) { e["grantee_topics"] = []string{"code_review"} }, "grantee_topics"},
		{"topics without grantee_topics", func(e map[string]any) { e["grant_scope"] = "topics" }, "grantee_topics"},
		{"a field without mapping", func(e map[string]any) { delete(mapping(e)["fields"].(map[string]any), "tools") }, "has no mapping"},
		{"a header written twice", func(e map[string]any) {
			mapping(e)["headers"] = map[string]string{"X-Model": "a"}
			mapping(e)["fields"].(map[string]any)["model"] = map[string]string{"in": "header", "at": "x-model"}
		}, "written twice"},
		{"a mapping that writes Host", func(e map[string]any) { mapping(e)["headers"] = map[string]string{"host": "x"} }, "adapter sets"},
		{"a path segment without field", func(e map[string]any) { mapping(e)["path"] = "/v1/{org}/responses" }, "no path field"},
		{"a json field outside a pointer", func(e map[string]any) {
			mapping(e)["fields"].(map[string]any)["model"] = map[string]string{"in": "json", "at": "model"}
		}, "JSON Pointer"},
		{"a field named like a variable", func(e map[string]any) {
			fields(e)["operation"] = map[string]string{"type": "string"}
			mapping(e)["fields"].(map[string]any)["operation"] = map[string]string{"in": "json", "at": "/operation"}
		}, "named like a variable"},
		{"a field outside the subset", func(e map[string]any) { fields(e)["model"] = map[string]any{"type": "string", "pattern": "x"} }, "outside the subset"},
		{"a response declaring status", func(e map[string]any) {
			op(e)["response"].(map[string]any)["properties"].(map[string]any)["status"] = map[string]string{"type": "integer"}
			mapping(e)["response"].(map[string]any)["status"] = map[string]string{"from": "header", "at": "X-Status"}
		}, "without declaring it"},
		{"a body without max_body_size", func(e map[string]any) {
			op(e)["body"] = map[string]bool{"request": false, "response": true}
			op(e)["response"] = map[string]any{"type": "object", "properties": map[string]any{}}
			mapping(e)["response"] = map[string]any{}
			e["billing_rule"] = map[string]any{"create_response": map[string]string{"cost": "0.0"}}
		}, "max_body_size"},
		{"an ingress interface", func(e map[string]any) {
			op(e)
			e["interface"].(map[string]any)["direction"] = "ingress"
		}, "egress interface"},
	}
	for _, c := range cases {
		_, err := ParseEntry(openAIEntry(t, c.edit))
		if err == nil || !strings.Contains(err.Error(), c.reject) {
			t.Errorf("%s: got %v, want an error with %q", c.name, err, c.reject)
		}
	}
}

// A field name has a single type in the whole interface (implementation §8.2).
func TestFieldTypeAcrossOperations(t *testing.T) {
	in := `{"protocol":"https","direction":"egress","operations":{
	  "a":{"fields":{"type":"object","properties":{"n":{"type":"integer"}}},"https":{"method":"GET","path":"/a","fields":{"n":{"in":"query","at":"n"}}}},
	  "b":{"fields":{"type":"object","properties":{"n":{"type":"string"}}},"https":{"method":"GET","path":"/b","fields":{"n":{"in":"query","at":"n"}}}}}}`
	if _, err := ParseInterface(json.RawMessage(in)); err == nil || !strings.Contains(err.Error(), "another type") {
		t.Errorf("got %v", err)
	}
	same := strings.Replace(in, `"n":{"type":"string"}`, `"n":{"type":"integer","description":"the same"}`, 1)
	if _, err := ParseInterface(json.RawMessage(same)); err != nil {
		t.Errorf("the same type with another description: %v", err)
	}
}
