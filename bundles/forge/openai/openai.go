// Package openai is the model slot of the bundle's OpenAI variant
// (implementation §7.2): the interface of the Responses API, the entry the
// variant proposes and the rate table it brings.
package openai

import (
	_ "embed"
	"encoding/json"

	"zobik.org/zobik/internal/egress"
)

// Interface is the interface of the model slot.
//
//go:embed responses.interface.json
var Interface []byte

// Rates is the per-model price table the variant brings, in USD (implementation §8.5).
//
//go:embed rates.json
var Rates []byte

const (
	// Operation is the interface's operation, POST /v1/responses.
	Operation = "create_response"
	// Target is the destination of the proposed entry.
	Target = "https://api.openai.com"
	// RateSource names the table in the rates bucket.
	RateSource = "openai"
)

// The billing_rule of create_response: the usage tokens times the rate of the
// model the response returns, and nothing for an error response.
const (
	cost = `response.status >= 400
  ? 0.0
  : double(response.usage.input_tokens - response.usage.input_tokens_details.cached_tokens) * rate[response.model].input
    + double(response.usage.input_tokens_details.cached_tokens) * rate[response.model].cached_input
    + double(response.usage.output_tokens) * rate[response.model].output`
	tokensIn  = `response.status >= 400 ? 0 : response.usage.input_tokens`
	tokensOut = `response.status >= 400 ? 0 : response.usage.output_tokens`
)

// Entry is the entry the variant proposes, on target, with the key under
// handle as Authorization: Bearer (implementation §7.2, §8.4). An empty handle
// leaves it without a credential, like an endpoint that does not authenticate.
func Entry(target, handle string) json.RawMessage {
	e := map[string]any{
		"interface":    json.RawMessage(Interface),
		"target":       target,
		"operations":   []string{Operation},
		"grant_scope":  egress.ScopeOpen,
		"rules":        []map[string]string{{"then": egress.Allow}},
		"billing_rule": map[string]any{Operation: map[string]string{"cost": cost, "tokens_in": tokensIn, "tokens_out": tokensOut}},
		"rate_source":  RateSource,
	}
	if handle != "" {
		e["credential_handle"] = handle
		e["credential_placement"] = map[string]string{"header": "Authorization", "scheme": "Bearer"}
	}
	raw, err := json.Marshal(e)
	if err != nil {
		panic(err)
	}
	return raw
}
