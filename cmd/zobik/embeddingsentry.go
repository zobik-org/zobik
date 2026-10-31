//go:build dev

package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"os"

	"zobik.org/zobik/internal/egress"
)

// zobik embeddings-entry prints an egress_registry entry of the embeddings
// interface the distribution brings, for the manual tests until the panel
// offers it (implementation §1.2.15): with --handle, the credential goes as
// Authorization: Bearer; with --rate-source, the cost is the usage's tokens times
// the input rate of the model the response returns (implementation §8.5).
func init() {
	devCommands["embeddings-entry"] = embeddingsEntryCmd
}

// embeddingsCost charges an error response nothing.
const embeddingsCost = `response.status >= 400 ? 0.0 : double(response.usage.prompt_tokens) * rate[response.model].input`

func embeddingsEntryCmd(_ context.Context, args []string) error {
	fs := flag.NewFlagSet("embeddings-entry", flag.ContinueOnError)
	target := fs.String("target", "", "the root URL of the endpoint, such as https://api.openai.com")
	handle := fs.String("handle", "", "the credential_handle, when the endpoint authenticates")
	rateSource := fs.String("rate-source", "", "the rate table, when the endpoint charges")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *target == "" || fs.NArg() != 0 {
		return errors.New("usage: zobik embeddings-entry --target <url> [--handle <handle>] [--rate-source <name>]")
	}
	var e map[string]any
	if err := json.Unmarshal(egress.EmbeddingsEntry(*target), &e); err != nil {
		return err
	}
	if *handle != "" {
		e["credential_handle"] = *handle
		e["credential_placement"] = map[string]string{"header": "Authorization", "scheme": "Bearer"}
	}
	if *rateSource != "" {
		e["rate_source"] = *rateSource
		billing := e["billing_rule"].(map[string]any)[egress.EmbeddingsOperation].(map[string]any)
		billing["cost"] = embeddingsCost
	}
	raw, err := json.Marshal(e)
	if err != nil {
		return err
	}
	// It is checked as the Config Store will check it.
	if _, err := egress.ParseEntry(raw); err != nil {
		return err
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetEscapeHTML(false)
	return enc.Encode(e)
}
