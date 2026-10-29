//go:build dev

package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"os"

	"zobik.org/zobik/bundles/forge/openai"
	"zobik.org/zobik/internal/egress"
)

// zobik model-entry prints the egress_registry entry of the OpenAI model slot
// that the bundle's variant proposes (implementation §7.2), for the manual tests
// until the console registers it when deploying the Forge: --then replaces its
// single rule's outcome, so the demo asks requester before each call.
func init() {
	devCommands["model-entry"] = modelEntryCmd
}

func modelEntryCmd(_ context.Context, args []string) error {
	fs := flag.NewFlagSet("model-entry", flag.ContinueOnError)
	target := fs.String("target", openai.Target, "the root URL of the endpoint")
	handle := fs.String("handle", "", "the credential_handle, when the endpoint authenticates")
	then := fs.String("then", egress.Allow, "the outcome of the single rule: allow, deny or an audience")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("usage: zobik model-entry [--target <url>] [--handle <handle>] [--then <outcome>]")
	}
	var e map[string]any
	if err := json.Unmarshal(openai.Entry(*target, *handle), &e); err != nil {
		return err
	}
	e["rules"] = []map[string]string{{"then": *then}}
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
