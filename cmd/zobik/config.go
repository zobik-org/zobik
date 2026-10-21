package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"slices"
	"strings"
	"time"

	"zobik.org/zobik/internal/console"
)

// zobik config get and zobik config set are the administration affordance on
// the command line: a client of the console, which signs in at its start and
// out at its end (architecture §3.17.1, implementation §6, The operator console).
func configCmd(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: zobik config get | set <key>=<value> [<key>=<value> ...]")
	}
	fs := flag.NewFlagSet("config "+args[0], flag.ContinueOnError)
	var n networkFlags
	n.register(fs)
	base := fs.String("base", "", "set: the config_version edited on (default: the head)")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if err := n.resolve(); err != nil {
		return err
	}
	var assignments map[string]json.RawMessage
	switch args[0] {
	case "get":
		if fs.NArg() != 0 {
			return errors.New("usage: zobik config get")
		}
	case "set":
		var err error
		if assignments, err = parseAssignments(fs.Args()); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown config command %q", args[0])
	}

	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	c, err := consoleClient(ctx, n)
	if err != nil {
		return err
	}
	defer func() { _ = c.SignOut(context.WithoutCancel(ctx)) }()

	if args[0] == "get" {
		snap, err := c.Config(ctx)
		if err != nil {
			return fmt.Errorf("reading the configuration: %w", err)
		}
		printSnapshot(snap)
		return nil
	}
	// The outcome arrives over the event stream, which opens before the act.
	outcomes, err := c.Outcomes(ctx)
	if err != nil {
		return err
	}
	opened, err := c.ConfigSet(ctx, *base, assignments)
	if err != nil {
		return err
	}
	fmt.Printf("trace %s\n", opened.TraceID)
	for o := range outcomes {
		if o.TraceID != opened.TraceID {
			continue
		}
		var d struct {
			FailureCode   string `json:"failure_code"`
			FailureReason string `json:"failure_reason"`
			Data          struct {
				ConfigVersion string `json:"config_version"`
			} `json:"data"`
		}
		if err := json.Unmarshal(o.Details, &d); err != nil {
			return fmt.Errorf("the outcome: %w", err)
		}
		if d.FailureCode != "" {
			return fmt.Errorf("rejected, %s: %s", d.FailureCode, d.FailureReason)
		}
		fmt.Printf("published %s\n", d.Data.ConfigVersion)
		return nil
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("waiting for the outcome: %w", err)
	}
	return errors.New("the console closed the event stream before the outcome")
}

// parseAssignments reads key=value pairs. A value is JSON, and one that is not
// is taken as a string: network_admission=open.
func parseAssignments(args []string) (map[string]json.RawMessage, error) {
	if len(args) == 0 {
		return nil, errors.New("usage: zobik config set <key>=<value> [<key>=<value> ...]")
	}
	out := map[string]json.RawMessage{}
	for _, a := range args {
		key, value, ok := strings.Cut(a, "=")
		if !ok || key == "" {
			return nil, fmt.Errorf("%q is not <key>=<value>", a)
		}
		if json.Valid([]byte(value)) {
			out[key] = json.RawMessage(value)
		} else {
			out[key], _ = json.Marshal(value)
		}
	}
	return out, nil
}

func printSnapshot(s *console.SnapshotJSON) {
	fmt.Printf("%s, published %s\n\n", s.ConfigVersion, s.PublishedAt)
	keys := make([]string, 0, len(s.Values))
	for k := range s.Values {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, k := range keys {
		fmt.Printf("  %-30s %s\n", k, s.Values[k])
	}
}
