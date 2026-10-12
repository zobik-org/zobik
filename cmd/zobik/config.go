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
	"zobik.org/zobik/internal/deploy"
	"zobik.org/zobik/internal/globalconfig"
)

// zobik config get and zobik config set are the administration affordance on
// the command line: they open the session in the same process, and talk to the
// Operator Channel (architecture §3.17.1, implementation §6, The operator console).
func configCmd(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: zobik config get | set <key>=<value>...")
	}
	fs := flag.NewFlagSet("config "+args[0], flag.ContinueOnError)
	var n networkFlags
	n.register(fs)
	base := fs.String("base", "", "set: the config_version edited on (default: the head it reads)")
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

	user, password, err := readCredentials(false)
	if err != nil {
		return err
	}
	session, err := deploy.OpenSession(n.dir, user, password)
	if err != nil {
		return fmt.Errorf("opening the session: %w", err)
	}
	id, addr, err := session.Channel("channel_operator")
	if err != nil {
		return err
	}
	client, closeConn, err := console.Dial(addr, session.ConsoleJWT, session.Console)
	if err != nil {
		return err
	}
	defer closeConn()
	op := &console.Operator{Root: session.Root, ChannelID: id, Client: client}

	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	snap, err := op.Snapshot(ctx)
	if err != nil {
		return fmt.Errorf("reading the configuration: %w", err)
	}
	if args[0] == "get" {
		printSnapshot(snap)
		return nil
	}
	if *base != "" && *base != snap.ConfigVersion {
		// An edit made on another version: the Config Store judges it (architecture §3.14.4).
		snap.ConfigVersion = *base
	}
	fmt.Printf("editing on %s\n", snap.ConfigVersion)
	out, err := op.ConfigSet(ctx, snap, assignments)
	var invalid *globalconfig.InvalidError
	if errors.As(err, &invalid) {
		return fmt.Errorf("not sent: %v", invalid)
	}
	if err != nil {
		return err
	}
	fmt.Printf("trace %s\n", out.TraceID)
	if out.FailureCode != "" {
		return fmt.Errorf("rejected, %s: %s", out.FailureCode, out.FailureReason)
	}
	fmt.Printf("published %s\n", out.ConfigVersion)
	return nil
}

// parseAssignments reads key=value pairs. A value is JSON, and one that is not
// is taken as a string: network_admission=open.
func parseAssignments(args []string) (map[string]json.RawMessage, error) {
	if len(args) == 0 {
		return nil, errors.New("usage: zobik config set <key>=<value>...")
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

func printSnapshot(s *console.Snapshot) {
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
