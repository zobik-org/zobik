package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"strings"
	"time"

	"zobik.org/zobik/internal/console"
	"zobik.org/zobik/internal/signing"
)

// zobik node acts on the fleet: a client of the console, which emits the
// request with the operator's proof and waits for its close (architecture
// §3.13.8, implementation §6, The operator console). provision instantiates a
// Catalog entry; remove, veto, unveto and purge emit node_retire.
func nodeCmd(ctx context.Context, args []string) error {
	const usage = "usage: zobik node provision [--slot <slot>=<entry> ...] <artifact_ref>\n" +
		"       zobik node remove <node_id>\n" +
		"       zobik node veto|unveto|purge <artifact_ref>"
	if len(args) == 0 {
		return errors.New(usage)
	}
	kind := args[0]
	switch kind {
	case "provision", "remove", "veto", "unveto", "purge":
	default:
		return errors.New(usage)
	}
	fs := flag.NewFlagSet("node "+kind, flag.ContinueOnError)
	var n networkFlags
	n.register(fs)
	var associations slotFlags
	if kind == "provision" {
		fs.Var(&associations, "slot", "an egress slot and the egress_registry entry the request associates it with, <slot>=<entry>; repeatable")
	}
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New(usage)
	}
	if err := n.resolve(); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, 20*time.Minute)
	defer cancel()
	c, err := consoleClient(ctx, n)
	if err != nil {
		return err
	}
	defer func() { _ = c.SignOut(context.WithoutCancel(ctx)) }()
	// The outcome arrives over the event stream, which opens before the act.
	outcomes, err := c.Outcomes(ctx)
	if err != nil {
		return err
	}
	var opened *console.Opened
	switch kind {
	case "provision":
		opened, err = c.NodeProvision(ctx, fs.Arg(0), associations)
	case "remove":
		opened, err = c.NodeRetire(ctx, console.RetireRequest{Kind: kind, NodeID: fs.Arg(0)})
	default:
		opened, err = c.NodeRetire(ctx, console.RetireRequest{Kind: kind, ArtifactRef: fs.Arg(0)})
	}
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
				NodeID  string   `json:"node_id"`
				NodeIDs []string `json:"node_ids"`
			} `json:"data"`
		}
		if err := json.Unmarshal(o.Details, &d); err != nil {
			return fmt.Errorf("the outcome: %w", err)
		}
		if d.FailureCode != "" || d.FailureReason != "" {
			return fmt.Errorf("rejected, %s: %s", d.FailureCode, d.FailureReason)
		}
		switch kind {
		case "provision":
			fmt.Printf("provisioned node %s\n", d.Data.NodeID)
		case "remove", "veto":
			fmt.Printf("%s: archived nodes %s\n", kind, strings.Join(d.Data.NodeIDs, " "))
		default:
			fmt.Printf("%s: done\n", kind)
		}
		return nil
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("waiting for the outcome: %w", err)
	}
	return errors.New("the console closed the event stream before the outcome")
}

// slotFlags are the repeated --slot <slot>=<entry>.
type slotFlags []signing.Association

func (s *slotFlags) String() string { return fmt.Sprint(*s) }

func (s *slotFlags) Set(v string) error {
	slot, entry, ok := strings.Cut(v, "=")
	if !ok || slot == "" || entry == "" {
		return fmt.Errorf("%q is not <slot>=<entry>", v)
	}
	*s = append(*s, signing.Association{Slot: slot, Entry: entry})
	return nil
}
