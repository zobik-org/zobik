package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"strings"
	"time"

	"zobik.org/zobik/internal/signing"
)

// zobik node provision instantiates a Catalog entry: a client of the console,
// which emits node_provision with the operator's proof and waits for its close
// (architecture §3.13.8, implementation §6, The operator console).
func nodeCmd(ctx context.Context, args []string) error {
	const usage = "usage: zobik node provision [--slot <slot>=<entry> ...] <artifact_ref>"
	if len(args) == 0 || args[0] != "provision" {
		return errors.New(usage)
	}
	fs := flag.NewFlagSet("node provision", flag.ContinueOnError)
	var n networkFlags
	n.register(fs)
	var associations slotFlags
	fs.Var(&associations, "slot", "an egress slot and the egress_registry entry the request associates it with, <slot>=<entry>; repeatable")
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
	opened, err := c.NodeProvision(ctx, fs.Arg(0), associations)
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
				NodeID string `json:"node_id"`
			} `json:"data"`
		}
		if err := json.Unmarshal(o.Details, &d); err != nil {
			return fmt.Errorf("the outcome: %w", err)
		}
		if d.FailureCode != "" || d.FailureReason != "" {
			return fmt.Errorf("rejected, %s: %s", d.FailureCode, d.FailureReason)
		}
		fmt.Printf("provisioned node %s\n", d.Data.NodeID)
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
