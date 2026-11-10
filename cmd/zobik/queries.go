package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"time"

	"zobik.org/zobik/internal/console"
)

// zobik catalog list opens node_catalog_query, and prints the library.
func catalogCmd(ctx context.Context, args []string) error {
	if len(args) == 0 || args[0] != "list" {
		return errors.New("usage: zobik catalog list")
	}
	return queryCmd(ctx, "catalog list", console.TopicNodeCatalogQuery, args[1:])
}

// zobik topic list opens topic_catalog_query, and prints the Topic Registry
// with the state of each topic in the network.
func topicCmd(ctx context.Context, args []string) error {
	if len(args) == 0 || args[0] != "list" {
		return errors.New("usage: zobik topic list")
	}
	return queryCmd(ctx, "topic list", console.TopicTopicCatalog, args[1:])
}

// queryCmd opens a query on the park as a client of the console, waits for
// its close and prints its answer as JSON: the portion of one answered by
// Claim-Check, or the data of the close (architecture §3.17.1).
func queryCmd(ctx context.Context, name, topic string, args []string) error {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	var n networkFlags
	n.register(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("usage: zobik %s", name)
	}
	if err := n.resolve(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	c, err := consoleClient(ctx, n)
	if err != nil {
		return err
	}
	defer func() { _ = c.SignOut(context.WithoutCancel(ctx)) }()
	// The outcome arrives over the event stream, which opens before the query.
	outcomes, err := c.Outcomes(ctx)
	if err != nil {
		return err
	}
	opened, err := c.Query(ctx, topic)
	if err != nil {
		return err
	}
	fmt.Printf("trace %s\n", opened.TraceID)
	for o := range outcomes {
		if o.TraceID != opened.TraceID {
			continue
		}
		var d struct {
			FailureCode   string          `json:"failure_code"`
			FailureReason string          `json:"failure_reason"`
			Data          json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(o.Details, &d); err != nil {
			return fmt.Errorf("the outcome: %w", err)
		}
		if d.FailureCode != "" || d.FailureReason != "" {
			return fmt.Errorf("rejected, %s: %s", d.FailureCode, d.FailureReason)
		}
		answer := o.Result
		if answer == nil {
			answer = d.Data
		}
		var out bytes.Buffer
		if err := json.Indent(&out, answer, "", "  "); err != nil {
			return fmt.Errorf("the answer: %w", err)
		}
		fmt.Println(out.String())
		return nil
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("waiting for the outcome: %w", err)
	}
	return errors.New("the console closed the event stream before the outcome")
}
