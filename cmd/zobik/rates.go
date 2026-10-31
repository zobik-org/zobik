package main

import (
	"context"
	"errors"
	"flag"
	"os"
	"time"
)

// zobik rates set writes a rate table through the console: an act of
// deployment, with the session open, whose values change without a
// config_change (implementation §8.5). The table is read from a file, so the
// username and the password keep standard input.
func ratesCmd(ctx context.Context, args []string) error {
	usage := errors.New("usage: zobik rates set <rate_source> <table.json>")
	if len(args) == 0 || args[0] != "set" {
		return usage
	}
	fs := flag.NewFlagSet("rates set", flag.ContinueOnError)
	var n networkFlags
	n.register(fs)
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if fs.NArg() != 2 {
		return usage
	}
	if err := n.resolve(); err != nil {
		return err
	}
	table, err := os.ReadFile(fs.Arg(1))
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	c, err := consoleClient(ctx, n)
	if err != nil {
		return err
	}
	defer func() { _ = c.SignOut(context.WithoutCancel(ctx)) }()
	return c.SetRates(ctx, fs.Arg(0), table)
}
