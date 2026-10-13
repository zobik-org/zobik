// Command zobik-sidecar is the Integration Sidecar: the generic half of every
// node, which speaks the Bus and serves the Node Runtime Interface.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/sidecar"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "zobik-sidecar:", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	st, err := sidecar.LoadStartup(sidecar.StartupPath)
	if err != nil {
		return err
	}
	nc, err := sidecar.Connect(bus.URL, sidecar.StartupPath)
	if err != nil {
		return err
	}
	defer nc.Close()
	return sidecar.Run(ctx, nc, st, sidecar.Options{})
}
