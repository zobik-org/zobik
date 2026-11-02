//go:build dev

package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"zobik.org/zobik/internal/deploy"
)

func init() {
	devCommands["base-image"] = baseImageCmd
	// A short life lets a test see the Spawner renew the nodes' identities
	// (implementation §1.2.4).
	devInitFlags = append(devInitFlags, func(fs *flag.FlagSet, o *deploy.Options) {
		fs.DurationVar(&o.NodeIdentityLifetime, "node-identity-lifetime", time.Duration(0), "the life of a node's user JWT (default: an hour)")
	})
}

// zobik base-image prints the reference the engine pulls the tenant's base
// image by, through the link: the BASE a blueprint's Dockerfile is built on.
func baseImageCmd(_ context.Context, args []string) error {
	fs := flag.NewFlagSet("base-image", flag.ContinueOnError)
	var n networkFlags
	n.register(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := n.resolve(); err != nil {
		return err
	}
	ref, err := deploy.BaseRef(n.dir)
	if err != nil {
		return err
	}
	fmt.Println(ref)
	return nil
}
