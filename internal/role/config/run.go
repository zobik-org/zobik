package config

import (
	"context"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"zobik.org/zobik/internal/role"
)

// Run is the config role. It is ready once it reads a current version of the
// Global Configuration (implementation §6, The health of the roles).
func Run(ctx context.Context, rt *role.Runtime, nc *nats.Conn) error {
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	l := rt.MainLoop()
	if _, err := role.AwaitHead(ctx, l, js); err != nil {
		return err
	}
	if _, err := role.AwaitConsumer(ctx, l, js, rt, "task"); err != nil {
		return err
	}
	rt.SetReady(true)
	return role.Idle(ctx, l)
}
