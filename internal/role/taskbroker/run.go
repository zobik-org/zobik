package taskbroker

import (
	"context"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"zobik.org/zobik/internal/role"
)

// Run is the task_broker role. It is ready once it reads the Global Configuration
// and binds its consumers (implementation §6, The health of the roles).
func Run(ctx context.Context, rt *role.Runtime, nc *nats.Conn) error {
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	l := rt.MainLoop()
	if _, err := role.AwaitHead(ctx, l, js); err != nil {
		return err
	}
	for _, stream := range []string{"task", "prop"} {
		if _, err := role.AwaitConsumer(ctx, l, js, rt, stream); err != nil {
			return err
		}
	}
	rt.SetReady(true)
	return role.Idle(ctx, l)
}
