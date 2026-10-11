package channel

import (
	"context"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"zobik.org/zobik/internal/role"
)

// The audiences the deployment's channels serve (implementation §6, The roles of the binary).
const (
	AudienceOperator = "operator"
	AudienceUser     = "user"
)

// Run is the channel role for audience. It is ready once it reads the Global
// Configuration and binds its consumer (implementation §6, The health of the roles).
func Run(ctx context.Context, rt *role.Runtime, nc *nats.Conn, audience string) error {
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
