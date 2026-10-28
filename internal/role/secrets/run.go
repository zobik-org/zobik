package secrets

import (
	"context"
	"encoding/json"
	"net"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/grpc"

	"zobik.org/zobik/internal/globalconfig"
	"zobik.org/zobik/internal/role"
	"zobik.org/zobik/internal/rpcauth"
	secretsv1 "zobik.org/zobik/proto/zobik/secrets/v1"
)

// Run is the secrets role: it serves the Secret Store through its own network
// interface, and uses the Bus only to read the configuration (implementation
// §1.2.18). It waits for nothing besides the NATS server: without a head, no
// handle is in anyone's scope.
func Run(ctx context.Context, rt *role.Runtime, nc *nats.Conn) error {
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	srv := &Server{Root: rt.Root, Dir: rt.Dir(), Head: func(ctx context.Context) (map[string]json.RawMessage, error) {
		head, _, err := globalconfig.Head(ctx, js)
		if err != nil {
			return nil, err
		}
		return head.Values, nil
	}}
	g := grpc.NewServer(grpc.Creds(&rpcauth.Server{Account: rt.Account}))
	secretsv1.RegisterSecretsServer(g, srv)
	ln, err := net.Listen("tcp", ":"+role.PortSecrets)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	role.ServeRPC(cancel, g, ln)
	defer g.Stop()
	rt.SetReady(true)
	if err := role.Idle(ctx, rt.MainLoop()); err != nil {
		return err
	}
	return role.ServeFailure(ctx)
}
