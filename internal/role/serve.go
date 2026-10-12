package role

import (
	"context"
	"errors"
	"fmt"
	"net"

	"google.golang.org/grpc"
)

// serveError is a failure of an off-Bus interface, as the cause of the role's
// context.
type serveError struct{ error }

// ServeRPC serves an off-Bus interface on ln until g stops. Any other end
// leaves the interface down, so it cancels ctx with the failure: Run returns it
// through ServeFailure, and the role's restart brings the interface back.
func ServeRPC(cancel context.CancelCauseFunc, g *grpc.Server, ln net.Listener) {
	go func() {
		if err := g.Serve(ln); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			cancel(serveError{fmt.Errorf("serving %s: %w", ln.Addr(), err)})
		}
	}()
}

// ServeFailure is the failure ServeRPC cancelled ctx with, or nil.
func ServeFailure(ctx context.Context) error {
	var e serveError
	if errors.As(context.Cause(ctx), &e) {
		return e.error
	}
	return nil
}
