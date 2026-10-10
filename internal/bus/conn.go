package bus

import (
	"context"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
)

// URL is where every container of a network reaches the NATS server: its alias
// on the network's Docker network and the standard port (implementation §6, The Docker network).
const URL = "nats://nats:4222"

// Creds formats a user JWT and its seed as a NATS credentials file.
func Creds(userJWT string, seed []byte) ([]byte, error) {
	return jwt.FormatUserConfig(userJWT, seed)
}

// Connect connects with a credentials file, retrying until ctx ends: whoever
// uses the Bus tolerates the NATS server's absence (implementation §6, The health of the roles).
func Connect(ctx context.Context, url, credsFile string, opts ...nats.Option) (*nats.Conn, error) {
	opts = append([]nats.Option{nats.UserCredentials(credsFile), nats.MaxReconnects(-1)}, opts...)
	for {
		nc, err := nats.Connect(url, opts...)
		if err == nil {
			return nc, nil
		}
		select {
		case <-ctx.Done():
			return nil, err
		case <-time.After(500 * time.Millisecond):
		}
	}
}
