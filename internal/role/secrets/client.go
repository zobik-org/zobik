package secrets

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	secretsv1 "zobik.org/zobik/proto/zobik/secrets/v1"
)

// Client resolves handles in the secrets role. It keeps no value between
// calls, so rotating a credential takes effect on the next one
// (architecture §3.16.6, implementation §1.2.18).
type Client struct {
	c secretsv1.SecretsClient
	// credential is the caller's capability credential; empty from a role.
	credential string
}

// Dial connects to the role at addr, authenticated with creds.
func Dial(addr string, creds credentials.TransportCredentials, credential string) (*Client, func() error, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, nil, err
	}
	return &Client{c: secretsv1.NewSecretsClient(conn), credential: credential}, conn.Close, nil
}

// Resolve returns the value behind handle.
func (c *Client) Resolve(ctx context.Context, handle string) ([]byte, error) {
	s, err := c.c.Resolve(ctx, &secretsv1.SecretRequest{Handle: handle, Credential: c.credential})
	if err != nil {
		return nil, err
	}
	return s.GetValue(), nil
}

// Check reports whether handle resolves, without its value.
func (c *Client) Check(ctx context.Context, handle string) error {
	_, err := c.c.Check(ctx, &secretsv1.SecretRequest{Handle: handle, Credential: c.credential})
	return err
}
