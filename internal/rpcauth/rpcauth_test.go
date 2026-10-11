package rpcauth

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"zobik.org/zobik/internal/bus"
)

type network struct {
	root    nkeys.KeyPair
	account *bus.Account
}

func newNetwork(t *testing.T) *network {
	t.Helper()
	root, _ := nkeys.CreateOperator()
	a, err := bus.NewAccount(root, "test", bus.RoleScopes)
	if err != nil {
		t.Fatal(err)
	}
	return &network{root: root, account: a}
}

func (n *network) user(t *testing.T, role string, expires time.Time) (*Client, string) {
	t.Helper()
	kp, _ := nkeys.CreateUser()
	pub, _ := kp.PublicKey()
	token, err := bus.IssueUser(n.account.SigningKeys[role], n.account.Public, pub, role, expires)
	if err != nil {
		t.Fatal(err)
	}
	return &Client{JWT: func() (string, error) { return token, nil }, Key: kp}, pub
}

// serve runs a gRPC server with the handshake whose account is n's, and returns
// its address and the identities its calls arrived with.
func (n *network) serve(t *testing.T) (string, <-chan Info) {
	t.Helper()
	rootPub, _ := n.root.PublicKey()
	srv := &Server{Account: func() (*jwt.AccountClaims, error) { return AccountFromJWT(n.account.JWT, rootPub) }}
	seen := make(chan Info, 8)
	g := grpc.NewServer(grpc.Creds(srv), grpc.UnaryInterceptor(
		func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
			if info, ok := FromContext(ctx); ok {
				seen <- info
			}
			return h(ctx, req)
		}))
	healthpb.RegisterHealthServer(g, health.NewServer())
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go g.Serve(ln)
	t.Cleanup(g.Stop)
	return ln.Addr().String(), seen
}

func call(addr string, c *Client) error {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(c))
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err = healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{}, grpc.WaitForReady(false))
	return err
}

func TestAuthenticated(t *testing.T) {
	n := newNetwork(t)
	addr, seen := n.serve(t)
	c, pub := n.user(t, bus.ConfigScope.Role, time.Now().Add(time.Hour))
	if err := call(addr, c); err != nil {
		t.Fatal(err)
	}
	info := <-seen
	if info.Identity != pub || info.Role != bus.ConfigScope.Role {
		t.Errorf("the call arrived as %+v", info)
	}
}

func TestRejected(t *testing.T) {
	n := newNetwork(t)
	addr, _ := n.serve(t)

	other := newNetwork(t)
	foreign, _ := other.user(t, bus.ConfigScope.Role, time.Now().Add(time.Hour))

	expired, _ := n.user(t, bus.ConfigScope.Role, time.Now().Add(-time.Minute))

	impostor, _ := n.user(t, bus.ConfigScope.Role, time.Now().Add(time.Hour))
	impostor.Key, _ = nkeys.CreateUser() // signs the nonce with another key

	revoked, revokedPub := n.user(t, bus.ConfigScope.Role, time.Now().Add(time.Hour))
	updated, err := bus.Revoke(n.root, n.account.JWT, revokedPub)
	if err != nil {
		t.Fatal(err)
	}
	n.account.JWT = updated
	time.Sleep(1100 * time.Millisecond) // a revocation applies to JWTs issued up to its second

	for name, c := range map[string]*Client{
		"another network's identity": foreign,
		"an expired JWT":             expired,
		"a nonce signed by another":  impostor,
		"a revoked identity":         revoked,
	} {
		if err := call(addr, c); err == nil {
			t.Errorf("%s was admitted", name)
		}
	}
}

// The connection closes when the JWT it was opened with expires.
func TestConnectionExpires(t *testing.T) {
	n := newNetwork(t)
	addr, _ := n.serve(t)
	c, _ := n.user(t, bus.ConfigScope.Role, time.Now().Add(2*time.Second))
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(c))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	hc := healthpb.NewHealthClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := hc.Watch(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	for {
		if _, err := stream.Recv(); err != nil {
			if time.Since(start) > 8*time.Second || !strings.Contains(err.Error(), "Unavailable") && !strings.Contains(err.Error(), "EOF") && !strings.Contains(err.Error(), "closing") {
				t.Errorf("the stream ended with %v after %s", err, time.Since(start))
			}
			return
		}
	}
}
