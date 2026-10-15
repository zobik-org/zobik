package sidecar

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/globalconfig"
	"zobik.org/zobik/internal/role"
	"zobik.org/zobik/internal/rpcauth"
	contextv1 "zobik.org/zobik/proto/zobik/context/v1"
	nriv1 "zobik.org/zobik/proto/zobik/nri/v1"
)

// Options are what Run takes besides the startup configuration.
type Options struct {
	// Shared is the volume shared with the Logic Container, SharedDir by
	// default; State, the private one, StateDir by default.
	Shared, State string
	// Socket is where it listens; nri.sock in Shared by default, the path the
	// NRI major version fixes (implementation §2.1).
	Socket string
	// Speaks is the range the binary speaks; Speaks by default.
	Speaks Range
	// StartupPath is the startup configuration, where it reads its JWT on
	// each connection to the Context Store; StartupPath by default.
	StartupPath string
	// ContextAddr is the Context Store; role.AddrContext by default.
	ContextAddr string
}

// The gRPC keepalive with which the Integration Sidecar checks that the session
// is alive (implementation §3, The session).
var (
	keepaliveServer = keepalive.ServerParameters{Time: 10 * time.Second, Timeout: 5 * time.Second}
	keepalivePolicy = keepalive.EnforcementPolicy{MinTime: 5 * time.Second, PermitWithoutStream: true}
)

// Run serves the NRI for the node the startup configuration describes, over
// nc, until ctx ends. It returns an error when its Logic Container's version
// is one it does not serve, without having bound to its consumer
// (implementation §3, The interface version).
func Run(ctx context.Context, nc *nats.Conn, startup *Startup, o Options) error {
	if o.Shared == "" {
		o.Shared = SharedDir
	}
	if o.State == "" {
		o.State = StateDir
	}
	if o.Socket == "" {
		o.Socket = filepath.Join(o.Shared, "nri.sock")
	}
	if o.StartupPath == "" {
		o.StartupPath = StartupPath
	}
	if o.ContextAddr == "" {
		o.ContextAddr = role.AddrContext
	}
	if o.Speaks == nil {
		o.Speaks = Speaks
	}
	declared, err := ParseVersion(startup.NRIVersion)
	if err != nil {
		return err
	}
	if !o.Speaks.Fits(declared) {
		return fmt.Errorf("sidecar: the Logic Container's image declares NRI %s, outside %v", declared, o.Speaks)
	}
	id, err := identity(startup)
	if err != nil {
		return err
	}
	cred, err := credential(startup.Credential, startup.Root)
	if err != nil {
		return err
	}
	if cred.Sub != id {
		return errors.New("sidecar: the credential names another identity")
	}
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	head, err := awaitHead(ctx, js)
	if err != nil {
		return err
	}
	cfg := newConfig(js, head)
	go cfg.follow(ctx)
	st, err := openState(o.State)
	if err != nil {
		return err
	}
	go prune(ctx, st)
	kp, err := nkeys.FromSeed([]byte(startup.Identity))
	if err != nil {
		return err
	}
	conn, err := grpc.NewClient(o.ContextAddr, grpc.WithTransportCredentials(&rpcauth.Client{
		JWT: func() (string, error) {
			st, err := LoadStartup(o.StartupPath)
			if err != nil {
				return "", err
			}
			return strings.TrimSpace(st.BusJWT), nil
		},
		Key: kp,
	}))
	if err != nil {
		return err
	}
	defer conn.Close()

	_ = os.Remove(o.Socket)
	ln, err := net.Listen("unix", o.Socket)
	if err != nil {
		return err
	}
	// Both halves run under the UID the Spawner sets, and the file's permission
	// is what governs who connects (implementation §2.1).
	if err := os.Chmod(o.Socket, 0o600); err != nil {
		ln.Close()
		return err
	}

	// The outbox outlives ctx for a moment, to send what the end of the
	// session leaves.
	outCtx, stopOut := context.WithCancel(context.WithoutCancel(ctx))
	defer stopOut()
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	out := newOutbox(outCtx, st, js, cfg)
	if err := out.resume(); err != nil {
		return err
	}
	n := newNode(id, startup.Credential, cred, js, nc, cfg, st, out, o.Shared)
	n.clearAreas()
	g := grpc.NewServer(grpc.KeepaliveParams(keepaliveServer), grpc.KeepaliveEnforcementPolicy(keepalivePolicy))
	nriv1.RegisterNodeRuntimeServer(g, &service{
		n: n, store: contextv1.NewContextStoreClient(conn),
		speaks: o.Speaks, declared: declared,
		fatal: func(err error) { cancel(err) },
	})
	go func() {
		if err := g.Serve(ln); err != nil {
			cancel(err)
		}
	}()
	log.Printf("sidecar: node %s on %s, serving NRI %v at %s", id, cred.Topic, o.Speaks, o.Socket)
	<-ctx.Done()
	// A session still open is cut: its tasks fail before the process ends
	// (implementation §2.4).
	n.cutOpen()
	// The cut ended the session's Attach; what is still in flight finishes and
	// sends its status, so a Logic Container whose version ends the process
	// reads why (implementation §3, The interface version). stopTimeout bounds
	// a request that does not finish.
	// The outbox drains meanwhile.
	stopped := make(chan struct{})
	go func() {
		g.GracefulStop()
		close(stopped)
	}()
	bound := time.NewTimer(stopTimeout)
	defer bound.Stop()
	out.drain(drainTimeout)
	select {
	case <-stopped:
	case <-bound.C:
		g.Stop()
	}
	if err := context.Cause(ctx); !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

// drainTimeout is how long the end of the process waits for the outbox, and
// stopTimeout for the requests still in flight.
const (
	drainTimeout = 3 * time.Second
	stopTimeout  = 3 * time.Second
)

// prune forgets the old delivered ids, at startup and every hour.
func prune(ctx context.Context, st *state) {
	for {
		if err := st.prune(); err != nil {
			log.Printf("sidecar: pruning the delivered ids: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Hour):
		}
	}
}

func identity(st *Startup) (string, error) {
	kp, err := nkeys.FromSeed([]byte(st.Identity))
	if err != nil {
		return "", fmt.Errorf("sidecar: identity: %w", err)
	}
	return kp.PublicKey()
}

// awaitHead reads the head of the Global Configuration, retrying until it
// can: the Integration Sidecar tolerates the NATS server's absence.
func awaitHead(ctx context.Context, js jetstream.JetStream) (*globalconfig.Record, error) {
	for {
		opCtx, cancel := context.WithTimeout(ctx, opTimeout)
		head, _, err := globalconfig.Head(opCtx, js)
		cancel()
		if err == nil {
			return head, nil
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("sidecar: reading the configuration: %w", err)
		case <-time.After(time.Second):
		}
	}
}

// Connect opens the node's connection to the Bus with its platform identity. The
// JWT is read from the startup configuration on each connection, so a
// reconnection takes the one the Spawner renewed (implementation §1.2.4).
func Connect(url, path string) (*nats.Conn, error) {
	st, err := LoadStartup(path)
	if err != nil {
		return nil, err
	}
	kp, err := nkeys.FromSeed([]byte(st.Identity))
	if err != nil {
		return nil, fmt.Errorf("sidecar: identity: %w", err)
	}
	id, err := kp.PublicKey()
	if err != nil {
		return nil, err
	}
	return nats.Connect(url,
		nats.UserJWT(
			func() (string, error) {
				st, err := LoadStartup(path)
				if err != nil {
					return "", err
				}
				return strings.TrimSpace(st.BusJWT), nil
			},
			kp.Sign,
		),
		nats.Name("zobik-sidecar"),
		nats.CustomInboxPrefix(bus.InboxPrefix(id)),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(500*time.Millisecond),
	)
}
