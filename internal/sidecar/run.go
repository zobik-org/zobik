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
	nriv1 "zobik.org/zobik/proto/zobik/nri/v1"
)

// Socket is where the Integration Sidecar serves the NRI: the NRI major version
// fixes the path (implementation §2.1).
var Socket = filepath.Join(SharedDir, "nri.sock")

// Options are what Run takes besides the startup configuration.
type Options struct {
	// Socket is where it listens; Socket by default.
	Socket string
	// Speaks is the range the binary speaks; Speaks by default.
	Speaks Range
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
func Run(ctx context.Context, nc *nats.Conn, st *Startup, o Options) error {
	if o.Socket == "" {
		o.Socket = Socket
	}
	if o.Speaks == nil {
		o.Speaks = Speaks
	}
	declared, err := ParseVersion(st.NRIVersion)
	if err != nil {
		return err
	}
	if !o.Speaks.Fits(declared) {
		return fmt.Errorf("sidecar: the Logic Container's image declares NRI %s, outside %v", declared, o.Speaks)
	}
	id, err := identity(st)
	if err != nil {
		return err
	}
	cred, err := credential(st.Credential, st.Root)
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

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	n := newNode(id, st.Credential, cred, js, nc, cfg)
	g := grpc.NewServer(grpc.KeepaliveParams(keepaliveServer), grpc.KeepaliveEnforcementPolicy(keepalivePolicy))
	nriv1.RegisterNodeRuntimeServer(g, &service{
		n:      n,
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
	g.Stop()
	if err := context.Cause(ctx); !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
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
