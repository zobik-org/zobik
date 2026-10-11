// Package role is the runtime every structural role of the zobik binary runs
// under, zobik run --role=<role>: its material, its connection to the Bus and its
// health (implementation §6, The roles of the binary, The health of the roles).
// Each role has a package of its own below this one.
package role

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/rpcauth"
)

// A role's material, in the volume mounted at Dir (implementation §6, The root and
// the split Operator Channel). zobik init writes it; the console renews FileBusJWT.
const (
	Dir                 = "/var/lib/zobik"
	FileIdentity        = "identity.nk"
	FileBusJWT          = "bus.jwt"
	FileRootPub         = "root.pub"
	FileCredential      = "credential.jws"
	FileRoleCertificate = "role_certificate.jws"
	// FileAccount is the network's account JWT, which the off-Bus interfaces
	// verify a caller's JWT against (implementation §6, The off-Bus interfaces).
	FileAccount = "account.jwt"

	// DataDir is where a role with a data volume mounts it: the Context Store's content.
	DataDir = "/var/lib/zobik-data"
)

// The fixed ports the binary brings, on each role's alias (implementation §6, The Docker network).
const (
	PortContext = "7001"
	// AddrContext is where the Integration Sidecars and the channels reach the Context Store.
	AddrContext = "context:" + PortContext
)

// Runtime is what a role runs with.
type Runtime struct {
	Name string
	// ID is the role's platform identity, its public nkey.
	ID   string
	key  nkeys.KeyPair
	dir  string
	Root string
	// Credential and Certificate are empty for a role that has none.
	Credential, Certificate string

	ready atomic.Bool
	conn  atomic.Pointer[nats.Conn]

	mu    sync.Mutex
	loops []*Loop
	// exit ends the process when a loop goes past its deadline; tests replace it.
	exit func(reason string)
}

// Load reads the role's material from dir.
func Load(name, dir string) (*Runtime, error) {
	seed, err := os.ReadFile(filepath.Join(dir, FileIdentity))
	if err != nil {
		return nil, fmt.Errorf("role: identity: %w", err)
	}
	key, err := nkeys.FromSeed(seed)
	if err != nil {
		return nil, fmt.Errorf("role: identity: %w", err)
	}
	id, err := key.PublicKey()
	if err != nil {
		return nil, err
	}
	rt := &Runtime{Name: name, ID: id, key: key, dir: dir, exit: func(reason string) {
		log.Printf("%s: watchdog: %s", name, reason)
		os.Exit(2)
	}}
	for file, dst := range map[string]*string{FileRootPub: &rt.Root, FileCredential: &rt.Credential, FileRoleCertificate: &rt.Certificate} {
		b, err := os.ReadFile(filepath.Join(dir, file))
		if errors.Is(err, fs.ErrNotExist) && file != FileRootPub {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("role: %s: %w", file, err)
		}
		*dst = strings.TrimSpace(string(b))
	}
	return rt, nil
}

// Ready says whether the role serves: it reached what it waits for and loaded
// what it reads, and it is connected to the Bus (implementation §6, The health of the roles).
func (rt *Runtime) Ready() bool {
	nc := rt.conn.Load()
	return rt.ready.Load() && nc != nil && nc.IsConnected()
}

// SetReady is the role's own half of Ready.
func (rt *Runtime) SetReady(ready bool) { rt.ready.Store(ready) }

// Connect opens the role's connection to the Bus. It returns at once and the
// connection keeps retrying in the background: a role tolerates the NATS
// server's absence, and is not ready while it lasts. The JWT is read from the
// volume on each connection, so a reconnection takes the one the console renewed.
func (rt *Runtime) Connect(url string) (*nats.Conn, error) {
	nc, err := nats.Connect(url,
		nats.UserJWT(
			func() (string, error) {
				b, err := os.ReadFile(filepath.Join(rt.dir, FileBusJWT))
				return strings.TrimSpace(string(b)), err
			},
			func(nonce []byte) ([]byte, error) { return rt.key.Sign(nonce) },
		),
		nats.Name(rt.Name),
		nats.CustomInboxPrefix(bus.InboxPrefix(rt.ID)),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(500*time.Millisecond),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if err != nil {
				log.Printf("%s: disconnected from the Bus: %v", rt.Name, err)
			}
		}),
		nats.ReconnectHandler(func(*nats.Conn) { log.Printf("%s: reconnected to the Bus", rt.Name) }),
	)
	if err != nil {
		return nil, err
	}
	rt.conn.Store(nc)
	return nc, nil
}

// Loop is a loop of the process that marks its progress between one operation
// and the next. If it goes past its deadline without marking, the watchdog ends
// the process and the restart policy brings it up again.
type Loop struct {
	name     string
	deadline time.Duration
	last     atomic.Int64
}

// Loop registers a loop with its deadline, marked now.
func (rt *Runtime) Loop(name string, deadline time.Duration) *Loop {
	l := &Loop{name: name, deadline: deadline}
	l.Mark()
	rt.mu.Lock()
	rt.loops = append(rt.loops, l)
	rt.mu.Unlock()
	return l
}

// Mark records progress.
func (l *Loop) Mark() { l.last.Store(time.Now().UnixNano()) }

// Watch checks the loops every period until ctx ends.
func (rt *Runtime) Watch(ctx context.Context, period time.Duration) {
	t := time.NewTicker(period)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			rt.mu.Lock()
			for _, l := range rt.loops {
				if since := now.Sub(time.Unix(0, l.last.Load())); since > l.deadline {
					rt.exit(fmt.Sprintf("loop %s made no progress for %s", l.name, since.Round(time.Second)))
				}
			}
			rt.mu.Unlock()
		}
	}
}

// Idle marks l until ctx ends: the loop of a role that has nothing else to do.
func Idle(ctx context.Context, l *Loop) error {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			l.Mark()
		}
	}
}

// Retry runs op until it succeeds or ctx ends, each attempt within timeout and
// marking l between attempts: a role that waits for what it is missing is alive
// and not ready.
func Retry(ctx context.Context, l *Loop, timeout time.Duration, op func(ctx context.Context) error) error {
	for {
		attempt, cancel := context.WithTimeout(ctx, timeout)
		err := op(attempt)
		cancel()
		l.Mark()
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// Account reads the network's account JWT from the volume and verifies it
// against the root; it is read on each call, so the console's updates apply.
func (rt *Runtime) Account() (*jwt.AccountClaims, error) {
	b, err := os.ReadFile(filepath.Join(rt.dir, FileAccount))
	if err != nil {
		return nil, err
	}
	return rpcauth.AccountFromJWT(strings.TrimSpace(string(b)), rt.Root)
}
