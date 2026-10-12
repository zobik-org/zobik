// Package testnet brings up, inside a test, what zobik init brings up: a Bus in
// operator mode with the substrate, the structural roles' material and the
// Context Store, all under the same root. Credentials a component does not
// issue yet are minted here with that root.
package testnet

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
	"google.golang.org/grpc"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/configschema"
	"zobik.org/zobik/internal/deploy"
	"zobik.org/zobik/internal/globalconfig"
	"zobik.org/zobik/internal/role"
	"zobik.org/zobik/internal/role/contextstore"
	"zobik.org/zobik/internal/rpcauth"
	"zobik.org/zobik/internal/signing"
	contextv1 "zobik.org/zobik/proto/zobik/context/v1"
)

// Net is a network inside a test.
type Net struct {
	T       *testing.T
	Ctx     context.Context
	Srv     *server.Server
	Account *bus.Account
	Root    nkeys.KeyPair
	RootPub string
	// Console is a connection under the console's scope.
	Console jetstream.JetStream
	// ContextAddr is where the Context Store listens.
	ContextAddr string
	// ConsoleKey is the console's platform identity.
	ConsoleKey nkeys.KeyPair

	ids  map[string]nkeys.KeyPair
	dirs map[string]string
}

// New brings up a network whose v0 is the schema's defaults with edit applied.
func New(t *testing.T, edit func(globalconfig.Values)) *Net {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	root, _ := nkeys.CreateOperator()
	rootPub, _ := root.PublicKey()
	scopes := append([]bus.Scope{bus.ConsoleScope, bus.TapScope}, bus.RoleScopes...)
	a, err := bus.NewAccount(root, "test", scopes)
	if err != nil {
		t.Fatal(err)
	}
	n := &Net{T: t, Ctx: ctx, Account: a, Root: root, RootPub: rootPub,
		ids: map[string]nkeys.KeyPair{}, dirs: map[string]string{}}
	n.ConsoleKey, _ = nkeys.CreateUser()
	n.Srv = startServer(t, a)
	n.Console = n.JetStream(n.Conn(bus.ConsoleScope.Role, nil))

	schema, err := configschema.Load()
	if err != nil {
		t.Fatal(err)
	}
	v0 := schema.V0()
	if edit != nil {
		edit(v0)
	}
	if err := bus.EnsureStreams(ctx, n.Console); err != nil {
		t.Fatal(err)
	}
	if err := bus.EnsureBuckets(ctx, n.Console, time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := globalconfig.WriteV0(ctx, n.Console, globalconfig.Record{
		ConfigVersion: globalconfig.V0Version, PublishedAt: time.Now().UTC(),
		AuthorizedBy: globalconfig.AuthorizedByDeployment, Values: v0,
	}); err != nil {
		t.Fatal(err)
	}
	// Every role's material and consumers, as zobik init leaves them.
	var consumers []bus.Consumer
	for _, r := range deploy.Roles {
		consumers = append(consumers, r.Consumers(n.mintRole(r))...)
	}
	if err := bus.EnsureConsumers(ctx, n.Console, consumers); err != nil {
		t.Fatal(err)
	}
	n.startContextStore()
	return n
}

// Set writes values as a value of the schema.
func Set(v globalconfig.Values, values map[string]any) {
	for key, val := range values {
		v[key], _ = json.Marshal(val)
	}
}

// mintRole writes a role's volume: identity, user JWT, root, account, and what
// the root signs for it. It returns the identity.
func (n *Net) mintRole(r deploy.Role) string {
	n.T.Helper()
	kp, _ := nkeys.CreateUser()
	pub, _ := kp.PublicKey()
	seed, _ := kp.Seed()
	userJWT, err := bus.IssueUser(n.Account.SigningKeys[r.Scope.Role], n.Account.Public, pub, r.Unit, time.Now().Add(time.Hour))
	if err != nil {
		n.T.Fatal(err)
	}
	files := map[string]string{
		role.FileIdentity: string(seed),
		role.FileBusJWT:   userJWT,
		role.FileRootPub:  n.RootPub,
		role.FileAccount:  n.Account.JWT,
	}
	if r.Topic != "" {
		files[role.FileCredential] = n.Sign(n.Root, signing.TypeCapability,
			signing.Capability{Sub: pub, Topic: r.Topic, ArtifactRef: "zobik:test", AttemptWindow: r.AttemptWindow}, "")
	}
	if r.ServesConsole {
		files[role.FileConsolePub], _ = n.ConsoleKey.PublicKey()
	}
	if r.Certificate != "" {
		files[role.FileRoleCertificate] = n.Sign(n.Root, signing.TypeRoleCertificate,
			signing.RoleCertificate{Sub: pub, Role: r.Certificate}, "")
	}
	dir := n.T.TempDir()
	for file, data := range files {
		if err := os.WriteFile(filepath.Join(dir, file), []byte(data), 0o600); err != nil {
			n.T.Fatal(err)
		}
	}
	n.ids[r.Unit], n.dirs[r.Unit] = kp, dir
	return pub
}

// Identity is a role's key pair, by unit.
func (n *Net) Identity(unit string) nkeys.KeyPair { return n.ids[unit] }

// ID is a role's platform identity, by unit.
func (n *Net) ID(unit string) string {
	pub, _ := n.ids[unit].PublicKey()
	return pub
}

// Credential is the credential zobik init left in a role's volume.
func (n *Net) Credential(unit string) string {
	b, err := os.ReadFile(filepath.Join(n.dirs[unit], role.FileCredential))
	if err != nil {
		n.T.Fatal(err)
	}
	return string(b)
}

// Start runs a role's body with the material of unit, and waits until it is
// ready. It stops when the test ends.
func (n *Net) Start(unit string, body role.Body) *role.Runtime {
	n.T.Helper()
	rt, err := role.Load(unit, n.dirs[unit])
	if err != nil {
		n.T.Fatal(err)
	}
	nc, err := rt.Connect(n.Srv.ClientURL())
	if err != nil {
		n.T.Fatal(err)
	}
	ctx, stop := context.WithCancel(n.Ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := body(ctx, rt, nc); err != nil {
			n.T.Errorf("%s: %v", unit, err)
		}
	}()
	n.T.Cleanup(func() { stop(); <-done; nc.Close() })
	for !rt.Ready() {
		select {
		case <-n.Ctx.Done():
			n.T.Fatalf("%s did not become ready", unit)
		case <-time.After(20 * time.Millisecond):
		}
	}
	return rt
}

// startContextStore serves the Context Store, which authenticates its callers
// against this network's account.
func (n *Net) startContextStore() {
	store, err := contextstore.Open(n.T.TempDir(), 1<<30, 1<<28)
	if err != nil {
		n.T.Fatal(err)
	}
	g := grpc.NewServer(grpc.Creds(&rpcauth.Server{Account: func() (*jwt.AccountClaims, error) {
		return rpcauth.AccountFromJWT(n.Account.JWT, n.RootPub)
	}}))
	contextv1.RegisterContextStoreServer(g, &contextstore.Server{Store: store, Root: n.RootPub})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		n.T.Fatal(err)
	}
	go g.Serve(ln)
	n.T.Cleanup(g.Stop)
	n.ContextAddr = ln.Addr().String()
}

// ContextClient connects to the Context Store as the identity kp under scope.
func (n *Net) ContextClient(scope string, kp nkeys.KeyPair) contextv1.ContextStoreClient {
	n.T.Helper()
	pub, _ := kp.PublicKey()
	token, err := bus.IssueUser(n.Account.SigningKeys[scope], n.Account.Public, pub, scope, time.Now().Add(time.Hour))
	if err != nil {
		n.T.Fatal(err)
	}
	conn, err := grpc.NewClient(n.ContextAddr, grpc.WithTransportCredentials(&rpcauth.Client{
		JWT: func() (string, error) { return token, nil }, Key: kp,
	}))
	if err != nil {
		n.T.Fatal(err)
	}
	n.T.Cleanup(func() { conn.Close() })
	return contextv1.NewContextStoreClient(conn)
}

// Conn opens a connection under scope; kp is its identity, or a new one.
func (n *Net) Conn(scope string, kp nkeys.KeyPair) *nats.Conn {
	n.T.Helper()
	if kp == nil {
		kp, _ = nkeys.CreateUser()
	}
	pub, _ := kp.PublicKey()
	seed, _ := kp.Seed()
	token, err := bus.IssueUser(n.Account.SigningKeys[scope], n.Account.Public, pub, scope, time.Now().Add(time.Hour))
	if err != nil {
		n.T.Fatal(err)
	}
	nc, err := nats.Connect(n.Srv.ClientURL(), nats.UserJWTAndSeed(token, string(seed)), nats.CustomInboxPrefix(bus.InboxPrefix(pub)))
	if err != nil {
		n.T.Fatal(err)
	}
	n.T.Cleanup(nc.Close)
	return nc
}

// JetStream wraps a connection.
func (n *Net) JetStream(nc *nats.Conn) jetstream.JetStream {
	n.T.Helper()
	js, err := jetstream.New(nc)
	if err != nil {
		n.T.Fatal(err)
	}
	return js
}

// Tap subscribes to subject under the tap's scope.
func (n *Net) Tap(subject string) chan *nats.Msg {
	n.T.Helper()
	ch := make(chan *nats.Msg, 64)
	nc := n.Conn(bus.TapScope.Role, nil)
	if _, err := nc.ChanSubscribe(subject, ch); err != nil {
		n.T.Fatal(err)
	}
	nc.Flush()
	return ch
}

// Sign issues a signed object.
func (n *Net) Sign(kp nkeys.KeyPair, typ signing.Type, claims any, signer string) string {
	n.T.Helper()
	tok, err := signing.Sign(kp, typ, claims, signer)
	if err != nil {
		n.T.Fatal(err)
	}
	return tok
}

func startServer(t *testing.T, a *bus.Account) *server.Server {
	t.Helper()
	op, err := jwt.DecodeOperatorClaims(a.Operator)
	if err != nil {
		t.Fatal(err)
	}
	resolver := &server.MemAccResolver{}
	for _, j := range []string{a.System, a.JWT} {
		c, err := jwt.DecodeAccountClaims(j)
		if err != nil {
			t.Fatal(err)
		}
		resolver.Store(c.Subject, j)
	}
	s, err := server.NewServer(&server.Options{
		Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(),
		TrustedOperators: []*jwt.OperatorClaims{op}, SystemAccount: op.SystemAccount,
		AccountResolver: resolver, NoLog: true, NoSigs: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	go s.Start()
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatal("server not ready")
	}
	t.Cleanup(s.Shutdown)
	return s
}
