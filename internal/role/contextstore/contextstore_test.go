package contextstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/rpcauth"
	"zobik.org/zobik/internal/signing"
	contextv1 "zobik.org/zobik/proto/zobik/context/v1"
	nriv1 "zobik.org/zobik/proto/zobik/nri/v1"
)

// fixture is a network with a Context Store, a Task Broker that mints tokens and
// the operator's channel, as zobik init provisions them.
type fixture struct {
	t       *testing.T
	root    nkeys.KeyPair
	rootPub string
	account *bus.Account
	broker  nkeys.KeyPair
	cert    string
	addr    string
	store   *Store
}

const (
	traceA = "4bf92f3577b34da6a3ce929d0e0e4736"
	traceB = "00f067aa0ba902b7a3ce929d0e0e4736"
)

func newFixture(t *testing.T, storeLimit, traceLimit int64) *fixture {
	t.Helper()
	root, _ := nkeys.CreateOperator()
	rootPub, _ := root.PublicKey()
	a, err := bus.NewAccount(root, "test", bus.RoleScopes)
	if err != nil {
		t.Fatal(err)
	}
	broker, _ := nkeys.CreateUser()
	brokerPub, _ := broker.PublicKey()
	cert, err := signing.Sign(root, signing.TypeRoleCertificate, signing.RoleCertificate{Sub: brokerPub, Role: signing.RoleTaskBroker}, "")
	if err != nil {
		t.Fatal(err)
	}
	store, err := Open(t.TempDir(), storeLimit, traceLimit)
	if err != nil {
		t.Fatal(err)
	}
	g := grpc.NewServer(grpc.Creds(&rpcauth.Server{Account: func() (*jwt.AccountClaims, error) {
		return rpcauth.AccountFromJWT(a.JWT, rootPub)
	}}))
	contextv1.RegisterContextStoreServer(g, &Server{Store: store, Root: rootPub})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go g.Serve(ln)
	t.Cleanup(g.Stop)
	return &fixture{t: t, root: root, rootPub: rootPub, account: a, broker: broker, cert: cert, addr: ln.Addr().String(), store: store}
}

// node is a caller connected under its own identity.
type node struct {
	id string
	kp nkeys.KeyPair
	c  contextv1.ContextStoreClient
}

func (f *fixture) node(scope string) *node {
	f.t.Helper()
	kp, _ := nkeys.CreateUser()
	pub, _ := kp.PublicKey()
	token, err := bus.IssueUser(f.account.SigningKeys[scope], f.account.Public, pub, scope, time.Now().Add(time.Hour))
	if err != nil {
		f.t.Fatal(err)
	}
	conn, err := grpc.NewClient(f.addr, grpc.WithTransportCredentials(&rpcauth.Client{
		JWT: func() (string, error) { return token, nil }, Key: kp,
	}))
	if err != nil {
		f.t.Fatal(err)
	}
	f.t.Cleanup(func() { conn.Close() })
	return &node{id: pub, kp: kp, c: contextv1.NewContextStoreClient(conn)}
}

// token is what the Task Broker mints on an assignment (architecture §3.14.3).
func (f *fixture) token(sub, trace, section string) string {
	f.t.Helper()
	tok, err := signing.Sign(f.broker, signing.TypeScopeToken, signing.ScopeToken{Sub: sub, TraceID: trace, Section: section}, f.cert)
	if err != nil {
		f.t.Fatal(err)
	}
	return tok
}

func write(ctx context.Context, n *node, token, name string, data []byte) error {
	w, err := n.c.Write(ctx)
	if err != nil {
		return err
	}
	if err := w.Send(&contextv1.StoreWrite{Body: &contextv1.StoreWrite_Header{Header: &contextv1.StoreWriteHeader{Token: token, Name: name}}}); err != nil {
		return err
	}
	for len(data) > 0 {
		n := min(len(data), 1000)
		if err := w.Send(&contextv1.StoreWrite{Body: &contextv1.StoreWrite_Chunk{Chunk: data[:n]}}); err != nil {
			break
		}
		data = data[n:]
	}
	_, err = w.CloseAndRecv()
	return err
}

func read(ctx context.Context, n *node, token string, ref *nriv1.ContextRef) ([]byte, error) {
	s, err := n.c.Read(ctx, &contextv1.StoreRead{Token: token, Ref: ref})
	if err != nil {
		return nil, err
	}
	var out bytes.Buffer
	for {
		c, err := s.Recv()
		if errors.Is(err, io.EOF) {
			return out.Bytes(), nil
		}
		if err != nil {
			return nil, err
		}
		out.Write(c.GetData())
	}
}

func code(t *testing.T, err error, want codes.Code, what string) {
	t.Helper()
	if status.Code(err) != want {
		t.Errorf("%s: got %v, want %s", what, err, want)
	}
}

func TestScopes(t *testing.T) {
	f := newFixture(t, 1<<20, 1<<20)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	a, b := f.node(bus.ConfigScope.Role), f.node(bus.ConfigScope.Role)
	tokA := f.token(a.id, traceA, "task-a")
	tokB := f.token(b.id, traceA, "task-b")

	if err := write(ctx, a, tokA, "result", []byte("hola")); err != nil {
		t.Fatal(err)
	}
	// Reading reaches the whole trace: b reads a's section.
	got, err := read(ctx, b, tokB, &nriv1.ContextRef{TraceId: traceA, TaskId: "task-a", Name: "result"})
	if err != nil || string(got) != "hola" {
		t.Fatalf("reading another section of the trace: %q, %v", got, err)
	}
	// Rewriting replaces the whole content.
	if err := write(ctx, a, tokA, "result", []byte("hi")); err != nil {
		t.Fatal(err)
	}
	if got, _ := read(ctx, a, tokA, &nriv1.ContextRef{TraceId: traceA, TaskId: "task-a", Name: "result"}); string(got) != "hi" {
		t.Errorf("after the rewrite: %q", got)
	}

	_, err = read(ctx, a, tokA, &nriv1.ContextRef{TraceId: traceB, TaskId: "task-a", Name: "result"})
	code(t, err, codes.PermissionDenied, "reading another trace")
	_, err = read(ctx, b, tokA, &nriv1.ContextRef{TraceId: traceA, TaskId: "task-a", Name: "result"})
	code(t, err, codes.PermissionDenied, "presenting another identity's token")
	_, err = read(ctx, a, tokA, &nriv1.ContextRef{TraceId: traceA, TaskId: "task-a", Name: "missing"})
	code(t, err, codes.NotFound, "reading a portion that does not exist")
	code(t, write(ctx, a, tokA, strings.Repeat("x", MaxName+1), []byte("x")), codes.InvalidArgument, "a name past its ceiling")

	other, _ := nkeys.CreateUser()
	forged, _ := signing.Sign(other, signing.TypeScopeToken, signing.ScopeToken{Sub: a.id, TraceID: traceA, Section: "task-x"}, f.cert)
	code(t, write(ctx, a, forged, "x", []byte("x")), codes.PermissionDenied, "a token whose signer the certificate does not accredit")

	list, err := b.c.List(ctx, &contextv1.StoreList{Token: tokB, TaskId: "task-a"})
	if err != nil || len(list.GetRefs()) != 1 || list.GetRefs()[0].GetName() != "result" {
		t.Errorf("listing: %v, %v", list, err)
	}
}

func TestPurge(t *testing.T) {
	f := newFixture(t, 1<<20, 1<<20)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ch := f.node(bus.ChannelScope.Role)
	worker := f.node(bus.ConfigScope.Role)

	// The channel mints its root's token with its credential (architecture §3.14.3).
	cred, _ := signing.Sign(f.root, signing.TypeCapability, signing.Capability{Sub: ch.id, Topic: "hitl_contact_operator"}, "")
	rootTok, err := signing.Sign(ch.kp, signing.TypeScopeToken, signing.ScopeToken{Sub: ch.id, TraceID: traceA, Section: traceA}, cred)
	if err != nil {
		t.Fatal(err)
	}
	if err := write(ctx, ch, rootTok, "request", []byte("payload")); err != nil {
		t.Fatal(err)
	}
	if err := write(ctx, worker, f.token(worker.id, traceA, "task-a"), "result", []byte("done")); err != nil {
		t.Fatal(err)
	}

	_, err = worker.c.Purge(ctx, &contextv1.StorePurge{Token: f.token(worker.id, traceA, "task-a")})
	code(t, err, codes.PermissionDenied, "purging with a task's token")
	if _, err := ch.c.Purge(ctx, &contextv1.StorePurge{Token: rootTok}); err != nil {
		t.Fatal(err)
	}
	if total, trace := f.store.Usage(traceA); total != 0 || trace != 0 {
		t.Errorf("after the purge the store holds %d, the trace %d", total, trace)
	}
}

func TestCeilings(t *testing.T) {
	f := newFixture(t, 3000, 2000)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	n := f.node(bus.ConfigScope.Role)
	tokA := f.token(n.id, traceA, "task-a")
	tokB := f.token(n.id, traceB, "task-b")

	if err := write(ctx, n, tokA, "one", make([]byte, 1500)); err != nil {
		t.Fatal(err)
	}
	code(t, write(ctx, n, tokA, "two", make([]byte, 600)), codes.ResourceExhausted, "past the trace's ceiling")
	// Rewriting counts what replaces, not what adds.
	if err := write(ctx, n, tokA, "one", make([]byte, 1900)); err != nil {
		t.Errorf("rewriting within the ceiling: %v", err)
	}
	code(t, write(ctx, n, tokB, "other", make([]byte, 1200)), codes.ResourceExhausted, "past the store's ceiling")
	if total, trace := f.store.Usage(traceA); total != 1900 || trace != 1900 {
		t.Errorf("a rejected write left %d in the store and %d in the trace", total, trace)
	}
}

// The store recounts what it holds on opening, and purges a closed trace once
// its grace has passed.
func TestReopenAndSweep(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, 1<<20, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	w, _ := s.Create(traceA, "task-a", "x")
	w.Write([]byte("12345"))
	w.Commit()
	os.WriteFile(filepath.Join(dir, traceA, "task-a", ".tmp-crashed"), []byte("half"), 0o600)

	s, err = Open(dir, 1<<20, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if total, _ := s.Usage(traceA); total != 5 {
		t.Errorf("reopened with %d", total)
	}
	if _, err := os.Stat(filepath.Join(dir, traceA, "task-a", ".tmp-crashed")); err == nil {
		t.Error("an interrupted write remains")
	}

	closed := time.Now()
	if err := s.MarkClosed(traceA, closed); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkClosed(traceB, closed); err != nil {
		t.Errorf("closing a trace without content: %v", err)
	}
	s.Sweep(time.Hour, closed.Add(time.Minute))
	if _, err := os.Stat(filepath.Join(dir, traceA)); err != nil {
		t.Error("purged within its grace")
	}
	s.Sweep(time.Hour, closed.Add(time.Hour))
	if _, err := os.Stat(filepath.Join(dir, traceA)); err == nil {
		t.Error("not purged once its grace passed")
	}
}
