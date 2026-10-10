package bus

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
)

// harnessScope stands for the components that will publish on the families,
// which this stage does not have yet.
var harnessScope = Scope{Role: "harness", Publish: []string{"task.>", "prop.>", "notice.>"}, Subscribe: []string{"_INBOX.>"}}

func newTestAccount(t *testing.T) *Account {
	t.Helper()
	root, err := nkeys.CreateOperator()
	if err != nil {
		t.Fatal(err)
	}
	a, err := NewAccount(root, "test", []Scope{ConsoleScope, TapScope, harnessScope})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// startServer runs an in-process NATS server trusting only a's operator.
func startServer(t *testing.T, a *Account) *server.Server {
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
		if err := resolver.Store(c.Subject, j); err != nil {
			t.Fatal(err)
		}
	}
	s, err := server.NewServer(&server.Options{
		Host:             "127.0.0.1",
		Port:             -1,
		JetStream:        true,
		StoreDir:         t.TempDir(),
		TrustedOperators: []*jwt.OperatorClaims{op},
		SystemAccount:    op.SystemAccount,
		AccountResolver:  resolver,
		NoLog:            true,
		NoSigs:           true,
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

// connect mints a user under role's scope and connects it, collecting the
// permission violations the server reports asynchronously.
func connect(t *testing.T, s *server.Server, a *Account, role string, expires time.Time) (*nats.Conn, <-chan error, error) {
	t.Helper()
	user, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	pub, _ := user.PublicKey()
	seed, _ := user.Seed()
	token, err := IssueUser(a.SigningKeys[role], a.Public, pub, role, expires)
	if err != nil {
		t.Fatal(err)
	}
	errs := make(chan error, 8)
	nc, err := nats.Connect(s.ClientURL(), nats.UserJWTAndSeed(token, string(seed)),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) { errs <- err }))
	if err == nil {
		t.Cleanup(nc.Close)
	}
	return nc, errs, err
}

func expectViolation(t *testing.T, errs <-chan error, what string) {
	t.Helper()
	select {
	case err := <-errs:
		if !strings.Contains(strings.ToLower(err.Error()), "permissions violation") {
			t.Errorf("%s: got %v", what, err)
		}
	case <-time.After(2 * time.Second):
		t.Errorf("%s: allowed", what)
	}
}

func TestScopesOnTheServer(t *testing.T) {
	a := newTestAccount(t)
	s := startServer(t, a)
	year := time.Now().Add(365 * 24 * time.Hour)

	tap, tapErrs, err := connect(t, s, a, TapScope.Role, year)
	if err != nil {
		t.Fatal(err)
	}
	sub, err := tap.SubscribeSync("task.>")
	if err != nil {
		t.Fatal(err)
	}
	tap.Flush()

	harness, _, err := connect(t, s, a, harnessScope.Role, year)
	if err != nil {
		t.Fatal(err)
	}
	if err := harness.Publish("task.announced.echo", []byte("{}")); err != nil {
		t.Fatal(err)
	}
	if msg, err := sub.NextMsg(2 * time.Second); err != nil || msg.Subject != "task.announced.echo" {
		t.Fatalf("tap did not see the event: %v", err)
	}

	tap.Publish("task.announced.echo", []byte("{}"))
	expectViolation(t, tapErrs, "tap publishing")
	tap.SubscribeSync("$JS.API.>")
	expectViolation(t, tapErrs, "tap subscribing outside the families")

	console, consoleErrs, err := connect(t, s, a, ConsoleScope.Role, year)
	if err != nil {
		t.Fatal(err)
	}
	js, err := jetstream.New(console)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "TASK", Subjects: []string{"task.>"}}); err != nil {
		t.Fatalf("console creating a stream: %v", err)
	}
	console.Publish("task.announced.echo", []byte("{}"))
	expectViolation(t, consoleErrs, "console publishing an event")
}

func TestExpiredUserIsRejected(t *testing.T) {
	a := newTestAccount(t)
	s := startServer(t, a)
	if _, _, err := connect(t, s, a, TapScope.Role, time.Now().Add(-time.Minute)); err == nil {
		t.Fatal("expired user connected")
	}
}

func TestForeignRootIsRejected(t *testing.T) {
	a := newTestAccount(t)
	other := newTestAccount(t)
	s := startServer(t, a)
	if _, _, err := connect(t, s, other, TapScope.Role, time.Now().Add(time.Hour)); err == nil {
		t.Fatal("user of another root connected")
	}
}

func TestRegisterScopesConverges(t *testing.T) {
	root, _ := nkeys.CreateOperator()
	a, err := NewAccount(root, "test", []Scope{ConsoleScope})
	if err != nil {
		t.Fatal(err)
	}
	again, keys, err := RegisterScopes(root, a.JWT, []Scope{ConsoleScope})
	if err != nil || again != a.JWT || len(keys) != 0 {
		t.Fatalf("re-registering changed the account: %d new keys, %v", len(keys), err)
	}
	added, keys, err := RegisterScopes(root, a.JWT, []Scope{ConsoleScope, TapScope})
	if err != nil || len(keys) != 1 || keys[TapScope.Role] == nil {
		t.Fatalf("adding tap: %d new keys, %v", len(keys), err)
	}
	c, err := jwt.DecodeAccountClaims(added)
	if err != nil || len(c.SigningKeys) != 2 {
		t.Fatalf("account has %d signing keys, %v", len(c.SigningKeys), err)
	}
}
