package console_test

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/console"
	"zobik.org/zobik/internal/globalconfig"
	"zobik.org/zobik/internal/role"
	"zobik.org/zobik/internal/role/channel"
	"zobik.org/zobik/internal/role/config"
	"zobik.org/zobik/internal/role/taskbroker"
	"zobik.org/zobik/internal/testnet"
)

const user, password = "operator", "pw"

// fixture is the console's service against a network with the Config Store,
// the Task Broker and the Operator Channel running.
type fixture struct {
	*testnet.Net
	base    string
	port    string
	svc     *console.Service
	channel string
	// closed counts the times the keys were discarded.
	closed atomic.Int32
}

func newFixture(t *testing.T, edit func(*console.Options)) *fixture {
	n := testnet.New(t, func(v globalconfig.Values) {
		testnet.Set(v, map[string]any{"auction_timeout": 200})
	})
	addr := freeAddr(t)
	n.Start("config", func(ctx context.Context, rt *role.Runtime, nc *nats.Conn) error {
		return config.Run(ctx, rt, nc, config.Options{ContextAddr: n.ContextAddr})
	})
	n.Start("task_broker", taskbroker.Run)
	n.Start("channel_operator", func(ctx context.Context, rt *role.Runtime, nc *nats.Conn) error {
		return channel.Run(ctx, rt, nc, channel.Options{Audience: channel.AudienceOperator, Listen: addr, ContextAddr: n.ContextAddr})
	})
	f := &fixture{Net: n, channel: n.ID("channel_operator")}
	o := console.Options{
		Open: func(u string, pw []byte) (*console.Keys, error) {
			if u != user || string(pw) != password {
				return nil, errors.New("wrong")
			}
			pub, _ := n.ConsoleKey.PublicKey()
			return &console.Keys{
				Root: n.Root, Console: n.ConsoleKey,
				JWT: func(exp time.Time) (string, error) {
					return bus.IssueUser(n.Account.SigningKeys[bus.ConsoleScope.Role], n.Account.Public, pub, bus.ConsoleScope.Role, exp)
				},
				Close: func() { f.closed.Add(1) },
			}, nil
		},
		Channels:    []console.Channel{{Audience: console.AudienceOperator, ID: f.channel, Addr: addr}},
		IdleTimeout: time.Minute,
	}
	if edit != nil {
		edit(&o)
	}
	f.svc = console.New(o)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_, f.port, _ = net.SplitHostPort(ln.Addr().String())
	f.base = "http://127.0.0.1:" + f.port
	ctx, cancel := context.WithCancel(n.Ctx)
	done := make(chan struct{})
	go func() { defer close(done); f.svc.Serve(ctx, ln) }()
	t.Cleanup(func() { cancel(); <-done })
	return f
}

func freeAddr(t *testing.T) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func (f *fixture) signIn() *console.Client {
	f.T.Helper()
	c, err := console.SignIn(f.Ctx, f.base, user, []byte(password))
	if err != nil {
		f.T.Fatal(err)
	}
	return c
}

func assign(values map[string]any) map[string]json.RawMessage {
	out := map[string]json.RawMessage{}
	for k, v := range values {
		out[k], _ = json.Marshal(v)
	}
	return out
}

// outcomeOf waits for the outcome of trace and returns its details.
func outcomeOf(t *testing.T, outcomes <-chan console.Outcome, trace string) map[string]any {
	t.Helper()
	timeout := time.After(15 * time.Second)
	for {
		select {
		case o, ok := <-outcomes:
			if !ok {
				t.Fatal("the event stream ended")
			}
			if o.TraceID != trace {
				continue
			}
			if o.Channel != console.AudienceOperator {
				t.Errorf("channel %q", o.Channel)
			}
			var d map[string]any
			json.Unmarshal(o.Details, &d)
			return d
		case <-timeout:
			t.Fatalf("no outcome for %s", trace)
		}
	}
}

// A command signs in, reads the head, opens a config_change and receives its
// outcome over the event stream, as the panel does.
func TestConfigSet(t *testing.T) {
	f := newFixture(t, nil)
	c := f.signIn()
	snap, err := c.Config(f.Ctx)
	if err != nil || snap.ConfigVersion != "v0" {
		t.Fatalf("config %+v %v", snap, err)
	}
	outcomes, err := c.Outcomes(f.Ctx)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := c.ConfigSet(f.Ctx, "", assign(map[string]any{"task_lease_ttl": 45000}))
	if err != nil {
		t.Fatal(err)
	}
	d := outcomeOf(t, outcomes, opened.TraceID)
	if d["type"] != bus.TypeCompleted || d["data"].(map[string]any)["config_version"] != "v1" {
		t.Errorf("outcome %v", d)
	}

	// A precondition the edit does not meet stops it before it is signed.
	var notSent *console.NotSentError
	if _, err := c.ConfigSet(f.Ctx, "", assign(map[string]any{"network_admission": "open"})); !errors.As(err, &notSent) {
		t.Errorf("precondition: %v", err)
	}

	// An edit on the version that is no longer the head is stale.
	opened, err = c.ConfigSet(f.Ctx, "v0", assign(map[string]any{"task_lease_ttl": 50000}))
	if err != nil {
		t.Fatal(err)
	}
	if d := outcomeOf(t, outcomes, opened.TraceID); d["failure_code"] != "base_version_stale" {
		t.Errorf("stale: %v", d)
	}
	if now, _ := c.Config(f.Ctx); now.ConfigVersion != "v1" || string(now.Values["task_lease_ttl"]) != "45000" {
		t.Errorf("head %+v", now)
	}
}

// status returns the status of a request to the API with cookie, or none.
func (f *fixture) status(method, path string, cookie *http.Cookie, header map[string]string) int {
	f.T.Helper()
	req, _ := http.NewRequestWithContext(f.Ctx, method, f.base+path, strings.NewReader(`{"assignments":{"task_lease_ttl":46000}}`))
	if cookie != nil {
		req.AddCookie(cookie)
	}
	for k, v := range header {
		if k == "Host" {
			req.Host = v
		} else {
			req.Header.Set(k, v)
		}
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		f.T.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

// Without a session the console signs nothing and opens nothing: a wrong
// password opens none, and signing out closes it and discards the keys.
func TestNoSession(t *testing.T) {
	f := newFixture(t, nil)
	if _, err := console.SignIn(f.Ctx, f.base, user, []byte("wrong")); err == nil {
		t.Error("a wrong password opened a session")
	}
	forged := &http.Cookie{Name: console.CookieSession, Value: "forged"}
	for _, r := range []struct{ method, path string }{
		{http.MethodGet, console.PathConfig}, {http.MethodPost, console.PathConfig}, {http.MethodGet, console.PathEvents},
	} {
		if s := f.status(r.method, r.path, nil, nil); s != http.StatusUnauthorized {
			t.Errorf("%s %s without a session: %d", r.method, r.path, s)
		}
		if s := f.status(r.method, r.path, forged, nil); s != http.StatusUnauthorized {
			t.Errorf("%s %s with a forged session: %d", r.method, r.path, s)
		}
	}

	c := f.signIn()
	if err := c.SignOut(f.Ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Config(f.Ctx); err == nil {
		t.Error("a session signed out still reads")
	}
	if f.closed.Load() != 1 {
		t.Errorf("the keys were discarded %d times", f.closed.Load())
	}
}

// A session closes after session_idle_timeout without a request from the
// person; the event stream does not count as one.
func TestIdleTimeout(t *testing.T) {
	f := newFixture(t, func(o *console.Options) { o.IdleTimeout = 500 * time.Millisecond })
	c := f.signIn()
	outcomes, err := c.Outcomes(f.Ctx)
	if err != nil {
		t.Fatal(err)
	}
	for range 3 { // activity keeps it open
		time.Sleep(300 * time.Millisecond)
		if _, err := c.Config(f.Ctx); err != nil {
			t.Fatalf("an active session closed: %v", err)
		}
	}
	select {
	case _, ok := <-outcomes:
		if ok {
			t.Fatal("an outcome nobody asked for")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the event stream outlived its session")
	}
	if _, err := c.Config(f.Ctx); err == nil {
		t.Error("an idle session still reads")
	}
	if f.closed.Load() != 1 {
		t.Errorf("the keys were discarded %d times", f.closed.Load())
	}
}

// The single-use link leaves the browser in the session, once.
func TestLink(t *testing.T) {
	f := newFixture(t, nil)
	path, err := f.svc.Link(user, []byte(password))
	if err != nil {
		t.Fatal(err)
	}
	noRedirect := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := noRedirect.Get(f.base + path)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	var cookie *http.Cookie
	for _, c := range resp.Cookies() {
		if c.Name == console.CookieSession {
			cookie = c
		}
	}
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/" || cookie == nil {
		t.Fatalf("redeeming: %d %q %v", resp.StatusCode, resp.Header.Get("Location"), cookie)
	}
	if !cookie.HttpOnly || cookie.SameSite != http.SameSiteStrictMode {
		t.Errorf("cookie %+v", cookie)
	}
	if s := f.status(http.MethodGet, console.PathConfig, cookie, nil); s != http.StatusOK {
		t.Errorf("the link's session: %d", s)
	}
	resp, err = noRedirect.Get(f.base + path)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("a link used twice: %d", resp.StatusCode)
	}
}

// The console answers only requests addressed to itself.
func TestAddressedToItself(t *testing.T) {
	f := newFixture(t, nil)
	path, _ := f.svc.Link(user, []byte(password))
	if s := f.status(http.MethodGet, path, nil, map[string]string{"Host": "rebound.example:" + f.port}); s != http.StatusMisdirectedRequest {
		t.Errorf("another Host: %d", s)
	}
	if s := f.status(http.MethodPost, console.PathSession, nil, map[string]string{"Origin": "http://evil.example"}); s != http.StatusForbidden {
		t.Errorf("another Origin: %d", s)
	}
	if s := f.status(http.MethodGet, "/", nil, map[string]string{"Host": "localhost:" + f.port, "Origin": "http://localhost:" + f.port}); s != http.StatusOK {
		t.Errorf("its own address: %d", s)
	}
}

// The console renews its session with the channel before the JWT expires, and
// the channel lets go of nothing: a delivery that waits for an event stream
// across renewals is delivered once one opens.
func TestRenew(t *testing.T) {
	f := newFixture(t, func(o *console.Options) { o.JWTLifetime = 2 * time.Second })
	failed := f.Tap(bus.SubjectTerminal(bus.TypeFailed, f.channel, ">"))
	c := f.signIn()
	opened, err := c.ConfigSet(f.Ctx, "", assign(map[string]any{"task_lease_ttl": 45000}))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Second) // past several renewals and the first JWT's expiry
	select {
	case m := <-failed:
		t.Fatalf("the channel let go of a query: %s", m.Data)
	default:
	}
	outcomes, err := c.Outcomes(f.Ctx)
	if err != nil {
		t.Fatal(err)
	}
	if d := outcomeOf(t, outcomes, opened.TraceID); d["type"] != bus.TypeCompleted {
		t.Errorf("outcome %v", d)
	}
	if _, err := c.Config(f.Ctx); err != nil {
		t.Errorf("reading after the renewals: %v", err)
	}
}
