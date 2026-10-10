package bus

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
)

// role is a connection under a role's scope, with its identity and inbox.
type role struct {
	nc   *nats.Conn
	id   string
	errs <-chan error
}

func connectRole(t *testing.T, s *server.Server, a *Account, scope string) *role {
	t.Helper()
	user, _ := nkeys.CreateUser()
	pub, _ := user.PublicKey()
	seed, _ := user.Seed()
	token, err := IssueUser(a.SigningKeys[scope], a.Public, pub, scope, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	errs := make(chan error, 8)
	nc, err := nats.Connect(s.ClientURL(), nats.UserJWTAndSeed(token, string(seed)),
		nats.CustomInboxPrefix(InboxPrefix(pub)),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) { errs <- err }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	return &role{nc: nc, id: pub, errs: errs}
}

func roleAccount(t *testing.T) *Account {
	t.Helper()
	root, _ := nkeys.CreateOperator()
	a, err := NewAccount(root, "test", append([]Scope{ConsoleScope, TapScope}, RoleScopes...))
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func (r *role) publishes(t *testing.T, tap <-chan *nats.Msg, subject string) {
	t.Helper()
	if err := r.nc.Publish(subject, []byte("{}")); err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-tap:
		if m.Subject != subject {
			t.Errorf("tap saw %s, want %s", m.Subject, subject)
		}
	case <-time.After(2 * time.Second):
		t.Errorf("%s did not reach the Bus", subject)
	}
}

func (r *role) denied(t *testing.T, subject string) {
	t.Helper()
	r.nc.Publish(subject, []byte("{}"))
	expectViolation(t, r.errs, "publishing "+subject)
}

// Each role publishes only what implementation §1.2.1 gives it.
func TestRolePublishPermissions(t *testing.T) {
	a := roleAccount(t)
	s := startServer(t, a)
	tapConn, _, err := connect(t, s, a, TapScope.Role, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	tap := make(chan *nats.Msg, 16)
	for _, family := range families {
		if _, err := tapConn.ChanSubscribe(family, tap); err != nil {
			t.Fatal(err)
		}
	}
	tapConn.Flush()

	cfg := connectRole(t, s, a, ConfigScope.Role)
	cfg.publishes(t, tap, "prop."+cfg.id+".config_change")
	cfg.publishes(t, tap, "lease."+cfg.id+".config_change.T1")
	cfg.publishes(t, tap, "task.completed.UCHANNEL.config_change")
	cfg.denied(t, "prop.UOTHER.config_change")
	cfg.denied(t, "task.assigned."+cfg.id)
	cfg.denied(t, "task.announced.intake")

	tb := connectRole(t, s, a, TaskBrokerScope.Role)
	tb.publishes(t, tap, "task.assigned.UNODE")
	tb.publishes(t, tap, "task.rejected.UNODE")
	tb.publishes(t, tap, "task.announced.echo")
	tb.denied(t, "prop."+tb.id+".echo")
	tb.denied(t, "task.completed.UNODE.echo")

	ch := connectRole(t, s, a, ChannelScope.Role)
	ch.publishes(t, tap, "task.announced.config_change")
	ch.publishes(t, tap, "prop."+ch.id+".hitl_contact_operator")
	ch.denied(t, "task.assigned."+ch.id)

	ctx := connectRole(t, s, a, ContextScope.Role)
	ctx.denied(t, "task.completed.UNODE.echo")
	ctx.denied(t, "task.announced.echo")
}

// A role receives replies only on its own inbox, and the Task Broker the renewals.
func TestRoleSubscribePermissions(t *testing.T) {
	a := roleAccount(t)
	s := startServer(t, a)
	cfg := connectRole(t, s, a, ConfigScope.Role)
	cfg.nc.SubscribeSync("_INBOX.>")
	expectViolation(t, cfg.errs, "config subscribing to every inbox")
	cfg.nc.SubscribeSync("task.>")
	expectViolation(t, cfg.errs, "config subscribing to a family")

	tb := connectRole(t, s, a, TaskBrokerScope.Role)
	if _, err := tb.nc.SubscribeSync("lease.>"); err != nil {
		t.Fatal(err)
	}
	tb.nc.Flush()
	select {
	case err := <-tb.errs:
		t.Errorf("task_broker subscribing to the renewals: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
}

// A role pulls and acknowledges from the consumer named by its identity, and from no other.
func TestRoleConsumers(t *testing.T) {
	a := roleAccount(t)
	s := startServer(t, a)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	console, _, err := connect(t, s, a, ConsoleScope.Role, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	admin, _ := jetstream.New(console)
	if err := EnsureStreams(ctx, admin); err != nil {
		t.Fatal(err)
	}

	cfg := connectRole(t, s, a, ConfigScope.Role)
	other := connectRole(t, s, a, ConfigScope.Role)
	for _, id := range []string{cfg.id, other.id} {
		if _, err := admin.CreateOrUpdateConsumer(ctx, "task", jetstream.ConsumerConfig{
			Durable:        id,
			FilterSubjects: []string{"task.announced.config_change", "task.assigned." + id},
			AckPolicy:      jetstream.AckExplicitPolicy,
		}); err != nil {
			t.Fatal(err)
		}
	}

	tb := connectRole(t, s, a, TaskBrokerScope.Role)
	tbjs, _ := jetstream.New(tb.nc)
	if _, err := tbjs.Publish(ctx, "task.announced.config_change", []byte("{}")); err != nil {
		t.Fatalf("task_broker announcing: %v", err)
	}

	js, _ := jetstream.New(cfg.nc)
	c, err := js.Consumer(ctx, "task", cfg.id)
	if err != nil {
		t.Fatalf("config binding its consumer: %v", err)
	}
	msgs, err := c.Fetch(1, jetstream.FetchMaxWait(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	got := 0
	for m := range msgs.Messages() {
		if err := m.DoubleAck(ctx); err != nil {
			t.Errorf("config acknowledging: %v", err)
		}
		got++
	}
	if got != 1 {
		t.Fatalf("config fetched %d messages: %v", got, msgs.Error())
	}

	if _, err := js.Consumer(ctx, "task", other.id); err == nil {
		t.Error("config bound another identity's consumer")
	}
}
