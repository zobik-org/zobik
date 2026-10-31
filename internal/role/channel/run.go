package channel

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/grpc"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/claim"
	"zobik.org/zobik/internal/egress"
	"zobik.org/zobik/internal/globalconfig"
	"zobik.org/zobik/internal/role"
	"zobik.org/zobik/internal/role/secrets"
	"zobik.org/zobik/internal/rpcauth"
	"zobik.org/zobik/internal/signing"
	channelv1 "zobik.org/zobik/proto/zobik/channel/v1"
	contextv1 "zobik.org/zobik/proto/zobik/context/v1"
)

// The audiences the deployment's channels serve (implementation §6, The roles of the binary).
const (
	AudienceOperator = "operator"
	AudienceUser     = "user"
)

// entries are the topics each deployment channel opens traces on: intake in the
// Entry Node, the acts and queries of architecture §3.17.1 in the Operator
// Channel (implementation §6, The service between the console and the channels).
var entries = map[string]map[string]bool{
	AudienceUser: {"intake": true},
	AudienceOperator: {
		"config_change": true, "node_catalog_publish": true, "node_provision": true,
		"node_create": true, "node_catalog_import": true, "node_retire": true,
		"node_query": true, "node_catalog_query": true, "topic_catalog_query": true,
	},
}

// opTimeout bounds each read and write on the Bus and on the Context Store.
const opTimeout = 5 * time.Second

// Options are where the role listens and what it reaches besides the Bus.
type Options struct {
	Audience string
	// Listen is where it serves the console; ContextAddr, the Context Store;
	// SecretsAddr, the secrets role, role.AddrSecrets by default.
	Listen, ContextAddr, SecretsAddr string
}

// Run is the channel role for an audience: the network half of the deployment's
// channel (implementation §6, The root and the split Operator Channel). It is
// ready once it reads the Global Configuration, binds its consumer and listens.
func Run(ctx context.Context, rt *role.Runtime, nc *nats.Conn, o Options) error {
	if entries[o.Audience] == nil {
		return fmt.Errorf("channel: unknown audience %q", o.Audience)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	cred, err := signing.VerifyCapability(rt.Credential, rt.Root)
	if err != nil {
		return fmt.Errorf("channel: credential: %w", err)
	}
	console, err := os.ReadFile(filepath.Join(rt.Dir(), role.FileConsolePub))
	if err != nil {
		return fmt.Errorf("channel: the console's identity: %w", err)
	}
	l := rt.MainLoop()
	head, err := role.AwaitHead(ctx, l, js)
	if err != nil {
		return err
	}
	consumer, err := role.AwaitConsumer(ctx, l, js, rt, "task")
	if err != nil {
		return err
	}
	store, err := openRoots(rt.Dir())
	if err != nil {
		return err
	}
	open, err := store.load()
	if err != nil {
		return err
	}
	closed := map[string]*root{}
	for id, r := range open {
		if !r.RetainUntil.IsZero() {
			closed[id] = r
			delete(open, id)
		}
	}
	conn, err := grpc.NewClient(o.ContextAddr, grpc.WithTransportCredentials(rt.RPCCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	if o.SecretsAddr == "" {
		o.SecretsAddr = role.AddrSecrets
	}
	// The Entry Node resolves the embedding model's credential and values its
	// calls with the current rate (implementation §1.2.15, §8.5).
	secretsClient, closeSecrets, err := secrets.Dial(o.SecretsAddr, rt.RPCCredentials(), "")
	if err != nil {
		return err
	}
	defer func() { _ = closeSecrets() }()

	topic := signing.HITLPrefix + o.Audience
	c := &channel{
		rt: rt, js: js, audience: o.Audience, topic: topic,
		claimant: claim.New(rt.ID, rt.Credential, js, nc, topic),
		context:  contextv1.NewContextStoreClient(conn),
		egress:   &egress.Executor{HTTP: egress.HTTPClient(nil), Credential: secretsClient.Resolve, Rates: egress.WatchRates(ctx, js).Table},
		roots:    store, open: open, closed: closed,
		byEntry: map[string]string{}, byDelivery: map[string]string{},
		queries: map[string]*query{}, windows: map[string]time.Duration{},
	}
	c.window = cred.Claims.AttemptWindow
	c.head.Store(head)
	for id, r := range open {
		c.byEntry[r.EntryTaskID] = id
		if r.Delivery != "" {
			c.byDelivery[r.Delivery] = id
		}
	}

	g := grpc.NewServer(grpc.Creds(&rpcauth.Server{Account: rt.Account}),
		grpc.UnaryInterceptor(onlyIdentity(strings.TrimSpace(string(console)))),
		grpc.StreamInterceptor(onlyIdentityStream(strings.TrimSpace(string(console)))))
	channelv1.RegisterChannelServer(g, &service{c: c})
	ln, err := net.Listen("tcp", o.Listen)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	c.ctx = ctx
	// What a previous run left pending goes out now.
	for _, r := range open {
		if r.Pending != nil {
			go c.send(r)
		}
	}
	role.ServeRPC(cancel, g, ln)
	defer g.Stop()

	go c.followHead(ctx, rt.Loop("head", time.Minute))
	rt.SetReady(true)
	if err := c.serve(ctx, l, consumer); err != nil {
		return err
	}
	return role.ServeFailure(ctx)
}

// channel is the role's state: the roots it opened, the queries it retains and
// the console's session.
type channel struct {
	// ctx is the role's: the pending announcements stop retrying with it.
	ctx      context.Context
	rt       *role.Runtime
	js       jetstream.JetStream
	audience string
	topic    string
	claimant *claim.Claimant
	context  contextv1.ContextStoreClient
	// egress executes the embeddings' calls.
	egress *egress.Executor
	window *signing.AttemptWindow
	roots  *roots
	// head is the Global Configuration as last read.
	head atomic.Pointer[globalconfig.Record]

	mu         sync.Mutex
	open       map[string]*root
	closed     map[string]*root  // closed roots, retained for the purge
	byEntry    map[string]string // entry subtask's TaskID → TraceID
	byDelivery map[string]string // delivery attempt's TaskID → TraceID
	queries    map[string]*query
	expired    []*channelv1.Expired
	session    *session
	// windows caches hitl_response_window by config_version: it is pinned per trace.
	windows map[string]time.Duration
}

// followHead reads the head every few seconds, for the current keys: the lease,
// the delivery mode and the grace of the closed roots, which it sweeps.
func (c *channel) followHead(ctx context.Context, l *role.Loop) {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			c.sweep(now)
		}
		l.Mark()
		opCtx, cancel := context.WithTimeout(ctx, opTimeout)
		if head, _, err := globalconfig.Head(opCtx, c.js); err == nil {
			c.head.Store(head)
		}
		cancel()
	}
}

func (c *channel) leaseTTL() time.Duration {
	return c.head.Load().Duration("task_lease_ttl")
}

// responseWindow is hitl_response_window as the trace pinned it (architecture §2.3).
func (c *channel) responseWindow(ctx context.Context, version string) (time.Duration, error) {
	c.mu.Lock()
	d, ok := c.windows[version]
	c.mu.Unlock()
	if ok {
		return d, nil
	}
	rec, err := globalconfig.Version(ctx, c.js, version)
	if err != nil {
		return 0, err
	}
	var ms int64
	if err := json.Unmarshal(rec.Values["hitl_response_window"], &ms); err != nil {
		return 0, fmt.Errorf("hitl_response_window of %s: %w", version, err)
	}
	d = time.Duration(ms) * time.Millisecond
	c.mu.Lock()
	c.windows[version] = d
	c.mu.Unlock()
	return d, nil
}

// serve pulls the announcements of its audience, the answers to its proposals
// and the terminal events of the tasks it opened.
func (c *channel) serve(ctx context.Context, l *role.Loop, consumer jetstream.Consumer) error {
	role.Consume(ctx, l, consumer, func(m jetstream.Msg) { c.handle(ctx, m) })
	return nil
}

func (c *channel) handle(ctx context.Context, m jetstream.Msg) {
	var e bus.Event
	if err := json.Unmarshal(m.Data(), &e); err != nil || e.Validate() != nil || e.Subject == "" {
		m.Ack()
		return
	}
	opCtx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	var err error
	switch e.Type {
	case bus.TypeAnnounced:
		err = c.onAnnounced(opCtx, &e)
	case bus.TypeAssigned:
		err = c.onAssigned(opCtx, &e)
	case bus.TypeRejected:
		c.claimant.Rejected(&e)
	case bus.TypeCompleted, bus.TypeFailed, bus.TypeAborted:
		err = c.onTerminal(opCtx, &e)
	}
	if err != nil {
		log.Printf("channel: %s %s: %v", e.Type, e.Subject, err)
		m.NakWithDelay(time.Second)
		return
	}
	m.Ack()
}
