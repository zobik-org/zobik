package console

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/nats-io/nkeys"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"zobik.org/zobik/internal/rpcauth"
	channelv1 "zobik.org/zobik/proto/zobik/channel/v1"
)

// Channel is a channel of the panel, as the console reaches it.
type Channel struct {
	// Audience is the one the channel serves: operator or user.
	Audience string
	// ID is the channel's platform identity, which the signed acts name as their requester.
	ID string
	// Addr is where it serves the console, on the host's 127.0.0.1.
	Addr string
}

// Keys are what a sign-in opens (implementation §6, The root and the split
// Operator Channel).
type Keys struct {
	Root, Console nkeys.KeyPair
	// Conversations is the key of the work area's conversations.
	Conversations []byte
	// JWT issues the console identity's user JWT, valid until exp.
	JWT func(exp time.Time) (string, error)
	// Close wipes the keys.
	Close func()
}

// link holds the console's session with a channel while some session is open:
// the Attach, and the client of the other calls on the same connection. It
// renews both before the JWT they were opened with expires, and reconnects when
// the channel ends the Attach (implementation §6, The service between the
// console and the channels).
type link struct {
	ch       Channel
	keys     *Keys
	lifetime time.Duration
	// receive is called with each message of the Attach, one at a time.
	receive func(l *link, m *channelv1.ChannelMessage)

	mu  sync.Mutex
	cur *generation
}

// generation is one connection to the channel with its Attach.
type generation struct {
	conn   *grpc.ClientConn
	client channelv1.ChannelClient
	cancel context.CancelFunc
	// renew is when the next generation replaces it: halfway through its JWT's validity.
	renew time.Time
	// ended closes when the Attach ends.
	ended chan struct{}

	sendMu sync.Mutex
	stream channelv1.Channel_AttachClient
}

// graceOld is how long the connection of a replaced generation stays open, for
// the calls in flight on it.
const graceOld = 30 * time.Second

// run keeps the link until ctx ends.
func (l *link) run(ctx context.Context) {
	backoff := time.Second
	for ctx.Err() == nil {
		g, err := l.attach(ctx)
		if err != nil {
			if ctx.Err() == nil {
				log.Printf("console: attaching to the %s channel: %v", l.ch.Audience, err)
			}
			select {
			case <-ctx.Done():
			case <-time.After(backoff):
			}
			backoff = min(2*backoff, 30*time.Second)
			continue
		}
		backoff = time.Second
		// The channel replaced the previous session with this one, so closing
		// it lets go of nothing.
		l.mu.Lock()
		prev := l.cur
		l.cur = g
		l.mu.Unlock()
		if prev != nil {
			prev.stop(graceOld)
		}
		select {
		case <-ctx.Done():
		case <-g.ended:
		case <-time.After(time.Until(g.renew)):
		}
	}
	l.mu.Lock()
	g := l.cur
	l.cur = nil
	l.mu.Unlock()
	if g != nil {
		g.stop(0)
	}
}

// attach opens a connection with a new JWT and the Attach on it, and returns
// once the channel has taken the session.
func (l *link) attach(ctx context.Context) (*generation, error) {
	// The JWT carries its expiry in whole seconds.
	issued := time.Now()
	exp := issued.Add(l.lifetime).Truncate(time.Second)
	token, err := l.keys.JWT(exp)
	if err != nil {
		return nil, err
	}
	conn, err := grpc.NewClient(l.ch.Addr, grpc.WithTransportCredentials(&rpcauth.Client{
		JWT: func() (string, error) { return token, nil }, Key: l.keys.Console,
	}))
	if err != nil {
		return nil, err
	}
	gctx, cancel := context.WithCancel(ctx)
	g := &generation{conn: conn, client: channelv1.NewChannelClient(conn), cancel: cancel, renew: issued.Add(exp.Sub(issued) / 2), ended: make(chan struct{})}
	if g.stream, err = g.client.Attach(gctx); err == nil {
		_, err = g.stream.Header()
	}
	if err != nil {
		g.stop(0)
		return nil, err
	}
	go func() {
		defer close(g.ended)
		for {
			m, err := g.stream.Recv()
			if err != nil {
				return
			}
			l.receive(l, m)
		}
	}()
	return g, nil
}

func (g *generation) stop(grace time.Duration) {
	g.cancel()
	if grace == 0 {
		g.conn.Close()
		return
	}
	time.AfterFunc(grace, func() { g.conn.Close() })
}

// errDetached is a call while the console holds no session with the channel.
var errDetached = status.Error(codes.Unavailable, "console: the channel is not connected")

// client returns the client of the current connection.
func (l *link) client() (channelv1.ChannelClient, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.cur == nil {
		return nil, errDetached
	}
	return l.cur.client, nil
}

// send sends m over the current Attach.
func (l *link) send(m *channelv1.ConsoleMessage) error {
	l.mu.Lock()
	g := l.cur
	l.mu.Unlock()
	if g == nil {
		return errDetached
	}
	g.sendMu.Lock()
	defer g.sendMu.Unlock()
	return g.stream.Send(m)
}

// waitAttached waits until the link holds a session, or ctx ends.
func (l *link) waitAttached(ctx context.Context) (channelv1.ChannelClient, error) {
	for {
		if c, err := l.client(); err == nil {
			return c, nil
		}
		select {
		case <-ctx.Done():
			return nil, errDetached
		case <-time.After(50 * time.Millisecond):
		}
	}
}
