package contextstore

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/grpc"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/globalconfig"
	"zobik.org/zobik/internal/role"
	"zobik.org/zobik/internal/rpcauth"
	contextv1 "zobik.org/zobik/proto/zobik/context/v1"
)

// Options are the deployment parameters the role enforces (implementation §6,
// The deployment parameters).
type Options struct {
	StoreSize, TraceSize int64
}

// sweepEvery is how often the role purges the traces whose grace has passed.
const sweepEvery = time.Minute

// Run is the context role: the Context Store's service, and the purge on trace
// close (implementation §1.2.9). It is ready once it reads the Global
// Configuration, binds its consumer and listens.
func Run(ctx context.Context, rt *role.Runtime, nc *nats.Conn, o Options) error {
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	l := rt.MainLoop()
	if _, err := role.AwaitHead(ctx, l, js); err != nil {
		return err
	}
	consumer, err := role.AwaitConsumer(ctx, l, js, rt, "task")
	if err != nil {
		return err
	}
	store, err := Open(role.DataDir, o.StoreSize, o.TraceSize)
	if err != nil {
		return err
	}

	g := grpc.NewServer(grpc.Creds(&rpcauth.Server{Account: rt.Account}))
	contextv1.RegisterContextStoreServer(g, &Server{Store: store, Root: rt.Root})
	ln, err := net.Listen("tcp", ":"+role.PortContext)
	if err != nil {
		return err
	}
	go g.Serve(ln)
	defer g.Stop()

	go observeCloses(ctx, rt.Loop("closes", time.Minute), consumer, store)
	rt.SetReady(true)

	t := time.NewTicker(time.Second)
	defer t.Stop()
	next := time.Now()
	for {
		select {
		case <-ctx.Done():
			return nil
		case now := <-t.C:
			l.Mark()
			if now.Before(next) {
				continue
			}
			next = now.Add(sweepEvery)
			if err := sweep(ctx, js, store, now); err != nil {
				log.Printf("context: sweep: %v", err)
			}
		}
	}
}

// sweep reads context_retention_grace from the head on each pass, so a change of
// the window also reaches traces already closed (implementation §1.2.9).
func sweep(ctx context.Context, js jetstream.JetStream, store *Store, now time.Time) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	head, _, err := globalconfig.Head(ctx, js)
	if err != nil {
		return err
	}
	var graceMs int64
	if err := json.Unmarshal(head.Values["context_retention_grace"], &graceMs); err != nil {
		return fmt.Errorf("context_retention_grace: %w", err)
	}
	return store.Sweep(time.Duration(graceMs)*time.Millisecond, now)
}

// observeCloses marks the close of each trace: the terminal event whose subject
// is the TraceID of its traceparent closes the root, and by the closing rule the
// whole tree has already finished (architecture §3.9.1, implementation §1.2.9).
func observeCloses(ctx context.Context, l *role.Loop, c jetstream.Consumer, store *Store) {
	role.Consume(ctx, l, c, func(m jetstream.Msg) {
		var e bus.Event
		if err := json.Unmarshal(m.Data(), &e); err == nil {
			if trace := bus.TraceID(e.TraceParent); trace != "" && e.Subject == trace {
				if err := store.MarkClosed(trace, time.Now()); err != nil {
					log.Printf("context: closing %s: %v", trace, err)
					m.Nak()
					return
				}
			}
		}
		m.Ack()
	})
}
