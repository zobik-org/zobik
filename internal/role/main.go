package role

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/globalconfig"
)

// Body is what a role does once its runtime is up.
type Body func(ctx context.Context, rt *Runtime, nc *nats.Conn) error

// The deadlines of a role's operations: a loop that does not mark within
// loopDeadline is stuck, and each operation that can block carries opTimeout.
const (
	loopDeadline = 30 * time.Second
	opTimeout    = 5 * time.Second
)

// Main runs a role: it loads its material, serves its health check, watches its
// loops, connects to the Bus and runs body until ctx ends.
func Main(ctx context.Context, name string, body Body) error {
	rt, err := Load(name, Dir)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := rt.ServeHealth(ctx, HealthSocket); err != nil {
		return fmt.Errorf("role: health socket: %w", err)
	}
	go rt.Watch(ctx, time.Second)
	nc, err := rt.Connect(bus.URL)
	if err != nil {
		return err
	}
	defer nc.Close()
	log.Printf("%s: running as %s", name, rt.ID)
	return body(ctx, rt, nc)
}

// MainLoop is the loop a role's body marks.
func (rt *Runtime) MainLoop() *Loop { return rt.Loop("main", loopDeadline) }

// AwaitHead reads the head of the Global Configuration, retrying until the Config
// Store has published one: what every role waits for (implementation §6, The roles of the binary).
func AwaitHead(ctx context.Context, l *Loop, js jetstream.JetStream) (*globalconfig.Record, error) {
	var head *globalconfig.Record
	err := Retry(ctx, l, opTimeout, func(ctx context.Context) error {
		var err error
		head, _, err = globalconfig.Head(ctx, js)
		return err
	})
	return head, err
}

// AwaitConsumer binds the durable consumer named by the role's identity on
// stream, retrying until it exists.
func AwaitConsumer(ctx context.Context, l *Loop, js jetstream.JetStream, rt *Runtime, stream string) (jetstream.Consumer, error) {
	var c jetstream.Consumer
	err := Retry(ctx, l, opTimeout, func(ctx context.Context) error {
		var err error
		c, err = js.Consumer(ctx, stream, rt.ID)
		return err
	})
	return c, err
}

// Consume pulls from a durable consumer and hands each message to handle, until
// ctx ends. It marks l between pulls, and a pull ends with ctx, so a role that
// stops does not wait for one in flight.
func Consume(ctx context.Context, l *Loop, c jetstream.Consumer, handle func(jetstream.Msg)) {
	for ctx.Err() == nil {
		l.Mark()
		pull, cancel := context.WithTimeout(ctx, 5*time.Second)
		batch, err := c.Fetch(64, jetstream.FetchContext(pull))
		if err != nil {
			cancel()
			if ctx.Err() == nil {
				log.Printf("fetching from %s: %v", c.CachedInfo().Stream, err)
				time.Sleep(time.Second)
			}
			continue
		}
		for m := range batch.Messages() {
			l.Mark()
			handle(m)
		}
		cancel()
	}
}
