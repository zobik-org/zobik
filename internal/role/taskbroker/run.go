package taskbroker

import (
	"context"
	"errors"
	"log"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/globalconfig"
	"zobik.org/zobik/internal/role"
	"zobik.org/zobik/internal/signing"
)

// Run is the task_broker role. It is ready once it reads the Global
// Configuration, binds its consumers and resumes the state of its range from
// the tasks bucket (implementation §1.2.7, §6, The health of the roles).
func Run(ctx context.Context, rt *role.Runtime, nc *nats.Conn) error {
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	l := rt.MainLoop()
	head, err := role.AwaitHead(ctx, l, js)
	if err != nil {
		return err
	}
	p, err := readParams(head.Values)
	if err != nil {
		return err
	}
	announcements, err := role.AwaitConsumer(ctx, l, js, rt, "task")
	if err != nil {
		return err
	}
	proposals, err := role.AwaitConsumer(ctx, l, js, rt, "prop")
	if err != nil {
		return err
	}
	var kv jetstream.KeyValue
	if err := role.Retry(ctx, l, opTimeout, func(ctx context.Context) error {
		if kv, err = js.KeyValue(ctx, bus.BucketTasks); err != nil {
			return err
		}
		return followRetention(ctx, js, p.retentionGrace)
	}); err != nil {
		return err
	}
	sign := func(typ signing.Type, claims any) (string, error) { return rt.Sign(typ, claims, rt.Certificate) }
	b := newBroker(ctx, rt.ID, rt.Root, js, kv, sign, p)
	return b.serve(ctx, rt, l, nc, announcements, proposals)
}

// serve resumes the range before taking anything new: an instance does not
// serve a range until it can read that range's state (architecture §3.19).
func (b *broker) serve(ctx context.Context, rt *role.Runtime, l *role.Loop, nc *nats.Conn, announcements, proposals jetstream.Consumer) error {
	if err := role.Retry(ctx, l, time.Minute, b.resumeAll); err != nil {
		return err
	}
	// Renewals arrive on NATS core, valid only on arrival (implementation §1.2.1):
	// lease.<identity>.<topic>.<TaskID>.
	sub, err := nc.Subscribe("lease.>", func(m *nats.Msg) {
		tokens := strings.Split(m.Subject, ".")
		if len(tokens) != 4 {
			return
		}
		identity, topic, taskID := tokens[1], tokens[2], tokens[3]
		b.do(topic, func(ctx context.Context, w *worker) { w.onRenewal(ctx, identity, taskID) })
	})
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()

	go b.consume(ctx, rt.Loop("announcements", time.Minute), announcements, func(m jetstream.Msg) {
		tokens := strings.Split(m.Subject(), ".")
		switch {
		case len(tokens) == 3 && tokens[1] == "announced":
			b.do(tokens[2], func(ctx context.Context, w *worker) { w.onAnnounced(ctx, m) })
		case len(tokens) == 4:
			b.do(tokens[3], func(ctx context.Context, w *worker) { w.onClosed(ctx, m) })
		default:
			m.Ack()
		}
	})
	go b.consume(ctx, rt.Loop("proposals", time.Minute), proposals, func(m jetstream.Msg) {
		tokens := strings.Split(m.Subject(), ".")
		if len(tokens) != 3 {
			m.Ack()
			return
		}
		identity, topic := tokens[1], tokens[2]
		b.do(topic, func(_ context.Context, w *worker) { w.onProposed(m, identity) })
	})
	rt.SetReady(true)
	return b.followConfig(ctx, l)
}

// resumeAll reads every task in the bucket and arms what each one waits for.
func (b *broker) resumeAll(ctx context.Context) error {
	lister, err := b.tasks.kv.ListKeys(ctx)
	if err != nil {
		return err
	}
	var keys []string
	// The lister stops on its own once it has delivered every key.
	for k := range lister.Keys() {
		keys = append(keys, k)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, key := range keys {
		topic, taskID, ok := strings.Cut(key, ".")
		if !ok {
			continue
		}
		t, _, err := b.tasks.get(ctx, key)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		b.do(topic, func(_ context.Context, w *worker) { w.resume(t, taskID) })
	}
	return nil
}

// consume pulls from a durable consumer and hands each message to route.
func (b *broker) consume(ctx context.Context, l *role.Loop, c jetstream.Consumer, route func(jetstream.Msg)) {
	for ctx.Err() == nil {
		l.Mark()
		batch, err := c.Fetch(64, jetstream.FetchMaxWait(5*time.Second))
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				log.Printf("task_broker: fetching from %s: %v", c.CachedInfo().Stream, err)
				time.Sleep(time.Second)
			}
			continue
		}
		for m := range batch.Messages() {
			l.Mark()
			route(m)
		}
	}
}

// followConfig reads the head every second: the Task Broker consults the
// current configuration in its own cycle, and nobody notifies it of a change
// (architecture §3.15.3, §3.18). It also keeps the bucket's lifetime on
// context_retention_grace (implementation §1.2.7).
func (b *broker) followConfig(ctx context.Context, l *role.Loop) error {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
		l.Mark()
		opCtx, cancel := context.WithTimeout(ctx, opTimeout)
		head, _, err := globalconfig.Head(opCtx, b.js)
		if err == nil {
			var p *params
			if p, err = readParams(head.Values); err == nil {
				if old := b.params.Load().retentionGrace; p.retentionGrace != old {
					if err = followRetention(opCtx, b.js, p.retentionGrace); err != nil {
						p.retentionGrace = old // tried again on the next read
					}
				}
				b.params.Store(p)
			}
		}
		cancel()
		if err != nil {
			log.Printf("task_broker: reading the Global Configuration: %v", err)
		}
	}
}
