package bus

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Consumer is a durable consumer zobik init creates for a role: named by the
// role's identity and filtered by the literal subjects it listens to
// (implementation §1.2.1).
type Consumer struct {
	Stream  string   `json:"stream"`
	Name    string   `json:"name"`
	Filters []string `json:"filters"`
}

// consumerStreams are the streams whose durable consumers are all zobik init's.
var consumerStreams = []string{"task", "prop"}

// EnsureConsumers creates the consumers in want, or brings their filters back to
// what want says, and deletes every other durable consumer of the family streams:
// the one an identity that is no longer used left behind.
func EnsureConsumers(ctx context.Context, js jetstream.JetStream, want []Consumer) error {
	keep := map[string]bool{}
	for _, c := range want {
		keep[c.Stream+"/"+c.Name] = true
		_, err := js.CreateOrUpdateConsumer(ctx, c.Stream, jetstream.ConsumerConfig{
			Durable:        c.Name,
			FilterSubjects: c.Filters,
			AckPolicy:      jetstream.AckExplicitPolicy,
			// A consumer receives what is published from its creation on: an
			// identity that replaces another does not replay the week the stream retains.
			DeliverPolicy: jetstream.DeliverNewPolicy,
		})
		if err != nil {
			return fmt.Errorf("bus: consumer %s on %s: %w", c.Name, c.Stream, err)
		}
	}
	for _, stream := range consumerStreams {
		s, err := js.Stream(ctx, stream)
		if err != nil {
			return err
		}
		names := s.ConsumerNames(ctx)
		for name := range names.Name() {
			if keep[stream+"/"+name] {
				continue
			}
			if err := s.DeleteConsumer(ctx, name); err != nil {
				return fmt.Errorf("bus: deleting consumer %s on %s: %w", name, stream, err)
			}
		}
		if err := names.Err(); err != nil {
			return err
		}
	}
	return nil
}

// historyStream keeps every published version of the Global Configuration: no
// age, no byte ceiling, and nothing in it can be deleted (architecture §2.3,
// implementation §1.2.5).
var historyStream = jetstream.StreamConfig{
	Name:        StreamHistory,
	Subjects:    []string{SubjectHistory + ".>"},
	Retention:   jetstream.LimitsPolicy,
	Storage:     jetstream.FileStorage,
	DenyDelete:  true,
	DenyPurge:   true,
	AllowDirect: true,
}

// EnsureBuckets creates the Config Store's head and history and the Task
// Broker's bucket (implementation §1.2.5, §1.2.7). The task bucket's lifetime
// starts at tasksTTL; from then on the Task Broker adjusts it, so an existing
// bucket is left as it is.
func EnsureBuckets(ctx context.Context, js jetstream.JetStream, tasksTTL time.Duration) error {
	if _, err := js.CreateOrUpdateStream(ctx, historyStream); err != nil {
		return fmt.Errorf("bus: stream %s: %w", StreamHistory, err)
	}
	if _, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:  BucketConfig,
		History: 1,
		Storage: jetstream.FileStorage,
	}); err != nil {
		return fmt.Errorf("bus: bucket %s: %w", BucketConfig, err)
	}
	_, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:  BucketTasks,
		History: 1,
		TTL:     tasksTTL,
		Storage: jetstream.FileStorage,
	})
	if err != nil && !errors.Is(err, jetstream.ErrBucketExists) {
		return fmt.Errorf("bus: bucket %s: %w", BucketTasks, err)
	}
	return nil
}
