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

// consumerStreams are the streams whose durable consumers are zobik init's,
// except the nodes'.
var consumerStreams = []string{"task", "prop", "notice"}

// metaNode marks a node's consumer, which the Spawner creates and deletes with
// the node and zobik init leaves alone.
const metaNode = "zobik_node"

// EnsureConsumers creates the consumers in want, or brings their filters back to
// what want says, and deletes every other durable consumer of the family streams
// except the nodes': the one a role identity that is no longer used left behind.
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
		infos := s.ListConsumers(ctx)
		for info := range infos.Info() {
			name := info.Name
			if keep[stream+"/"+name] || info.Config.Metadata[metaNode] != "" {
				continue
			}
			if err := s.DeleteConsumer(ctx, name); err != nil {
				return fmt.Errorf("bus: deleting consumer %s on %s: %w", name, stream, err)
			}
		}
		if err := infos.Err(); err != nil {
			return err
		}
	}
	return nil
}

// NodeConsumer creates the durable consumer of the node whose platform identity
// is id, which the act of provisioning creates with the deployment
// (architecture §3.13.1, implementation §1.2.1): the announcements of its topic,
// the award and the rejection addressed to it, and the terminal events of the
// tasks it opens.
func NodeConsumer(ctx context.Context, js jetstream.JetStream, id, topic string) error {
	_, err := js.CreateOrUpdateConsumer(ctx, "task", jetstream.ConsumerConfig{
		Durable: id,
		FilterSubjects: []string{
			SubjectAnnounced(topic),
			SubjectAddressed(TypeAssigned, id),
			SubjectAddressed(TypeRejected, id),
			SubjectTerminal(TypeCompleted, id, ">"),
			SubjectTerminal(TypeFailed, id, ">"),
			SubjectTerminal(TypeAborted, id, ">"),
		},
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverNewPolicy,
		Metadata:      map[string]string{metaNode: topic},
	})
	if err != nil {
		return fmt.Errorf("bus: consumer of node %s: %w", id, err)
	}
	return nil
}

// DeleteNodeConsumer deletes a node's consumer; one that does not exist is no error.
func DeleteNodeConsumer(ctx context.Context, js jetstream.JetStream, id string) error {
	err := js.DeleteConsumer(ctx, "task", id)
	if errors.Is(err, jetstream.ErrConsumerNotFound) {
		return nil
	}
	return err
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

// EnsureBuckets creates the Config Store's head and history, the rates bucket,
// empty, and the Task Broker's bucket (implementation §1.2.5, §1.2.7, §8.5). The task bucket's lifetime
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
	// The rates bucket keeps only the current value of each table (implementation §8.5).
	if _, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:  BucketRates,
		History: 1,
		Storage: jetstream.FileStorage,
	}); err != nil {
		return fmt.Errorf("bus: bucket %s: %w", BucketRates, err)
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
