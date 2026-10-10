package bus

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// The streams are a fixed set per subject family, each retaining one week with a
// ceiling of 512 MB that discards the oldest (implementation §1.2.1). lease.> has
// none: the renewal travels on NATS core.
const (
	familyRetention = 7 * 24 * time.Hour
	familyMaxBytes  = 512 << 20
)

// Streams are the family streams zobik init creates and no operation of the network alters.
var Streams = []jetstream.StreamConfig{
	familyStream("task", "task.>"),
	familyStream("prop", "prop.>"),
	familyStream("notice", "notice.>"),
}

func familyStream(name, subject string) jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:      name,
		Subjects:  []string{subject},
		Retention: jetstream.LimitsPolicy,
		Storage:   jetstream.FileStorage,
		MaxAge:    familyRetention,
		MaxBytes:  familyMaxBytes,
		Discard:   jetstream.DiscardOld,
	}
}

// EnsureStreams creates the family streams, or brings them back to their
// definition, so running it again converges.
func EnsureStreams(ctx context.Context, js jetstream.JetStream) error {
	for _, cfg := range Streams {
		if _, err := js.CreateOrUpdateStream(ctx, cfg); err != nil {
			return fmt.Errorf("bus: stream %s: %w", cfg.Name, err)
		}
	}
	return nil
}
