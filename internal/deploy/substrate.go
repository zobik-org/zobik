package deploy

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/configschema"
	"zobik.org/zobik/internal/globalconfig"
)

// substrate is what zobik init creates on the Bus and no operation of the network
// alters: the family streams, the Config Store's head and history with v0, the
// Task Broker's bucket and each role's consumers (implementation §6, Installation
// and zobik init, step 2 and step 4). The console computes it; the act applies it.
type substrate struct {
	Consumers  []bus.Consumer      `json:"consumers"`
	TasksTTLMs int64               `json:"tasks_ttl_ms"`
	V0         globalconfig.Record `json:"v0"`
}

// newSubstrate builds the substrate for the roles' identities ids, by unit.
func newSubstrate(ids map[string]string) ([]byte, error) {
	schema, err := configschema.Load()
	if err != nil {
		return nil, err
	}
	v0 := schema.V0()
	// The task bucket's lifetime is context_retention_grace (implementation §1.2.7).
	var grace int64
	if err := json.Unmarshal(v0["context_retention_grace"], &grace); err != nil {
		return nil, fmt.Errorf("deploy: context_retention_grace: %w", err)
	}
	s := substrate{
		TasksTTLMs: grace,
		V0: globalconfig.Record{
			ConfigVersion: globalconfig.V0Version,
			PublishedAt:   time.Now().UTC(),
			AuthorizedBy:  globalconfig.AuthorizedByDeployment,
			Values:        v0,
		},
	}
	for _, r := range Roles {
		if r.Consumers != nil {
			s.Consumers = append(s.Consumers, r.Consumers(ids[r.Unit])...)
		}
	}
	return json.Marshal(s)
}

func (s *substrate) ensure(ctx context.Context, js jetstream.JetStream) error {
	if err := bus.EnsureStreams(ctx, js); err != nil {
		return err
	}
	if err := bus.EnsureBuckets(ctx, js, time.Duration(s.TasksTTLMs)*time.Millisecond); err != nil {
		return err
	}
	if err := globalconfig.WriteV0(ctx, js, s.V0); err != nil {
		return err
	}
	return bus.EnsureConsumers(ctx, js, s.Consumers)
}
