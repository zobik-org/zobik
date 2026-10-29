package sidecar

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"zobik.org/zobik/internal/egress"
	"zobik.org/zobik/internal/globalconfig"
)

// ConfigSchema is the fragment of the Global Configuration schema with the keys
// this package reads (implementation §1.2.5).
//
//go:embed config.schema.json
var ConfigSchema []byte

// config is the Global Configuration as the Integration Sidecar reads it: the
// current keys from the head, which it reads in its own cycle, and the pinned
// ones from the version each trace stamped (architecture §2.3).
type config struct {
	js jetstream.JetStream
	// lease and auction are task_lease_ttl and auction_timeout as of the last
	// head read, in nanoseconds.
	lease, auction atomic.Int64
	// current is the last head read, whose embedding_model the announcements
	// are embedded under (architecture §3.7.4).
	current atomic.Pointer[globalconfig.Record]

	mu sync.Mutex
	// pinned caches similarity_precision_decimals by config_version: a
	// published version never changes.
	pinned map[string]int
	// compiled is the egress_registry of version, compiled.
	compiled struct {
		version  string
		registry map[string]*egress.Entry
	}
}

func newConfig(js jetstream.JetStream, head *globalconfig.Record) *config {
	c := &config{js: js, pinned: map[string]int{}}
	c.store(head)
	return c
}

func (c *config) store(head *globalconfig.Record) {
	c.current.Store(head)
	c.lease.Store(int64(head.Duration("task_lease_ttl")))
	c.auction.Store(int64(head.Duration("auction_timeout")))
}

// follow reads the head every few seconds until ctx ends.
func (c *config) follow(ctx context.Context) {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		opCtx, cancel := context.WithTimeout(ctx, opTimeout)
		if head, _, err := globalconfig.Head(opCtx, c.js); err == nil {
			c.store(head)
		}
		cancel()
	}
}

func (c *config) head() *globalconfig.Record    { return c.current.Load() }
func (c *config) leaseTTL() time.Duration       { return time.Duration(c.lease.Load()) }
func (c *config) auctionTimeout() time.Duration { return time.Duration(c.auction.Load()) }

// registry is the egress_registry of the head, compiled once per version: the
// rules belong to the current class (architecture §2.3, §3.16.4).
func (c *config) registry() (map[string]*egress.Entry, string, error) {
	head := c.head()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.compiled.registry != nil && c.compiled.version == head.ConfigVersion {
		return c.compiled.registry, head.ConfigVersion, nil
	}
	reg := map[string]*egress.Entry{}
	if raw, ok := head.Values["egress_registry"]; ok {
		var err error
		if reg, err = egress.ParseRegistry(raw); err != nil {
			return nil, "", fmt.Errorf("egress_registry of %s: %w", head.ConfigVersion, err)
		}
	}
	c.compiled.version, c.compiled.registry = head.ConfigVersion, reg
	return reg, head.ConfigVersion, nil
}

// decimals is similarity_precision_decimals of the version a trace pinned.
func (c *config) decimals(ctx context.Context, version string) (int, error) {
	c.mu.Lock()
	d, ok := c.pinned[version]
	c.mu.Unlock()
	if ok {
		return d, nil
	}
	rec, err := globalconfig.Version(ctx, c.js, version)
	if err != nil {
		return 0, err
	}
	if err := json.Unmarshal(rec.Values["similarity_precision_decimals"], &d); err != nil {
		return 0, fmt.Errorf("similarity_precision_decimals of %s: %w", version, err)
	}
	c.mu.Lock()
	c.pinned[version] = d
	c.mu.Unlock()
	return d, nil
}
