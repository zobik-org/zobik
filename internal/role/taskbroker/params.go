package taskbroker

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/globalconfig"
)

// params are the current-class keys the Task Broker reads from the head
// (architecture §2.3): the auction, the backoff, the lease, the pause and the
// bucket's lifetime.
type params struct {
	auctionTimeout   time.Duration
	backoffBase      time.Duration
	backoffMax       time.Duration
	leaseTTL         time.Duration
	networkAdmission string
	retentionGrace   time.Duration
}

func readParams(v globalconfig.Values) (*params, error) {
	p := &params{}
	for key, dst := range map[string]*time.Duration{
		"auction_timeout":         &p.auctionTimeout,
		"reannounce_backoff_base": &p.backoffBase,
		"max_reannounce_backoff":  &p.backoffMax,
		"task_lease_ttl":          &p.leaseTTL,
		"context_retention_grace": &p.retentionGrace,
	} {
		var ms int64
		if err := json.Unmarshal(v[key], &ms); err != nil {
			return nil, fmt.Errorf("%s: %w", key, err)
		}
		*dst = time.Duration(ms) * time.Millisecond
	}
	if err := json.Unmarshal(v["network_admission"], &p.networkAdmission); err != nil {
		return nil, fmt.Errorf("network_admission: %w", err)
	}
	return p, nil
}

// backoff is the interval before the re-announcement that follows n earlier
// ones: reannounce_backoff_base doubled n times, up to max_reannounce_backoff
// (architecture §3.1).
func (p *params) backoff(n int) time.Duration {
	d := p.backoffBase
	for range n {
		if d >= p.backoffMax {
			break
		}
		d *= 2
	}
	return min(d, p.backoffMax)
}

// followRetention gives the tasks bucket a lifetime of context_retention_grace,
// counted from the last write of each key (implementation §1.2.7). It edits the
// MaxAge of the bucket's stream and keeps the rest of its configuration.
func followRetention(ctx context.Context, js jetstream.JetStream, grace time.Duration) error {
	s, err := js.Stream(ctx, "KV_"+bus.BucketTasks)
	if err != nil {
		return err
	}
	cfg := s.CachedInfo().Config
	if cfg.MaxAge == grace {
		return nil
	}
	cfg.MaxAge = grace
	_, err = js.UpdateStream(ctx, cfg)
	return err
}

// pinned reads similarity_precision_decimals from the snapshot a trace pinned
// (architecture §2.3): a published version never changes, so each is read once.
type pinned struct {
	js jetstream.JetStream
	mu sync.Mutex
	by map[string]int
}

func (p *pinned) decimals(ctx context.Context, version string) (int, error) {
	p.mu.Lock()
	d, ok := p.by[version]
	p.mu.Unlock()
	if ok {
		return d, nil
	}
	rec, err := globalconfig.Version(ctx, p.js, version)
	if err != nil {
		return 0, err
	}
	if err := json.Unmarshal(rec.Values["similarity_precision_decimals"], &d); err != nil {
		return 0, fmt.Errorf("similarity_precision_decimals of %s: %w", version, err)
	}
	p.mu.Lock()
	p.by[version] = d
	p.mu.Unlock()
	return d, nil
}
