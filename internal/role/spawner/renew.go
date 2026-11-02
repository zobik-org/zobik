package spawner

import (
	"context"
	"errors"
	"log"
	"strings"
	"time"

	"zobik.org/zobik/internal/globalconfig"
	"zobik.org/zobik/internal/role"
	"zobik.org/zobik/internal/role/catalog"
)

const presencePrefix = "presence:"

// presenceTTL is catalog_presence_ttl.
func (s *spawner) presenceTTL() time.Duration {
	return s.head.Load().Duration("catalog_presence_ttl")
}

// markPresence writes the presence mark of digest, which expires on its own if
// the Spawner stops renewing it (architecture §3.13.3).
func (s *spawner) markPresence(ctx context.Context, cat Catalog, digest string) error {
	return cat.PutRecord(ctx, s.o.Network, presencePrefix+digest, map[string]any{}, catalog.Write{Expires: s.presenceTTL()})
}

// serveRenewal is the Spawner's own cycle over its fleet: it renews each node's
// platform identity at a third of its life, and each presence mark at a fifth
// of catalog_presence_ttl (architecture §3.13.1, §3.13.3, implementation
// §1.2.4, §6, The initial Global Configuration).
func (s *spawner) serveRenewal(ctx context.Context, l *role.Loop) {
	for {
		l.Mark()
		cycle := max(min(s.presenceTTL()/5, s.o.IdentityLifetime/6), time.Second)
		cycleCtx, cancel := context.WithTimeout(ctx, max(cycle, opTimeout))
		s.renew(cycleCtx)
		cancel()
		select {
		case <-ctx.Done():
			return
		case <-time.After(cycle):
		}
	}
}

func (s *spawner) renew(ctx context.Context) {
	if head, _, err := globalconfig.Head(ctx, s.js); err == nil {
		s.head.Store(head)
	}
	cat, err := s.o.Catalog(ctx)
	if err != nil && !errors.Is(err, ErrNoCatalog) {
		log.Printf("spawner: renewal: %v", err)
	}
	// The marks are listed before the acts in flight and the fleet are read, so
	// a mark an act writes afterwards is not removed: its revision moved.
	var marks []catalog.Record
	if cat != nil {
		if marks, err = cat.Records(ctx, presencePrefix, s.o.Network); err != nil {
			log.Printf("spawner: renewal: listing the presence marks: %v", err)
			cat = nil
		}
	}
	s.mu.Lock()
	running := map[string]bool{}
	for digest := range s.coming {
		running[digest] = true
	}
	s.mu.Unlock()
	fleet, err := s.o.Driver.Fleet(ctx)
	if err != nil {
		log.Printf("spawner: renewal: listing the fleet: %v", err)
		return
	}

	now := time.Now()
	alive := map[string]bool{}
	for _, n := range fleet {
		running[n.ArtifactRef] = true
		alive[n.NodeID] = true
		if now.Sub(s.renewed[n.NodeID]) < s.o.IdentityLifetime/3 {
			continue
		}
		if err := s.o.Driver.Renew(ctx, n); err != nil {
			log.Printf("spawner: renewing node %s: %v", n.NodeID, err)
			continue
		}
		s.renewed[n.NodeID] = now
	}
	for id := range s.renewed {
		if !alive[id] {
			delete(s.renewed, id)
		}
	}

	if cat == nil {
		return
	}
	for digest := range running {
		if err := s.markPresence(ctx, cat, digest); err != nil {
			log.Printf("spawner: renewing the presence of %s: %v", digest, err)
		}
	}
	for _, m := range marks {
		if digest := strings.TrimPrefix(m.Key, presencePrefix); !running[digest] {
			err := cat.DeleteRecord(ctx, s.o.Network, m.Key, m.Revision)
			if err != nil && !errors.Is(err, catalog.ErrPrecondition) {
				log.Printf("spawner: removing the presence of %s: %v", digest, err)
			}
		}
	}
}
