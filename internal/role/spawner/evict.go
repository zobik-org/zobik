package spawner

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"log"
	"slices"
	"strings"
	"time"

	"zobik.org/zobik/internal/role/catalog"
	"zobik.org/zobik/internal/signing"
)

// The network-scope keys of the pressure on capacity and on storage
// (architecture §3.13.3, §3.13.10).
const (
	keyCapacityPressure = "capacity_pressure"
	keyStoragePressure  = "storage_pressure"
)

// capacityPressure is the capacity-pressure key: since when, and how many
// evictions the window had when it was stamped.
type capacityPressure struct {
	Since     time.Time `json:"since"`
	Evictions int       `json:"evictions"`
}

// storagePressure is the storage-pressure key: since when, and what was
// occupied of what capacity.
type storagePressure struct {
	Since    time.Time `json:"since"`
	Occupied int64     `json:"occupied"`
	Capacity int64     `json:"capacity"`
}

// makeRoom evicts free nodes until the node p brings up fits in the fleet's
// capacity: the free node of its partition with the oldest assignment first
// (architecture §3.13.10). Without enough free nodes it evicts none and
// refuses with capacity_exhausted.
func (s *spawner) makeRoom(ctx context.Context, cat Catalog, p Provisioning) error {
	short, err := s.o.Driver.Shortfall(ctx, p)
	if err != nil || short == 0 {
		return err
	}
	fleet, err := s.o.Driver.Fleet(ctx)
	if err != nil {
		return err
	}
	victims := s.victims(fleet, short, time.Now())
	if victims == nil {
		return Refuse(CodeCapacityExhausted, "the fleet lacks %d MB and no free node frees them", short)
	}
	ids, err := s.archive(ctx, cat, victims, TriggerCapacityEviction)
	if err != nil {
		return err
	}
	for _, id := range ids {
		s.obs.forgetNode(id)
	}
	log.Printf("spawner: evicted %v to make room for %s", ids, p.Entry.ArtifactRef)
	s.evicted(ctx, cat, len(ids))
	return nil
}

// victims are the free nodes to evict to free short MB, oldest assignment
// first; a node that received no assignment counts its provisioning
// (implementation §1.2.4). A channel is never evicted (architecture §3.13.10).
// It returns nil if the free nodes do not add up to short.
func (s *spawner) victims(fleet []Running, short int64, now time.Time) []Running {
	type aged struct {
		n    Running
		last time.Time
	}
	var free []aged
	for _, n := range fleet {
		if strings.HasPrefix(n.Topic, signing.HITLPrefix) || s.obs.busy(n.NodeID, now) {
			continue
		}
		last, ok := s.obs.lastAssigned(n.NodeID)
		if !ok || last.Before(n.Provisioned) {
			last = n.Provisioned
		}
		free = append(free, aged{n, last})
	}
	slices.SortFunc(free, func(a, b aged) int {
		if c := a.last.Compare(b.last); c != 0 {
			return c
		}
		return cmp.Compare(a.n.NodeID, b.n.NodeID)
	})
	var out []Running
	var freed int64
	for _, f := range free {
		if freed >= short {
			break
		}
		out = append(out, f.n)
		freed += f.n.MemoryMB
	}
	if freed < short {
		return nil
	}
	return out
}

// evicted counts n evictions, and stamps the capacity-pressure key when the
// window holds more than eviction_pressure_threshold (architecture §3.13.10).
// The notice enters with La contención.
func (s *spawner) evicted(ctx context.Context, cat Catalog, n int) {
	window := s.head.Load().Duration("eviction_pressure_window")
	var threshold int
	if err := json.Unmarshal(s.head.Load().Values["eviction_pressure_threshold"], &threshold); err != nil {
		log.Printf("spawner: eviction_pressure_threshold: %v", err)
		return
	}
	now := time.Now()
	s.mu.Lock()
	for range n {
		s.evictions = append(s.evictions, now)
	}
	s.evictions = slices.DeleteFunc(s.evictions, func(t time.Time) bool { return now.Sub(t) > window })
	count := len(s.evictions)
	s.mu.Unlock()
	if count <= threshold {
		return
	}
	key := capacityPressure{Since: now.UTC(), Evictions: count}
	var prev capacityPressure
	if _, ok, err := cat.Record(ctx, s.o.Network, keyCapacityPressure, &prev); err == nil && ok {
		key.Since = prev.Since
	}
	if err := cat.PutRecord(ctx, s.o.Network, keyCapacityPressure, key, catalog.Write{}); err != nil {
		log.Printf("spawner: stamping the capacity pressure: %v", err)
	}
}

// pressure is the renewal's check of both pressure keys: it deletes the
// capacity one after a whole window without evictions, and stamps or deletes
// the storage one against artifact_storage_pressure_share (architecture
// §3.13.3, §3.13.10).
func (s *spawner) pressure(ctx context.Context, cat Catalog) {
	window := s.head.Load().Duration("eviction_pressure_window")
	now := time.Now()
	s.mu.Lock()
	quiet := len(s.evictions) == 0 || now.Sub(s.evictions[len(s.evictions)-1]) > window
	if quiet {
		s.evictions = nil
	}
	s.mu.Unlock()
	if quiet {
		deleteKey(ctx, cat, s.o.Network, keyCapacityPressure)
	}

	var share float64
	if err := json.Unmarshal(s.head.Load().Values["artifact_storage_pressure_share"], &share); err != nil {
		log.Printf("spawner: artifact_storage_pressure_share: %v", err)
		return
	}
	status, err := cat.Status(ctx)
	if err != nil {
		log.Printf("spawner: renewal: reading the Catalog's status: %v", err)
		return
	}
	if status.Capacity <= 0 || float64(status.Occupied) <= share*float64(status.Capacity) {
		deleteKey(ctx, cat, s.o.Network, keyStoragePressure)
		return
	}
	key := storagePressure{Since: now.UTC(), Occupied: status.Occupied, Capacity: status.Capacity}
	var prev storagePressure
	if _, ok, err := cat.Record(ctx, s.o.Network, keyStoragePressure, &prev); err == nil && ok {
		key.Since = prev.Since
	}
	if err := cat.PutRecord(ctx, s.o.Network, keyStoragePressure, key, catalog.Write{}); err != nil {
		log.Printf("spawner: stamping the storage pressure: %v", err)
	}
}

// deleteKey deletes a record of network if it exists.
func deleteKey(ctx context.Context, cat Catalog, network, key string) {
	rev, ok, err := cat.Record(ctx, network, key, nil)
	if err == nil && ok {
		err = cat.DeleteRecord(ctx, network, key, rev)
	}
	if err != nil && !errors.Is(err, catalog.ErrPrecondition) {
		log.Printf("spawner: deleting %s: %v", key, err)
	}
}
