package spawner

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"log"
	"slices"
	"strconv"
	"strings"
	"time"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/claim"
	"zobik.org/zobik/internal/globalconfig"
	"zobik.org/zobik/internal/role/catalog"
)

// The queries the Spawner answers about its Catalog, by auction without
// similarity (architecture §3.7.6, §3.13.3).
const (
	TopicCatalogQuery = "node_catalog_query"
	TopicTopicQuery   = "topic_catalog_query"
)

// The state of a topic in topic_catalog_query (architecture §3.7.6).
const (
	TopicActive    = "active"
	TopicAvailable = "available"
	TopicUncovered = "uncovered"
)

// The state of an entry in node_catalog_query (architecture §3.13.3).
const (
	EntryActive     = "active"
	EntryArchived   = "archived"
	EntryRegistered = "registered"
)

// portionLibrary is where node_catalog_query's answer goes, in the query
// task's section (implementation §10.1).
const portionLibrary = "library"

// queryTimeout bounds the reads of one answer.
const queryTimeout = time.Minute

// isQuery reports whether topic is one of the queries, which are answered
// apart from the acts: they change nothing, so they neither wait behind an act
// nor take the fleet.
func isQuery(topic string) bool { return topic == TopicCatalogQuery || topic == TopicTopicQuery }

// answer answers a query it was awarded and closes it. A failure of the Bus,
// of the Catalog, of the engine or of the Context Store leaves it without
// closing: the lease expires and it comes back with its re-announcement.
func (s *spawner) answer(ctx context.Context, topic string, c *claim.Claimant, a *claim.Assignment) {
	if head, _, err := globalconfig.Head(ctx, s.js); err == nil {
		s.head.Store(head)
	}
	stop := c.KeepLease(a, s.leaseTTL)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
	out, err := s.query(ctx, topic, a)
	if err != nil {
		log.Printf("spawner: %s left to its re-announcement: %v", a.TaskID, err)
		return
	}
	if err := c.Close(ctx, a, out.Type, out.Data); err != nil {
		log.Printf("spawner: closing %s: %v", a.TaskID, err)
	}
}

func (s *spawner) query(ctx context.Context, topic string, a *claim.Assignment) (outcome, error) {
	if err := a.Announcement.ValidateTopic(); err != nil {
		return failed(Refuse(CodeRequestInvalid, "%v", err)), nil
	}
	cat, err := s.o.Catalog(ctx)
	if errors.Is(err, ErrNoCatalog) {
		return failed(Refuse(CodeRequestInvalid, "%v", err)), nil
	}
	if err != nil {
		return outcome{}, err
	}
	if topic == TopicTopicQuery {
		topics, err := s.topicListing(ctx, cat)
		if err != nil {
			return outcome{}, err
		}
		return completed(map[string]any{"topics": topics}), nil
	}
	lib, err := s.library(ctx, cat)
	if err != nil {
		return outcome{}, err
	}
	raw, err := json.Marshal(lib)
	if err != nil {
		return outcome{}, err
	}
	if s.o.Context == nil {
		return outcome{}, errors.New("the Spawner reaches no Context Store")
	}
	ref, err := claim.Answer(ctx, s.o.Context, a, portionLibrary, "node_catalog_query.library.schema.json", raw)
	if err != nil {
		return outcome{}, err
	}
	return completed(map[string]any{"context_ref": ref}), nil
}

// TopicRow is a topic of topic_catalog_query's listing.
type TopicRow struct {
	Topic       string `json:"topic"`
	Description string `json:"description"`
	State       string `json:"state"`
	// Capped is the niche's capped mark in this network (architecture §3.7.6, §3.13.5).
	Capped bool `json:"capped,omitempty"`
}

// topicListing is every topic of the Topic Registry with its description and
// its state in this network (architecture §3.7.6).
func (s *spawner) topicListing(ctx context.Context, cat Catalog) ([]TopicRow, error) {
	topics, err := cat.Topics(ctx)
	if err != nil {
		return nil, err
	}
	entries, err := cat.Entries(ctx)
	if err != nil {
		return nil, err
	}
	fleet, err := s.o.Driver.Fleet(ctx)
	if err != nil {
		return nil, err
	}
	states, err := s.topicStates(ctx, cat, entries, fleet)
	if err != nil {
		return nil, err
	}
	niches, err := cat.Records(ctx, nichePrefix, s.o.Network)
	if err != nil {
		return nil, err
	}
	capped := map[string]bool{}
	for _, r := range niches {
		var n niche
		if json.Unmarshal(r.Value, &n) == nil && n.Capped {
			capped[strings.TrimPrefix(r.Key, nichePrefix)] = true
		}
	}
	out := make([]TopicRow, 0, len(topics))
	for _, t := range topics {
		state := cmp.Or(states[t.Topic], TopicUncovered)
		out = append(out, TopicRow{Topic: t.Topic, Description: t.Description, State: state, Capped: capped[t.Topic]})
	}
	slices.SortFunc(out, func(a, b TopicRow) int { return strings.Compare(a.Topic, b.Topic) })
	return out, nil
}

// topicStates is the state of each topic some node runs on or some entry
// claims: active if a node runs on it, available if none does but an entry
// that claims it is eligible (architecture §3.7.6). A topic it does not name
// is uncovered.
func (s *spawner) topicStates(ctx context.Context, cat Catalog, entries []catalog.Entry, fleet []Running) (map[string]string, error) {
	out := map[string]string{}
	for _, n := range fleet {
		out[n.Topic] = TopicActive
	}
	status, err := cat.Status(ctx)
	if err != nil {
		return nil, err
	}
	for i := range entries {
		if out[entries[i].Topic] != "" {
			continue
		}
		ok, err := s.eligible(ctx, cat, &entries[i], status)
		if err != nil {
			return nil, err
		}
		if ok {
			out[entries[i].Topic] = TopicAvailable
		}
	}
	return out, nil
}

// Library is node_catalog_query's answer, with the fields of
// node_catalog_query.library.schema.json.
type Library struct {
	Entries         []LibraryEntry   `json:"entries"`
	StorageOccupied int64            `json:"storage_occupied"`
	StorageCapacity int64            `json:"storage_capacity"`
	Bundles         []catalog.Bundle `json:"bundles"`
	Conditions      Conditions       `json:"conditions"`
}

// Conditions are the keys of this network that back a notice on network_risk,
// each with what it stamps (architecture §3.13.3, §3.15.7). A key absent from
// the Catalog is absent here.
type Conditions struct {
	VectorSpace      *vectorSpace      `json:"vector_space,omitempty"`
	Audiences        []AudienceRow     `json:"audiences"`
	CapacityPressure *capacityPressure `json:"capacity_pressure,omitempty"`
	StoragePressure  *storagePressure  `json:"storage_pressure,omitempty"`
}

// AudienceRow is the key of an audience without a channel.
type AudienceRow struct {
	Audience string    `json:"audience"`
	Since    time.Time `json:"since"`
}

// LibraryEntry is an entry of the library as this network sees it
// (architecture §3.13.3).
type LibraryEntry struct {
	ArtifactRef    string            `json:"artifact_ref"`
	Topic          string            `json:"topic"`
	State          string            `json:"state"`
	CapabilityText string            `json:"capability_text"`
	Acquisition    *acquisitionRow   `json:"acquisition,omitempty"`
	ActiveFloors   []float64         `json:"active_floors"`
	FloorHistory   []floorHistoryRow `json:"floor_history"`
	ArtifactSize   int64             `json:"artifact_size"`
	DeletionMark   bool              `json:"deletion_mark"`
	VetoMark       bool              `json:"veto_mark"`
	PresentIn      []string          `json:"present_in"`
}

// acquisitionRow is what the library carries of the record acquisition:<digest>.
type acquisitionRow struct {
	Provenance string    `json:"provenance"`
	Cost       float64   `json:"cost"`
	AsOf       time.Time `json:"as_of"`
}

// floorHistoryRow is a row of the floor history with its floor.
type floorHistoryRow struct {
	SimilarityFloor float64 `json:"similarity_floor"`
	floorRow
}

// library reads every entry of the Catalog with what this network knows of
// it: its state, by cross-checking with the Active Node Registry, its floors,
// its acquisition, its veto and the networks with a current presence mark
// (architecture §3.13.3).
func (s *spawner) library(ctx context.Context, cat Catalog) (*Library, error) {
	entries, err := cat.Entries(ctx)
	if err != nil {
		return nil, err
	}
	fleet, err := s.o.Driver.Fleet(ctx)
	if err != nil {
		return nil, err
	}
	status, err := cat.Status(ctx)
	if err != nil {
		return nil, err
	}
	bundles, err := cat.Bundles(ctx)
	if err != nil {
		return nil, err
	}
	// Each prefix is read once, and its rows are grouped by digest.
	read := func(prefix, network string) (map[string][]catalog.Record, error) {
		recs, err := cat.Records(ctx, prefix, network)
		if err != nil {
			return nil, err
		}
		out := map[string][]catalog.Record{}
		for _, r := range recs {
			digest := strings.TrimPrefix(r.Key, prefix)
			if prefix == floorPrefix {
				// floor:<digest>:<floor>, and a digest carries a colon of its own.
				i := strings.LastIndex(digest, ":")
				if i < 0 {
					continue
				}
				digest = digest[:i]
			}
			out[digest] = append(out[digest], r)
		}
		return out, nil
	}
	floors, err := read(floorPrefix, s.o.Network)
	if err != nil {
		return nil, err
	}
	vetoes, err := read(vetoPrefix, s.o.Network)
	if err != nil {
		return nil, err
	}
	acquisitions, err := read(acquisitionPrefix, s.o.Network)
	if err != nil {
		return nil, err
	}
	presence, err := read(presencePrefix, "")
	if err != nil {
		return nil, err
	}

	lib := &Library{
		Entries: make([]LibraryEntry, 0, len(entries)), Bundles: bundles,
		StorageOccupied: status.Occupied, StorageCapacity: status.Capacity,
	}
	if lib.Bundles == nil {
		lib.Bundles = []catalog.Bundle{}
	}
	if lib.Conditions, err = s.conditions(ctx, cat); err != nil {
		return nil, err
	}
	for _, e := range entries {
		le := LibraryEntry{
			ArtifactRef: e.ArtifactRef, Topic: e.Topic, CapabilityText: e.CapabilityText,
			ArtifactSize: e.ArtifactSize, DeletionMark: e.DeletionMark,
			VetoMark:     len(vetoes[e.ArtifactRef]) > 0,
			ActiveFloors: []float64{}, FloorHistory: []floorHistoryRow{}, PresentIn: []string{},
		}
		for _, n := range running(fleet, e.ArtifactRef) {
			le.ActiveFloors = append(le.ActiveFloors, n.SimilarityFloor)
		}
		slices.Sort(le.ActiveFloors)
		for _, r := range floors[e.ArtifactRef] {
			var row floorHistoryRow
			f, err := strconv.ParseFloat(r.Key[strings.LastIndex(r.Key, ":")+1:], 64)
			if err != nil || json.Unmarshal(r.Value, &row.floorRow) != nil {
				continue
			}
			row.SimilarityFloor = f
			le.FloorHistory = append(le.FloorHistory, row)
		}
		slices.SortFunc(le.FloorHistory, func(a, b floorHistoryRow) int { return cmp.Compare(a.SimilarityFloor, b.SimilarityFloor) })
		if recs := acquisitions[e.ArtifactRef]; len(recs) > 0 {
			var acq acquisitionRow
			if json.Unmarshal(recs[0].Value, &acq) == nil && acq.Provenance != "" {
				le.Acquisition = &acq
			}
		}
		for _, r := range presence[e.ArtifactRef] {
			le.PresentIn = append(le.PresentIn, r.Network)
		}
		slices.Sort(le.PresentIn)
		switch {
		case len(le.ActiveFloors) > 0:
			le.State = EntryActive
		case len(le.FloorHistory) > 0:
			le.State = EntryArchived
		default:
			le.State = EntryRegistered
		}
		lib.Entries = append(lib.Entries, le)
	}
	slices.SortFunc(lib.Entries, func(a, b LibraryEntry) int {
		return cmp.Or(strings.Compare(a.Topic, b.Topic), strings.Compare(a.ArtifactRef, b.ArtifactRef))
	})
	return lib, nil
}

// conditions reads the keys of this network that back a notice on
// network_risk (architecture §3.13.3).
func (s *spawner) conditions(ctx context.Context, cat Catalog) (Conditions, error) {
	out := Conditions{Audiences: []AudienceRow{}}
	var vs vectorSpace
	if _, ok, err := cat.Record(ctx, s.o.Network, keyVectorSpace, &vs); err != nil {
		return out, err
	} else if ok {
		out.VectorSpace = &vs
	}
	var cp capacityPressure
	if _, ok, err := cat.Record(ctx, s.o.Network, keyCapacityPressure, &cp); err != nil {
		return out, err
	} else if ok {
		out.CapacityPressure = &cp
	}
	var sp storagePressure
	if _, ok, err := cat.Record(ctx, s.o.Network, keyStoragePressure, &sp); err != nil {
		return out, err
	} else if ok {
		out.StoragePressure = &sp
	}
	recs, err := cat.Records(ctx, audiencePrefix, s.o.Network)
	if err != nil {
		return out, err
	}
	for _, r := range recs {
		var k audienceKey
		if json.Unmarshal(r.Value, &k) == nil {
			out.Audiences = append(out.Audiences, AudienceRow{Audience: strings.TrimPrefix(r.Key, audiencePrefix), Since: k.Since})
		}
	}
	slices.SortFunc(out.Audiences, func(a, b AudienceRow) int { return strings.Compare(a.Audience, b.Audience) })
	return out, nil
}

func completed(data map[string]any) outcome {
	return outcome{Type: bus.TypeCompleted, Data: data}
}
