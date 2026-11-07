package spawner

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"slices"
	"strings"
	"time"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/claim"
	"zobik.org/zobik/internal/embedding"
	"zobik.org/zobik/internal/globalconfig"
	"zobik.org/zobik/internal/role/catalog"
	"zobik.org/zobik/internal/signing"
)

// The prefixes and keys of the qualified records the vacancy reads and writes
// (implementation §1.2.11).
const (
	embeddingPrefix = "embedding:"
	topicPrefix     = "topic:"
	keyVectorSpace  = "vector_space"
)

// The trigger_reason values that keep an entry out of what vacancy chooses
// (architecture §3.13.2).
const (
	triggerNoticeProvenance = "notice_provenance"
	triggerMemoryExceeded   = "memory_exceeded"
)

// vectorRecord is an entry's competition triple in the network's space, without
// the text, which the entry carries (architecture §3.7.3).
type vectorRecord struct {
	Embedding string `json:"embedding"`
	ModelID   string `json:"model_id"`
}

// topicRow is a row of the Topic Registry (architecture §3.7.6).
type topicRow struct {
	Topic       string `json:"topic"`
	Embedding   string `json:"embedding"`
	ModelID     string `json:"model_id"`
	Description string `json:"description"`
}

// errGeneration is an embedding the model did not generate: the entry is
// discarded from the search, and the discard may make the Spawner abstain
// (architecture §3.13.3).
var errGeneration = errors.New("the embedding could not be generated")

// emergent reports whether topic is an emergent one, the population topic
// governance covers (architecture §3.7.6, §3.7.7).
func emergent(topic string) bool {
	return topic != bus.TopicIntake && !bus.StructuralTopics[topic] && !strings.HasPrefix(topic, signing.HITLPrefix)
}

// document generates the embedding of text in the current space, which must be
// modelID: a configuration that changed meanwhile fails the generation.
func (s *spawner) document(ctx context.Context, modelID, text string) (string, error) {
	got, v, err := s.o.Driver.Document(ctx, text)
	if err != nil {
		return "", fmt.Errorf("%w: %w", errGeneration, err)
	}
	if got != modelID {
		return "", fmt.Errorf("%w: the space changed to %s", errGeneration, got)
	}
	return v, nil
}

// entryVector is the entry's vector in the space modelID, regenerated from its
// capability_text if the network's row is missing or of another space
// (architecture §3.13.3).
func (s *spawner) entryVector(ctx context.Context, cat Catalog, modelID string, e *catalog.Entry) (string, error) {
	var r vectorRecord
	if _, ok, err := cat.Record(ctx, s.o.Network, embeddingPrefix+e.ArtifactRef, &r); err != nil {
		return "", err
	} else if ok && r.ModelID == modelID {
		return r.Embedding, nil
	}
	s.triggerSweep()
	v, err := s.document(ctx, modelID, e.CapabilityText)
	if err != nil {
		return "", err
	}
	err = cat.PutRecord(ctx, s.o.Network, embeddingPrefix+e.ArtifactRef, vectorRecord{Embedding: v, ModelID: modelID}, catalog.Write{})
	return v, err
}

// topicRows are the Topic Registry's rows in the space modelID; those of
// another space are regenerated from their description, and the names of
// those that could not be are returned apart (architecture §3.13.3).
func (s *spawner) topicRows(ctx context.Context, cat Catalog, modelID string) (rows []topicRow, failed []string, err error) {
	records, err := cat.Records(ctx, topicPrefix, s.o.Network)
	if err != nil {
		return nil, nil, err
	}
	for _, rec := range records {
		var row topicRow
		if json.Unmarshal(rec.Value, &row) != nil {
			continue
		}
		if row.ModelID != modelID {
			s.triggerSweep()
			v, err := s.document(ctx, modelID, row.Description)
			if errors.Is(err, errGeneration) {
				failed = append(failed, topicPrefix+row.Topic)
				continue
			}
			if err != nil {
				return nil, nil, err
			}
			row.Embedding, row.ModelID = v, modelID
			if err := cat.PutRecord(ctx, s.o.Network, rec.Key, row, catalog.Write{}); err != nil {
				return nil, nil, err
			}
		}
		rows = append(rows, row)
	}
	return rows, failed, nil
}

// registerTopic registers an emergent topic the Topic Registry does not have,
// with the vector and the capability_text of the entry whose node the act
// brought up (architecture §3.7.6).
func (s *spawner) registerTopic(ctx context.Context, cat Catalog, topic string, node *Node, text string) error {
	if !emergent(topic) {
		return nil
	}
	row := topicRow{Topic: topic, Embedding: node.Embedding, ModelID: node.ModelID, Description: text}
	err := cat.PutRecord(ctx, s.o.Network, topicPrefix+topic, row, catalog.Write{IfAbsent: true})
	if errors.Is(err, catalog.ErrPrecondition) {
		return nil
	}
	return err
}

// redundant searches the Topic Registry for a topic with nodes running whose
// purpose the task's embedding reaches at topic_similarity_floor (architecture
// §3.13.2). failed names the stale rows that could not be regenerated, whose
// discard may have hidden the neighbor.
func (s *spawner) redundant(ctx context.Context, cat Catalog, modelID string, task []float32, topic string, fleet []Running) (suggested string, failed []string, err error) {
	var floor float64
	if err := json.Unmarshal(s.head.Load().Values["topic_similarity_floor"], &floor); err != nil {
		return "", nil, fmt.Errorf("topic_similarity_floor: %w", err)
	}
	rows, failed, err := s.topicRows(ctx, cat, modelID)
	if err != nil {
		return "", nil, err
	}
	covered := map[string]bool{}
	for _, n := range fleet {
		covered[n.Topic] = true
	}
	best := -2.0
	for _, row := range rows {
		if row.Topic == topic || !covered[row.Topic] {
			continue
		}
		v, err := embedding.Decode(row.Embedding)
		if err != nil {
			continue
		}
		sim, err := embedding.Similarity(task, v)
		if err != nil || sim < floor {
			continue
		}
		if sim > best || (sim == best && row.Topic < suggested) {
			best, suggested = sim, row.Topic
		}
	}
	return suggested, failed, nil
}

// candidate is a Local Catalog entry that would have proposed for the vacant task.
type candidate struct {
	entry      catalog.Entry
	similarity float64
	running    bool
}

// candidates are the entries of the task's topic that would have proposed for
// it, with the floor they would be provisioned with (architecture §3.13.2), in
// the order the decision strategy chooses them (implementation §4.3). Without
// live signals or snapshots no entry is evaluable: the one that runs goes
// first, then the closest to the task. failed names the entries whose vector
// could not be generated.
func (s *spawner) candidates(ctx context.Context, cat Catalog, modelID string, ann *bus.Event, entries []catalog.Entry, fleet []Running) (out []candidate, failed []string, err error) {
	status, err := cat.Status(ctx)
	if err != nil {
		return nil, nil, err
	}
	decimals, err := s.decimals(ctx, ann.ConfigVersion)
	if err != nil {
		return nil, nil, err
	}
	var taskFloor float64
	if err := json.Unmarshal(s.head.Load().Values["task_similarity_floor"], &taskFloor); err != nil {
		return nil, nil, fmt.Errorf("task_similarity_floor: %w", err)
	}
	round := claim.ReadRound(ann)
	for _, e := range entries {
		if e.DeletionMark || e.ScanResult.Verdict != "pass" || e.ScanResult.RulesetVersion != status.RulesetVersion ||
			!claim.Admits(e.AttemptWindow, round) {
			continue
		}
		if _, vetoed, err := cat.Record(ctx, s.o.Network, vetoPrefix+e.ArtifactRef, nil); err != nil {
			return nil, nil, err
		} else if vetoed {
			continue
		}
		if excluded, err := s.excluded(ctx, cat, e.ArtifactRef); err != nil {
			return nil, nil, err
		} else if excluded {
			continue
		}
		v, err := s.entryVector(ctx, cat, modelID, &e)
		if errors.Is(err, errGeneration) {
			failed = append(failed, e.ArtifactRef)
			continue
		}
		if err != nil {
			return nil, nil, err
		}
		floor, err := s.floor(ctx, cat, e.ArtifactRef)
		if err != nil {
			return nil, nil, err
		}
		if floor == nil {
			floor = &taskFloor
		}
		cred := &signing.Capability{ModelID: modelID, CapabilityEmbedding: v, SimilarityFloor: *floor}
		sim, admissible := claim.Similarity(cred, ann, decimals)
		if !admissible {
			continue
		}
		out = append(out, candidate{entry: e, similarity: sim, running: len(running(fleet, e.ArtifactRef)) > 0})
	}
	slices.SortFunc(out, func(a, b candidate) int {
		if a.running != b.running {
			if a.running {
				return -1
			}
			return 1
		}
		if c := cmp.Compare(b.similarity, a.similarity); c != 0 {
			return c
		}
		return strings.Compare(a.entry.ArtifactRef, b.entry.ArtifactRef)
	})
	return out, failed, nil
}

// excluded reports whether the entry's floor history records an archiving for
// notice_provenance or memory_exceeded: bringing it up again is an operator's
// node_provision (architecture §3.13.2).
func (s *spawner) excluded(ctx context.Context, cat Catalog, digest string) (bool, error) {
	rows, err := cat.Records(ctx, floorPrefix+digest+":", s.o.Network)
	if err != nil {
		return false, err
	}
	for _, r := range rows {
		var row floorRow
		if json.Unmarshal(r.Value, &row) == nil && (row.TriggerReason == triggerNoticeProvenance || row.TriggerReason == triggerMemoryExceeded) {
			return true, nil
		}
	}
	return false, nil
}

// decimals is similarity_precision_decimals of the snapshot the announcement
// pinned, as the Task Broker reads it (architecture §2.3, §3.7.5).
func (s *spawner) decimals(ctx context.Context, version string) (int, error) {
	s.mu.Lock()
	d, ok := s.pinned[version]
	s.mu.Unlock()
	if ok {
		return d, nil
	}
	rec, err := globalconfig.Version(ctx, s.js, version)
	if err != nil {
		return 0, err
	}
	if err := json.Unmarshal(rec.Values["similarity_precision_decimals"], &d); err != nil {
		return 0, fmt.Errorf("similarity_precision_decimals of %s: %w", version, err)
	}
	s.mu.Lock()
	s.pinned[version] = d
	s.mu.Unlock()
	return d, nil
}

// vectorSpace is the irreparable-vector-space key: what is pending, under
// which model and since when (architecture §3.13.7).
type vectorSpace struct {
	ModelID string    `json:"model_id"`
	Pending []string  `json:"pending"`
	Since   time.Time `json:"since"`
}

// abstain stamps the irreparable-vector-space key with what could not be
// regenerated, keeping the moment the condition started (architecture §3.13.7).
// The query to the operator and the notice enter with La contención.
func (s *spawner) abstain(ctx context.Context, cat Catalog, modelID string, pending []string) {
	key := vectorSpace{ModelID: modelID, Pending: pending, Since: time.Now().UTC()}
	var prev vectorSpace
	if _, ok, err := cat.Record(ctx, s.o.Network, keyVectorSpace, &prev); err == nil && ok && prev.ModelID == modelID {
		key.Since = prev.Since
	}
	if err := cat.PutRecord(ctx, s.o.Network, keyVectorSpace, key, catalog.Write{}); err != nil {
		log.Printf("spawner: stamping the vector space key: %v", err)
	}
}

// triggerSweep asks for the regeneration of every stale vector in the background.
func (s *spawner) triggerSweep() {
	select {
	case s.sweep <- struct{}{}:
	default:
	}
}

// serveSweep regenerates every entry and Topic Registry row whose vector is
// missing or of another space, retrying by embedding_regeneration_backoff, and
// deletes the vector space key once the space is consistent (architecture
// §3.13.7). Regenerating is idempotent, so it needs no coordination.
func (s *spawner) serveSweep(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.sweep:
		}
		var backoff struct {
			Initial  int64 `json:"initial"`
			Attempts int   `json:"attempts"`
		}
		if err := json.Unmarshal(s.head.Load().Values["embedding_regeneration_backoff"], &backoff); err != nil {
			log.Printf("spawner: sweep: embedding_regeneration_backoff: %v", err)
			continue
		}
		wait := time.Duration(backoff.Initial) * time.Millisecond
		for attempt := 1; ; attempt++ {
			pending, err := s.sweepOnce(ctx)
			if err == nil && len(pending) == 0 {
				break
			}
			if err != nil {
				log.Printf("spawner: sweep: %v", err)
			}
			if attempt >= backoff.Attempts {
				log.Printf("spawner: sweep: %d vectors still stale", len(pending))
				break
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(wait):
			}
			wait *= 2
		}
		// What the pass itself asked for is the pass just run.
		select {
		case <-s.sweep:
		default:
		}
	}
}

// sweepOnce runs one pass of the sweep and returns what is still pending.
func (s *spawner) sweepOnce(ctx context.Context) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, actTimeout)
	defer cancel()
	cat, err := s.o.Catalog(ctx)
	if err != nil {
		return nil, err
	}
	space, err := embedding.Resolve(s.head.Load().Values)
	if err != nil {
		return nil, err
	}
	entries, err := cat.Entries(ctx)
	if err != nil {
		return nil, err
	}
	var pending []string
	for i := range entries {
		if _, err := s.entryVector(ctx, cat, space.ModelID, &entries[i]); errors.Is(err, errGeneration) {
			pending = append(pending, entries[i].ArtifactRef)
		} else if err != nil {
			return nil, err
		}
	}
	_, failed, err := s.topicRows(ctx, cat, space.ModelID)
	if err != nil {
		return nil, err
	}
	pending = append(pending, failed...)
	if len(pending) > 0 {
		return pending, nil
	}
	if rev, ok, err := cat.Record(ctx, s.o.Network, keyVectorSpace, nil); err == nil && ok {
		if err := cat.DeleteRecord(ctx, s.o.Network, keyVectorSpace, rev); err != nil && !errors.Is(err, catalog.ErrPrecondition) {
			return nil, err
		}
	}
	return nil, nil
}
