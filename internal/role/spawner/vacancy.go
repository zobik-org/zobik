package spawner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/claim"
	"zobik.org/zobik/internal/embedding"
	"zobik.org/zobik/internal/globalconfig"
	"zobik.org/zobik/internal/role/catalog"
	"zobik.org/zobik/internal/signing"
)

// The keys of the niche the vacancy writes (implementation §1.2.11).
const (
	nichePrefix     = "niche:"
	provisionPrefix = "provision:"
)

// niche is the record niche:<topic>: the successive-candidates counter and the
// capped mark (architecture §3.13.2, §3.13.5). The query that a capped niche
// holds enters with La contención, so the mark carries no TaskID yet.
type niche struct {
	SuccessiveCandidates int    `json:"successive_candidates"`
	Capped               bool   `json:"capped"`
	TaskID               string `json:"task_id,omitempty"`
}

// pendingFor is how long a sentinel proposal keeps its task from another one
// while no award answers it.
const pendingFor = time.Minute

// observeVacancy proposes with sentinel similarity on an announcement whose
// task already went vacancy_rounds_threshold rounds without a valid proposal
// (architecture §3.13.2). The Spawner does not intervene on a fixed-convention
// topic with a structural claimant (architecture §3.7.7), nor on a niche it
// capped. The hitl_contact_* family has its own close (closeVacantQuery).
func (s *spawner) observeVacancy(ctx context.Context, e *bus.Event, topic string, rounds int) error {
	var threshold int
	if err := json.Unmarshal(s.head.Load().Values["vacancy_rounds_threshold"], &threshold); err != nil {
		return fmt.Errorf("vacancy_rounds_threshold: %w", err)
	}
	if rounds < threshold || bus.StructuralTopics[topic] || strings.HasPrefix(topic, signing.HITLPrefix) {
		return nil
	}
	now := time.Now()
	s.vmu.Lock()
	if s.niches[topic].Capped || now.Before(s.pending[e.Subject]) {
		s.vmu.Unlock()
		return nil
	}
	c := s.vacants[topic]
	if c == nil {
		c = claim.New(s.rt.ID, s.rt.Credential, s.js, s.nc, topic)
		s.vacants[topic] = c
	}
	s.pending[e.Subject] = now.Add(pendingFor)
	s.vmu.Unlock()
	// A nil declared similarity is the lowest possible: any legitimate proposal
	// of the same round wins (architecture §3.13.2).
	if err := c.Propose(ctx, e, nil); err != nil {
		s.clearPending(e.Subject)
		return err
	}
	log.Printf("spawner: %s on %s vacant for %d rounds: proposing", e.Subject, topic, rounds)
	return nil
}

// vacancyAssigned takes a vacant task awarded to the Spawner, and
// vacancyRejected forgets one whose award went to another node.
func (s *spawner) vacancyAssigned(e *bus.Event) {
	s.vmu.Lock()
	defer s.vmu.Unlock()
	for topic, c := range s.vacants {
		a, ok := c.Assigned(e)
		if !ok {
			continue
		}
		select {
		case s.acts <- act{topic: topic, c: c, a: a, vacant: true}:
			s.pending[a.TaskID] = time.Now().Add(actTimeout)
		default:
			delete(s.pending, a.TaskID)
			log.Printf("spawner: %s left to its re-announcement: too many acts waiting", a.TaskID)
		}
		return
	}
}

func (s *spawner) vacancyRejected(e *bus.Event) {
	s.vmu.Lock()
	defer s.vmu.Unlock()
	for _, c := range s.vacants {
		c.Rejected(e)
	}
	delete(s.pending, e.Subject)
}

func (s *spawner) clearPending(taskID string) {
	s.vmu.Lock()
	delete(s.pending, taskID)
	s.vmu.Unlock()
}

// covered resets the niche of topic, which stopped being vacant: a node other
// than the Spawner was awarded one of its tasks (architecture §3.13.2).
func (s *spawner) covered(ctx context.Context, topic string) {
	s.vmu.Lock()
	_, known := s.niches[topic]
	delete(s.niches, topic)
	s.vmu.Unlock()
	if !known {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), opTimeout)
		defer cancel()
		cat, err := s.o.Catalog(ctx)
		if err != nil {
			return
		}
		if rev, ok, err := cat.Record(ctx, s.o.Network, nichePrefix+topic, nil); err == nil && ok {
			if err := cat.DeleteRecord(ctx, s.o.Network, nichePrefix+topic, rev); err != nil && !errors.Is(err, catalog.ErrPrecondition) {
				log.Printf("spawner: resetting the niche %s: %v", topic, err)
			}
		}
	}()
}

// loadNiches reads the niches the network has counted, so the observation of
// the stream resets and respects them after a restart.
func (s *spawner) loadNiches(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	cat, err := s.o.Catalog(ctx)
	if err != nil {
		return
	}
	records, err := cat.Records(ctx, nichePrefix, s.o.Network)
	if err != nil {
		log.Printf("spawner: reading the niches: %v", err)
		return
	}
	s.vmu.Lock()
	defer s.vmu.Unlock()
	for _, r := range records {
		var n niche
		if json.Unmarshal(r.Value, &n) == nil {
			s.niches[strings.TrimPrefix(r.Key, nichePrefix)] = n
		}
	}
}

// readNiche reads the niche of topic from the Catalog, which is the
// authoritative copy, and caches it.
func (s *spawner) readNiche(ctx context.Context, cat Catalog, topic string) (niche, error) {
	var n niche
	_, ok, err := cat.Record(ctx, s.o.Network, nichePrefix+topic, &n)
	if err != nil {
		return n, err
	}
	s.vmu.Lock()
	if ok {
		s.niches[topic] = n
	} else {
		delete(s.niches, topic)
	}
	s.vmu.Unlock()
	return n, nil
}

func (s *spawner) writeNiche(ctx context.Context, cat Catalog, topic string, n niche) error {
	if err := cat.PutRecord(ctx, s.o.Network, nichePrefix+topic, n, catalog.Write{}); err != nil {
		return err
	}
	s.vmu.Lock()
	s.niches[topic] = n
	s.vmu.Unlock()
	return nil
}

// vacancy runs the path of architecture §3.13.2 on a vacant task the Spawner
// was awarded, after the trigger and the hitl_contact_* filter. It does not
// renew the lease: a task it does not fail returns with its re-announcement,
// within reach of what it provisioned.
func (s *spawner) vacancy(ctx context.Context, x act) {
	a := x.a
	defer s.clearPending(a.TaskID)
	ctx, cancel := context.WithTimeout(ctx, actTimeout)
	defer cancel()
	if head, _, err := globalconfig.Head(ctx, s.js); err == nil {
		s.head.Store(head)
	}
	if err := s.resolveVacancy(ctx, x); err != nil {
		log.Printf("spawner: %s left to its re-announcement: %v", a.TaskID, err)
	}
}

func (s *spawner) resolveVacancy(ctx context.Context, x act) error {
	a, topic := x.a, x.topic
	var d struct {
		ModelID       string `json:"model_id"`
		TaskEmbedding string `json:"task_embedding"`
	}
	_ = json.Unmarshal(a.Announcement.Data, &d) // a field of another type reads as absent
	space, err := embedding.Resolve(s.head.Load().Values)
	if err != nil {
		return err
	}
	// Stale-space branch: it gates the others, which compare against the
	// task's embedding (architecture §3.13.2).
	if d.ModelID != space.ModelID {
		return s.failVacant(ctx, x, CodeStaleVectorSpace,
			fmt.Sprintf("the task's model_id %q is not the current embedding_model's %q", d.ModelID, space.ModelID), nil)
	}
	task, err := embedding.Decode(d.TaskEmbedding)
	if err != nil {
		return fmt.Errorf("task_embedding: %w", err)
	}
	cat, err := s.o.Catalog(ctx)
	if err != nil {
		return err
	}
	all, err := cat.Entries(ctx)
	if err != nil {
		return err
	}
	var entries []catalog.Entry
	for _, e := range all {
		if e.Topic == topic {
			entries = append(entries, e)
		}
	}
	fleet, err := s.o.Driver.Fleet(ctx)
	if err != nil {
		return err
	}

	// Is the topic redundant? Only for an emergent topic no entry claims and
	// the Topic Registry does not have (architecture §3.13.2).
	if emergent(topic) && len(entries) == 0 {
		_, registered, err := cat.Record(ctx, s.o.Network, topicPrefix+topic, nil)
		if err != nil {
			return err
		}
		if !registered {
			suggested, failed, err := s.redundant(ctx, cat, space.ModelID, task, topic, fleet)
			if err != nil {
				return err
			}
			if suggested != "" {
				return s.failVacant(ctx, x, CodeRedundantTopic,
					fmt.Sprintf("the topic %s is redundant with %s, which has nodes running", topic, suggested),
					map[string]any{"suggested_topic": suggested})
			}
			if len(failed) > 0 {
				s.abstain(ctx, cat, space.ModelID, failed)
				return fmt.Errorf("abstaining: the Topic Registry has stale rows that could not be regenerated")
			}
		}
	}

	// Does the needed node already exist? Among the entries that would have
	// proposed, the best one (architecture §3.13.2, implementation §4.3).
	candidates, failed, err := s.candidates(ctx, cat, space.ModelID, a.Announcement, entries, fleet)
	if err != nil {
		return err
	}
	if len(candidates) == 0 {
		if len(failed) > 0 {
			s.abstain(ctx, cat, space.ModelID, failed)
			return fmt.Errorf("abstaining: entries of %s could not be regenerated", topic)
		}
		// Without the Shared Catalog and the Forge, the niche stays vacant with
		// its count running (architecture §3.13.2, §6.1).
		return fmt.Errorf("no entry of the Local Catalog would have proposed on %s", topic)
	}

	var maxCandidates int
	if err := json.Unmarshal(s.head.Load().Values["max_successive_candidates"], &maxCandidates); err != nil {
		return fmt.Errorf("max_successive_candidates: %w", err)
	}
	n, err := s.readNiche(ctx, cat, topic)
	if err != nil {
		return err
	}
	if n.Capped {
		return fmt.Errorf("the niche %s is capped", topic)
	}
	if n.SuccessiveCandidates >= maxCandidates {
		n.Capped = true
		if err := s.writeNiche(ctx, cat, topic, n); err != nil {
			return err
		}
		return fmt.Errorf("the niche %s reached max_successive_candidates and is capped", topic)
	}

	// The write that arbitrates two instances that believe they own the topic
	// (architecture §3.13.3, §3.19).
	key := provisionPrefix + topic
	err = cat.PutRecord(ctx, s.o.Network, key, map[string]string{"task_id": a.TaskID}, catalog.Write{IfAbsent: true, Expires: actTimeout})
	if errors.Is(err, catalog.ErrPrecondition) {
		return fmt.Errorf("another instance is provisioning on %s", topic)
	}
	if err != nil {
		return err
	}
	defer func() {
		if rev, ok, err := cat.Record(ctx, s.o.Network, key, nil); err == nil && ok {
			_ = cat.DeleteRecord(ctx, s.o.Network, key, rev)
		}
	}()

	for _, c := range candidates {
		node, err := s.bringUp(ctx, cat, upRequest{entry: c.entry})
		var r *Refusal
		if errors.As(err, &r) {
			switch r.Code {
			case CodeCapacityExhausted:
				// Without room and without a free node, the vacancy waits
				// (architecture §3.13.10).
				return fmt.Errorf("%s waits for room: %s", topic, r.Reason)
			case CodeEgressSlotUnbound:
				// The query to the operator of §3.16.5 enters with La contención.
				return fmt.Errorf("%s stays vacant: %s", topic, r.Reason)
			}
			// A precondition the entry does not meet: the next candidate.
			log.Printf("spawner: %s: %s is not provisionable: %s", a.TaskID, c.entry.ArtifactRef, r.Reason)
			continue
		}
		if err != nil {
			return err
		}
		n.SuccessiveCandidates++
		if err := s.writeNiche(ctx, cat, topic, n); err != nil {
			log.Printf("spawner: counting the niche %s: %v", topic, err)
		}
		log.Printf("spawner: %s on %s vacant: provisioned %s as node %s", a.TaskID, topic, c.entry.ArtifactRef, node.ID)
		return nil
	}
	return fmt.Errorf("no candidate on %s is provisionable", topic)
}

// failVacant closes the vacant task with task.failed and its code: the
// requester's Integration Sidecar or Logic Container decides what follows
// (architecture §3.3).
func (s *spawner) failVacant(ctx context.Context, x act, code, reason string, extra map[string]any) error {
	data := map[string]any{"failure_reason": reason, "failure_code": code}
	for k, v := range extra {
		data[k] = v
	}
	ctx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	if err := x.c.Close(ctx, x.a, bus.TypeFailed, data); err != nil {
		return err
	}
	log.Printf("spawner: %s on %s failed: %s: %s", x.a.TaskID, x.topic, code, reason)
	return nil
}
