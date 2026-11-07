package spawner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"slices"
	"strconv"
	"strings"
	"time"

	"zobik.org/zobik/internal/claim"
	"zobik.org/zobik/internal/embedding"
	"zobik.org/zobik/internal/role/catalog"
)

// The kinds of node_retire (architecture §3.13.8).
const (
	KindRemove = "remove"
	KindVeto   = "veto"
	KindUnveto = "unveto"
	KindPurge  = "purge"
)

// The prefixes of the qualified records retiring reads and writes
// (implementation §1.2.11).
const (
	floorPrefix       = "floor:"
	vetoPrefix        = "veto:"
	acquisitionPrefix = "acquisition:"
)

// retireRequest is what the Spawner reads of a node_retire.
type retireRequest struct {
	Kind          string `json:"kind"`
	NodeID        string `json:"node_id"`
	ArtifactRef   string `json:"artifact_ref"`
	TargetTopic   string `json:"target_topic"`
	OperatorProof string `json:"operator_proof"`
}

// retire validates a node_retire—its shape, the proof and the node or entry it
// names—and executes its kind (architecture §3.13.8).
func (s *spawner) retire(ctx context.Context, cat Catalog, a *claim.Assignment) (map[string]any, error) {
	ann := a.Announcement
	if err := ann.ValidateTopic(); err != nil {
		return nil, Refuse(CodeRequestInvalid, "%v", err)
	}
	var req retireRequest
	if err := json.Unmarshal(ann.Data, &req); err != nil {
		return nil, Refuse(CodeRequestInvalid, "data: %v", err)
	}
	if _, err := verifyProof(s.rt.Root, TopicRetire, a.TaskID, ann.Data, req.OperatorProof); err != nil {
		return nil, err
	}

	if req.Kind == KindRemove {
		fleet, err := s.o.Driver.Fleet(ctx)
		if err != nil {
			return nil, err
		}
		i := slices.IndexFunc(fleet, func(n Running) bool { return n.NodeID == req.NodeID })
		// A structural node is taken down through the deployment platform.
		if i < 0 {
			return nil, Refuse(CodeRequestInvalid, "%s is not a node this Spawner provisioned", req.NodeID)
		}
		// A target_topic other than the node's sends the request to a partition
		// that does not own the niche.
		if fleet[i].Topic != req.TargetTopic {
			return nil, Refuse(CodeRequestInvalid, "the node's topic is %s, not %s", fleet[i].Topic, req.TargetTopic)
		}
		ids, err := s.archive(ctx, cat, fleet[i:i+1], TriggerOperatorRemoval)
		return map[string]any{"node_ids": ids}, err
	}

	entry, ok, err := cat.Entry(ctx, req.ArtifactRef)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, Refuse(CodeRequestInvalid, "the Catalog has no entry %s", req.ArtifactRef)
	}
	if entry.Topic != req.TargetTopic {
		return nil, Refuse(CodeRequestInvalid, "the entry's topic is %s, not %s", entry.Topic, req.TargetTopic)
	}
	digest := entry.ArtifactRef
	switch req.Kind {
	case KindVeto:
		// The mark goes first: from then on no act brings the artifact up.
		if err := cat.PutRecord(ctx, s.o.Network, vetoPrefix+digest, map[string]any{}, catalog.Write{}); err != nil {
			return nil, err
		}
		fleet, err := s.o.Driver.Fleet(ctx)
		if err != nil {
			return nil, err
		}
		ids, err := s.archive(ctx, cat, running(fleet, digest), TriggerOperatorRemoval)
		return map[string]any{"node_ids": ids}, err
	case KindUnveto:
		rev, ok, err := cat.Record(ctx, s.o.Network, vetoPrefix+digest, nil)
		if ok {
			err = cat.DeleteRecord(ctx, s.o.Network, vetoPrefix+digest, rev)
		}
		return map[string]any{}, err
	case KindPurge:
		return map[string]any{}, s.purge(ctx, cat, digest)
	}
	return nil, Refuse(CodeRequestInvalid, "kind %q", req.Kind)
}

// purge deletes the artifact's image from the registry, only if no network of
// the tenant runs it: the deletion mark is written before the presence marks
// are read, as provisioning writes its presence before reading the deletion
// mark (architecture §3.13.8).
func (s *spawner) purge(ctx context.Context, cat Catalog, digest string) error {
	// A seeded artifact does not come back through any path of the network.
	acquisitions, err := cat.Records(ctx, acquisitionPrefix+digest, "")
	if err != nil {
		return err
	}
	for _, r := range acquisitions {
		var a struct {
			Provenance string `json:"provenance"`
		}
		if json.Unmarshal(r.Value, &a) == nil && a.Provenance == catalog.ProvenanceSeeded {
			return Refuse(CodeRequestInvalid, "the network %s has the artifact seeded", r.Network)
		}
	}
	if err := cat.SetDeletionMark(ctx, digest, true); err != nil {
		return err
	}
	presence, err := cat.Records(ctx, presencePrefix+digest, "")
	if err != nil {
		return err
	}
	if len(presence) > 0 {
		var networks []string
		for _, r := range presence {
			networks = append(networks, r.Network)
		}
		slices.Sort(networks)
		if err := cat.SetDeletionMark(ctx, digest, false); err != nil {
			return err
		}
		return Refuse(CodeRequestInvalid, "the artifact runs in %s", strings.Join(slices.Compact(networks), ", "))
	}
	if s.o.Registry == nil {
		return errors.New("the Spawner reaches no image registry")
	}
	return s.o.Registry.Delete(ctx, digest)
}

// floorRow is a row of the floor history (architecture §3.13.3), keyed by the
// entry and the floor. It carries no snapshot until the point form of
// node_query exists: the cheap half-failure of archiving.
type floorRow struct {
	TriggerReason string    `json:"trigger_reason"`
	ModelID       string    `json:"model_id"`
	ArchivedAt    time.Time `json:"archived_at"`
}

func floorKey(digest string, floor float64) string {
	return floorPrefix + digest + ":" + strconv.FormatFloat(floor, 'f', -1, 64)
}

// archive archives nodes with reason as their trigger_reason, in the order of architecture
// §3.13.3: it decommissions them and confirms it in the Active Node Registry,
// and only then writes the floor history. It removes the presence mark and the
// engine's image of each entry no instance of which remains, and returns the
// archived node_ids.
func (s *spawner) archive(ctx context.Context, cat Catalog, nodes []Running, reason string) ([]string, error) {
	ids := []string{}
	for _, n := range nodes {
		if err := s.o.Driver.Decommission(ctx, n); err != nil {
			return nil, fmt.Errorf("decommissioning %s: %w", n.NodeID, err)
		}
		ids = append(ids, n.NodeID)
	}
	fleet, err := s.o.Driver.Fleet(ctx)
	if err != nil {
		return nil, err
	}
	for _, n := range fleet {
		if slices.Contains(ids, n.NodeID) {
			return nil, fmt.Errorf("the node %s is still in the Active Node Registry", n.NodeID)
		}
	}

	now := time.Now().UTC()
	digests := map[string]bool{}
	for _, n := range nodes {
		row := floorRow{TriggerReason: reason, ModelID: n.ModelID, ArchivedAt: now}
		if err := cat.PutRecord(ctx, s.o.Network, floorKey(n.ArtifactRef, n.SimilarityFloor), row, catalog.Write{}); err != nil {
			return nil, err
		}
		digests[n.ArtifactRef] = true
	}
	for digest := range digests {
		if len(running(fleet, digest)) > 0 {
			continue
		}
		rev, ok, err := cat.Record(ctx, s.o.Network, presencePrefix+digest, nil)
		if err == nil && ok {
			err = cat.DeleteRecord(ctx, s.o.Network, presencePrefix+digest, rev)
		}
		if err != nil && !errors.Is(err, catalog.ErrPrecondition) {
			return nil, err
		}
		if err := s.o.Driver.DeleteImage(ctx, digest); err != nil {
			log.Printf("spawner: deleting the image of %s: %v", digest, err)
		}
	}
	return ids, nil
}

// running are the nodes of fleet that run digest.
func running(fleet []Running, digest string) []Running {
	var out []Running
	for _, n := range fleet {
		if n.ArtifactRef == digest {
			out = append(out, n)
		}
	}
	return out
}

// floor is the similarity_floor a new instance of digest competes with: that
// of an instance already running, or that of the most recent row of the floor
// history in the current vector space; nil without either, which is
// task_similarity_floor (architecture §3.13.3, implementation §4.6).
func (s *spawner) floor(ctx context.Context, cat Catalog, digest string) (*float64, error) {
	fleet, err := s.o.Driver.Fleet(ctx)
	if err != nil {
		return nil, err
	}
	if same := running(fleet, digest); len(same) > 0 {
		n := slices.MinFunc(same, func(a, b Running) int { return strings.Compare(a.NodeID, b.NodeID) })
		return &n.SimilarityFloor, nil
	}
	space, err := embedding.Resolve(s.head.Load().Values)
	if err != nil {
		// Without a vector space no row is eligible, and the act fails generating the embedding.
		return nil, nil
	}
	rows, err := cat.Records(ctx, floorPrefix+digest+":", s.o.Network)
	if err != nil {
		return nil, err
	}
	var best *float64
	var at time.Time
	for _, r := range rows {
		var row floorRow
		f, err := strconv.ParseFloat(strings.TrimPrefix(r.Key, floorPrefix+digest+":"), 64)
		if err != nil || json.Unmarshal(r.Value, &row) != nil || row.ModelID != space.ModelID {
			continue
		}
		if best == nil || row.ArchivedAt.After(at) {
			best, at = &f, row.ArchivedAt
		}
	}
	return best, nil
}
