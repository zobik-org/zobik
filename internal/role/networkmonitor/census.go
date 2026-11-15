package networkmonitor

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"slices"
	"strings"
	"time"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/claim"
	"zobik.org/zobik/internal/globalconfig"
)

// Topic is the fixed-convention topic the Network Monitor claims (architecture §3.12.3).
const Topic = "node_query"

// portion is where the census goes, in the query task's section
// (implementation §10.1).
const portion = "census"

// codeRequestInvalid closes a query whose announcement does not validate
// (implementation §10.1, §10.2).
const codeRequestInvalid = "request_invalid"

const opTimeout = 10 * time.Second

// Node is a row of the census: a node the Spawner provisioned, with the fields
// of the Active Node Registry (architecture §3.11, §3.12.3). Its fields are
// those of node_query.census.schema.json.
type Node struct {
	NodeID          string          `json:"node_id"`
	ArtifactRef     string          `json:"artifact_ref"`
	Topic           string          `json:"topic"`
	ModelID         string          `json:"model_id"`
	SignerID        string          `json:"signer_id"`
	SimilarityFloor float64         `json:"similarity_floor"`
	EgressGrants    json.RawMessage `json:"egress_grants"`
	IngressGrants   json.RawMessage `json:"ingress_grants"`
	MemoryRequired  int64           `json:"memory_required"`
}

// pointForm reports whether the query names a node.
func pointForm(e *bus.Event) bool {
	var d struct {
		NodeID string `json:"node_id"`
	}
	_ = json.Unmarshal(e.Data, &d) // a field of another type reads as absent
	return d.NodeID != ""
}

// answer closes the query (architecture §3.12.3): the census, written to the
// Context Store from the fleet, or the point form, with the node's signals in
// the close. A failure of the Bus, of the Context Store
// or of the engine leaves the query without closing: the lease expires and it
// comes back with its re-announcement.
func (m *monitor) answer(ctx context.Context, a *claim.Assignment) {
	if head, _, err := globalconfig.Head(ctx, m.js); err == nil {
		m.leaseTTL.Store(int64(head.Duration("task_lease_ttl")))
	}
	stop := m.claimant.KeepLease(a, func() time.Duration { return time.Duration(m.leaseTTL.Load()) })
	defer stop()
	opCtx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	if err := a.Announcement.ValidateTopic(); err != nil {
		if err := m.claimant.Fail(opCtx, a, codeRequestInvalid, err.Error()); err != nil {
			log.Printf("network_monitor: closing %s: %v", a.TaskID, err)
		}
		return
	}
	var data map[string]any
	var err error
	if pointForm(a.Announcement) {
		data, err = m.point(opCtx, a)
	} else {
		var ref map[string]any
		if ref, err = m.census(opCtx, a); err == nil {
			data = map[string]any{"context_ref": ref}
		}
	}
	var r *rejection
	if errors.As(err, &r) {
		if err := m.claimant.Fail(opCtx, a, codeRequestInvalid, r.reason); err != nil {
			log.Printf("network_monitor: closing %s: %v", a.TaskID, err)
		}
		return
	}
	if err == nil {
		err = m.claimant.Complete(opCtx, a, data)
	}
	if err != nil {
		log.Printf("network_monitor: %s left to its re-announcement: %v", a.TaskID, err)
	}
}

// census writes the fleet as the query's portion, in the order of its node_id.
func (m *monitor) census(ctx context.Context, a *claim.Assignment) (map[string]any, error) {
	nodes, err := m.o.Fleet(ctx)
	if err != nil {
		return nil, err
	}
	slices.SortFunc(nodes, func(x, y Node) int { return strings.Compare(x.NodeID, y.NodeID) })
	if nodes == nil {
		nodes = []Node{}
	}
	raw, err := json.Marshal(map[string]any{"nodes": nodes})
	if err != nil {
		return nil, err
	}
	return claim.Answer(ctx, m.context, a, portion, "node_query.census.schema.json", raw)
}
