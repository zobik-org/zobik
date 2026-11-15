package networkmonitor

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"zobik.org/zobik/internal/claim"
)

// pointRequest is what the point form of node_query names (architecture §3.12.3).
type pointRequest struct {
	NodeID      string `json:"node_id"`
	TargetTopic string `json:"target_topic"`
}

// rejection is a point query the role closes with request_invalid.
type rejection struct{ reason string }

func (r *rejection) Error() string { return r.reason }

// point answers the point form from the node's key, with the vector of
// architecture §3.12.2. A node_id outside the roster—where no structural node
// is—and a target_topic other than the node's are rejected (implementation
// §1.2.8). A node the roster does not hold yet may have been provisioned since
// the last change the role saw, so the roster is read again before rejecting.
func (m *monitor) point(ctx context.Context, a *claim.Assignment) (map[string]any, error) {
	var req pointRequest
	if err := json.Unmarshal(a.Announcement.Data, &req); err != nil {
		return nil, &rejection{fmt.Sprintf("data: %v", err)}
	}
	if req.TargetTopic == "" {
		return nil, &rejection{"the point form names the target_topic of the node"}
	}
	v, topic, ok := m.vector(req.NodeID)
	if !ok {
		if err := m.syncRoster(ctx, false); err != nil {
			return nil, err
		}
		v, topic, ok = m.vector(req.NodeID)
	}
	switch {
	case !ok:
		return nil, &rejection{fmt.Sprintf("%s is not a node the Spawner provisioned", req.NodeID)}
	case topic != req.TargetTopic:
		return nil, &rejection{fmt.Sprintf("the node's topic is %s, not %s", topic, req.TargetTopic)}
	}
	return map[string]any{"node_id": req.NodeID, "target_topic": topic, "signals": v}, nil
}

// vector computes the signals of a node of the roster, with its topic.
func (m *monitor) vector(id string) (Vector, string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	a, ok := m.st.nodes[id]
	if !ok {
		return Vector{}, "", false
	}
	return a.Vector(m.params, time.Now().UTC()), a.Topic, true
}
