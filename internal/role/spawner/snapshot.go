package spawner

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log"
	"time"

	"github.com/google/uuid"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/claim"
)

// TopicNodeQuery is the Network Monitor's query, whose point form returns a
// node's signal vector (architecture §3.12.3).
const TopicNodeQuery = "node_query"

// snapshotWait bounds how long archiving waits for the point query's close;
// without it, it archives without the snapshot (implementation §1.2.4).
const snapshotWait = 10 * time.Second

// snapshot asks the Network Monitor for the node's signal vector through the
// point form of node_query, before decommissioning it: the Network Monitor
// lets go of a node when it leaves the roster (implementation §1.2.4). The
// query is a subtask of the operator's request the Spawner executes, or the
// root of a trace of its own when request is nil. It returns nil if the query
// does not close with the vector within snapshotWait: the cheap half-failure
// of architecture §3.13.3.
func (s *spawner) snapshot(ctx context.Context, request *claim.Assignment, n Running) json.RawMessage {
	taskID := uuid.NewString()
	data := map[string]any{
		"topic": TopicNodeQuery, "requester_identity": s.rt.ID,
		"node_id": n.NodeID, "target_topic": n.Topic,
	}
	var traceParent, version string
	if request != nil {
		for k, v := range request.Inherited() {
			data[k] = v
		}
		traceParent, version = request.ChildTraceParent(), request.Assigned.ConfigVersion
	} else {
		trace, span := make([]byte, 16), make([]byte, 8)
		rand.Read(trace)
		rand.Read(span)
		traceParent, version = "00-"+hex.EncodeToString(trace)+"-"+hex.EncodeToString(span)+"-01", s.head.Load().ConfigVersion
	}
	e, err := bus.NewEvent(bus.TypeAnnounced, s.rt.ID, taskID, data)
	if err != nil {
		log.Printf("spawner: the snapshot of %s: %v", n.NodeID, err)
		return nil
	}
	e.TraceParent, e.ConfigVersion = traceParent, version
	if err := e.ValidateTopic(); err != nil {
		log.Printf("spawner: the snapshot of %s: %v", n.NodeID, err)
		return nil
	}

	closed := make(chan *bus.Event, 1)
	s.qmu.Lock()
	s.queries[taskID] = closed
	s.qmu.Unlock()
	defer func() {
		s.qmu.Lock()
		delete(s.queries, taskID)
		s.qmu.Unlock()
	}()
	ctx, cancel := context.WithTimeout(ctx, snapshotWait)
	defer cancel()
	if err := bus.Publish(ctx, s.js, bus.SubjectAnnounced(TopicNodeQuery), e); err != nil {
		log.Printf("spawner: the snapshot of %s: %v", n.NodeID, err)
		return nil
	}
	select {
	case c := <-closed:
		var d struct {
			Signals       json.RawMessage `json:"signals"`
			FailureReason string          `json:"failure_reason"`
		}
		_ = json.Unmarshal(c.Data, &d) // a field of another type reads as absent
		if c.Type != bus.TypeCompleted || c.ValidateTopic() != nil || d.Signals == nil {
			log.Printf("spawner: the snapshot of %s: the query closed %s: %s", n.NodeID, c.Type, d.FailureReason)
			return nil
		}
		return d.Signals
	case <-ctx.Done():
		log.Printf("spawner: the snapshot of %s: the query did not close within %v", n.NodeID, snapshotWait)
		return nil
	}
}

// queryClosed hands the close of a query the Spawner opened to whoever waits
// for it.
func (s *spawner) queryClosed(e *bus.Event) {
	s.qmu.Lock()
	closed := s.queries[e.Subject]
	s.qmu.Unlock()
	if closed != nil {
		select {
		case closed <- e:
		default:
		}
	}
}
