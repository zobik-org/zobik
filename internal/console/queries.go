package console

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
)

// PathQueries is where the panel and the command line open a query on the
// park: POST opens one on the topic its body names (architecture §3.17.1).
const PathQueries = "/api/queries"

// The queries on the park the console opens: the census of the fleet and the
// signals of one of its nodes, the library and the Topic Registry
// (architecture §3.7.6, §3.12.3, §3.13.3).
const (
	TopicNodeQuery        = "node_query"
	TopicNodeCatalogQuery = "node_catalog_query"
	TopicTopicCatalog     = "topic_catalog_query"
)

var parkQueries = map[string]bool{TopicNodeQuery: true, TopicNodeCatalogQuery: true, TopicTopicCatalog: true}

// queryRequest is the body of a POST to PathQueries. A node_query with
// node_id is the point form, whose target_topic the console reads from the
// Active Node Registry; without it, the census (architecture §3.12.3).
type queryRequest struct {
	Topic  string `json:"topic"`
	NodeID string `json:"node_id,omitempty"`
}

func (s *Service) queryRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST "+PathQueries, s.withSession(true, s.postQuery))
}

func (s *Service) postQuery(w http.ResponseWriter, r *http.Request, _ string) {
	var req queryRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil || !parkQueries[req.Topic] ||
		(req.NodeID != "" && req.Topic != TopicNodeQuery) {
		writeError(w, http.StatusBadRequest, "the body names the topic: node_query, node_catalog_query or topic_catalog_query, and node_query the node_id of its point form")
		return
	}
	var data map[string]any
	if req.NodeID != "" {
		if s.o.NodeTopic == nil {
			writeError(w, http.StatusServiceUnavailable, "this console cannot read the topic of a node")
			return
		}
		topic, err := s.o.NodeTopic(r.Context(), req.NodeID)
		switch {
		case errors.Is(err, ErrNoNode):
			writeError(w, http.StatusUnprocessableEntity, err.Error())
			return
		case err != nil:
			writeError(w, http.StatusBadGateway, err.Error())
			return
		}
		data = map[string]any{"node_id": req.NodeID, "target_topic": topic}
	}
	op, err := s.operator(r.Context())
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	opened, err := op.Query(r.Context(), req.Topic, data)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, opened)
}

// Query opens a query on topic. Its outcome arrives over the event stream.
func (c *Client) Query(ctx context.Context, topic string) (*Opened, error) {
	return c.openNodes(ctx, PathQueries, queryRequest{Topic: topic})
}

// NodeMetrics opens the point form of node_query on nodeID. Its outcome, with
// the node's signals, arrives over the event stream.
func (c *Client) NodeMetrics(ctx context.Context, nodeID string) (*Opened, error) {
	return c.openNodes(ctx, PathQueries, queryRequest{Topic: TopicNodeQuery, NodeID: nodeID})
}
