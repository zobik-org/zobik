package console

import (
	"context"
	"encoding/json"
	"net/http"
)

// PathQueries is where the panel and the command line open a query on the
// park: POST opens one on the topic its body names (architecture §3.17.1).
const PathQueries = "/api/queries"

// The queries on the park the console opens: the census of the fleet, the
// library and the Topic Registry (architecture §3.7.6, §3.12.3, §3.13.3).
const (
	TopicNodeQuery        = "node_query"
	TopicNodeCatalogQuery = "node_catalog_query"
	TopicTopicCatalog     = "topic_catalog_query"
)

var parkQueries = map[string]bool{TopicNodeQuery: true, TopicNodeCatalogQuery: true, TopicTopicCatalog: true}

// queryRequest is the body of a POST to PathQueries. A node_query without
// node_id is the census; the console does not open the point form until El
// ciclo de vida, which brings its answer.
type queryRequest struct {
	Topic string `json:"topic"`
}

func (s *Service) queryRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST "+PathQueries, s.withSession(true, s.postQuery))
}

func (s *Service) postQuery(w http.ResponseWriter, r *http.Request, _ string) {
	var req queryRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil || !parkQueries[req.Topic] {
		writeError(w, http.StatusBadRequest, "the body names the topic: node_query, node_catalog_query or topic_catalog_query")
		return
	}
	op, err := s.operator(r.Context())
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	opened, err := op.Query(r.Context(), req.Topic)
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
