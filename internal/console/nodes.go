package console

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"zobik.org/zobik/internal/egress"
	"zobik.org/zobik/internal/signing"
)

// PathNodes is where the panel and the command line instantiate a Catalog
// entry: POST emits a node_provision (architecture §3.13.8).
const PathNodes = "/api/nodes"

// ErrNoEntry is an artifact_ref the Catalog has no entry for.
var ErrNoEntry = errors.New("console: the Catalog has no such entry")

// topicNodeProvision is the request that instantiates an entry (architecture §3.13.8).
const topicNodeProvision = "node_provision"

// provisionRequest is the body of a POST to PathNodes: the entry, by its
// artifact_ref, and the egress slots the person associates.
type provisionRequest struct {
	ArtifactRef        string                `json:"artifact_ref"`
	EgressAssociations []signing.Association `json:"egress_associations,omitempty"`
}

// NodeProvision opens a node_provision of the entry artifactRef, whose topic is
// topic. The proof carries in the clear the artifact_ref and the associations
// with provision entries of the registry in base (implementation §9.2). Its
// outcome arrives as a delivery over the session.
func (o *Operator) NodeProvision(ctx context.Context, base *Snapshot, artifactRef, topic string, associations []signing.Association) (*Opened, error) {
	registry := map[string]*egress.Entry{}
	if raw, ok := base.Values["egress_registry"]; ok {
		var err error
		if registry, err = egress.ParseRegistry(raw); err != nil {
			return nil, err
		}
	}
	proof := signing.OperatorProof{ArtifactRef: artifactRef}
	for _, a := range associations {
		if e := registry[a.Entry]; e != nil && e.GrantScope == egress.ScopeProvision {
			proof.ProvisionGrants = append(proof.ProvisionGrants, a)
		}
	}
	data := map[string]any{"target_topic": topic, "artifact_ref": artifactRef}
	if len(associations) > 0 {
		data["egress_associations"] = associations
	}
	return o.open(ctx, topicNodeProvision, data, proof, nil)
}

func (s *Service) nodeRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST "+PathNodes, s.withSession(true, s.postNode))
}

// postNode opens a node_provision. The console takes the target_topic from the
// entry, which it reads from the Catalog, so the person names only the entry.
func (s *Service) postNode(w http.ResponseWriter, r *http.Request, _ string) {
	var req provisionRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil || req.ArtifactRef == "" {
		writeError(w, http.StatusBadRequest, "the body names the entry's artifact_ref")
		return
	}
	if s.o.EntryTopic == nil {
		writeError(w, http.StatusServiceUnavailable, "this console reads no Catalog")
		return
	}
	topic, err := s.o.EntryTopic(r.Context(), req.ArtifactRef)
	switch {
	case errors.Is(err, ErrNoEntry):
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	case err != nil:
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	op, err := s.operator(r.Context())
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	snap, err := op.Snapshot(r.Context())
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	opened, err := op.NodeProvision(r.Context(), snap, req.ArtifactRef, topic, req.EgressAssociations)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, opened)
}

// NodeProvision opens a node_provision of the entry artifactRef. Its outcome
// arrives over the event stream.
func (c *Client) NodeProvision(ctx context.Context, artifactRef string, associations []signing.Association) (*Opened, error) {
	resp, err := c.do(ctx, http.MethodPost, PathNodes, provisionRequest{ArtifactRef: artifactRef, EgressAssociations: associations})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnprocessableEntity {
		var p Problem
		_ = json.NewDecoder(resp.Body).Decode(&p)
		return nil, &NotSentError{Reason: p.Error}
	}
	if err := problem(resp, http.StatusAccepted); err != nil {
		return nil, err
	}
	var o Opened
	return &o, json.NewDecoder(resp.Body).Decode(&o)
}
