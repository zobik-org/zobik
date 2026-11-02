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

// PathNodeRetire is where they remove a node, veto an artifact, lift its veto
// or delete its image: POST emits a node_retire (architecture §3.13.8).
const PathNodeRetire = "/api/nodes/retire"

// ErrNoEntry is an artifact_ref the Catalog has no entry for, and ErrNoNode a
// node_id the network does not run.
var (
	ErrNoEntry = errors.New("console: the Catalog has no such entry")
	ErrNoNode  = errors.New("console: the network runs no such node")
)

// The requests on the fleet the console emits (architecture §3.13.8).
const (
	topicNodeProvision = "node_provision"
	topicNodeRetire    = "node_retire"
)

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

// RetireRequest is the body of a POST to PathNodeRetire: the kind, and the node
// a remove names or the entry the other kinds name.
type RetireRequest struct {
	Kind        string `json:"kind"`
	NodeID      string `json:"node_id,omitempty"`
	ArtifactRef string `json:"artifact_ref,omitempty"`
}

// NodeRetire opens a node_retire of req on topic, the topic of the node or of
// the entry it names. Its outcome arrives as a delivery over the session.
func (o *Operator) NodeRetire(ctx context.Context, req RetireRequest, topic string) (*Opened, error) {
	data := map[string]any{"kind": req.Kind, "target_topic": topic}
	if req.Kind == "remove" {
		data["node_id"] = req.NodeID
	} else {
		data["artifact_ref"] = req.ArtifactRef
	}
	return o.open(ctx, topicNodeRetire, data, signing.OperatorProof{}, nil)
}

func (s *Service) nodeRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST "+PathNodes, s.withSession(true, s.postNode))
	mux.HandleFunc("POST "+PathNodeRetire, s.withSession(true, s.postRetire))
}

// postRetire opens a node_retire. The console takes the target_topic from the
// node a remove names, which it reads from the Active Node Registry, or from
// the entry the other kinds name.
func (s *Service) postRetire(w http.ResponseWriter, r *http.Request, _ string) {
	var req RetireRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	const shape = "the body names a kind—remove, veto, unveto or purge—and the node_id or the artifact_ref it acts on"
	var lookup func(context.Context, string) (string, error)
	var name string
	switch req.Kind {
	case "remove":
		lookup, name = s.o.NodeTopic, req.NodeID
	case "veto", "unveto", "purge":
		lookup, name = s.o.EntryTopic, req.ArtifactRef
	}
	if name == "" {
		writeError(w, http.StatusBadRequest, shape)
		return
	}
	if lookup == nil {
		writeError(w, http.StatusServiceUnavailable, "this console cannot read the topic of what the request names")
		return
	}
	topic, err := lookup(r.Context(), name)
	switch {
	case errors.Is(err, ErrNoEntry), errors.Is(err, ErrNoNode):
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
	opened, err := op.NodeRetire(r.Context(), req, topic)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, opened)
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
	return c.openNodes(ctx, PathNodes, provisionRequest{ArtifactRef: artifactRef, EgressAssociations: associations})
}

// NodeRetire opens a node_retire of req. Its outcome arrives over the event
// stream.
func (c *Client) NodeRetire(ctx context.Context, req RetireRequest) (*Opened, error) {
	return c.openNodes(ctx, PathNodeRetire, req)
}

func (c *Client) openNodes(ctx context.Context, path string, body any) (*Opened, error) {
	resp, err := c.do(ctx, http.MethodPost, path, body)
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
