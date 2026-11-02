package spawner

import (
	"context"
	"errors"
	"fmt"

	"zobik.org/zobik/internal/role/catalog"
	"zobik.org/zobik/internal/signing"
)

// TopicProvision is the operator's request that instantiates a Catalog entry
// (architecture §3.13.8).
const TopicProvision = "node_provision"

// TopicRetire is the operator's request that removes a node, vetoes an
// artifact, lifts its veto or deletes its image (architecture §3.13.8).
const TopicRetire = "node_retire"

// TriggerOperatorRemoval is the trigger_reason of what the operator's request
// archives, a value the Network Monitor does not emit (architecture §3.13.8).
const TriggerOperatorRemoval = "operator_removal"

// The failure codes the Spawner closes its requests with (implementation §10.1).
const (
	CodeRequestInvalid       = "request_invalid"
	CodeProofInvalid         = "proof_invalid"
	CodeArtifactVetoed       = "artifact_vetoed"
	CodeAdmissionFailed      = "admission_failed"
	CodeRuntimeUnsupported   = "runtime_interface_unsupported"
	CodeEgressSlotUnbound    = "egress_slot_unbound"
	CodeIngressSlotUnbound   = "ingress_slot_unbound"
	CodeCapacityExhausted    = "capacity_exhausted"
	CodeEmbeddingUnavailable = "embedding_unavailable"
)

// Refusal is a precondition the act does not meet: the request closes with
// task.failed, its code and its reason (architecture §3.13.8).
type Refusal struct {
	Code, Reason string
}

func (r *Refusal) Error() string { return r.Code + ": " + r.Reason }

// Refuse is the Refusal with code and the formatted reason.
func Refuse(code, format string, a ...any) error {
	return &Refusal{Code: code, Reason: fmt.Sprintf(format, a...)}
}

// Provisioning is what one act of provisioning brings up (architecture §3.13.1).
type Provisioning struct {
	Topic string
	Entry *catalog.Entry
	// Associations are the egress slots the request associates, slot to
	// egress_registry entry (architecture §3.16.3).
	Associations map[string]string
	// Proof is the request's operator proof as it arrived, which the credential
	// embeds when a grant needs it, and ProvisionGrants what it carries in the
	// clear (implementation §9.2).
	Proof           string
	ProvisionGrants []signing.Association
	// MemoryMB is the entry's effective reservation in this network, or zero if
	// it has none yet (architecture §3.13.10).
	MemoryMB int64
	// Floor is the similarity_floor the node competes with, or nil for
	// task_similarity_floor (architecture §3.13.3).
	Floor *float64
}

// Node is a provisioned node: its platform identity, the vector it competes
// with and the reservation it was brought up with.
type Node struct {
	ID        string
	ModelID   string
	Embedding string
	MemoryMB  int64
}

// Running is a node of the fleet as the Active Node Registry lists it
// (architecture §3.11).
type Running struct {
	NodeID, Topic, ArtifactRef string
	ModelID                    string
	SimilarityFloor            float64
}

// Driver is the deployment platform the Spawner provisions on (architecture
// §3.14.1, implementation §1.2.4).
type Driver interface {
	// Provision executes the act, admission against the fleet's capacity
	// included. A precondition it does not meet is a *Refusal.
	Provision(ctx context.Context, p Provisioning) (*Node, error)
	// Fleet lists the nodes the Spawner provisioned on this network.
	Fleet(ctx context.Context) ([]Running, error)
	// Renew renews the node's platform identity (implementation §1.2.4).
	Renew(ctx context.Context, n Running) error
	// Decommission stops the node and removes it with its volumes; one already
	// gone is no error (implementation §1.2.4).
	Decommission(ctx context.Context, n Running) error
	// DeleteImage deletes the artifact's image from the engine; an image another
	// node still uses stays, and that is no error (implementation §1.2.4).
	DeleteImage(ctx context.Context, digest string) error
}

// Registry is the tenant's image registry, where purge deletes an artifact's
// image (implementation §1.2.16).
type Registry interface {
	// Delete deletes the manifest digest with its tags; one already gone is no error.
	Delete(ctx context.Context, digest string) error
}

// Catalog is what the Spawner reads and writes of the Specialty Catalog with
// its network's spawner credential (implementation §1.2.11).
type Catalog interface {
	Entry(ctx context.Context, digest string) (*catalog.Entry, bool, error)
	Status(ctx context.Context) (*catalog.Status, error)
	Record(ctx context.Context, network, key string, out any) (int64, bool, error)
	Records(ctx context.Context, prefix, network string) ([]catalog.Record, error)
	PutRecord(ctx context.Context, network, key string, value any, w catalog.Write) error
	DeleteRecord(ctx context.Context, network, key string, revision int64) error
	SetDeletionMark(ctx context.Context, digest string, on bool) error
}

// ErrNoCatalog is a network without Catalog credentials: it provisions nothing.
var ErrNoCatalog = errors.New("the network has no Catalog credentials")
