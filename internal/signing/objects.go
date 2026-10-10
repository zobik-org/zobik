package signing

// The fields of each signed object (implementation §9.2). None carries exp or nbf:
// no object expires. A verifier ignores the fields it does not know (implementation §9.3),
// which encoding/json already does.

// Grant associates a blueprint slot with a registry entry.
type Grant struct {
	Slot  string `json:"slot"`
	Entry string `json:"entry"`
}

// AttemptWindow has the shape of the act of registration (implementation §1.2.11).
// A nil limit is no limit, and a nil FinalRound is true.
type AttemptWindow struct {
	From       *int  `json:"from,omitempty"`
	To         *int  `json:"to,omitempty"`
	FinalRound *bool `json:"final_round,omitempty"`
}

// Capability is the capability credential, typ capability.
type Capability struct {
	Sub                 string         `json:"sub"`
	Iat                 int64          `json:"iat"`
	Topic               string         `json:"topic"`
	ArtifactRef         string         `json:"artifact_ref"`
	ModelID             string         `json:"model_id,omitempty"`
	CapabilityEmbedding string         `json:"capability_embedding,omitempty"`
	SimilarityFloor     float64        `json:"similarity_floor"`
	AttemptWindow       *AttemptWindow `json:"attempt_window,omitempty"`
	EgressGrants        []Grant        `json:"egress_grants,omitempty"`
	IngressGrants       []Grant        `json:"ingress_grants,omitempty"`
	OperatorProof       string         `json:"operator_proof,omitempty"`
}

// Role names a role_certificate may carry.
const (
	RoleSpawner    = "spawner"
	RoleTaskBroker = "task_broker"
)

// RoleCertificate is typ role_certificate.
type RoleCertificate struct {
	Sub  string `json:"sub"`
	Iat  int64  `json:"iat"`
	Role string `json:"role"`
}

// ActNodeProvision is the act whose proof a Spawner-signed channel credential embeds.
const ActNodeProvision = "node_provision"

// OperatorProof is typ operator_proof. It carries no sub: it is bound to an act.
type OperatorProof struct {
	Iat           int64  `json:"iat"`
	Act           string `json:"act"`
	TaskID        string `json:"task_id"`
	DataDigest    string `json:"data_digest"`
	PayloadDigest string `json:"payload_digest,omitempty"`
	// Only in a node_provision.
	ArtifactRef     string  `json:"artifact_ref,omitempty"`
	Audience        string  `json:"audience,omitempty"`
	EntryTopic      string  `json:"entry_topic,omitempty"`
	ProvisionGrants []Grant `json:"provision_grants,omitempty"`
}

// ScopeToken is typ scope_token.
type ScopeToken struct {
	Sub     string `json:"sub"`
	Iat     int64  `json:"iat"`
	TraceID string `json:"trace_id"`
	Section string `json:"section"`
}
