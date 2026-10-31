package egress

import (
	"slices"

	"zobik.org/zobik/internal/signing"
)

// Reach is what of an entry decides which nodes it serves: its scope and the
// interface it implements (architecture §3.16.3).
type Reach struct {
	GrantScope    string
	GranteeTopics []string
	// Interface is the digest of the entry's interface.
	Interface string
}

// Reach is the entry's.
func (e *Entry) Reach() Reach {
	return Reach{GrantScope: e.GrantScope, GranteeTopics: e.GranteeTopics, Interface: e.Interface.Digest}
}

// Serves reports whether the entry still serves the grant to the node the
// credential describes: it still implements the slot's interface and still
// reaches the node. It is step 2 of architecture §3.16.4, which the secrets
// role also applies (architecture §3.16.6).
func (r Reach) Serves(g signing.Grant, cred *signing.Capability, root string) bool {
	if g.Interface != r.Interface {
		return false
	}
	switch r.GrantScope {
	case "", ScopeOpen:
		return true
	case ScopeTopics:
		return slices.Contains(r.GranteeTopics, cred.Topic)
	case ScopeProvision:
		return provisioned(g, cred, root)
	}
	return false
}

// provisioned reports whether the credential carries the proof of a
// node_provision that associates g and names the running artifact_ref
// (implementation §9.2): a Spawner that lies about the grants cannot fabricate it.
func provisioned(g signing.Grant, cred *signing.Capability, root string) bool {
	proof, err := signing.VerifyOperatorProof(cred.OperatorProof, root)
	if err != nil {
		return false
	}
	p := proof.Claims
	return p.Act == signing.ActNodeProvision && p.ArtifactRef == cred.ArtifactRef &&
		slices.Contains(p.ProvisionGrants, g.Association())
}
