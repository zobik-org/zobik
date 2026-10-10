package signing

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Verified is an object whose signature and chain up to the root have been verified.
type Verified[T any] struct {
	Header Header
	Claims T
}

// The verifiers below admit only the forms of implementation §9.1 and reject any other.
// The chain travels in the object: root is the only key a verifier brings.

// VerifyRoleCertificate verifies a role_certificate, which the root signs.
func VerifyRoleCertificate(token, root string) (*Verified[RoleCertificate], error) {
	return verifyRootSigned[RoleCertificate](token, TypeRoleCertificate, root)
}

// VerifyOperatorProof verifies an operator_proof, which the root signs.
func VerifyOperatorProof(token, root string) (*Verified[OperatorProof], error) {
	return verifyRootSigned[OperatorProof](token, TypeOperatorProof, root)
}

// VerifyCapability verifies a capability credential: signed by the root for a
// structural node, or by a Spawner that presents its role_certificate.
func VerifyCapability(token, root string) (*Verified[Capability], error) {
	p, err := parse(token, TypeCapability)
	if err != nil {
		return nil, err
	}
	if p.header.SignerCredential == "" {
		if err := requireRoot(p, root); err != nil {
			return nil, err
		}
	} else if err := requireRoleCertificate(p, root, RoleSpawner); err != nil {
		return nil, err
	}
	return decode[Capability](p)
}

// VerifyScopeToken verifies a scope_token: signed by a Task Broker that presents its
// role_certificate, or by the channel that opens the trace, which presents its
// capability credential. A channel a Spawner accredited also needs the embedded
// proof of a node_provision that grants entry_topic (architecture §3.14.3).
func VerifyScopeToken(token, root string) (*Verified[ScopeToken], error) {
	p, err := parse(token, TypeScopeToken)
	if err != nil {
		return nil, err
	}
	switch peekType(p.header.SignerCredential) {
	case TypeRoleCertificate:
		if err := requireRoleCertificate(p, root, RoleTaskBroker); err != nil {
			return nil, err
		}
	case TypeCapability:
		if err := requireChannel(p, root); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("%w: scope_token signer", ErrForm)
	}
	return decode[ScopeToken](p)
}

func verifyRootSigned[T any](token string, typ Type, root string) (*Verified[T], error) {
	p, err := parse(token, typ)
	if err != nil {
		return nil, err
	}
	if err := requireRoot(p, root); err != nil {
		return nil, err
	}
	return decode[T](p)
}

func requireRoot(p *parsed, root string) error {
	if p.header.SignerCredential != "" {
		return fmt.Errorf("%w: %s signed with signer_credential", ErrForm, p.header.Typ)
	}
	if p.header.Kid != root {
		return fmt.Errorf("%w: %s not signed by the root", ErrUntrustedKey, p.header.Typ)
	}
	return nil
}

func requireRoleCertificate(p *parsed, root, role string) error {
	cert, err := VerifyRoleCertificate(p.header.SignerCredential, root)
	if err != nil {
		return fmt.Errorf("signer_credential: %w", err)
	}
	if cert.Claims.Role != role {
		return fmt.Errorf("%w: %s signed by role %q", ErrForm, p.header.Typ, cert.Claims.Role)
	}
	if cert.Claims.Sub != p.header.Kid {
		return fmt.Errorf("%w: role_certificate sub is not kid", ErrUntrustedKey)
	}
	return nil
}

func requireChannel(p *parsed, root string) error {
	cred, err := VerifyCapability(p.header.SignerCredential, root)
	if err != nil {
		return fmt.Errorf("signer_credential: %w", err)
	}
	if cred.Claims.Sub != p.header.Kid {
		return fmt.Errorf("%w: capability sub is not kid", ErrUntrustedKey)
	}
	if cred.Header.SignerCredential == "" {
		return nil // a structural channel, accredited by the root
	}
	proof, err := VerifyOperatorProof(cred.Claims.OperatorProof, root)
	if err != nil {
		return fmt.Errorf("operator_proof: %w", err)
	}
	if proof.Claims.Act != ActNodeProvision || proof.Claims.EntryTopic == "" {
		return fmt.Errorf("%w: channel proof grants no entry_topic", ErrForm)
	}
	return nil
}

// peekType reads the typ of an unverified token, only to choose which verifier
// it goes to; that verifier compares typ again before trusting anything.
func peekType(token string) Type {
	header, _, ok := strings.Cut(token, ".")
	if !ok {
		return ""
	}
	raw, err := b64.DecodeString(header)
	if err != nil {
		return ""
	}
	var h struct {
		Typ Type `json:"typ"`
	}
	if json.Unmarshal(raw, &h) != nil {
		return ""
	}
	return h.Typ
}

func decode[T any](p *parsed) (*Verified[T], error) {
	v := &Verified[T]{Header: p.header}
	if err := p.claims(&v.Claims); err != nil {
		return nil, err
	}
	return v, nil
}
