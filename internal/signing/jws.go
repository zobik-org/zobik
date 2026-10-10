package signing

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/nats-io/nkeys"
)

// Type is the object type the header carries in typ (implementation §9.1).
type Type string

const (
	TypeCapability      Type = "capability"
	TypeRoleCertificate Type = "role_certificate"
	TypeOperatorProof   Type = "operator_proof"
	TypeScopeToken      Type = "scope_token"
)

// algEdDSA is the only alg a verifier accepts (implementation §9.1).
const algEdDSA = "EdDSA"

// Header is the protected header of every signed object (implementation §9.1).
type Header struct {
	Alg string `json:"alg"`
	Typ Type   `json:"typ"`
	// Kid is the signer's public key, in the nkeys encoding.
	Kid string `json:"kid"`
	// SignerCredential is the compact JWS that accredits Kid; empty when Kid is the root.
	SignerCredential string `json:"signer_credential,omitempty"`
	// Crit is read only to reject it: no header parameter is critical here.
	Crit []string `json:"crit,omitempty"`
}

var (
	ErrMalformed    = errors.New("signing: malformed compact JWS")
	ErrAlg          = errors.New("signing: alg is not EdDSA")
	ErrType         = errors.New("signing: unexpected typ")
	ErrSignature    = errors.New("signing: invalid signature")
	ErrForm         = errors.New("signing: not an admitted signer form")
	ErrUntrustedKey = errors.New("signing: kid is not accredited")
)

var b64 = base64.RawURLEncoding

// Sign issues a compact JWS of type typ over claims, signed by kp. signerCredential
// is the object that accredits kp's public key, or empty when kp is the root.
func Sign(kp nkeys.KeyPair, typ Type, claims any, signerCredential string) (string, error) {
	kid, err := kp.PublicKey()
	if err != nil {
		return "", err
	}
	header, err := json.Marshal(Header{Alg: algEdDSA, Typ: typ, Kid: kid, SignerCredential: signerCredential})
	if err != nil {
		return "", err
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	input := b64.EncodeToString(header) + "." + b64.EncodeToString(payload)
	sig, err := kp.Sign([]byte(input))
	if err != nil {
		return "", err
	}
	return input + "." + b64.EncodeToString(sig), nil
}

// parsed is a compact JWS whose signature has been checked against its own kid.
type parsed struct {
	header  Header
	payload []byte
}

// parse decodes token, compares its typ with want before reading any field,
// and verifies the signature against the kid it names. It does not judge whether
// kid is trusted: that is the chain's job (verifyChain).
func parse(token string, want Type) (*parsed, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, ErrMalformed
	}
	rawHeader, err := b64.DecodeString(parts[0])
	if err != nil {
		return nil, ErrMalformed
	}
	var h Header
	if err := json.Unmarshal(rawHeader, &h); err != nil {
		return nil, ErrMalformed
	}
	if h.Alg != algEdDSA {
		return nil, ErrAlg
	}
	if len(h.Crit) > 0 {
		return nil, fmt.Errorf("%w: crit %v", ErrMalformed, h.Crit)
	}
	// implementation §9.1: typ is compared before reading the fields.
	if h.Typ != want {
		return nil, fmt.Errorf("%w: got %q, want %q", ErrType, h.Typ, want)
	}
	signer, err := nkeys.FromPublicKey(h.Kid)
	if err != nil {
		return nil, fmt.Errorf("%w: kid: %v", ErrMalformed, err)
	}
	sig, err := b64.DecodeString(parts[2])
	if err != nil {
		return nil, ErrMalformed
	}
	if err := signer.Verify([]byte(parts[0]+"."+parts[1]), sig); err != nil {
		return nil, ErrSignature
	}
	payload, err := b64.DecodeString(parts[1])
	if err != nil {
		return nil, ErrMalformed
	}
	return &parsed{header: h, payload: payload}, nil
}

func (p *parsed) claims(v any) error {
	if err := json.Unmarshal(p.payload, v); err != nil {
		return fmt.Errorf("%w: claims: %v", ErrMalformed, err)
	}
	return nil
}
