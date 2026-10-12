package config

import (
	"zobik.org/zobik/internal/digest"
	"zobik.org/zobik/internal/signing"
)

// verifyProof checks the operator's proof against the event: that the root
// signed it, and that it binds this act, this TaskID and this data
// (architecture §3.14.2, implementation §9.2).
func verifyProof(root, taskID string, data []byte, token string) (*signing.OperatorProof, *rejection) {
	proof, err := signing.VerifyOperatorProof(token, root)
	if err != nil {
		return nil, reject(codeProofInvalid, "operator_proof: %v", err)
	}
	p := &proof.Claims
	dataDigest, err := digest.RequestData(data)
	if err != nil {
		return nil, reject(codeRequestInvalid, "data: %v", err)
	}
	switch {
	case p.Act != Topic:
		return nil, reject(codeProofInvalid, "the proof is of act %q", p.Act)
	case p.TaskID != taskID:
		return nil, reject(codeProofInvalid, "the proof is of task %s", p.TaskID)
	case p.DataDigest != dataDigest:
		return nil, reject(codeProofInvalid, "the proof does not cover this data")
	}
	return p, nil
}

// checkPayloadDigest checks that the proof covers the bytes of the edit the
// request names (implementation §9.2).
func checkPayloadDigest(p *signing.OperatorProof, payload []byte) *rejection {
	if p.PayloadDigest != digest.Bytes(payload) {
		return reject(codeProofInvalid, "the proof does not cover the edit in context_ref")
	}
	return nil
}
