package spawner

import (
	"zobik.org/zobik/internal/digest"
	"zobik.org/zobik/internal/signing"
)

// verifyProof checks the operator's proof against the event: that the root
// signed it, and that it binds the act of the topic act, this TaskID and this
// data (architecture §3.14.2, implementation §9.2).
func verifyProof(root, act, taskID string, data []byte, token string) (*signing.OperatorProof, error) {
	proof, err := signing.VerifyOperatorProof(token, root)
	if err != nil {
		return nil, Refuse(CodeProofInvalid, "operator_proof: %v", err)
	}
	p := &proof.Claims
	dataDigest, err := digest.RequestData(data)
	if err != nil {
		return nil, Refuse(CodeRequestInvalid, "data: %v", err)
	}
	switch {
	case p.Act != act:
		return nil, Refuse(CodeProofInvalid, "the proof is of act %q", p.Act)
	case p.TaskID != taskID:
		return nil, Refuse(CodeProofInvalid, "the proof is of task %s", p.TaskID)
	case p.DataDigest != dataDigest:
		return nil, Refuse(CodeProofInvalid, "the proof does not cover this data")
	}
	return p, nil
}
