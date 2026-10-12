// Package digest computes the digests of the boundary: sha256: followed by the
// hexadecimal SHA-256, over the JCS canonical form of a JSON document or over
// raw bytes (implementation §8.1).
package digest

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/cyberphone/json-canonicalization/go/src/webpki.org/jsoncanonicalizer"
)

// Bytes is the digest of b as it is.
func Bytes(b []byte) string {
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// JSON is the digest of doc's JCS canonical form (RFC 8785), so two
// serializations of the same document give the same digest.
func JSON(doc []byte) (string, error) {
	canonical, err := jsoncanonicalizer.Transform(doc)
	if err != nil {
		return "", err
	}
	return Bytes(canonical), nil
}

// requestOnly are the fields the data_digest of an operator proof leaves out:
// the proof itself and the Context Store references the channel adds after the
// signature (implementation §9.2).
var requestOnly = []string{"operator_proof", "context_ref", "bodies"}

// RequestData is the data_digest of a signed act's data (implementation §9.2).
func RequestData(data []byte) (string, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return "", err
	}
	for _, f := range requestOnly {
		delete(fields, f)
	}
	doc, err := json.Marshal(fields)
	if err != nil {
		return "", err
	}
	return JSON(doc)
}
