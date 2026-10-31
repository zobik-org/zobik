package secrets

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"filippo.io/age"
)

// The Secret Store in the role's volume, which the console writes
// (implementation §1.2.18):
//
//	secrets.age     the handle→value map as a JSON object, encrypted whole with age
//	age.key         the age identity it is encrypted to
//	entry_node.pub  the Entry Node's platform identity, the only channel it serves
const (
	FileStore     = "secrets.age"
	FileKey       = "age.key"
	FileEntryNode = "entry_node.pub"
)

// NewKey generates the age identity the Secret Store is encrypted to.
func NewKey() (string, error) {
	id, err := age.GenerateX25519Identity()
	if err != nil {
		return "", err
	}
	return id.String(), nil
}

// Seal encrypts values whole to key.
func Seal(key string, values map[string][]byte) ([]byte, error) {
	id, err := age.ParseX25519Identity(strings.TrimSpace(key))
	if err != nil {
		return nil, err
	}
	plain, err := json.Marshal(values)
	if err != nil {
		return nil, err
	}
	var out bytes.Buffer
	w, err := age.Encrypt(&out, id.Recipient())
	if err != nil {
		return nil, err
	}
	if _, err := w.Write(plain); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

// Unseal decrypts a Secret Store with key; an empty one has no values.
func Unseal(key string, sealed []byte) (map[string][]byte, error) {
	values := map[string][]byte{}
	if len(sealed) == 0 {
		return values, nil
	}
	id, err := age.ParseX25519Identity(strings.TrimSpace(key))
	if err != nil {
		return nil, err
	}
	r, err := age.Decrypt(bytes.NewReader(sealed), id)
	if err != nil {
		return nil, fmt.Errorf("secrets: %w", err)
	}
	plain, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("secrets: %w", err)
	}
	if err := json.Unmarshal(plain, &values); err != nil {
		return nil, fmt.Errorf("secrets: %w", err)
	}
	return values, nil
}
