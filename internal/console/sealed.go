package console

import (
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/nats-io/nkeys"
	"golang.org/x/crypto/argon2"
	"golang.org/x/crypto/chacha20poly1305"
)

// A sealed key is an nkeys seed encrypted with a key derived from the operator's
// password with Argon2id: the root, and the console's own platform identity
// (implementation §6, The root and the split Operator Channel). The file is what
// the panel offers as a backup, so it carries everything needed to open it but the password.

const sealedFormat = "zobik-sealed-key-v1"

// kdfParams are the Argon2id parameters, kept in the file so they can be raised
// without breaking the keys sealed before.
type kdfParams struct {
	Format  string `json:"format"`
	Time    uint32 `json:"time"`
	Memory  uint32 `json:"memory_kib"`
	Threads uint8  `json:"threads"`
	Salt    []byte `json:"salt"`
}

type sealedKey struct {
	kdfParams
	Nonce      []byte `json:"nonce"`
	Ciphertext []byte `json:"ciphertext"`
}

// defaultParams is RFC 9106's second recommended option: 64 MiB, which a
// console sign-in pays once.
func defaultParams() (kdfParams, error) {
	p := kdfParams{Format: sealedFormat, Time: 3, Memory: 64 * 1024, Threads: 4, Salt: make([]byte, 16)}
	_, err := rand.Read(p.Salt)
	return p, err
}

const maxMemoryKiB = 1 << 20 // 1 GiB

var ErrWrongPassword = errors.New("console: wrong password or corrupted key")

// SealKey encrypts kp's seed with password.
func SealKey(kp nkeys.KeyPair, password []byte) ([]byte, error) {
	seed, err := kp.Seed()
	if err != nil {
		return nil, err
	}
	p, err := defaultParams()
	if err != nil {
		return nil, err
	}
	aead, ad, err := p.aead(password)
	if err != nil {
		return nil, err
	}
	s := sealedKey{kdfParams: p, Nonce: make([]byte, aead.NonceSize())}
	if _, err := rand.Read(s.Nonce); err != nil {
		return nil, err
	}
	s.Ciphertext = aead.Seal(nil, s.Nonce, seed, ad)
	return json.MarshalIndent(s, "", "  ")
}

// OpenKey decrypts a sealed key with password.
func OpenKey(sealed, password []byte) (nkeys.KeyPair, error) {
	var s sealedKey
	if err := json.Unmarshal(sealed, &s); err != nil {
		return nil, fmt.Errorf("console: sealed key: %w", err)
	}
	if s.Format != sealedFormat {
		return nil, fmt.Errorf("console: sealed key format %q", s.Format)
	}
	aead, ad, err := s.kdfParams.aead(password)
	if err != nil {
		return nil, err
	}
	if len(s.Nonce) != aead.NonceSize() {
		return nil, ErrWrongPassword
	}
	seed, err := aead.Open(nil, s.Nonce, s.Ciphertext, ad)
	if err != nil {
		return nil, ErrWrongPassword
	}
	return nkeys.FromSeed(seed)
}

// ResealKey re-encrypts a sealed key under a new password. The key does not
// change, so the network does not find out.
func ResealKey(sealed, oldPassword, newPassword []byte) ([]byte, error) {
	kp, err := OpenKey(sealed, oldPassword)
	if err != nil {
		return nil, err
	}
	return SealKey(kp, newPassword)
}

// aead derives the key from password and returns the cipher together with the
// associated data, which binds the parameters to the ciphertext.
func (p kdfParams) aead(password []byte) (cipher.AEAD, []byte, error) {
	// The upper bound keeps a crafted backup from exhausting the host's memory.
	if p.Time == 0 || p.Time > 64 || p.Memory == 0 || p.Memory > maxMemoryKiB || p.Threads == 0 || len(p.Salt) < 16 {
		return nil, nil, fmt.Errorf("console: sealed key: invalid kdf parameters")
	}
	ad, err := json.Marshal(p)
	if err != nil {
		return nil, nil, err
	}
	key := argon2.IDKey(password, p.Salt, p.Time, p.Memory, p.Threads, chacha20poly1305.KeySize)
	aead, err := chacha20poly1305.NewX(key)
	return aead, ad, err
}
