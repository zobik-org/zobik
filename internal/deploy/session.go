package deploy

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"

	"zobik.org/zobik/internal/bus"
)

// Session is what a sign-in opens: the root and the console's platform
// identity, both with the operator's password (implementation §6, The root and
// the split Operator Channel).
type Session struct {
	Root    nkeys.KeyPair
	Console nkeys.KeyPair
	// signingKey is the console's scoped signing key, which lives in the clear
	// on the host, and account the account it signs under.
	signingKey nkeys.KeyPair
	account    string
}

// OpenSession opens the session with the operator's username and password.
func OpenSession(dir, user string, password []byte) (*Session, error) {
	root, err := OpenRoot(dir, user, password)
	if err != nil {
		return nil, err
	}
	consoleKey, err := openSealed(dir, fileConsole, password)
	if err != nil {
		return nil, err
	}
	seed, err := os.ReadFile(filepath.Join(dir, dirKeys, bus.ConsoleScope.Role+".nk"))
	if err != nil {
		return nil, err
	}
	signingKey, err := nkeys.FromSeed(seed)
	if err != nil {
		return nil, err
	}
	accountJWT, err := os.ReadFile(filepath.Join(dir, fileAccount))
	if err != nil {
		return nil, err
	}
	account, err := jwt.DecodeAccountClaims(string(accountJWT))
	if err != nil {
		return nil, err
	}
	return &Session{Root: root, Console: consoleKey, signingKey: signingKey, account: account.Subject}, nil
}

// ConsoleJWT issues the console identity's user JWT, valid until exp, under the
// console's scoped signing key; the channels authenticate it.
func (s *Session) ConsoleJWT(exp time.Time) (string, error) {
	pub, err := s.Console.PublicKey()
	if err != nil {
		return "", err
	}
	return bus.IssueUser(s.signingKey, s.account, pub, bus.ConsoleScope.Role, exp)
}

// Close wipes the keys the session opened.
func (s *Session) Close() {
	for _, kp := range []nkeys.KeyPair{s.Root, s.Console} {
		kp.Wipe()
	}
}

// ErrNoChannel is a channel the network does not run.
var ErrNoChannel = errors.New("deploy: the network runs no such channel")

// Channel returns the platform identity of the channel that unit runs and the
// address where it serves the console, on the host's 127.0.0.1.
func Channel(dir, unit string) (id, addr string, err error) {
	b, err := os.ReadFile(filepath.Join(dir, dirIdentities, unit+".pub"))
	if errors.Is(err, os.ErrNotExist) {
		return "", "", ErrNoChannel
	}
	if err != nil {
		return "", "", err
	}
	port, err := os.ReadFile(filepath.Join(dir, dirPorts, unit))
	if err != nil {
		return "", "", fmt.Errorf("deploy: the port of %s: %w", unit, err)
	}
	return strings.TrimSpace(string(b)), "127.0.0.1:" + strings.TrimSpace(string(port)), nil
}
