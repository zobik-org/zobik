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

// sessionLifetime bounds the console identity's JWT in a session, which lasts as
// long as a command (implementation §6, The service between the console and the channels).
const sessionLifetime = 30 * time.Minute

// Session is what a console command holds while it runs: the root and the
// console's platform identity, both opened with the operator's password
// (implementation §6, The root and the split Operator Channel).
type Session struct {
	Root    nkeys.KeyPair
	Console nkeys.KeyPair
	// ConsoleJWT is the console identity's user JWT, issued under the console's
	// scoped signing key; the channels authenticate it.
	ConsoleJWT string
	dir        string
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
	pub, err := consoleKey.PublicKey()
	if err != nil {
		return nil, err
	}
	token, err := bus.IssueUser(signingKey, account.Subject, pub, bus.ConsoleScope.Role, time.Now().Add(sessionLifetime))
	if err != nil {
		return nil, err
	}
	return &Session{Root: root, Console: consoleKey, ConsoleJWT: token, dir: dir}, nil
}

// ErrNoChannel is a channel the network does not run.
var ErrNoChannel = errors.New("deploy: the network runs no such channel")

// Channel returns the platform identity of the channel that unit runs and the
// address where it serves the console, on the host's 127.0.0.1.
func (s *Session) Channel(unit string) (id, addr string, err error) {
	b, err := os.ReadFile(filepath.Join(s.dir, dirIdentities, unit+".pub"))
	if errors.Is(err, os.ErrNotExist) {
		return "", "", ErrNoChannel
	}
	if err != nil {
		return "", "", err
	}
	port, err := os.ReadFile(filepath.Join(s.dir, dirPorts, unit))
	if err != nil {
		return "", "", fmt.Errorf("deploy: the port of %s: %w", unit, err)
	}
	return strings.TrimSpace(string(b)), "127.0.0.1:" + strings.TrimSpace(string(port)), nil
}
