package deploy

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/console"
)

// The console's directory on the host, readable only by the user
// (implementation §6, The root and the split Operator Channel):
//
//	root.sealed      the root, encrypted with the password
//	system.sealed    the system account's key, which only acts of deployment use
//	console.sealed   the console's own platform identity
//	operator.jwt, system.jwt, account.jwt
//	keys/<role>.nk   the scoped signing keys, in the clear, so the console
//	                 renews identities without a session
//
// root.sealed is written last: its presence means the rest is complete.
const (
	fileRoot     = "root.sealed"
	fileSystem   = "system.sealed"
	fileConsole  = "console.sealed"
	fileOperator = "operator.jwt"
	fileSysJWT   = "system.jwt"
	fileAccount  = "account.jwt"
	dirKeys      = "keys"
)

// material is what the console holds with the session open.
type material struct {
	root    nkeys.KeyPair
	account *bus.Account
	console nkeys.KeyPair
}

// loadOrMint opens the material in dir with password, minting it the first time.
// It never mints the root again (implementation §6, Installation and zobik init):
// it only registers the scopes the account does not have yet.
func loadOrMint(dir, network string, password []byte, scopes []bus.Scope) (*material, error) {
	sealedRoot, err := os.ReadFile(filepath.Join(dir, fileRoot))
	if errors.Is(err, fs.ErrNotExist) {
		return mint(dir, network, password, scopes)
	}
	if err != nil {
		return nil, err
	}
	m := &material{account: &bus.Account{SigningKeys: map[string]nkeys.KeyPair{}}}
	if m.root, err = console.OpenKey(sealedRoot, password); err != nil {
		return nil, err
	}
	if m.account.SystemKey, err = openSealed(dir, fileSystem, password); err != nil {
		return nil, err
	}
	if m.console, err = openSealed(dir, fileConsole, password); err != nil {
		return nil, err
	}
	for name, dst := range map[string]*string{fileOperator: &m.account.Operator, fileSysJWT: &m.account.System, fileAccount: &m.account.JWT} {
		b, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return nil, err
		}
		*dst = string(b)
	}
	ac, err := jwt.DecodeAccountClaims(m.account.JWT)
	if err != nil {
		return nil, err
	}
	m.account.Public = ac.Subject
	if err := m.loadKeys(dir); err != nil {
		return nil, err
	}

	updated, added, err := bus.RegisterScopes(m.root, m.account.JWT, scopes)
	if err != nil {
		return nil, err
	}
	if len(added) > 0 {
		if err := writeKeys(dir, added); err != nil {
			return nil, err
		}
		if err := writeFile(dir, fileAccount, []byte(updated)); err != nil {
			return nil, err
		}
		m.account.JWT = updated
		for role, kp := range added {
			m.account.SigningKeys[role] = kp
		}
	}
	return m, nil
}

func mint(dir, network string, password []byte, scopes []bus.Scope) (*material, error) {
	if err := os.MkdirAll(filepath.Join(dir, dirKeys), 0o700); err != nil {
		return nil, err
	}
	root, err := nkeys.CreateOperator()
	if err != nil {
		return nil, err
	}
	account, err := bus.NewAccount(root, network, scopes)
	if err != nil {
		return nil, err
	}
	consoleKey, err := nkeys.CreateUser()
	if err != nil {
		return nil, err
	}
	if err := writeKeys(dir, account.SigningKeys); err != nil {
		return nil, err
	}
	for name, data := range map[string]string{fileOperator: account.Operator, fileSysJWT: account.System, fileAccount: account.JWT} {
		if err := writeFile(dir, name, []byte(data)); err != nil {
			return nil, err
		}
	}
	if err := writeSealed(dir, fileSystem, account.SystemKey, password); err != nil {
		return nil, err
	}
	if err := writeSealed(dir, fileConsole, consoleKey, password); err != nil {
		return nil, err
	}
	if err := writeSealed(dir, fileRoot, root, password); err != nil {
		return nil, err
	}
	return &material{root: root, account: account, console: consoleKey}, nil
}

func (m *material) loadKeys(dir string) error {
	entries, err := os.ReadDir(filepath.Join(dir, dirKeys))
	if err != nil {
		return err
	}
	for _, e := range entries {
		role, ok := strings.CutSuffix(e.Name(), ".nk")
		if !ok {
			continue
		}
		seed, err := os.ReadFile(filepath.Join(dir, dirKeys, e.Name()))
		if err != nil {
			return err
		}
		if m.account.SigningKeys[role], err = nkeys.FromSeed(seed); err != nil {
			return fmt.Errorf("deploy: key %s: %w", role, err)
		}
	}
	return nil
}

func writeKeys(dir string, keys map[string]nkeys.KeyPair) error {
	for role, kp := range keys {
		seed, err := kp.Seed()
		if err != nil {
			return err
		}
		if err := writeFile(dir, filepath.Join(dirKeys, role+".nk"), seed); err != nil {
			return err
		}
	}
	return nil
}

func writeSealed(dir, name string, kp nkeys.KeyPair, password []byte) error {
	sealed, err := console.SealKey(kp, password)
	if err != nil {
		return err
	}
	return writeFile(dir, name, sealed)
}

func openSealed(dir, name string, password []byte) (nkeys.KeyPair, error) {
	sealed, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		return nil, err
	}
	return console.OpenKey(sealed, password)
}

// writeFile writes through a temporary file, so an interrupted write leaves the
// previous content or none.
func writeFile(dir, name string, data []byte) error {
	path := filepath.Join(dir, name)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
