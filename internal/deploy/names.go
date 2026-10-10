package deploy

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/nats-io/jwt/v2"
)

// names derives the engine's names from the network's (implementation §6, The
// Docker network): the container name carries the network because Docker requires
// it to be unique on the host. Every resource also carries the root that owns it.
type names struct{ network, root string }

func (n names) dockerNetwork() string        { return "zobik-" + n.network }
func (n names) container(role string) string { return "zobik-" + n.network + "-" + role }
func (n names) volume(role string) string    { return "zobik-" + n.network + "-" + role }

// The labels of every resource of a network: its name, and the root's public key.
const (
	labelNetwork = "org.zobik.network"
	labelRoot    = "org.zobik.root"
)

func (n names) labels() map[string]string {
	return map[string]string{labelNetwork: n.network, labelRoot: n.root}
}

// readRoot returns the root's public key from the console's directory, which the
// operator's JWT carries in the clear, or "" if the directory has no root yet.
func readRoot(dir string) (string, error) {
	op, err := os.ReadFile(filepath.Join(dir, fileOperator))
	if errors.Is(err, fs.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	c, err := jwt.DecodeOperatorClaims(string(op))
	if err != nil {
		return "", err
	}
	return c.Subject, nil
}
