package deploy

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"

	"zobik.org/zobik/internal/bus"
)

// BusSpec is an ephemeral container of image on the network's Docker network,
// with the identity it connects with copied in at CredsPath. The name carries a
// random suffix, so several can run at once.
func BusSpec(network, purpose, image string, cmd []string, creds []byte, files ...File) Spec {
	n := names{network}
	return Spec{
		Name:    n.container(purpose + "-" + uuid.NewString()[:8]),
		Network: n.dockerNetwork(),
		NetName: network,
		Image:   image,
		Cmd:     cmd,
		Files:   append(files, File{Path: CredsPath, Mode: 0o600, Data: creds}),
	}
}

// EphemeralIdentity issues a fresh platform identity under role's scoped signing
// key. The scoped keys are in the clear in the console's directory, so it needs no session.
func EphemeralIdentity(dir, role string, lifetime time.Duration) ([]byte, error) {
	seed, err := os.ReadFile(filepath.Join(dir, dirKeys, role+".nk"))
	if err != nil {
		return nil, fmt.Errorf("deploy: the network has no %s scope: %w", role, err)
	}
	signer, err := nkeys.FromSeed(seed)
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
	user, err := nkeys.CreateUser()
	if err != nil {
		return nil, err
	}
	return identityCreds(user, func(pub string) (string, error) {
		return bus.IssueUser(signer, account.Subject, pub, role, time.Now().Add(lifetime))
	})
}
