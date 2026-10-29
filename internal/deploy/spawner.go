package deploy

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/moby/moby/api/types/mount"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/egress"
	"zobik.org/zobik/internal/role"
	"zobik.org/zobik/internal/role/secrets"
)

// engineSocket is where the container engine's socket is, on the host and in
// the container that mounts it.
const engineSocket = "/var/run/docker.sock"

// SpawnerActSpec is an ephemeral container of image that runs one of the
// Spawner's acts with its material: the spawner role's volume at role.Dir, the
// container engine's socket, and the network's Docker network, where the Bus
// and the embedding endpoint are.
func SpawnerActSpec(dir, network, image string, cmd []string, files ...File) (Spec, error) {
	root, err := readRoot(dir)
	if err != nil {
		return Spec{}, err
	}
	if root == "" {
		return Spec{}, fmt.Errorf("deploy: %s holds no network: run zobik init", dir)
	}
	n := names{network, root}
	return Spec{
		Name:    n.container("spawner-act-" + uuid.NewString()[:8]),
		Network: n.dockerNetwork(),
		Labels:  n.labels(),
		Image:   image,
		Cmd:     cmd,
		Mounts: []mount.Mount{
			{Type: mount.TypeVolume, Source: n.volume("spawner"), Target: role.Dir},
			{Type: mount.TypeBind, Source: engineSocket, Target: engineSocket},
		},
		Files: files,
	}, nil
}

// SpawnerIssuer loads the Spawner's material from its volume at dir and
// connects to the Bus with its identity, waiting until ctx ends.
func SpawnerIssuer(ctx context.Context, dir string) (*Issuer, *nats.Conn, error) {
	rt, err := role.Load("spawner", dir)
	if err != nil {
		return nil, nil, err
	}
	seed, err := os.ReadFile(filepath.Join(dir, role.FileSigningKey))
	if err != nil {
		return nil, nil, fmt.Errorf("deploy: the spawner's signing key: %w", err)
	}
	key, err := nkeys.FromSeed([]byte(strings.TrimSpace(string(seed))))
	if err != nil {
		return nil, nil, fmt.Errorf("deploy: the spawner's signing key: %w", err)
	}
	account, err := rt.Account()
	if err != nil {
		return nil, nil, err
	}
	nc, err := rt.Connect(bus.URL)
	if err != nil {
		return nil, nil, err
	}
	for !nc.IsConnected() {
		select {
		case <-ctx.Done():
			nc.Close()
			return nil, nil, fmt.Errorf("deploy: connecting to the Bus: %w", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, nil, err
	}
	// The Spawner resolves the embedding model's credential and reads the rate
	// its embeddings are valued with (implementation §1.2.15, §8.5).
	sc, closeSecrets, err := secrets.Dial(role.AddrSecrets, rt.RPCCredentials(), "")
	if err != nil {
		nc.Close()
		return nil, nil, err
	}
	rates := func(name string) (*egress.Table, bool) {
		t, ok, err := egress.ReadRate(ctx, js, name)
		return t, ok && err == nil
	}
	return &Issuer{
		ID: rt.ID, Sign: rt.Sign, Certificate: rt.Certificate, SigningKey: key,
		Account: account.Subject, Root: rt.Root,
		Egress: &egress.Executor{HTTP: egress.HTTPClient(), Credential: sc.Resolve, Rates: rates},
		close:  closeSecrets,
	}, nc, nil
}

// Close closes the issuer's connection to the secrets role.
func (iss *Issuer) Close() {
	if iss.close != nil {
		_ = iss.close()
	}
}
