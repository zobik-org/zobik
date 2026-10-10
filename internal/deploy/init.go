package deploy

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nkeys"

	"zobik.org/zobik/internal/bus"
)

// Options are the inputs of zobik init.
type Options struct {
	// Network names the network; it prefixes every engine resource.
	Network string
	// Dir is the console's directory for this network on the host.
	Dir string
	// User and Password are the only thing zobik init asks the person
	// (implementation §6, The root and the split Operator Channel).
	User     string
	Password []byte
	// Image is the zobik image the roles and the ephemeral acts run.
	Image string
	// Scopes are the roles the account issues for.
	Scopes []bus.Scope
}

// actIdentityLifetime bounds the identity an ephemeral act connects with: it
// lives as long as the act.
const actIdentityLifetime = 10 * time.Minute

// Init brings up what exists of the network, and converges: it creates what is
// missing and leaves what exists (implementation §6, Installation and zobik init).
func Init(ctx context.Context, e *Engine, o Options) error {
	// What the engine holds under this name has to be this root's before
	// anything is minted or touched.
	root, err := readRoot(o.Dir)
	if err != nil {
		return err
	}
	if err := e.CheckOwner(ctx, names{o.Network, root}); err != nil {
		return err
	}
	m, err := loadOrMint(o.Dir, o.Network, o.User, o.Password, o.Scopes)
	if err != nil {
		return err
	}
	if root, err = m.root.PublicKey(); err != nil {
		return err
	}
	n := names{o.Network, root}

	if err := e.EnsureNetwork(ctx, n.dockerNetwork(), n.labels()); err != nil {
		return fmt.Errorf("deploy: network: %w", err)
	}
	if err := e.EnsureImage(ctx, NATSImage); err != nil {
		return fmt.Errorf("deploy: NATS image: %w", err)
	}
	if _, err := e.EnsureVolume(ctx, n.volume("nats"), n.labels()); err != nil {
		return fmt.Errorf("deploy: NATS volume: %w", err)
	}
	config, err := natsConfig(natsDataDir, m.account.Operator, m.account.System, m.account.JWT)
	if err != nil {
		return err
	}
	if err := e.EnsureRunning(ctx, natsSpec(n, config)); err != nil {
		return fmt.Errorf("deploy: NATS server: %w", err)
	}
	if err := e.EnsureImage(ctx, o.Image); err != nil {
		return fmt.Errorf("deploy: zobik image %s: %w", o.Image, err)
	}
	if err := ensureRoles(ctx, e, n, m, o.Dir, o.Image); err != nil {
		return err
	}

	// The server may hold an older account, from before a scope was registered.
	sysCreds, err := m.systemCreds()
	if err != nil {
		return err
	}
	if err := runAct(ctx, e, n, o.Image, actAccount, sysCreds, File{Path: actAccountPath, Mode: 0o600, Data: []byte(m.account.JWT)}); err != nil {
		return err
	}

	consoleCreds, err := m.consoleCreds()
	if err != nil {
		return err
	}
	return runAct(ctx, e, n, o.Image, actStreams, consoleCreds)
}

func runAct(ctx context.Context, e *Engine, n names, image, act string, creds []byte, files ...File) error {
	if _, err := e.RunEphemeral(ctx, busSpec(n, "act-"+act, image, []string{ActCommand, act}, creds, files...)); err != nil {
		return fmt.Errorf("deploy: act %s: %w", act, err)
	}
	return nil
}

// consoleCreds is the console's identity on the Bus, for one act.
func (m *material) consoleCreds() ([]byte, error) {
	key, ok := m.account.SigningKeys[bus.ConsoleScope.Role]
	if !ok {
		return nil, fmt.Errorf("deploy: the account has no %s scope", bus.ConsoleScope.Role)
	}
	return identityCreds(m.console, func(pub string) (string, error) {
		return bus.IssueUser(key, m.account.Public, pub, bus.ConsoleScope.Role, time.Now().Add(actIdentityLifetime))
	})
}

// systemCreds is the console's identity in the system account, for one act.
func (m *material) systemCreds() ([]byte, error) {
	return identityCreds(m.console, func(pub string) (string, error) {
		return bus.IssueSystemUser(m.account.SystemKey, pub, time.Now().Add(actIdentityLifetime))
	})
}

func identityCreds(kp nkeys.KeyPair, issue func(pub string) (string, error)) ([]byte, error) {
	pub, err := kp.PublicKey()
	if err != nil {
		return nil, err
	}
	token, err := issue(pub)
	if err != nil {
		return nil, err
	}
	seed, err := kp.Seed()
	if err != nil {
		return nil, err
	}
	return bus.Creds(token, seed)
}
