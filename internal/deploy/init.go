package deploy

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/nats-io/nkeys"

	"zobik.org/zobik/internal/bus"
)

// Options are the inputs of zobik init.
type Options struct {
	// Network names the network; it prefixes every engine resource.
	Network string
	// Dir is the console's directory for this network on the host, NetworkDir
	// of its home, where the tenant's directory also lives.
	Dir string
	// User and Password are the only thing zobik init asks the person
	// (implementation §6, The root and the split Operator Channel).
	User     string
	Password []byte
	// Image is the zobik image the roles and the ephemeral acts run.
	Image string
	// BaseImage is the Logic Container base image zobik init seeds in the
	// tenant's registry, as the engine has it (implementation §1.2.13).
	BaseImage string
	// Scopes are the roles the account issues for.
	Scopes []bus.Scope
	// Tenant names a tenant on this host for the network to join; empty creates
	// a new one (implementation §6, The deployment parameters).
	Tenant string
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
	if err := checkLegacyAuthority(o.Dir); err != nil {
		return err
	}
	_, recorded, err := recordedTenant(o.Dir)
	if err != nil {
		return err
	}
	t, err := chooseTenant(ctx, e, o.Dir, o.Tenant)
	if err != nil {
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

	// The tenant CA goes before the roles, which receive its certificate
	// (implementation §6, The tenant CA, the link and the join code).
	tdir := tenantDir(o.Dir, t)
	ca, err := tenantAuthority(tdir, o.Network, t, !recorded && o.Tenant == "", o.Password)
	if err != nil {
		return err
	}
	if err := recordTenant(o.Dir, t); err != nil {
		return err
	}
	tenantCA, err := readTenantCA(tdir)
	if err != nil {
		return err
	}

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
	ids, err := ensureRoles(ctx, e, n, m, o.Dir, o.Image, tenantCA)
	if err != nil {
		return err
	}
	substrate, err := newSubstrate(ids)
	if err != nil {
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
	if err := runAct(ctx, e, n, o.Image, actSubstrate, consoleCreds, File{Path: actSubstratePath, Mode: 0o600, Data: substrate}); err != nil {
		return err
	}

	// The tenant-wide pieces (implementation §6, Installation and zobik init).
	if err := ensureTracing(ctx, e, n, t, ca, tdir); err != nil {
		return err
	}
	if err := ensureRegistry(ctx, e, n, t, ca, tdir, o.Image); err != nil {
		return err
	}
	if err := seedBase(ctx, e, n, o.Image, o.BaseImage, tdir, tenantCA); err != nil {
		return err
	}
	return startRoles(ctx, e, n, o.Dir, o.Image)
}

// tenantAuthority opens the tenant CA in the tenant's directory tdir when
// network created the tenant t, and mints it when creates says network creates
// it now; a network that joined the tenant opens none.
func tenantAuthority(tdir, network string, t tenant, creates bool, password []byte) (*authority, error) {
	creator, err := os.ReadFile(filepath.Join(tdir, fileCreator))
	switch {
	case err == nil && string(creator) == network:
		// A run cut short before writing the key mints the CA again.
		ca, err := openAuthority(tdir, password)
		if err != nil || ca != nil {
			return ca, err
		}
	case err == nil:
		return nil, nil
	case !errors.Is(err, fs.ErrNotExist):
		return nil, err
	case !creates:
		return nil, fmt.Errorf("deploy: the tenant %s has no CA in %s", t, tdir)
	}
	return writeAuthority(tdir, t, network, password)
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
