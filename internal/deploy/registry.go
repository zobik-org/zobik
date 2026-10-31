package deploy

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/moby/moby/api/types/mount"
)

// RegistryImage is the image registry this version of zobik installs, pinned by
// the digest of the official image (implementation §1.2.16).
const RegistryImage = "ghcr.io/project-zot/zot-minimal:v2.1.22@sha256:6704abd9c4c988398d77915549d79c5daa2a2c5731964833e31ca039c9ac4ed3"

// The image registry's fixed name, port and paths (implementation §6, The
// tenant CA, the link and the join code), and the single repository every image
// lives in (implementation §1.2.16).
const (
	RegistryAlias   = "registry.zobik.internal"
	RegistryPort    = "5000"
	Repository      = "artifacts"
	registryConfig  = "/etc/zot/config.json"
	registryCert    = "/etc/zot/server.pem"
	registryKey     = "/etc/zot/server.key"
	registryDataDir = "/var/lib/registry"
)

// The tags that mark what the registry retains (implementation §1.2.16).
const (
	TagBase  = "base"
	TagEntry = "entry"
)

// registry and link name the image registry's container and volume, and the
// host's link to the tenant's pieces.
func (t tenant) registry() string { return "zobik-tenant-" + string(t) + "-registry" }
func (t tenant) link() string     { return "zobik-tenant-" + string(t) + "-link" }

// unitRegistry names, among the tenant's ports, the registry's on the link
// (implementation §6, The deployment parameters).
const unitRegistry = "registry"

// ensureRegistry brings up the tenant's image registry and the host's link if
// they are not up, connects both to the network's Docker network, the registry
// with its fixed name, and waits until the link reaches the registry. ca is the
// tenant CA when the network created the tenant: the registry then takes its
// server certificate, and the link publishes it on the port of the tenant
// directory tdir's ports/registry. A network that joined the tenant finds both
// up and only connects to them. image is the zobik image the link runs.
func ensureRegistry(ctx context.Context, e *Engine, n names, t tenant, ca *authority, tdir, image string) error {
	if ca == nil {
		for _, c := range []struct{ name, alias string }{{t.registry(), RegistryAlias}, {t.link(), ""}} {
			if ok, err := e.exists(ctx, c.name); err != nil {
				return err
			} else if !ok {
				return fmt.Errorf("deploy: the tenant %s has no %s, and only the network that created it brings one up", t, c.name)
			}
			if err := e.connect(ctx, c.name, n.dockerNetwork(), aliases(c.alias)); err != nil {
				return fmt.Errorf("deploy: %s: %w", c.name, err)
			}
		}
		return e.WaitHealthy(ctx, t.link(), roleReadyTimeout)
	}

	if err := e.EnsureImage(ctx, RegistryImage); err != nil {
		return fmt.Errorf("deploy: registry image: %w", err)
	}
	if _, err := e.EnsureVolume(ctx, t.registry(), t.labels()); err != nil {
		return fmt.Errorf("deploy: registry volume: %w", err)
	}
	cert, key, err := ca.issue(RegistryAlias, x509.ExtKeyUsageServerAuth)
	if err != nil {
		return err
	}
	config := registryConfigJSON()
	if err := e.EnsureShared(ctx, Spec{
		Name:    t.registry(),
		Alias:   RegistryAlias,
		Network: n.dockerNetwork(),
		Labels:  sharedLabels(t, ca, config),
		Image:   RegistryImage,
		Mounts:  []mount.Mount{{Type: mount.TypeVolume, Source: t.registry(), Target: registryDataDir}},
		Files: []File{
			{Path: registryConfig, Mode: 0o644, Data: config},
			{Path: registryCert, Mode: 0o644, Data: cert},
			{Path: registryKey, Mode: 0o600, Data: key},
		},
		Restart: true,
	}); err != nil {
		return fmt.Errorf("deploy: registry: %w", err)
	}

	port, err := ConsolePort(tdir, unitRegistry)
	if err != nil {
		return err
	}
	if err := e.EnsureShared(ctx, Spec{
		Name:        t.link(),
		Network:     n.dockerNetwork(),
		Labels:      sharedLabels(t, ca, nil),
		Image:       image,
		Cmd:         []string{"run", "--role=tenant_link"},
		Files:       []File{{Path: LinkCAPath, Mode: 0o644, Data: ca.certPEM()}},
		Restart:     true,
		Healthcheck: roleHealthcheck,
		Publish:     map[string]string{LinkPort + "/tcp": port},
	}); err != nil {
		return fmt.Errorf("deploy: link: %w", err)
	}
	return e.WaitHealthy(ctx, t.link(), roleReadyTimeout)
}

// The link's port inside its container, and where it finds the tenant CA.
const (
	LinkPort   = "5000"
	LinkCAPath = "/etc/zobik/tenant_ca.pem"
)

// sharedLabels are the labels of a tenant-wide container: the tenant, the
// fingerprint of the CA its certificates come from, and the digest of its
// configuration, certificates aside, which replaces it when it changes.
func sharedLabels(t tenant, ca *authority, config []byte) map[string]string {
	sum := sha256.Sum256(config)
	labels := t.labels()
	labels[labelTenantCA] = ca.fingerprint()
	labels[labelConfig] = hex.EncodeToString(sum[:])
	return labels
}

// registryConfigJSON is zot's configuration: the distribution API over TLS, the
// garbage collection at its defaults and the retention of implementation
// §1.2.16, which keeps the base-* and entry-* tags and a pending-* one for 72
// hours.
func registryConfigJSON() []byte {
	return fmt.Appendf(nil, `{
  "distSpecVersion": "1.1.1",
  "storage": {
    "rootDirectory": %[1]q,
    "retention": {
      "policies": [{
        "repositories": ["**"],
        "keepTags": [
          {"patterns": ["^%[2]s-", "^%[3]s-"]},
          {"patterns": ["^pending-"], "pushedWithin": "72h"}
        ]
      }]
    }
  },
  "http": {
    "address": "0.0.0.0",
    "port": %[4]q,
    "tls": {"cert": %[5]q, "key": %[6]q}
  },
  "log": {"level": "warn"}
}
`, registryDataDir, TagBase, TagEntry, RegistryPort, registryCert, registryKey)
}

// ErrNoRegistry is a tenant whose link has no port on this host.
var ErrNoRegistry = errors.New("deploy: the tenant has no registry on this host")

// RegistryAddr is the address on 127.0.0.1 where the container engine pulls
// from the registry of the tenant of the network in dir, through the link.
func RegistryAddr(dir string) (string, error) {
	t, ok, err := recordedTenant(dir)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", ErrNoRegistry
	}
	b, err := os.ReadFile(filepath.Join(tenantDir(dir, t), dirPorts, unitRegistry))
	if errors.Is(err, fs.ErrNotExist) {
		return "", ErrNoRegistry
	}
	if err != nil {
		return "", err
	}
	return "127.0.0.1:" + strings.TrimSpace(string(b)), nil
}

// PullRef is the reference the engine pulls digest by, from the registry at
// addr (implementation §1.2.16).
func PullRef(addr, digest string) string { return addr + "/" + Repository + "@" + digest }
