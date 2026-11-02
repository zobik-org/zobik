package deploy

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/moby/moby/api/types/mount"

	"zobik.org/zobik/internal/console"
	"zobik.org/zobik/internal/role/catalog"
	"zobik.org/zobik/internal/role/scanner"
	"zobik.org/zobik/internal/role/secrets"
)

// The Specialty Catalog's fixed name (implementation §6, The tenant CA, the
// link and the join code).
const CatalogAlias = "catalog.zobik.internal"

// storageCapacity is the artifact storage capacity zobik init declares to the
// catalog role: 2 GB on the reference machine (implementation §1.2.16).
const storageCapacity = 2_000_000_000

// The handles of the network's Catalog credentials in its Secret Store
// (implementation §1.2.18): the Spawner's, which the secrets role resolves for
// it, and the read and register ones, which the console reads.
const (
	HandleCatalogRead     = secrets.HandleCatalog + "/read"
	HandleCatalogRegister = secrets.HandleCatalog + "/register"
)

// fileCatalogAdmin, in the tenant's directory, is the Catalog administration
// credential sealed with the creator network's password (implementation
// §1.2.11).
const fileCatalogAdmin = "catalog_admin.sealed"

// unitCatalog names, among the tenant's ports, the Catalog's toward the console
// (implementation §6, The deployment parameters).
const unitCatalog = "catalog"

// catalog and scanner name the Catalog's container and its data volume, and
// the Admission Scanner's container; scannerSocket, the volume of the
// scanner's socket, which only the two mount (implementation §1.2.19).
func (t tenant) catalog() string       { return "zobik-tenant-" + string(t) + "-catalog" }
func (t tenant) scanner() string       { return "zobik-tenant-" + string(t) + "-scanner" }
func (t tenant) scannerSocket() string { return "zobik-tenant-" + string(t) + "-scanner_socket" }

// catalogAdmin opens the administration credential in the tenant's directory
// tdir with the password, minting it if the tenant has none yet.
func catalogAdmin(tdir string, password []byte) (string, error) {
	sealed, err := os.ReadFile(filepath.Join(tdir, fileCatalogAdmin))
	if errors.Is(err, fs.ErrNotExist) {
		cred, err := catalog.NewCredential()
		if err != nil {
			return "", err
		}
		return cred, writeSecret(tdir, fileCatalogAdmin, []byte(cred), password)
	}
	if err != nil {
		return "", err
	}
	cred, err := console.OpenSecret(sealed, password)
	if err != nil {
		return "", fmt.Errorf("deploy: the Catalog administration credential: %w", err)
	}
	return string(cred), nil
}

// ensureCatalog brings up the tenant's Admission Scanner and Specialty Catalog
// if they are not up, connects both to the network's Docker network, the
// Catalog with its fixed name, and waits until they are ready. ca is the tenant
// CA when the network created the tenant: the Catalog then takes its server
// certificate and the hash of the administration credential admin, and
// publishes its port on the tenant directory tdir's ports/catalog. A network
// that joined the tenant finds both up and only connects to them. image is the
// zobik image both run.
func ensureCatalog(ctx context.Context, e *Engine, n names, t tenant, ca *authority, tdir, image, admin string) error {
	if ca == nil {
		for _, c := range []struct{ name, alias string }{{t.scanner(), ""}, {t.catalog(), CatalogAlias}} {
			if ok, err := e.exists(ctx, c.name); err != nil {
				return err
			} else if !ok {
				return fmt.Errorf("deploy: the tenant %s has no %s, and only the network that created it brings one up", t, c.name)
			}
			if err := e.connect(ctx, c.name, n.dockerNetwork(), aliases(c.alias)); err != nil {
				return fmt.Errorf("deploy: %s: %w", c.name, err)
			}
		}
		return e.WaitHealthy(ctx, t.catalog(), roleReadyTimeout)
	}

	for _, v := range []string{t.catalog(), t.scannerSocket()} {
		if _, err := e.EnsureVolume(ctx, v, t.labels()); err != nil {
			return fmt.Errorf("deploy: volume %s: %w", v, err)
		}
	}
	socket := mount.Mount{Type: mount.TypeVolume, Source: t.scannerSocket(), Target: filepath.Dir(scanner.Socket)}
	// It reaches the registry by its fixed name, and analyzes in /tmp.
	if err := e.EnsureShared(ctx, Spec{
		Name:        t.scanner(),
		Network:     n.dockerNetwork(),
		Labels:      sharedLabels(t, ca, nil),
		Image:       image,
		Cmd:         []string{"run", "--role=scanner"},
		Mounts:      []mount.Mount{socket},
		Tmpfs:       []string{"/tmp"},
		Files:       []File{{Path: TenantCAPath, Mode: 0o644, Data: ca.certPEM()}},
		Restart:     true,
		Healthcheck: roleHealthcheck,
	}); err != nil {
		return fmt.Errorf("deploy: scanner: %w", err)
	}

	port, err := ConsolePort(tdir, unitCatalog)
	if err != nil {
		return err
	}
	cert, key, err := ca.issue(CatalogAlias, x509.ExtKeyUsageServerAuth)
	if err != nil {
		return err
	}
	cmd := []string{"run", "--role=catalog", "--storage-capacity=" + strconv.Itoa(storageCapacity)}
	adminHash := catalog.HashCredential(admin)
	if err := e.EnsureShared(ctx, Spec{
		Name:    t.catalog(),
		Alias:   CatalogAlias,
		Network: n.dockerNetwork(),
		Labels:  sharedLabels(t, ca, []byte(strings.Join(append(cmd, adminHash), "\n"))),
		Image:   image,
		Cmd:     cmd,
		Mounts: []mount.Mount{
			{Type: mount.TypeVolume, Source: t.catalog(), Target: catalog.DataDir},
			socket,
		},
		Files: []File{
			{Path: TenantCAPath, Mode: 0o644, Data: ca.certPEM()},
			{Path: catalog.CertPath, Mode: 0o644, Data: cert},
			{Path: catalog.KeyPath, Mode: 0o600, Data: key},
			{Path: catalog.AdminPath, Mode: 0o600, Data: []byte(adminHash)},
		},
		Restart:     true,
		Healthcheck: roleHealthcheck,
		Publish:     map[string]string{catalog.Port + "/tcp": port},
	}); err != nil {
		return fmt.Errorf("deploy: catalog: %w", err)
	}
	if err := e.WaitHealthy(ctx, t.scanner(), roleReadyTimeout); err != nil {
		return err
	}
	return e.WaitHealthy(ctx, t.catalog(), roleReadyTimeout)
}

// ensureCatalogCredentials issues the network's read, register and spawner
// credentials with the administration credential, and writes them to its
// Secret Store, if the store has none yet (implementation §1.2.11, §1.2.18).
func ensureCatalogCredentials(ctx context.Context, e *Engine, dir, network, image, admin string) error {
	if v, err := ReadSecret(dir, secrets.HandleCatalog); err != nil || v != nil {
		return err
	}
	c, err := CatalogClient(dir, admin)
	if err != nil {
		return err
	}
	// Credentials issued by a run cut short before writing them are left
	// orphaned; revoking first leaves only the ones written.
	if err := c.RevokeCredentials(ctx, network); err != nil {
		return err
	}
	values := map[string][]byte{}
	for handle, scope := range map[string]string{
		secrets.HandleCatalog: catalog.ScopeSpawner,
		HandleCatalogRead:     catalog.ScopeRead,
		HandleCatalogRegister: catalog.ScopeRegister,
	} {
		cred, err := c.IssueCredential(ctx, network, scope)
		if err != nil {
			return fmt.Errorf("deploy: the network's %s credential: %w", scope, err)
		}
		values[handle] = []byte(cred)
	}
	return writeSecrets(ctx, e, dir, network, image, values)
}

// ErrNoCatalog is a network whose tenant has no Catalog port on this host.
var ErrNoCatalog = errors.New("deploy: the tenant has no Catalog on this host")

// CatalogClient is the console's client of the Catalog of the tenant of the
// network in dir, with credential: over the port the Catalog publishes on
// 127.0.0.1, with TLS verified against the tenant CA (implementation §6, The
// tenant CA, the link and the join code).
func CatalogClient(dir, credential string) (*catalog.Client, error) {
	t, ok, err := recordedTenant(dir)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrNoCatalog
	}
	tdir := tenantDir(dir, t)
	port, err := os.ReadFile(filepath.Join(tdir, dirPorts, unitCatalog))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, ErrNoCatalog
	}
	if err != nil {
		return nil, err
	}
	caPEM, err := readTenantCA(tdir)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("deploy: tenant CA: no certificate")
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.Proxy = nil
	tr.TLSClientConfig = &tls.Config{RootCAs: pool, ServerName: CatalogAlias, MinVersion: tls.VersionTLS13}
	return &catalog.Client{
		URL:        "https://127.0.0.1:" + strings.TrimSpace(string(port)),
		Credential: credential,
		HTTP:       &http.Client{Transport: tr, Timeout: 10 * time.Minute},
	}, nil
}
