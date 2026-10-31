package deploy

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base32"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
)

// TracingImage is the tracing backend this version of zobik installs, pinned by
// the digest of the official image (implementation §1.2.17).
const TracingImage = "jaegertracing/jaeger:2.22.0@sha256:836b967b5abcf1f7758066aa6b348619267a2f1459f72d472840570f9bd88a17"

// The tracing backend's fixed name, ports and paths (implementation §6, The
// tenant CA, the link and the join code). The image runs as tracingUID.
const (
	TracingAlias   = "tracing.zobik.internal"
	TracingOTLP    = "4317"  // OTLP over gRPC, which the Tracing Collector exports to
	TracingQuery   = "16686" // the query service: the UI and the API v3 over HTTP
	tracingGRPC    = "16685" // the query service over gRPC
	tracingHealth  = "13133"
	tracingConfig  = "/etc/jaeger/config.yaml"
	tracingCA      = "/etc/jaeger/ca.pem"
	tracingCert    = "/etc/jaeger/server.pem"
	tracingKey     = "/etc/jaeger/server.key"
	tracingDataDir = "/badger"
	tracingUID     = 10001
)

// tracingRetention is the spans' TTL in Badger: the evidence_staleness of v0
// (implementation §1.2.17).
const tracingRetention = 30 * 24 * time.Hour

// The label every tenant-wide resource carries, with the tenant's identifier;
// the one the tracing backend carries with the fingerprint of the CA its
// certificates come from; and the one with the digest of its configuration,
// certificates aside, which replaces the container when it changes.
const (
	labelTenant   = "org.zobik.tenant"
	labelTenantCA = "org.zobik.tenant_ca"
	labelConfig   = "org.zobik.config"
)

// The console's home holds a directory per network, networks/<network>, and
// one per tenant, tenants/<tenant>, which every network of the tenant on the
// host reads.
const (
	dirNetworks = "networks"
	dirTenants  = "tenants"
)

// NetworkDir is the console's directory for network in home.
func NetworkDir(home, network string) string { return filepath.Join(home, dirNetworks, network) }

// tenantDir is the directory of t in the home of the network directory dir.
func tenantDir(dir string, t tenant) string {
	return filepath.Join(filepath.Dir(filepath.Dir(dir)), dirTenants, string(t))
}

// fileTenant, in the console's directory, records the tenant the network belongs to.
const fileTenant = "tenant"

// recordedTenant is the tenant the directory records, if any.
func recordedTenant(dir string) (tenant, bool, error) {
	b, err := os.ReadFile(filepath.Join(dir, fileTenant))
	if errors.Is(err, fs.ErrNotExist) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return tenant(b), true, nil
}

// createdTenant returns the directory of the tenant the network in dir created,
// or false if it joined its tenant: only the creator's password opens what the
// tenant's directory seals.
func createdTenant(dir string) (string, bool, error) {
	t, ok, err := recordedTenant(dir)
	if err != nil || !ok {
		return "", false, err
	}
	tdir := tenantDir(dir, t)
	creator, err := os.ReadFile(filepath.Join(tdir, fileCreator))
	if errors.Is(err, fs.ErrNotExist) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return tdir, string(creator) == filepath.Base(dir), nil
}

// tenant is a tenant's identifier: eight random lowercase characters, a valid
// DNS label in zobik-tenant-<tenant> (implementation §6, The tenant CA, the link
// and the join code).
type tenant string

var tenantPattern = regexp.MustCompile(`^[a-z2-7]{8}$`)

func newTenant() (tenant, error) {
	b := make([]byte, 5)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return tenant(strings.ToLower(base32.StdEncoding.EncodeToString(b))), nil
}

// tracing names the tracing backend's container and its volume. The tenant's
// resources carry the tenant and no network, so removing a network leaves them.
func (t tenant) tracing() string           { return "zobik-tenant-" + string(t) + "-tracing" }
func (t tenant) labels() map[string]string { return map[string]string{labelTenant: string(t)} }

// chooseTenant returns the tenant of the network in dir, without writing
// anything: the one the directory records, or with none, join, or a new one. A
// join names a tenant whose pieces are on this host and whose directory is in
// the same home.
func chooseTenant(ctx context.Context, e *Engine, dir, join string) (tenant, error) {
	recorded, ok, err := recordedTenant(dir)
	switch {
	case err != nil:
		return "", err
	case ok:
		if join != "" && tenant(join) != recorded {
			return "", fmt.Errorf("deploy: the network belongs to the tenant %s", recorded)
		}
		return recorded, e.checkTenant(ctx, recorded)
	case join == "":
		return newTenant()
	}
	t := tenant(join)
	if !tenantPattern.MatchString(join) {
		return "", fmt.Errorf("deploy: %q is not a tenant identifier", join)
	}
	if err := e.checkTenant(ctx, t); err != nil {
		return "", err
	}
	if exists, err := e.exists(ctx, t.tracing()); err != nil {
		return "", err
	} else if !exists {
		return "", fmt.Errorf("deploy: there is no tenant %s on this host", t)
	}
	if _, err := os.Stat(filepath.Join(tenantDir(dir, t), fileCACert)); err != nil {
		return "", fmt.Errorf("deploy: the tenant %s has no directory beside this network's: %w", t, err)
	}
	return t, nil
}

// checkLegacyAuthority refuses a network directory that keeps the tenant CA,
// from before the tenant's directory: such a tenant is not converted.
func checkLegacyAuthority(dir string) error {
	if _, err := os.Stat(filepath.Join(dir, fileCAKey)); err == nil {
		return errors.New("deploy: the network keeps its tenant's CA in its own directory: remove the tenant's networks and its tracing backend, and create them again")
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

// checkTenant fails if the engine holds a resource under t's names that does
// not carry t's label.
func (e *Engine) checkTenant(ctx context.Context, t tenant) error {
	if labels, ok, err := e.containerLabels(ctx, t.tracing()); err != nil {
		return err
	} else if ok && labels[labelTenant] != string(t) {
		return fmt.Errorf("deploy: the container %s does not belong to the tenant %s", t.tracing(), t)
	}
	if labels, ok, err := e.volumeLabels(ctx, t.tracing()); err != nil {
		return err
	} else if ok && labels[labelTenant] != string(t) {
		return fmt.Errorf("deploy: the volume %s does not belong to the tenant %s", t.tracing(), t)
	}
	return nil
}

// ensureTracing brings up the tenant's tracing backend if it is not up, connects
// it to the network's Docker network with its fixed name, and waits until it is
// ready. ca is the tenant CA when the network created the tenant: the backend
// then takes its server certificate for OTLP and the query, asks the query for
// a client certificate from the CA, and publishes it on the port of the tenant
// directory tdir's ports/tracing (implementation §1.2.17). A network that
// joined the tenant finds the backend up and only connects to it.
func ensureTracing(ctx context.Context, e *Engine, n names, t tenant, ca *authority, tdir string) error {
	labels, ok, err := e.containerLabels(ctx, t.tracing())
	if err != nil {
		return err
	}
	if ok && (labels[labelTenantCA] == "" || ca != nil && labels[labelTenantCA] != ca.fingerprint()) {
		return fmt.Errorf("deploy: the tracing backend %s predates the tenant CA: remove the tenant's networks and %s, and create them again", t.tracing(), t.tracing())
	}
	if ca == nil {
		if !ok {
			return fmt.Errorf("deploy: the tenant %s has no tracing backend, and only the network that created it brings one up", t)
		}
		if err := e.connect(ctx, t.tracing(), n.dockerNetwork(), []string{TracingAlias}); err != nil {
			return fmt.Errorf("deploy: tracing backend: %w", err)
		}
		return e.WaitHealthy(ctx, t.tracing(), roleReadyTimeout)
	}

	if err := e.EnsureImage(ctx, TracingImage); err != nil {
		return fmt.Errorf("deploy: tracing image: %w", err)
	}
	created, err := e.EnsureVolume(ctx, t.tracing(), t.labels())
	if err != nil {
		return fmt.Errorf("deploy: tracing volume: %w", err)
	}
	if created {
		// The image's user owns Badger's directories.
		if err := e.WriteVolume(ctx, t.tracing(), TracingImage, tracingDataDir, []File{{Path: ".", Dir: true, Mode: 0o700, UID: tracingUID}}); err != nil {
			return fmt.Errorf("deploy: tracing volume: %w", err)
		}
	}
	port, err := ConsolePort(tdir, unitTracing)
	if err != nil {
		return err
	}
	// The pieces' certificates are issued again on each run (implementation §6,
	// The tenant CA, the link and the join code); a container takes them when it
	// is created.
	cert, key, err := ca.issue(TracingAlias, x509.ExtKeyUsageServerAuth)
	if err != nil {
		return err
	}
	config := tracingConfigYAML()
	sum := sha256.Sum256(config)
	withCA := t.labels()
	withCA[labelTenantCA] = ca.fingerprint()
	withCA[labelConfig] = hex.EncodeToString(sum[:])
	if err := e.EnsureShared(ctx, Spec{
		Name:    t.tracing(),
		Alias:   TracingAlias,
		Network: n.dockerNetwork(),
		Labels:  withCA,
		Image:   TracingImage,
		Cmd:     []string{"--config", tracingConfig},
		Mounts:  []mount.Mount{{Type: mount.TypeVolume, Source: t.tracing(), Target: tracingDataDir}},
		Files: []File{
			{Path: tracingConfig, Mode: 0o644, Data: config},
			{Path: tracingCA, Mode: 0o644, Data: ca.certPEM()},
			{Path: tracingCert, Mode: 0o644, Data: cert},
			{Path: tracingKey, Mode: 0o600, UID: tracingUID, Data: key},
		},
		Restart:     true,
		Healthcheck: tracingHealthcheck,
		Publish:     map[string]string{TracingQuery + "/tcp": port},
	}); err != nil {
		return fmt.Errorf("deploy: tracing backend: %w", err)
	}
	return e.WaitHealthy(ctx, t.tracing(), roleReadyTimeout)
}

// unitTracing names, among the ports, the tracing backend's query
// (implementation §6, The deployment parameters).
const unitTracing = "tracing"

// ErrNoTracing is a network that did not create its tenant: its console has no
// client certificate for the tracing backend.
var ErrNoTracing = errors.New("deploy: the network did not create its tenant")

// TracingAddr is the address on 127.0.0.1 of the tracing backend's query, which
// only the console of the network that created the tenant reaches.
func TracingAddr(dir string) (string, error) {
	tdir, ok, err := createdTenant(dir)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", ErrNoTracing
	}
	b, err := os.ReadFile(filepath.Join(tdir, dirPorts, unitTracing))
	if errors.Is(err, fs.ErrNotExist) {
		return "", ErrNoTracing
	}
	if err != nil {
		return "", err
	}
	return "127.0.0.1:" + strings.TrimSpace(string(b)), nil
}

var tracingHealthcheck = &container.HealthConfig{
	Test:          []string{"CMD", "wget", "-q", "-O", "/dev/null", "http://localhost:" + tracingHealth + "/status"},
	Interval:      10 * time.Second,
	Timeout:       3 * time.Second,
	Retries:       3,
	StartPeriod:   time.Minute,
	StartInterval: time.Second,
}

// tracingConfigYAML is Jaeger's configuration: OTLP over gRPC with TLS in,
// without a client certificate, Badger with the spans' TTL, and the query
// service, over HTTP and gRPC, only for clients with a certificate from the
// tenant CA (implementation §1.2.17).
func tracingConfigYAML() []byte {
	return fmt.Appendf(nil, `service:
  extensions: [jaeger_storage, jaeger_query, healthcheckv2]
  pipelines:
    traces:
      receivers: [otlp]
      processors: [batch]
      exporters: [jaeger_storage_exporter]
  telemetry:
    metrics:
      level: none
extensions:
  healthcheckv2:
    use_v2: true
    http:
      endpoint: 0.0.0.0:%[1]s
  jaeger_storage:
    backends:
      badger:
        badger:
          directories:
            keys: %[4]s/keys
            values: %[4]s/values
          ephemeral: false
          ttl:
            spans: %[5]s
  jaeger_query:
    storage:
      traces: badger
    http:
      endpoint: 0.0.0.0:%[3]s
      tls:
        cert_file: %[7]s
        key_file: %[8]s
        client_ca_file: %[6]s
        min_version: "1.3"
    grpc:
      endpoint: 0.0.0.0:%[9]s
      tls:
        cert_file: %[7]s
        key_file: %[8]s
        client_ca_file: %[6]s
        min_version: "1.3"
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:%[2]s
        tls:
          cert_file: %[7]s
          key_file: %[8]s
          min_version: "1.3"
processors:
  batch:
exporters:
  jaeger_storage_exporter:
    trace_storage: badger
`, tracingHealth, TracingOTLP, TracingQuery, tracingDataDir, tracingRetention, tracingCA, tracingCert, tracingKey, tracingGRPC)
}

// recordTenant writes the network's tenant to the console's directory.
func recordTenant(dir string, t tenant) error {
	return writeFile(dir, fileTenant, []byte(t))
}
