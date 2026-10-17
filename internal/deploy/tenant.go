package deploy

import (
	"context"
	"crypto/rand"
	"encoding/base32"
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
	tracingHealth  = "13133"
	tracingConfig  = "/etc/jaeger/config.yaml"
	tracingDataDir = "/badger"
	tracingUID     = 10001
)

// tracingRetention is the spans' TTL in Badger: the evidence_staleness of v0
// (implementation §1.2.17).
const tracingRetention = 30 * 24 * time.Hour

// The label every tenant-wide resource carries, with the tenant's identifier.
const labelTenant = "org.zobik.tenant"

// fileTenant, in the console's directory, records the tenant the network belongs to.
const fileTenant = "tenant"

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
// join names a tenant whose pieces are on this host.
func chooseTenant(ctx context.Context, e *Engine, dir, join string) (tenant, error) {
	b, err := os.ReadFile(filepath.Join(dir, fileTenant))
	switch {
	case err == nil:
		recorded := tenant(b)
		if join != "" && tenant(join) != recorded {
			return "", fmt.Errorf("deploy: the network belongs to the tenant %s", recorded)
		}
		return recorded, e.checkTenant(ctx, recorded)
	case !errors.Is(err, fs.ErrNotExist):
		return "", err
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
	return t, nil
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
// ready. ui is the host port on 127.0.0.1 where a new backend publishes its query
// service, or none if empty.
func ensureTracing(ctx context.Context, e *Engine, n names, t tenant, ui string) error {
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
	var publish map[string]string
	if ui != "" {
		publish = map[string]string{TracingQuery + "/tcp": ui}
	}
	if err := e.EnsureShared(ctx, Spec{
		Name:        t.tracing(),
		Alias:       TracingAlias,
		Network:     n.dockerNetwork(),
		Labels:      t.labels(),
		Image:       TracingImage,
		Cmd:         []string{"--config", tracingConfig},
		Mounts:      []mount.Mount{{Type: mount.TypeVolume, Source: t.tracing(), Target: tracingDataDir}},
		Files:       []File{{Path: tracingConfig, Mode: 0o644, Data: tracingConfigYAML()}},
		Restart:     true,
		Healthcheck: tracingHealthcheck,
		Publish:     publish,
	}); err != nil {
		return fmt.Errorf("deploy: tracing backend: %w", err)
	}
	return e.WaitHealthy(ctx, t.tracing(), roleReadyTimeout)
}

var tracingHealthcheck = &container.HealthConfig{
	Test:          []string{"CMD", "wget", "-q", "-O", "/dev/null", "http://localhost:" + tracingHealth + "/status"},
	Interval:      10 * time.Second,
	Timeout:       3 * time.Second,
	Retries:       3,
	StartPeriod:   time.Minute,
	StartInterval: time.Second,
}

// tracingConfigYAML is Jaeger's configuration: OTLP over gRPC in, Badger with
// the spans' TTL, and the query service.
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
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:%[2]s
processors:
  batch:
exporters:
  jaeger_storage_exporter:
    trace_storage: badger
`, tracingHealth, TracingOTLP, TracingQuery, tracingDataDir, tracingRetention)
}

// recordTenant writes the network's tenant to the console's directory.
func recordTenant(dir string, t tenant) error {
	return writeFile(dir, fileTenant, []byte(t))
}
