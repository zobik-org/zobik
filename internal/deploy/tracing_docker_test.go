//go:build e2e

package deploy

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/client"
)

// testNetwork creates a Docker network for the test, removed when it ends.
func testNetwork(t *testing.T, ctx context.Context, e *Engine, prefix string) names {
	t.Helper()
	suffix := make([]byte, 4)
	rand.Read(suffix)
	n := names{prefix + "-" + hex.EncodeToString(suffix), "test-root"}
	if err := e.EnsureNetwork(ctx, n.dockerNetwork(), n.labels()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := e.RemoveNetwork(context.Background(), n.dockerNetwork()); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	})
	return n
}

// testEngine connects to the engine for the test.
func testEngine(t *testing.T, ctx context.Context) *Engine {
	t.Helper()
	e, err := NewEngine(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() })
	return e
}

// Two networks on the same host share their tenant's Jaeger, each reaches it by
// its fixed name, and removing one leaves it. Its query answers only a client
// with a certificate from the tenant CA (implementation §1.2.17).
func TestTracingShared(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := testEngine(t, ctx)
	tn, err := newTenant()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := e.Remove(context.Background(), []string{tn.tracing()}, []string{tn.tracing()}); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	})
	a := testNetwork(t, ctx, e, "tracing-a")
	b := testNetwork(t, ctx, e, "tracing-b")
	dir, password := t.TempDir(), []byte("pw")
	ca, err := writeAuthority(dir, tn, password)
	if err != nil {
		t.Fatal(err)
	}

	if err := ensureTracing(ctx, e, a, tn, ca, dir); err != nil {
		t.Fatal(err)
	}
	first := containerID(t, ctx, e, tn.tracing())
	if err := ensureTracing(ctx, e, b, tn, nil, t.TempDir()); err != nil { // joins
		t.Fatal(err)
	}
	if err := ensureTracing(ctx, e, a, tn, ca, dir); err != nil { // converges
		t.Fatal(err)
	}
	if id := containerID(t, ctx, e, tn.tracing()); id != first {
		t.Fatalf("the second network replaced the backend: %s, then %s", first, id)
	}
	reach(t, ctx, e, a)
	reach(t, ctx, e, b)

	addr, err := TracingAddr(dir)
	if err != nil {
		t.Fatal(err)
	}
	own, err := OpenTracingClient(dir, password)
	if err != nil {
		t.Fatal(err)
	}
	if err := query(ctx, addr, own); err != nil {
		t.Errorf("with the console's certificate: %v", err)
	}
	if err := query(ctx, addr, &tls.Config{RootCAs: own.RootCAs, ServerName: TracingAlias}); err == nil {
		t.Error("the query answered without a client certificate")
	}
	otherDir := t.TempDir()
	other, err := writeAuthority(otherDir, tn, password)
	if err != nil {
		t.Fatal(err)
	}
	foreign, err := OpenTracingClient(otherDir, password)
	if err != nil {
		t.Fatal(err)
	}
	foreign.RootCAs = own.RootCAs
	if err := query(ctx, addr, foreign); err == nil {
		t.Error("the query answered a certificate from another CA")
	}
	if err := ensureTracing(ctx, e, a, tn, other, otherDir); err == nil {
		t.Error("a backend of another CA was taken as the tenant's")
	}

	if err := e.RemoveNetwork(ctx, a.dockerNetwork()); err != nil {
		t.Fatal(err)
	}
	info, err := e.c.ContainerInspect(ctx, tn.tracing(), client.ContainerInspectOptions{})
	if err != nil {
		t.Fatalf("removing a network removed the backend: %v", err)
	}
	if !info.Container.State.Running {
		t.Fatal("removing a network stopped the backend")
	}
	reach(t, ctx, e, b)
}

// A network joins only a tenant whose backend is on the host, and stays in the
// tenant its directory records.
func TestChooseTenant(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := testEngine(t, ctx)
	tn, err := newTenant()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Remove(context.Background(), []string{tn.tracing()}, []string{tn.tracing()}) })
	n := testNetwork(t, ctx, e, "tenant")
	dir := t.TempDir()
	ca, err := writeAuthority(dir, tn, []byte("pw"))
	if err != nil {
		t.Fatal(err)
	}
	if err := ensureTracing(ctx, e, n, tn, ca, dir); err != nil {
		t.Fatal(err)
	}

	got, err := chooseTenant(ctx, e, t.TempDir(), string(tn))
	if err != nil || got != tn {
		t.Fatalf("joining %s: %s, %v", tn, got, err)
	}
	other, _ := newTenant()
	if _, err := chooseTenant(ctx, e, t.TempDir(), string(other)); err == nil {
		t.Error("a network joined a tenant that is not on the host")
	}
	if _, err := chooseTenant(ctx, e, t.TempDir(), "Not-A-Tenant"); err == nil {
		t.Error("a network joined a malformed tenant")
	}
	fresh, err := chooseTenant(ctx, e, t.TempDir(), "")
	if err != nil || !tenantPattern.MatchString(string(fresh)) || fresh == tn {
		t.Errorf("a new tenant: %q, %v", fresh, err)
	}

	dir = t.TempDir()
	if err := recordTenant(dir, tn); err != nil {
		t.Fatal(err)
	}
	if got, err := chooseTenant(ctx, e, dir, ""); err != nil || got != tn {
		t.Errorf("the recorded tenant: %s, %v", got, err)
	}
	if _, err := chooseTenant(ctx, e, dir, string(other)); err == nil {
		t.Error("a network moved to another tenant")
	}
}

// A shared container replaced for its image keeps its ports and its networks.
func TestEnsureSharedReplaced(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := testEngine(t, ctx)
	before := testImage(t, ctx, e, "LABEL version=1", `CMD ["sleep", "3600"]`)
	after := testImage(t, ctx, e, "LABEL version=2", `CMD ["sleep", "3600"]`)
	a := testNetwork(t, ctx, e, "shared-a")
	b := testNetwork(t, ctx, e, "shared-b")
	name := "zobik-" + a.network + "-shared"
	t.Cleanup(func() { e.Remove(context.Background(), []string{name}, nil) })
	port := freePort(t)

	spec := Spec{Name: name, Alias: "shared", Network: a.dockerNetwork(), Image: before, Publish: map[string]string{"80/tcp": port}}
	if err := e.EnsureShared(ctx, spec); err != nil {
		t.Fatal(err)
	}
	spec.Network = b.dockerNetwork()
	if err := e.EnsureShared(ctx, spec); err != nil {
		t.Fatal(err)
	}
	spec.Image, spec.Publish = after, nil
	if err := e.EnsureShared(ctx, spec); err != nil {
		t.Fatal(err)
	}

	info, err := e.c.ContainerInspect(ctx, name, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if info.Container.Image != after {
		t.Errorf("the container runs %s, not the new image", info.Container.Image)
	}
	if got := published(info.Container.HostConfig); got["80/tcp"] != port {
		t.Errorf("the replaced container publishes %v, not port %s", got, port)
	}
	for _, net := range []string{a.dockerNetwork(), b.dockerNetwork()} {
		ep := info.Container.NetworkSettings.Networks[net]
		if ep == nil {
			t.Errorf("the replaced container is not on %s", net)
			continue
		}
		if !strings.Contains(strings.Join(ep.Aliases, " "), "shared") {
			t.Errorf("on %s the replaced container has the aliases %v", net, ep.Aliases)
		}
	}
}

func containerID(t *testing.T, ctx context.Context, e *Engine, name string) string {
	t.Helper()
	info, err := e.c.ContainerInspect(ctx, name, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return info.Container.ID
}

// reach asks the backend for its health from a container on n's Docker
// network, by its fixed name, and checks that its query, over HTTP and gRPC,
// asks for TLS there too.
func reach(t *testing.T, ctx context.Context, e *Engine, n names) {
	t.Helper()
	if err := e.EnsureImage(ctx, testBase); err != nil {
		t.Fatal(err)
	}
	probe := func(url string) (string, error) {
		out, err := e.RunEphemeral(ctx, Spec{
			Name:    n.container("probe"),
			Network: n.dockerNetwork(),
			Image:   testBase,
			Cmd:     []string{"wget", "-q", "-T", "5", "-O", "-", url},
		})
		return string(out), err
	}
	out, err := probe("http://" + TracingAlias + ":" + tracingHealth + "/status")
	if err != nil {
		t.Fatalf("from %s: %v", n.network, err)
	}
	if !strings.Contains(out, "StatusOK") {
		t.Fatalf("from %s the backend's health said: %s", n.network, out)
	}
	for _, port := range []string{TracingQuery, tracingGRPC} {
		if out, err := probe("http://" + TracingAlias + ":" + port + "/"); err == nil {
			t.Errorf("from %s the query on %s answered in the clear: %s", n.network, port, out)
		}
	}
}

// query asks the query service on addr for its services over TLS.
func query(ctx context.Context, addr string, config *tls.Config) error {
	c := &http.Client{Transport: &http.Transport{TLSClientConfig: config}, Timeout: 10 * time.Second}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+addr+"/api/v3/services", nil)
	if err != nil {
		return err
	}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s", resp.Status)
	}
	return nil
}

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return strconv.Itoa(l.Addr().(*net.TCPAddr).Port)
}
