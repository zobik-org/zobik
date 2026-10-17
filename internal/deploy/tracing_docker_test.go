//go:build e2e

package deploy

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net"
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
// its fixed name, and removing one leaves it.
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

	if err := ensureTracing(ctx, e, a, tn, ""); err != nil {
		t.Fatal(err)
	}
	first := containerID(t, ctx, e, tn.tracing())
	if err := ensureTracing(ctx, e, b, tn, ""); err != nil {
		t.Fatal(err)
	}
	if err := ensureTracing(ctx, e, a, tn, ""); err != nil { // converges
		t.Fatal(err)
	}
	if id := containerID(t, ctx, e, tn.tracing()); id != first {
		t.Fatalf("the second network replaced the backend: %s, then %s", first, id)
	}
	reach(t, ctx, e, a)
	reach(t, ctx, e, b)

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
	if err := ensureTracing(ctx, e, n, tn, ""); err != nil {
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

	dir := t.TempDir()
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

// reach asks the query service for its services from a container on n's
// Docker network, by the backend's fixed name.
func reach(t *testing.T, ctx context.Context, e *Engine, n names) {
	t.Helper()
	if err := e.EnsureImage(ctx, testBase); err != nil {
		t.Fatal(err)
	}
	out, err := e.RunEphemeral(ctx, Spec{
		Name:    n.container("probe"),
		Network: n.dockerNetwork(),
		Image:   testBase,
		Cmd:     []string{"wget", "-q", "-O", "-", "http://" + TracingAlias + ":" + TracingQuery + "/api/v3/services"},
	})
	if err != nil {
		t.Fatalf("from %s: %v", n.network, err)
	}
	if !strings.Contains(string(out), "services") {
		t.Fatalf("from %s the query service said: %s", n.network, out)
	}
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
