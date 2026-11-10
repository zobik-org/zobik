//go:build e2e

package e2e

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"zobik.org/zobik/internal/deploy"
	"zobik.org/zobik/internal/role/catalog"
)

// El Catalog: zobik init brings up the tenant's Catalog with its Admission
// Scanner and gives the network its credentials; zobik register runs the act of
// registration against zot and the real analysis.
func TestCatalog(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	z := newNetwork(t, ctx)
	z.init(t, ctx)
	z.blueprint(t, ctx, "examples/blueprints/echo/Dockerfile", "zobik-echo:dev")
	blueprint := filepath.Join(z.root, "examples/blueprints/echo/blueprint.json")

	register := func(args ...string) (string, error) {
		out, err := z.cmd(ctx, "", append([]string{"register"}, args...)...).CombinedOutput()
		return string(out), err
	}
	out, err := register("--topic", "echo", "--topic-description", topicDescription("echo"), blueprint)
	if err != nil {
		t.Fatalf("registering echo: %v\n%s", err, out)
	}
	var e struct {
		Topic        string             `json:"topic"`
		ArtifactRef  string             `json:"artifact_ref"`
		ScanResult   catalog.ScanResult `json:"scan_result"`
		NRIVersion   string             `json:"nri_version"`
		ArtifactSize int64              `json:"artifact_size"`
	}
	if err := json.Unmarshal([]byte(lastLine(out)), &e); err != nil {
		t.Fatalf("register said: %s", out)
	}
	if e.Topic != "echo" || e.ScanResult.Verdict != "pass" || e.NRIVersion != "1.0" || e.ArtifactSize <= 0 || e.ArtifactSize > 10<<20 {
		t.Errorf("entry %+v: want pass with NRI 1.0 and the size of echo's own layer", e)
	}
	if again, err := register("--topic", "echo", "--topic-description", topicDescription("echo"), blueprint); err != nil || lastLine(again) != lastLine(out) {
		t.Errorf("registering twice: %v\n%s", err, again)
	}

	// An embedded private key fails the analysis, and an image without the NRI
	// label is not registered.
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	der, _ := x509.MarshalECPrivateKey(key)
	base := strings.TrimSpace(z.run(t, ctx, "", "base-image"))
	for _, c := range []struct {
		name, file, dockerfile, reason string
	}{
		{"embedded key", "key.pem", "FROM " + base + "\nCOPY key.pem /opt/app/key.pem\n", catalog.ReasonScanFailed},
		{"no NRI label", "key.pem", "FROM " + base + "\nLABEL io.zobik.nri.version=\"\"\n", catalog.ReasonNRIVersionMissing},
	} {
		dir := t.TempDir()
		os.WriteFile(filepath.Join(dir, c.file), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der}), 0o644)
		os.WriteFile(filepath.Join(dir, "Dockerfile"), []byte(c.dockerfile), 0o644)
		tag := "zobik-catalog-e2e:" + strings.ReplaceAll(c.name, " ", "-")
		build := exec.CommandContext(ctx, "docker", "build", "-q", "-t", tag, dir)
		if b, err := build.CombinedOutput(); err != nil {
			t.Fatalf("building %s: %v\n%s", tag, err, b)
		}
		t.Cleanup(func() { exec.Command("docker", "rmi", "-f", tag).Run() })
		bp := filepath.Join(dir, "blueprint.json")
		os.WriteFile(bp, []byte(`{"artifact_ref":"`+tag+`","capability_text":"x","requires":{}}`), 0o644)
		if out, err := register("--topic", "rejected", "--topic-description", topicDescription("rejected"), bp); err == nil || !strings.Contains(out, c.reason) {
			t.Errorf("%s: %v, want %s\n%s", c.name, err, c.reason, out)
		}
	}

	// The network's read credential reads and does not register.
	read, err := deploy.ReadSecret(z.dir, deploy.HandleCatalogRead)
	if err != nil || read == nil {
		t.Fatalf("the read credential: %v", err)
	}
	c, err := deploy.CatalogClient(z.dir, string(read))
	if err != nil {
		t.Fatal(err)
	}
	// The registration of echo registered its topic (architecture §3.7.6).
	if topic, ok, err := c.Topic(ctx, "echo"); err != nil || !ok || topic.Description != topicDescription("echo") {
		t.Errorf("the topic echo: %+v %v %v", topic, ok, err)
	}
	st, err := c.Status(ctx)
	if err != nil || st.Occupied != e.ArtifactSize || st.Capacity <= 0 {
		t.Errorf("status %+v, %v: want echo's size occupied", st, err)
	}
	var ce *catalog.Error
	if _, err := c.Register(ctx, catalog.Registration{Topic: "echo", ArtifactRef: e.ArtifactRef, CapabilityText: "x", AuthorID: "x", Requires: map[string]json.RawMessage{}}); !errors.As(err, &ce) || ce.Status != http.StatusForbidden {
		t.Errorf("registering with read: %v, want 403", err)
	}
}

func lastLine(s string) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	return lines[len(lines)-1]
}
