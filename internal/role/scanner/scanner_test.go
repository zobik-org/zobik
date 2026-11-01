package scanner

import (
	"context"
	"encoding/pem"
	"io"
	"log"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/random"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	scannerv1 "zobik.org/zobik/proto/zobik/scanner/v1"
)

// currentRuleset is the ruleset_version of this ruleset. Changing a rule
// changes it, and updating it here is acknowledging that every stored entry is
// analyzed again (implementation §1.2.19).
const currentRuleset = "sha256:31d6f5ddae0780aab37e1163770469d363ba4a9116705eaea39d153d9ae5724f"

func TestRulesetVersion(t *testing.T) {
	if got := RulesetVersion(); got != currentRuleset {
		t.Errorf("ruleset_version %s, want %s: a rule changed, so update currentRuleset", got, currentRuleset)
	}
	check := Check{ID: "zobik-test", Revision: 1}
	with := rulesetVersion([]Check{check})
	if with == rulesetVersion(nil) {
		t.Error("adding a structural check leaves the ruleset_version")
	}
	check.Revision = 2
	if rulesetVersion([]Check{check}) == with {
		t.Error("a check's new revision leaves the ruleset_version")
	}
}

// TestTrivyVersion compares TrivyVersion with the image the role image copies
// the binary from.
func TestTrivyVersion(t *testing.T) {
	b, err := os.ReadFile("../../../images/zobik.Dockerfile")
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`(?m)^FROM ghcr\.io/aquasecurity/trivy:([0-9.]+)@sha256:[0-9a-f]{64} AS trivy$`).FindSubmatch(b)
	if m == nil {
		t.Fatal("images/zobik.Dockerfile copies Trivy from no pinned image")
	}
	if string(m[1]) != TrivyVersion {
		t.Errorf("the role image brings Trivy %s, and TrivyVersion is %s", m[1], TrivyVersion)
	}
}

// fakeTrivy is a Trivy that reports what the test writes to its report file.
type fakeTrivy struct{ path, report string }

func newFakeTrivy(t *testing.T) *fakeTrivy {
	dir := t.TempDir()
	f := &fakeTrivy{path: filepath.Join(dir, "trivy"), report: filepath.Join(dir, "report.json")}
	script := "#!/bin/sh\nif [ \"$1\" = version ]; then echo '{\"Version\":\"" + TrivyVersion + "\"}'; exit 0; fi\nexec /bin/cat " + f.report + "\n"
	if err := os.WriteFile(f.path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	f.reports(t)
	return f
}

// reports makes the fake report the rule identifiers ids.
func (f *fakeTrivy) reports(t *testing.T, ids ...string) {
	var secrets []string
	for _, id := range ids {
		secrets = append(secrets, `{"RuleID":"`+id+`"}`)
	}
	report := `{"Results":[{"Target":"x","Secrets":[` + strings.Join(secrets, ",") + `]}]}`
	if err := os.WriteFile(f.report, []byte(report), 0o644); err != nil {
		t.Fatal(err)
	}
}

// testRegistry is a registry over TLS, with the file of its certificate.
type testRegistry struct {
	repo   name.Repository
	caFile string
	opt    remote.Option
}

func newTestRegistry(t *testing.T, opts ...registry.Option) *testRegistry {
	srv := httptest.NewTLSServer(registry.New(append([]registry.Option{registry.Logger(log.New(io.Discard, "", 0))}, opts...)...))
	t.Cleanup(srv.Close)
	caFile := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}), 0o644); err != nil {
		t.Fatal(err)
	}
	repo, err := name.NewRepository(strings.TrimPrefix(srv.URL, "https://") + "/artifacts")
	if err != nil {
		t.Fatal(err)
	}
	return &testRegistry{repo: repo, caFile: caFile, opt: remote.WithTransport(srv.Client().Transport)}
}

// push pushes img and returns its digest.
func (r *testRegistry) push(t *testing.T, img v1.Image) string {
	t.Helper()
	d, err := img.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.Write(r.repo.Digest(d.String()), img, r.opt); err != nil {
		t.Fatal(err)
	}
	return d.String()
}

func TestScan(t *testing.T) {
	ctx := context.Background()
	reg := newTestRegistry(t)
	trivy := newFakeTrivy(t)
	srv, err := New(ctx, Options{Repository: reg.repo.String(), CAFile: reg.caFile, Trivy: trivy.path, WorkDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}

	img, err := random.Image(64, 1)
	if err != nil {
		t.Fatal(err)
	}
	img, err = mutate.Config(img, v1.Config{Labels: map[string]string{LabelNRIVersion: "1.0"}})
	if err != nil {
		t.Fatal(err)
	}
	labeled := reg.push(t, img)
	unlabeled := reg.push(t, empty.Image)

	t.Run("clean", func(t *testing.T) {
		res, err := srv.Scan(ctx, &scannerv1.ScanRequest{ArtifactRef: labeled})
		if err != nil {
			t.Fatal(err)
		}
		if res.Verdict != VerdictPass || len(res.Findings) != 0 || res.RulesetVersion != RulesetVersion() || res.NriVersion != "1.0" {
			t.Errorf("result %v", res)
		}
	})

	t.Run("findings", func(t *testing.T) {
		trivy.reports(t, "private-key", "zobik-unicode-tags", "private-key")
		srv.Checks = []Check{
			{ID: "zobik-broken", Revision: 1, Breaks: func(context.Context, v1.Image) (bool, error) { return true, nil }},
			{ID: "zobik-kept", Revision: 1, Breaks: func(context.Context, v1.Image) (bool, error) { return false, nil }},
		}
		t.Cleanup(func() { trivy.reports(t); srv.Checks = nil })
		res, err := srv.Scan(ctx, &scannerv1.ScanRequest{ArtifactRef: labeled})
		if err != nil {
			t.Fatal(err)
		}
		want := []string{"private-key", "zobik-broken", "zobik-unicode-tags"}
		if res.Verdict != VerdictFail || !slices.Equal(res.Findings, want) {
			t.Errorf("verdict %s with findings %v, want fail with %v", res.Verdict, res.Findings, want)
		}
	})

	t.Run("no label", func(t *testing.T) {
		res, err := srv.Scan(ctx, &scannerv1.ScanRequest{ArtifactRef: unlabeled})
		if err != nil {
			t.Fatal(err)
		}
		if res.Verdict != VerdictPass || res.NriVersion != "" {
			t.Errorf("result %v, want pass with no NRI version", res)
		}
	})

	for _, c := range []struct {
		name, ref string
		code      codes.Code
	}{
		{"not a digest", "artifacts:latest", codes.InvalidArgument},
		{"unresolvable", "sha256:" + strings.Repeat("0", 64), codes.NotFound},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := srv.Scan(ctx, &scannerv1.ScanRequest{ArtifactRef: c.ref})
			if status.Code(err) != c.code {
				t.Errorf("error %v, want %s", err, c.code)
			}
		})
	}
}

func TestNewRejectsAnotherTrivy(t *testing.T) {
	reg := newTestRegistry(t)
	trivy := newFakeTrivy(t)
	script := "#!/bin/sh\necho '{\"Version\":\"0.0.1\"}'\n"
	if err := os.WriteFile(trivy.path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := New(context.Background(), Options{Repository: reg.repo.String(), CAFile: reg.caFile, Trivy: trivy.path, WorkDir: t.TempDir()}); err == nil {
		t.Error("New accepts a Trivy the ruleset does not name")
	}
}
