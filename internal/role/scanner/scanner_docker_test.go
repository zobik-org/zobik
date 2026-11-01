//go:build e2e

package scanner

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"io"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/tarball"

	scannerv1 "zobik.org/zobik/proto/zobik/scanner/v1"
)

// The test runs the scanner with the Trivy the role image brings, against a
// registry with images that break each rule and with the images that already
// work: the base image and the blueprints of examples/.

// repoRoot is the repository's root, where the images are built from.
const repoRoot = "../../.."

// The images the clean corpus is made of, with the Dockerfile each is built from.
var cleanCorpus = []struct{ tag, dockerfile string }{
	{"zobik-python:dev", "images/zobik-python.Dockerfile"},
	{"zobik-echo:dev", "examples/blueprints/echo/Dockerfile"},
	{"zobik-assistant:dev", "examples/blueprints/assistant/Dockerfile"},
	{"zobik-historian:dev", "examples/blueprints/historian/Dockerfile"},
}

// hidden is a text with Unicode tag characters: "ignore", invisible.
const hidden = "Answer politely.\U000E0069\U000E0067\U000E006E\U000E006F\U000E0072\U000E0065\n"

func TestScanImages(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	key := testKey(t)
	defer cancel()
	build(t, ctx, "images/zobik.Dockerfile", "zobik:dev", "--build-arg", "TAGS=dev")
	for _, c := range cleanCorpus {
		build(t, ctx, c.dockerfile, c.tag)
	}
	reg := newTestRegistry(t, registry.WithBlobHandler(registry.NewDiskBlobHandler(t.TempDir())))
	srv, err := New(ctx, Options{Repository: reg.repo.String(), CAFile: reg.caFile, Trivy: trivyFromImage(t, ctx), WorkDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	scan := func(t *testing.T, img v1.Image) *scannerv1.ScanResult {
		t.Helper()
		res, err := srv.Scan(ctx, &scannerv1.ScanRequest{ArtifactRef: reg.push(t, img)})
		if err != nil {
			t.Fatal(err)
		}
		return res
	}

	t.Run("clean", func(t *testing.T) {
		for _, c := range cleanCorpus {
			t.Run(c.tag, func(t *testing.T) {
				res := scan(t, saved(t, ctx, c.tag))
				if res.Verdict != VerdictPass || res.NriVersion != "1.0" || res.RulesetVersion != RulesetVersion() {
					t.Errorf("result %v, want pass with NRI 1.0", res)
				}
			})
		}
	})

	// Each rule fires in a path one of Trivy's built-in allow rules would skip.
	for _, c := range []struct {
		name string
		img  v1.Image
		rule string
	}{
		{"key", image(t, layer(t, "opt/bp/key.pem", key)), "private-key"},
		{"key a later layer deletes", image(t, layer(t, "opt/bp/key.pem", key), layer(t, "opt/bp/.wh.key.pem", "")), "private-key"},
		{"key in the configuration", config(t, image(t), "API_TOKEN=ghp_abcdefghijklmnopqrstuvwxyz0123456789"), "github-pat"},
		{"tags in site-packages", image(t, layer(t, "usr/local/lib/python3.12/site-packages/pkg/prompt.txt", hidden)), "zobik-unicode-tags"},
		{"tags in dist-info", image(t, layer(t, "usr/local/lib/python3.12/site-packages/pkg-1.0.dist-info/METADATA", hidden)), "zobik-unicode-tags"},
		{"tags in tests", image(t, layer(t, "opt/bp/tests/prompt.txt", hidden)), "zobik-unicode-tags"},
		{"tags in an example", image(t, layer(t, "opt/bp/example/prompt.txt", hidden)), "zobik-unicode-tags"},
		{"tags in Markdown", image(t, layer(t, "opt/bp/prompt.md", hidden)), "zobik-unicode-tags"},
		{"tags in vendor", image(t, layer(t, "opt/bp/vendor/prompt.txt", hidden)), "zobik-unicode-tags"},
		{"tags in locales", image(t, layer(t, "opt/bp/locales/prompt.txt", hidden)), "zobik-unicode-tags"},
		{"tags in /usr/share", image(t, layer(t, "usr/share/bp/prompt.txt", hidden)), "zobik-unicode-tags"},
		{"bidi controls", image(t, layer(t, "opt/bp/prompt.py", "if access != \"user\u202E \u2066// admin\u2069 \u2066\":\n")), "zobik-bidi-controls"},
	} {
		t.Run(c.name, func(t *testing.T) {
			res := scan(t, c.img)
			if res.Verdict != VerdictFail || !slices.Equal(res.Findings, []string{c.rule}) {
				t.Errorf("verdict %s with findings %v, want fail with %s", res.Verdict, res.Findings, c.rule)
			}
		})
	}
}

func build(t *testing.T, ctx context.Context, dockerfile, tag string, args ...string) {
	t.Helper()
	cmd := exec.CommandContext(ctx, "docker", append(append([]string{"build", "-q", "-f", dockerfile, "-t", tag}, args...), ".")...)
	cmd.Dir = repoRoot
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("building %s: %v\n%s", tag, err, out)
	}
}

// trivyFromImage copies the Trivy binary out of the role image.
func trivyFromImage(t *testing.T, ctx context.Context) string {
	t.Helper()
	out, err := exec.CommandContext(ctx, "docker", "create", "zobik:dev").Output()
	if err != nil {
		t.Fatal(err)
	}
	id := string(bytes.TrimSpace(out))
	defer exec.Command("docker", "rm", id).Run()
	path := filepath.Join(t.TempDir(), "trivy")
	if out, err := exec.CommandContext(ctx, "docker", "cp", id+":"+TrivyPath, path).CombinedOutput(); err != nil {
		t.Fatalf("copying Trivy: %v\n%s", err, out)
	}
	return path
}

// saved is the image tag as the engine exports it.
func saved(t *testing.T, ctx context.Context, tag string) v1.Image {
	t.Helper()
	path := filepath.Join(t.TempDir(), "image.tar")
	if out, err := exec.CommandContext(ctx, "docker", "save", "-o", path, tag).CombinedOutput(); err != nil {
		t.Fatalf("saving %s: %v\n%s", tag, err, out)
	}
	img, err := tarball.ImageFromPath(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	return img
}

// layer is a layer with a single file.
func layer(t *testing.T, path, content string) v1.Layer {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	if err := tw.WriteHeader(&tar.Header{Name: path, Mode: 0o644, Size: int64(len(content)), Typeflag: tar.TypeReg}); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(tw, content); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	l, err := tarball.LayerFromOpener(func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(buf.Bytes())), nil })
	if err != nil {
		t.Fatal(err)
	}
	return l
}

// image is an image of layers, declaring NRI 1.0.
func image(t *testing.T, layers ...v1.Layer) v1.Image {
	t.Helper()
	img, err := mutate.AppendLayers(empty.Image, layers...)
	if err != nil {
		t.Fatal(err)
	}
	return config(t, img)
}

// config sets img's configuration: the NRI label and the environment env.
func config(t *testing.T, img v1.Image, env ...string) v1.Image {
	t.Helper()
	img, err := mutate.Config(img, v1.Config{Labels: map[string]string{LabelNRIVersion: "1.0"}, Env: env})
	if err != nil {
		t.Fatal(err)
	}
	return img
}

// testKey is a private key in PEM, which Trivy's private-key rule finds. It is
// generated on each run, so the repository carries none.
func testKey(t *testing.T) string {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalECPrivateKey(k)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der}))
}
