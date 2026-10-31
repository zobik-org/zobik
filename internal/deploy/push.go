package deploy

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	"github.com/google/go-containerregistry/pkg/v1/types"
	"github.com/google/uuid"
)

// The console pushes an image to the registry from an ephemeral container on
// the network's Docker network, because the link's port only reads
// (implementation §6, The Docker network). It copies in the image as the engine
// exports it and the tenant CA, and the container prints the digest.
const (
	// PushCommand is the subcommand the ephemeral container runs: zobik
	// registry-push <tag prefix>.
	PushCommand   = "registry-push"
	pushImagePath = "/run/zobik/image.tar"
	pushCAPath    = "/run/zobik/tenant_ca.pem"
	pushedPrefix  = "pushed "
)

// digestPattern is an artifact_ref: the digest of an image's manifest
// (implementation §1.2.16).
var digestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// IsDigest says whether ref is a digest, which is what an artifact_ref is.
func IsDigest(ref string) bool { return digestPattern.MatchString(ref) }

// Tag is the tag prefix-<digest> marks digest with: a tag does not admit ':'
// (implementation §1.2.16).
func Tag(prefix, digest string) string {
	return prefix + "-" + strings.Replace(digest, ":", "-", 1)
}

// Push pushes the image the engine exported to path to the repository of the
// registry at addr, tagged prefix-<digest>, and returns its digest.
func Push(ctx context.Context, path, addr, prefix string, transport http.RoundTripper, opts ...name.Option) (string, error) {
	saved, err := tarball.ImageFromPath(path, nil)
	if err != nil {
		return "", fmt.Errorf("reading the image: %w", err)
	}
	img, err := ociImage(saved)
	if err != nil {
		return "", err
	}
	d, err := img.Digest()
	if err != nil {
		return "", err
	}
	ref, err := name.NewTag(addr+"/"+Repository+":"+Tag(prefix, d.String()), opts...)
	if err != nil {
		return "", err
	}
	if err := remote.Write(ref, img, remote.WithContext(ctx), remote.WithTransport(transport)); err != nil {
		return "", fmt.Errorf("pushing to %s: %w", addr, err)
	}
	return d.String(), nil
}

// ociImage is img with the OCI media types, which the registry takes: the
// engine exports the Docker ones. The layers and the configuration stay the same.
func ociImage(img v1.Image) (v1.Image, error) {
	layers, err := img.Layers()
	if err != nil {
		return nil, err
	}
	config, err := img.ConfigFile()
	if err != nil {
		return nil, err
	}
	adds := make([]mutate.Addendum, len(layers))
	for i, l := range layers {
		adds[i] = mutate.Addendum{Layer: l, MediaType: types.OCILayer}
	}
	oci := mutate.ConfigMediaType(mutate.MediaType(empty.Image, types.OCIManifestSchema1), types.OCIConfigJSON)
	if oci, err = mutate.Append(oci, adds...); err != nil {
		return nil, err
	}
	return mutate.ConfigFile(oci, config)
}

// RunPush executes the push inside the ephemeral container, against the
// registry by its fixed name.
func RunPush(ctx context.Context, prefix string) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	caPEM, err := os.ReadFile(pushCAPath)
	if err != nil {
		return err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return errors.New("no certificate in the tenant CA")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS13}
	d, err := Push(ctx, pushImagePath, RegistryAlias+":"+RegistryPort, prefix, transport)
	if err != nil {
		return err
	}
	fmt.Println(pushedPrefix + d)
	return nil
}

// pushImage pushes the image the engine has as ref to the tenant's registry,
// tagged prefix-<digest>, from an ephemeral container of image on n's Docker
// network, and returns its digest.
func pushImage(ctx context.Context, e *Engine, n names, image, ref, prefix string, tenantCA []byte) (string, error) {
	saved, done, err := e.SaveImage(ctx, ref)
	if err != nil {
		return "", fmt.Errorf("deploy: exporting %s: %w", ref, err)
	}
	defer done()
	info, err := saved.Stat()
	if err != nil {
		return "", err
	}
	out, err := e.RunEphemeral(ctx, Spec{
		Name:    n.container("act-push-" + uuid.NewString()[:8]),
		Network: n.dockerNetwork(),
		Labels:  n.labels(),
		Image:   image,
		Cmd:     []string{PushCommand, prefix},
		Files:   []File{{Path: pushCAPath, Mode: 0o644, Data: tenantCA}},
		Blob:    &Blob{Path: pushImagePath, Size: info.Size(), Content: saved},
	})
	if err != nil {
		return "", fmt.Errorf("deploy: pushing %s: %w", ref, err)
	}
	for line := range bytes.Lines(out) {
		if d, ok := strings.CutPrefix(strings.TrimSpace(string(line)), pushedPrefix); ok && IsDigest(d) {
			return d, nil
		}
	}
	return "", fmt.Errorf("deploy: pushing %s: no digest in %q", ref, out)
}

// PushImage pushes the image the engine has as ref to the registry of the
// tenant of the network in dir, tagged prefix-<digest>, and returns its digest.
func PushImage(ctx context.Context, e *Engine, dir, network, image, ref, prefix string) (string, error) {
	root, err := readRoot(dir)
	if err != nil {
		return "", err
	}
	t, ok, err := recordedTenant(dir)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", ErrNoRegistry
	}
	tenantCA, err := readTenantCA(tenantDir(dir, t))
	if err != nil {
		return "", err
	}
	return pushImage(ctx, e, names{network, root}, image, ref, prefix, tenantCA)
}

// The base image in the tenant's directory: the digest the registry has it
// under, and the engine's identifier of the image it was pushed from, which
// spares pushing the same one again.
const (
	fileBase       = "base"
	fileBaseSource = "base_source"
)

// seedBase pushes the Logic Container base image the engine has as ref to the
// tenant's registry, tagged base-<digest>, and records it in the tenant's
// directory tdir (implementation §1.2.13, §1.2.16).
func seedBase(ctx context.Context, e *Engine, n names, image, ref, tdir string, tenantCA []byte) error {
	if err := e.EnsureImage(ctx, ref); err != nil {
		return fmt.Errorf("deploy: the base image %s: %w", ref, err)
	}
	img, err := e.InspectImage(ctx, ref)
	if err != nil {
		return fmt.Errorf("deploy: the base image %s: %w", ref, err)
	}
	recorded, err := os.ReadFile(filepath.Join(tdir, fileBaseSource))
	if err == nil && string(recorded) == img.ID {
		return nil
	}
	d, err := pushImage(ctx, e, n, image, ref, TagBase, tenantCA)
	if err != nil {
		return err
	}
	if err := writeFile(tdir, fileBase, []byte(d)); err != nil {
		return err
	}
	return writeFile(tdir, fileBaseSource, []byte(img.ID))
}

// BaseRef is the reference the engine pulls the base image of the tenant of the
// network in dir by, through the link: what a blueprint is built on.
func BaseRef(dir string) (string, error) {
	addr, err := RegistryAddr(dir)
	if err != nil {
		return "", err
	}
	t, _, err := recordedTenant(dir)
	if err != nil {
		return "", err
	}
	d, err := os.ReadFile(filepath.Join(tenantDir(dir, t), fileBase))
	if err != nil {
		return "", fmt.Errorf("deploy: the tenant's base image: %w", err)
	}
	return PullRef(addr, string(d)), nil
}
