package scanner

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// scanTimeout bounds one Trivy run over an image.
const scanTimeout = 10 * time.Minute

// trivy runs the Trivy binary with secret scanning alone and the own patterns
// (implementation §1.2.19).
type trivy struct {
	path string
	// env is all Trivy's environment: its home and temporary directory inside
	// the role's work directory, and the tenant CA it verifies the registry with.
	env []string
	// config is the patterns file. Trivy skips a file of the image whose path is
	// a suffix of this one, so its name is random.
	config string
	cache  string
}

func newTrivy(path, caFile, work string) (*trivy, error) {
	tmp := filepath.Join(work, "tmp")
	if err := os.MkdirAll(tmp, 0o700); err != nil {
		return nil, err
	}
	dir, err := os.MkdirTemp(work, "patterns-")
	if err != nil {
		return nil, err
	}
	f, err := os.CreateTemp(dir, "*.yaml")
	if err != nil {
		return nil, err
	}
	if _, err := f.Write(patterns); err != nil {
		f.Close()
		return nil, err
	}
	if err := f.Close(); err != nil {
		return nil, err
	}
	return &trivy{
		path:   path,
		env:    []string{"HOME=" + work, "TMPDIR=" + tmp, "SSL_CERT_FILE=" + caFile},
		config: f.Name(),
		cache:  filepath.Join(work, "cache"),
	}, nil
}

// version is the version of the binary.
func (t *trivy) version(ctx context.Context) (string, error) {
	out, err := t.run(ctx, "version", "--format", "json")
	if err != nil {
		return "", err
	}
	var v struct{ Version string }
	if err := json.Unmarshal(out, &v); err != nil {
		return "", fmt.Errorf("trivy version: %w", err)
	}
	return v.Version, nil
}

// scan returns the identifiers of the rules the image at ref breaks, in its
// files layer by layer and in its configuration. It consults no feed: no
// database, no version check and no telemetry (implementation §1.2.19).
func (t *trivy) scan(ctx context.Context, ref string) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, scanTimeout)
	defer cancel()
	out, err := t.run(ctx, "image",
		"--image-src", "remote",
		"--scanners", "secret",
		"--image-config-scanners", "secret",
		"--secret-config", t.config,
		"--skip-db-update", "--skip-java-db-update", "--offline-scan",
		"--skip-version-check", "--disable-telemetry",
		"--cache-dir", t.cache,
		"--timeout", scanTimeout.String(),
		"--quiet", "--format", "json",
		ref)
	if err != nil {
		return nil, err
	}
	var report struct {
		Results []struct {
			Secrets []struct{ RuleID string }
		}
	}
	if err := json.Unmarshal(out, &report); err != nil {
		return nil, fmt.Errorf("trivy report: %w", err)
	}
	var ids []string
	for _, r := range report.Results {
		for _, s := range r.Secrets {
			ids = append(ids, s.RuleID)
		}
	}
	return ids, nil
}

// stderrTail is how much of Trivy's error output an error carries.
const stderrTail = 2048

func (t *trivy) run(ctx context.Context, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, t.path, args...) //nolint:gosec // the binary the role image brings; the only argument from a request is a reference built from a validated digest
	cmd.Env = t.env
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if len(msg) > stderrTail {
			msg = msg[len(msg)-stderrTail:]
		}
		return nil, fmt.Errorf("trivy %s: %w: %s", args[0], err, msg)
	}
	return stdout.Bytes(), nil
}
