package scanner

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"zobik.org/zobik/internal/role"
	scannerv1 "zobik.org/zobik/proto/zobik/scanner/v1"
)

// Socket is where the role serves Scan: a volume only the catalog role mounts,
// which is why it does not authenticate its caller (implementation §1.2.19).
const Socket = "/run/zobik/scanner/scanner.sock"

// TrivyPath is where the role image brings the Trivy binary (implementation §6,
// The roles of the binary).
const TrivyPath = "/usr/local/bin/trivy"

// LabelNRIVersion is the label an image declares its NRI version in
// (implementation §3, The interface version).
const LabelNRIVersion = "io.zobik.nri.version"

// The verdicts of a ScanResult.
const (
	VerdictPass = "pass"
	VerdictFail = "fail"
)

// Options are what the role analyzes with.
type Options struct {
	// Repository is the registry's repository by its fixed name, which every
	// artifact lives in (implementation §1.2.16).
	Repository string
	// CAFile is the tenant CA's certificate, in PEM, which the role verifies
	// the registry with.
	CAFile string
	// Trivy is the Trivy binary.
	Trivy string
	// WorkDir is the role's own directory: Trivy's cache and temporary files.
	WorkDir string
}

// Server is the Admission Scanner's service (implementation §1.2.19).
type Server struct {
	scannerv1.UnimplementedScannerServer
	repo      name.Repository
	transport http.RoundTripper
	trivy     *trivy
	// Checks are the structural checks it applies, and RulesetVersion the
	// version of the ruleset it applies.
	Checks         []Check
	RulesetVersion string

	// mu keeps one analysis at a time, so its memory stays that of one image.
	mu sync.Mutex
}

// New prepares a Server, and checks that the Trivy binary is the one the
// ruleset names.
func New(ctx context.Context, o Options) (*Server, error) {
	repo, err := name.NewRepository(o.Repository)
	if err != nil {
		return nil, err
	}
	ca, err := os.ReadFile(o.CAFile)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, errors.New("scanner: no certificate in the tenant CA")
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.Proxy = nil
	tr.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS13}
	t, err := newTrivy(o.Trivy, o.CAFile, o.WorkDir)
	if err != nil {
		return nil, err
	}
	if v, err := t.version(ctx); err != nil {
		return nil, err
	} else if v != TrivyVersion {
		return nil, fmt.Errorf("scanner: Trivy %s, and the ruleset names %s", v, TrivyVersion)
	}
	return &Server{repo: repo, transport: tr, trivy: t, Checks: Checks, RulesetVersion: RulesetVersion()}, nil
}

// Scan analyzes the image at the request's digest. A digest that does not
// resolve in the registry returns NOT_FOUND.
func (s *Server) Scan(ctx context.Context, req *scannerv1.ScanRequest) (*scannerv1.ScanResult, error) {
	h, err := v1.NewHash(req.ArtifactRef)
	if err != nil || h.Algorithm != "sha256" {
		return nil, status.Errorf(codes.InvalidArgument, "artifact_ref %q is not a sha256 digest", req.ArtifactRef)
	}
	ref := s.repo.Digest(h.String())
	s.mu.Lock()
	defer s.mu.Unlock()

	img, err := remote.Image(ref, remote.WithContext(ctx), remote.WithTransport(s.transport))
	if err != nil {
		var te *transport.Error
		if errors.As(err, &te) && te.StatusCode == http.StatusNotFound {
			return nil, status.Errorf(codes.NotFound, "%s does not resolve in the registry", req.ArtifactRef)
		}
		return nil, status.Errorf(codes.Unavailable, "reading %s: %v", req.ArtifactRef, err)
	}
	cfg, err := img.ConfigFile()
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "reading the configuration of %s: %v", req.ArtifactRef, err)
	}

	findings, err := s.trivy.scan(ctx, ref.String())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "analyzing %s: %v", req.ArtifactRef, err)
	}
	for _, c := range s.Checks {
		broken, err := c.Breaks(ctx, img)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "%s on %s: %v", c.ID, req.ArtifactRef, err)
		}
		if broken {
			findings = append(findings, c.ID)
		}
	}
	slices.Sort(findings)
	findings = slices.Compact(findings)

	verdict := VerdictPass
	if len(findings) > 0 {
		verdict = VerdictFail
	}
	return &scannerv1.ScanResult{
		Verdict:        verdict,
		RulesetVersion: s.RulesetVersion,
		Findings:       findings,
		NriVersion:     cfg.Config.Labels[LabelNRIVersion],
	}, nil
}

// Run is the scanner role: it serves Scan on its socket, without the Bus, until
// ctx ends (implementation §6, The roles of the binary). It is ready once the
// Trivy binary is checked and the socket serves.
func Run(ctx context.Context, o Options, socket string) error {
	srv, err := New(ctx, o)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(socket), 0o750); err != nil {
		return err
	}
	os.Remove(socket) // one a previous process left
	ln, err := net.Listen("unix", socket)
	if err != nil {
		return err
	}
	g := grpc.NewServer()
	scannerv1.RegisterScannerServer(g, srv)
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	role.ServeRPC(cancel, g, ln)
	defer g.Stop()
	if err := role.ServeHealth(ctx, role.HealthSocket, func() bool { return true }); err != nil {
		return err
	}
	log.Printf("scanner: serving %s with ruleset %s", socket, srv.RulesetVersion)
	<-ctx.Done()
	return role.ServeFailure(ctx)
}
