package catalog

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
	"strings"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"zobik.org/zobik/internal/role"
	"zobik.org/zobik/internal/role/scanner"
	scannerv1 "zobik.org/zobik/proto/zobik/scanner/v1"
)

// The role's fixed port and paths inside its container (implementation §6, The
// tenant CA, the link and the join code).
const (
	Port      = "443"
	CertPath  = "/etc/zobik/catalog.pem"
	KeyPath   = "/etc/zobik/catalog.key"
	AdminPath = "/etc/zobik/catalog_admin.sha256"
	DataDir   = "/var/lib/catalog"
	dbFile    = "catalog.db"
)

// Options are what the role serves with.
type Options struct {
	Listen string
	// CertFile and KeyFile are its server certificate from the tenant CA, and
	// CAFile the CA's certificate, which it verifies the registry with.
	CertFile, KeyFile, CAFile string
	// AdminFile holds the hash of the administration credential.
	AdminFile string
	// DataDir holds the SQLite file.
	DataDir string
	// Repository is the registry's repository by its fixed name
	// (implementation §1.2.16).
	Repository string
	// Scanner is the Admission Scanner's socket.
	Scanner string
	// Capacity is the artifact storage capacity, in bytes.
	Capacity int64
}

// New opens the Catalog over its file at path.
func New(path string, reg *registry, sc Scanner, capacity int64, adminHash string) (*Catalog, error) {
	st, err := openStore(path)
	if err != nil {
		return nil, err
	}
	return &Catalog{store: st, registry: reg, scanner: sc, ruleset: scanner.RulesetVersion(), capacity: capacity, adminHash: adminHash}, nil
}

// Close closes the file.
func (c *Catalog) Close() error { return c.store.close() }

// Run is the catalog role: it serves /v1 over HTTPS, without the Bus, until
// ctx ends, and analyzes again in the background the entries of an earlier
// ruleset (implementation §1.2.19). It is ready once it serves.
func Run(ctx context.Context, o Options) error {
	if o.Capacity <= 0 {
		return errors.New("catalog: the storage capacity is required")
	}
	admin, err := os.ReadFile(o.AdminFile)
	if err != nil {
		return fmt.Errorf("catalog: the administration credential: %w", err)
	}
	ca, err := os.ReadFile(o.CAFile)
	if err != nil {
		return err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return errors.New("catalog: no certificate in the tenant CA")
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.Proxy = nil
	tr.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS13}
	reg, err := newRegistry(o.Repository, tr)
	if err != nil {
		return err
	}
	conn, err := grpc.NewClient("unix://"+o.Scanner, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := os.MkdirAll(o.DataDir, 0o700); err != nil {
		return err
	}
	c, err := New(filepath.Join(o.DataDir, dbFile), reg, scannerv1.NewScannerClient(conn), o.Capacity, strings.TrimSpace(string(admin)))
	if err != nil {
		return err
	}
	defer c.Close()

	cert, err := tls.LoadX509KeyPair(o.CertFile, o.KeyFile)
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", o.Listen)
	if err != nil {
		return err
	}
	srv := &http.Server{
		Handler:           c.Handler(),
		TLSConfig:         &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13},
		ReadHeaderTimeout: 10 * time.Second,
	}
	var ready atomic.Bool
	if err := role.ServeHealth(ctx, role.HealthSocket, ready.Load); err != nil {
		return err
	}
	ready.Store(true)
	go c.Rescan(ctx, time.Minute)
	log.Printf("catalog: serving /v1 on %s with ruleset %s, capacity %d bytes", o.Listen, c.ruleset, o.Capacity)
	return serve(ctx, srv, func() error { return srv.ServeTLS(ln, "", "") })
}

// Rescan analyzes again, one at a time, the entries whose verdict is of
// another ruleset, those running in some network first (implementation
// §1.2.19). A failure to analyze waits retry and starts the pass again; it
// returns once no entry is left, or when ctx ends.
func (c *Catalog) Rescan(ctx context.Context, retry time.Duration) {
	for {
		err := c.rescanPass(ctx)
		if err == nil || ctx.Err() != nil {
			return
		}
		log.Printf("catalog: analyzing again: %v", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(retry):
		}
	}
}

func (c *Catalog) rescanPass(ctx context.Context) error {
	stale, err := c.store.stale(ctx, c.ruleset)
	if err != nil {
		return err
	}
	for _, d := range stale {
		res, err := c.scanner.Scan(ctx, &scannerv1.ScanRequest{ArtifactRef: d})
		if status.Code(err) == codes.NotFound {
			// The image is gone without a deletion mark; it stays with its
			// verdict, and provisioning does not bring it up.
			log.Printf("catalog: %s does not resolve in the registry", d)
			continue
		}
		if err != nil {
			return fmt.Errorf("%s: %w", d, err)
		}
		r := ScanResult{Verdict: res.Verdict, RulesetVersion: res.RulesetVersion, AsOf: c.store.stamp()}
		if res.Verdict != verdictPass {
			r.Findings = res.Findings
		}
		if err := c.store.setScan(ctx, d, r); err != nil {
			return err
		}
		log.Printf("catalog: %s analyzed again: %s", d, res.Verdict)
	}
	return nil
}
