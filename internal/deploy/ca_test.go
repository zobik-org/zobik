package deploy

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// The tenant CA certifies only names under zobik.internal, and the console's
// client certificate opens only with the password of the network that created
// the tenant.
func TestAuthority(t *testing.T) {
	home := t.TempDir()
	tn := tenant("abcdefgh")
	dir := NetworkDir(home, "a")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := recordTenant(dir, tn); err != nil {
		t.Fatal(err)
	}
	tdir := tenantDir(dir, tn)
	if tdir != filepath.Join(home, dirTenants, string(tn)) {
		t.Fatalf("the tenant's directory is %s", tdir)
	}
	password := []byte("pw")
	a, err := writeAuthority(tdir, tn, "a", password)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(a.cert)

	verify := func(name string, usage x509.ExtKeyUsage) error {
		certPEM, _, err := a.issue(name, usage)
		if err != nil {
			t.Fatal(err)
		}
		block, _ := pem.Decode(certPEM)
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			t.Fatal(err)
		}
		_, err = cert.Verify(x509.VerifyOptions{Roots: pool, DNSName: name, KeyUsages: []x509.ExtKeyUsage{usage}})
		return err
	}
	if err := verify(TracingAlias, x509.ExtKeyUsageServerAuth); err != nil {
		t.Errorf("the tracing backend's certificate: %v", err)
	}
	if err := verify("example.com", x509.ExtKeyUsageServerAuth); err == nil {
		t.Error("the CA certified a public name")
	}

	opened, err := openAuthority(tdir, password)
	if err != nil || opened == nil || opened.fingerprint() != a.fingerprint() {
		t.Fatalf("opening the CA: %v", err)
	}
	if _, err := openAuthority(tdir, []byte("wrong")); err == nil {
		t.Error("the CA opened with a wrong password")
	}
	if ca, err := tenantAuthority(tdir, "a", tn, false, password); err != nil || ca == nil || ca.fingerprint() != a.fingerprint() {
		t.Errorf("the creator's tenantAuthority: %v", err)
	}
	if ca, err := tenantAuthority(tdir, "b", tn, false, []byte("other")); err != nil || ca != nil {
		t.Errorf("a joined network's tenantAuthority: %v, %v", ca, err)
	}
	if _, err := tenantAuthority(filepath.Join(home, dirTenants, "zzzzzzzz"), "b", "zzzzzzzz", false, password); err == nil {
		t.Error("a network joined a tenant without a CA")
	}

	client, err := OpenTracingClient(dir, password)
	if err != nil {
		t.Fatal(err)
	}
	leaf := client.Certificates[0].Leaf
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: pool, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}); err != nil || leaf.DNSNames[0] != consoleName {
		t.Errorf("the console's client certificate %v: %v", leaf.DNSNames, err)
	}
	if client.ServerName != TracingAlias || client.MinVersion != tls.VersionTLS13 {
		t.Errorf("the client's TLS: %+v", client)
	}
	if _, err := OpenTracingClient(dir, []byte("wrong")); err == nil {
		t.Error("the client certificate opened with a wrong password")
	}

	// A network that joined the tenant reads the CA's certificate and opens nothing.
	joined := NetworkDir(home, "b")
	if err := os.MkdirAll(joined, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := recordTenant(joined, tn); err != nil {
		t.Fatal(err)
	}
	if c, err := OpenTracingClient(joined, []byte("other")); c != nil || err != nil {
		t.Errorf("a network that joined its tenant: %v, %v", c, err)
	}
	if _, err := TracingAddr(joined); !errors.Is(err, ErrNoTracing) {
		t.Errorf("TracingAddr of a joined network: %v", err)
	}
	if b, err := readTenantCA(tenantDir(joined, tn)); err != nil || string(b) != string(a.certPEM()) {
		t.Errorf("the CA's certificate from a joined network: %v", err)
	}

	none := NetworkDir(home, "c")
	if c, err := OpenTracingClient(none, password); c != nil || err != nil {
		t.Errorf("a network without a tenant: %v, %v", c, err)
	}
	if _, err := TracingAddr(none); !errors.Is(err, ErrNoTracing) {
		t.Errorf("TracingAddr without a tenant: %v", err)
	}
}

// A network directory that keeps the tenant CA is not converted.
func TestLegacyAuthority(t *testing.T) {
	dir := t.TempDir()
	if err := checkLegacyAuthority(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, fileCAKey), []byte("sealed"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := checkLegacyAuthority(dir); err == nil {
		t.Error("a network directory with the tenant CA was accepted")
	}
}
