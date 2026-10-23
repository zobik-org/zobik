package deploy

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"testing"
)

// The tenant CA certifies only names under zobik.internal, and the console's
// client certificate opens only with the password.
func TestAuthority(t *testing.T) {
	dir := t.TempDir()
	password := []byte("pw")
	a, err := writeAuthority(dir, "abcdefgh", password)
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

	opened, err := openAuthority(dir, password)
	if err != nil || opened == nil || opened.fingerprint() != a.fingerprint() {
		t.Fatalf("opening the CA: %v", err)
	}
	if _, err := openAuthority(dir, []byte("wrong")); err == nil {
		t.Error("the CA opened with a wrong password")
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

	none := t.TempDir()
	if c, err := OpenTracingClient(none, password); c != nil || err != nil {
		t.Errorf("a network that did not create its tenant: %v, %v", c, err)
	}
	if _, err := TracingAddr(none); !errors.Is(err, ErrNoTracing) {
		t.Errorf("TracingAddr without the certificate: %v", err)
	}
}
