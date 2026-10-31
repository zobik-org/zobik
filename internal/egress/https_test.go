package egress

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// The adapter trusts a destination with a certificate from the tenant CA, and
// the CA's name constraint keeps it from certifying a name outside
// zobik.internal (implementation §1.2.12).
func TestTenantCA(t *testing.T) {
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	caTmpl := &x509.Certificate{
		SerialNumber:                big.NewInt(1),
		Subject:                     pkix.Name{CommonName: "zobik tenant test"},
		NotBefore:                   time.Now().Add(-time.Minute),
		NotAfter:                    time.Now().Add(time.Hour),
		IsCA:                        true,
		BasicConstraintsValid:       true,
		KeyUsage:                    x509.KeyUsageCertSign,
		PermittedDNSDomains:         []string{"zobik.internal"},
		PermittedDNSDomainsCritical: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	ca, _ := x509.ParseCertificate(caDER)
	roots, err := Roots(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}))
	if err != nil {
		t.Fatal(err)
	}

	// serve answers over TLS with a certificate from the CA for name, and
	// returns a client of the adapter that reaches it at any name.
	serve := func(name string) *http.Client {
		key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		der, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{
			SerialNumber: big.NewInt(2),
			DNSNames:     []string{name},
			NotBefore:    time.Now().Add(-time.Minute),
			NotAfter:     time.Now().Add(time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		}, ca, &key.PublicKey, caKey)
		if err != nil {
			t.Fatal(err)
		}
		srv := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		srv.TLS = &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}}
		srv.StartTLS()
		t.Cleanup(srv.Close)
		c := HTTPClient(roots)
		c.Transport.(*http.Transport).DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, srv.Listener.Addr().String())
		}
		return c
	}

	resp, err := serve("catalog.zobik.internal").Get("https://catalog.zobik.internal/")
	if err != nil {
		t.Fatalf("a piece of the tenant: %v", err)
	}
	resp.Body.Close()

	resp, err = serve("api.example.com").Get("https://api.example.com/")
	if err == nil {
		resp.Body.Close()
	}
	var invalid x509.CertificateInvalidError
	if !errors.As(err, &invalid) || invalid.Reason != x509.CANotAuthorizedForThisName {
		t.Errorf("a name outside zobik.internal: %v", err)
	}

	if r, err := Roots(nil); r != nil || err != nil {
		t.Errorf("without the tenant CA: %v, %v", r, err)
	}
	if _, err := Roots([]byte("not a certificate")); err == nil {
		t.Error("a tenant CA without a certificate was accepted")
	}
}
