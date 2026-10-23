package deploy

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"io/fs"
	"math/big"
	"os"
	"path/filepath"
	"time"

	"zobik.org/zobik/internal/console"
)

// The tenant CA in the console's directory of the network that creates the
// tenant (implementation §6, The tenant CA, the link and the join code):
//
//	tenant_ca.pem          the CA's certificate
//	tenant_ca.sealed       its key, encrypted with the password
//	tracing_client.pem     the console's client certificate for the tracing
//	                       backend's query (implementation §1.2.17)
//	tracing_client.sealed  its key, encrypted with the password
//
// tenant_ca.sealed is written last: its presence means the rest is complete.
const (
	fileCACert          = "tenant_ca.pem"
	fileCAKey           = "tenant_ca.sealed"
	fileTracingCert     = "tracing_client.pem"
	fileTracingKey      = "tracing_client.sealed"
	tenantDomain        = "zobik.internal"
	consoleName         = "console." + tenantDomain
	certificateLifetime = 10 * 365 * 24 * time.Hour
)

// authority is the tenant CA with its key open.
type authority struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
}

// mintAuthority mints the tenant CA, which signs only names under
// zobik.internal.
func mintAuthority(t tenant) (*authority, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber:                serial(),
		Subject:                     pkix.Name{CommonName: "zobik tenant " + string(t)},
		NotBefore:                   now.Add(-time.Minute),
		NotAfter:                    now.Add(certificateLifetime),
		IsCA:                        true,
		BasicConstraintsValid:       true,
		MaxPathLenZero:              true,
		KeyUsage:                    x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		PermittedDNSDomains:         []string{tenantDomain},
		PermittedDNSDomainsCritical: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}
	return &authority{cert: cert, key: key}, nil
}

// issue issues the certificate of name for usage, and returns it and its key
// in PEM.
func (a *authority) issue(name string, usage x509.ExtKeyUsage) (certPEM, keyPEM []byte, err error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: serial(),
		Subject:      pkix.Name{CommonName: name},
		DNSNames:     []string{name},
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(certificateLifetime),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{usage},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, a.cert, &key.PublicKey, a.key)
	if err != nil {
		return nil, nil, err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, nil, err
	}
	return pemBlock("CERTIFICATE", der), pemBlock("PRIVATE KEY", keyDER), nil
}

func (a *authority) certPEM() []byte { return pemBlock("CERTIFICATE", a.cert.Raw) }

// fingerprint is the SHA-256 of the CA's certificate, in hex.
func (a *authority) fingerprint() string {
	sum := sha256.Sum256(a.cert.Raw)
	return hex.EncodeToString(sum[:])
}

// writeAuthority mints the tenant CA and the console's client certificate,
// and writes them to dir sealed with the password.
func writeAuthority(dir string, t tenant, password []byte) (*authority, error) {
	a, err := mintAuthority(t)
	if err != nil {
		return nil, err
	}
	clientCert, clientKey, err := a.issue(consoleName, x509.ExtKeyUsageClientAuth)
	if err != nil {
		return nil, err
	}
	caKey, err := x509.MarshalPKCS8PrivateKey(a.key)
	if err != nil {
		return nil, err
	}
	if err := writeFile(dir, fileTracingCert, clientCert); err != nil {
		return nil, err
	}
	if err := writeSecret(dir, fileTracingKey, clientKey, password); err != nil {
		return nil, err
	}
	if err := writeFile(dir, fileCACert, a.certPEM()); err != nil {
		return nil, err
	}
	return a, writeSecret(dir, fileCAKey, pemBlock("PRIVATE KEY", caKey), password)
}

// openAuthority opens the tenant CA in dir with the password, or returns nil
// if the network did not create its tenant.
func openAuthority(dir string, password []byte) (*authority, error) {
	sealed, err := os.ReadFile(filepath.Join(dir, fileCAKey))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	keyPEM, err := console.OpenSecret(sealed, password)
	if err != nil {
		return nil, err
	}
	certPEM, err := os.ReadFile(filepath.Join(dir, fileCACert))
	if err != nil {
		return nil, err
	}
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("deploy: tenant CA: %w", err)
	}
	key, ok := pair.PrivateKey.(*ecdsa.PrivateKey)
	if !ok {
		return nil, errors.New("deploy: tenant CA: the key is not ECDSA")
	}
	return &authority{cert: pair.Leaf, key: key}, nil
}

// OpenTracingClient returns the TLS with which the console reaches the tracing
// backend's query (implementation §1.2.17), opened with the password, or nil if
// the network did not create its tenant.
func OpenTracingClient(dir string, password []byte) (*tls.Config, error) {
	sealed, err := os.ReadFile(filepath.Join(dir, fileTracingKey))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	keyPEM, err := console.OpenSecret(sealed, password)
	if err != nil {
		return nil, err
	}
	certPEM, err := os.ReadFile(filepath.Join(dir, fileTracingCert))
	if err != nil {
		return nil, err
	}
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	clear(keyPEM)
	if err != nil {
		return nil, fmt.Errorf("deploy: tracing client certificate: %w", err)
	}
	caPEM, err := os.ReadFile(filepath.Join(dir, fileCACert))
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("deploy: tenant CA: no certificate")
	}
	return &tls.Config{
		Certificates: []tls.Certificate{pair},
		RootCAs:      pool,
		ServerName:   TracingAlias,
		MinVersion:   tls.VersionTLS13,
	}, nil
}

func writeSecret(dir, name string, secret, password []byte) error {
	sealed, err := console.SealSecret(secret, password)
	if err != nil {
		return err
	}
	return writeFile(dir, name, sealed)
}

func pemBlock(kind string, der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: kind, Bytes: der})
}

func serial() *big.Int {
	n, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 127))
	if err != nil {
		panic(err)
	}
	return n
}
