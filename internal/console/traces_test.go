package console_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"zobik.org/zobik/internal/console"
)

// tracingBackend stands for the tracing backend's query: it answers only a
// client with a certificate from its CA, and reports the path and whether a
// cookie reached it.
func tracingBackend(t *testing.T) (addr string, client *tls.Config) {
	t.Helper()
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	caTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test CA"},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	ca, _ := x509.ParseCertificate(caDER)
	leaf := func(name string, usage x509.ExtKeyUsage) tls.Certificate {
		key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		tmpl := &x509.Certificate{
			SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: name}, DNSNames: []string{name},
			NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
			KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage},
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
		if err != nil {
			t.Fatal(err)
		}
		return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
	}
	pool := x509.NewCertPool()
	pool.AddCert(ca)

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(r.URL.Path + " cookie=" + r.Header.Get("Cookie")))
	}))
	srv.TLS = &tls.Config{
		Certificates: []tls.Certificate{leaf("tracing.zobik.internal", x509.ExtKeyUsageServerAuth)},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pool,
	}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv.Listener.Addr().String(), &tls.Config{
		Certificates: []tls.Certificate{leaf("console.zobik.internal", x509.ExtKeyUsageClientAuth)},
		RootCAs:      pool,
		ServerName:   "tracing.zobik.internal",
	}
}

// withTracing gives the fixture's console the tracing backend at addr, reached
// with client, and serves its UI; it returns the UI's port.
func withTracing(t *testing.T, addr string, client *tls.Config) (func(*console.Options), func(*fixture) string) {
	edit := func(o *console.Options) {
		o.TracingAddr = addr
		open := o.Open
		o.Open = func(u string, pw []byte) (*console.Keys, error) {
			k, err := open(u, pw)
			if k != nil {
				k.Tracing = client
			}
			return k, err
		}
	}
	serve := func(f *fixture) string {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		_, port, _ := net.SplitHostPort(ln.Addr().String())
		ctx, cancel := context.WithCancel(f.Ctx)
		done := make(chan struct{})
		go func() { defer close(done); f.svc.ServeTraces(ctx, ln) }()
		t.Cleanup(func() { cancel(); <-done })
		return port
	}
	return edit, serve
}

// sessionCookie signs in through the single-use link and returns the cookie
// the browser keeps.
func (f *fixture) sessionCookie() *http.Cookie {
	f.T.Helper()
	path, err := f.svc.Link(user, []byte(password))
	if err != nil {
		f.T.Fatal(err)
	}
	noRedirect := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := noRedirect.Get(f.base + path)
	if err != nil {
		f.T.Fatal(err)
	}
	resp.Body.Close()
	for _, c := range resp.Cookies() {
		if c.Name == console.CookieSession {
			return c
		}
	}
	f.T.Fatal("no session cookie")
	return nil
}

func get(t *testing.T, method, url, host string, cookie *http.Cookie) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(method, url, nil)
	if host != "" {
		req.Host = host
	}
	if cookie != nil {
		req.AddCookie(cookie)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// The console serves the tracing backend's UI on its own port, only with a
// session open, only to read, and reaches the backend with its client
// certificate without passing the session on.
func TestTraceUI(t *testing.T) {
	addr, client := tracingBackend(t)
	edit, serve := withTracing(t, addr, client)
	f := newFixture(t, edit)
	port := serve(f)
	ui := "http://127.0.0.1:" + port

	if s, _ := get(t, http.MethodGet, ui+"/trace/abc", "", nil); s != http.StatusUnauthorized {
		t.Errorf("without a session: %d", s)
	}
	cookie := f.sessionCookie()
	s, body := get(t, http.MethodGet, ui+"/trace/abc", "", cookie)
	if s != http.StatusOK || body != "/trace/abc cookie=" {
		t.Errorf("with a session: %d %q", s, body)
	}
	if s, _ := get(t, http.MethodPost, ui+"/api/v3/traces", "", cookie); s != http.StatusMethodNotAllowed {
		t.Errorf("a POST: %d", s)
	}
	if s, _ := get(t, http.MethodGet, ui+"/trace/abc", "rebound.example:"+port, cookie); s != http.StatusMisdirectedRequest {
		t.Errorf("another Host: %d", s)
	}
	// A page of the UI cannot act on the panel's API.
	if s := f.status(http.MethodPost, console.PathConfig, cookie, map[string]string{"Origin": "http://127.0.0.1:" + port}); s != http.StatusForbidden {
		t.Errorf("the API from the UI's origin: %d", s)
	}

	s, body = get(t, http.MethodGet, f.base+console.PathSession, "localhost:"+f.port, cookie)
	var session console.SessionJSON
	if s != http.StatusOK || json.Unmarshal([]byte(body), &session) != nil || session.TraceUI != "http://localhost:"+port {
		t.Errorf("the session: %d %q", s, body)
	}

	if s, _ := get(t, http.MethodDelete, f.base+console.PathSession, "", cookie); s != http.StatusNoContent {
		t.Fatalf("signing out: %d", s)
	}
	if s, _ := get(t, http.MethodGet, ui+"/trace/abc", "", cookie); s != http.StatusUnauthorized {
		t.Errorf("after signing out: %d", s)
	}
}

// A console without the client certificate serves no UI, and the panel shows
// no link.
func TestTraceUIWithout(t *testing.T) {
	f := newFixture(t, nil)
	cookie := f.sessionCookie()
	s, body := get(t, http.MethodGet, f.base+console.PathSession, "", cookie)
	var session console.SessionJSON
	if s != http.StatusOK || json.Unmarshal([]byte(body), &session) != nil || session.TraceUI != "" {
		t.Errorf("the session: %d %q", s, body)
	}
}
