package tenantlink

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync/atomic"
	"time"

	"zobik.org/zobik/internal/role"
)

// Options are what the link serves.
type Options struct {
	// Listen is where it publishes the registry for the container engine.
	Listen string
	// Registry is the registry's host:port by its fixed name, and CA the tenant
	// CA's certificate it verifies the registry with (implementation §6, The
	// tenant CA, the link and the join code).
	Registry string
	CA       []byte
}

// probeInterval is how often the link checks that the registry answers once it
// does; until then, every second.
const probeInterval = 10 * time.Second

// Run serves the registry on a port the host publishes on 127.0.0.1, in
// cleartext toward the engine and over TLS toward the registry, until ctx
// ends. It is ready while the registry answers.
func Run(ctx context.Context, o Options) error {
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(o.CA) {
		return errors.New("tenant_link: no certificate in the tenant CA")
	}
	host, _, err := net.SplitHostPort(o.Registry)
	if err != nil {
		return err
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{RootCAs: pool, ServerName: host, MinVersion: tls.VersionTLS13}
	upstream := &url.URL{Scheme: "https", Host: o.Registry}

	var ready atomic.Bool
	if err := role.ServeHealth(ctx, role.HealthSocket, ready.Load); err != nil {
		return err
	}
	go watch(ctx, &http.Client{Transport: transport, Timeout: 3 * time.Second}, upstream.JoinPath("/v2/").String(), &ready)

	srv := &http.Server{
		Addr: o.Listen,
		Handler: Handler(&httputil.ReverseProxy{
			Rewrite: func(r *httputil.ProxyRequest) {
				r.SetURL(upstream)
				r.Out.Host = o.Registry
			},
			Transport: transport,
		}),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		<-ctx.Done()
		srv.Close()
	}()
	log.Printf("tenant_link: serving %s on %s", o.Registry, o.Listen)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Handler lets through only the reads: the port authenticates no one, and
// pulling is all the engine does (implementation §6, The tenant CA, the link
// and the join code).
func Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, "the link only reads", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// watch records whether the registry answers at u, until ctx ends.
func watch(ctx context.Context, c *http.Client, u string, ready *atomic.Bool) {
	for {
		ok := answers(ctx, c, u)
		if ok != ready.Load() {
			log.Printf("tenant_link: the registry answers: %t", ok)
		}
		ready.Store(ok)
		wait := time.Second
		if ok {
			wait = probeInterval
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

func answers(ctx context.Context, c *http.Client, u string) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return false
	}
	resp, err := c.Do(req)
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}
