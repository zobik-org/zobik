package console

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"
)

// The tracing backend's UI, which the console serves behind the session on a
// port of its own (implementation §1.2.17): the UI's code shows text the nodes
// write, and from another origin it cannot call the panel's API, which rejects
// a foreign Origin.

// traceProxy reaches the tracing backend's query with the console's client
// certificate while some session is open.
type traceProxy struct {
	proxy     *httputil.ReverseProxy
	transport *http.Transport
}

func newTraceProxy(addr string, tlsConfig *tls.Config) *traceProxy {
	target := &url.URL{Scheme: "https", Host: addr}
	transport := &http.Transport{TLSClientConfig: tlsConfig, IdleConnTimeout: time.Minute}
	return &traceProxy{
		transport: transport,
		proxy: &httputil.ReverseProxy{
			Transport: transport,
			Rewrite: func(r *httputil.ProxyRequest) {
				r.SetURL(target)
				// The session stays in the console.
				r.Out.Header.Del("Cookie")
			},
		},
	}
}

func (p *traceProxy) close() { p.transport.CloseIdleConnections() }

// SessionJSON is what the panel learns of the session it holds.
type SessionJSON struct {
	// TraceUI is the address of the tracing backend's UI, under which a trace
	// is /trace/<TraceID>; empty if the console does not serve it.
	TraceUI string `json:"trace_ui,omitempty"`
}

func (s *Service) getSession(w http.ResponseWriter, r *http.Request, _ string) {
	var out SessionJSON
	s.mu.Lock()
	if s.traces != nil && s.tracePort != "" {
		// The same host name the panel is on, so that the browser sends the
		// session's cookie, which does not depend on the port.
		host, _, err := net.SplitHostPort(r.Host)
		if err == nil {
			out.TraceUI = "http://" + net.JoinHostPort(host, s.tracePort)
		}
	}
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, out)
}

// TracesHandler serves the tracing backend's UI on port, on localhost and
// 127.0.0.1, to a browser with a session open. It only reads.
func (s *Service) TracesHandler(port string) http.Handler {
	ui := s.withSession(true, func(w http.ResponseWriter, r *http.Request, _ string) {
		s.mu.Lock()
		p := s.traces
		s.mu.Unlock()
		if p == nil {
			writeError(w, http.StatusServiceUnavailable, "the console has no access to the tracing backend")
			return
		}
		p.proxy.ServeHTTP(w, r)
	})
	return addressedToItself(port, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Security-Policy", "frame-ancestors 'none'")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			writeError(w, http.StatusMethodNotAllowed, "the tracing backend's UI only reads")
			return
		}
		ui(w, r)
	}))
}

// ServeTraces serves the tracing backend's UI on ln until ctx ends.
func (s *Service) ServeTraces(ctx context.Context, ln net.Listener) error {
	_, port, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.tracePort = port
	s.mu.Unlock()
	srv := &http.Server{Handler: s.TracesHandler(port), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		srv.Close()
	}()
	if err := srv.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
