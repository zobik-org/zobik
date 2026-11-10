package dockerreader

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"zobik.org/zobik/internal/role"
)

// SocketDir is where the role serves its socket: a volume only network_monitor
// and ingress mount (implementation §6, The roles of the binary).
const SocketDir = "/run/zobik/docker_reader"

// Socket is the role's socket in SocketDir.
const Socket = SocketDir + "/docker.sock"

// LabelNetwork is the label that names the network of each of its containers.
const LabelNetwork = "org.zobik.network"

// routes are the reads that pass, compared literally after the version prefix:
// filtered says the role adds its network's filter (implementation §6).
var routes = map[string]bool{
	"/containers/json": true,
	"/events":          true,
	"/_ping":           false,
	"/version":         false,
}

// versioned is the API version prefix a client puts before the route.
var versioned = regexp.MustCompile(`^/v[0-9]+\.[0-9]+(/.*)$`)

// Handler lets through to engine the GET requests on the closed list of routes,
// with the filter label=org.zobik.network=<network> added to those that list
// containers or their events, and answers 403 to everything else. The engine
// combines filters on the same key with and, so a request cannot widen it.
func Handler(network string, engine http.RoundTripper) http.Handler {
	proxy := &httputil.ReverseProxy{
		Transport: engine,
		// /events streams for as long as the client reads it.
		FlushInterval: -1,
		Rewrite: func(r *httputil.ProxyRequest) {
			r.Out.URL.Scheme, r.Out.URL.Host, r.Out.Host = "http", "docker", "docker"
		},
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		route := r.URL.Path
		if m := versioned.FindStringSubmatch(route); m != nil {
			route = m[1]
		}
		filtered, ok := routes[route]
		if r.Method != http.MethodGet || !ok || r.URL.RawPath != "" {
			http.Error(w, "docker_reader: not a read the role lets through", http.StatusForbidden)
			return
		}
		if filtered {
			q := r.URL.Query()
			f, err := withNetwork(q.Get("filters"), network)
			if err != nil {
				http.Error(w, "docker_reader: filters: "+err.Error(), http.StatusBadRequest)
				return
			}
			q.Set("filters", f)
			r.URL.RawQuery = q.Encode()
		}
		proxy.ServeHTTP(w, r)
	})
}

// withNetwork adds the network's label to the request's filters. The engine
// takes them as a map from key to a list of values, or to a set of them; the
// role passes on the set.
func withNetwork(raw, network string) (string, error) {
	sets := map[string]map[string]bool{}
	if raw != "" {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal([]byte(raw), &fields); err != nil {
			return "", err
		}
		for k, v := range fields {
			set := map[string]bool{}
			var list []string
			if err := json.Unmarshal(v, &list); err == nil {
				for _, s := range list {
					set[s] = true
				}
			} else if err := json.Unmarshal(v, &set); err != nil {
				return "", errors.New("the values of " + k + " are neither a list nor a set")
			}
			sets[k] = set
		}
	}
	if sets["label"] == nil {
		sets["label"] = map[string]bool{}
	}
	sets["label"][LabelNetwork+"="+network] = true
	b, err := json.Marshal(sets)
	return string(b), err
}

// Engine is the transport to the container engine's socket at path.
func Engine(path string) http.RoundTripper {
	return &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", path)
		},
		IdleConnTimeout: time.Minute,
	}
}

// Options are what the role serves.
type Options struct {
	// Network is the network whose containers it lets be seen.
	Network string
	// Engine is the path of the container engine's socket.
	Engine string
	// Socket is where it serves.
	Socket string
}

// Run is the docker_reader role: it serves Handler on its socket, without the
// Bus, until ctx ends. It is ready once the socket serves.
func Run(ctx context.Context, o Options) error {
	if o.Network == "" {
		return errors.New("docker_reader: no network")
	}
	if err := os.MkdirAll(filepath.Dir(o.Socket), 0o750); err != nil {
		return err
	}
	os.Remove(o.Socket) // one a previous process left
	ln, err := net.Listen("unix", o.Socket)
	if err != nil {
		return err
	}
	srv := &http.Server{Handler: Handler(o.Network, Engine(o.Engine)), ReadHeaderTimeout: 10 * time.Second}
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	go func() {
		if err := srv.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
			cancel(err)
		}
	}()
	defer srv.Close()
	if err := role.ServeHealth(ctx, role.HealthSocket, func() bool { return true }); err != nil {
		return err
	}
	log.Printf("docker_reader: serving %s for %s", o.Socket, o.Network)
	<-ctx.Done()
	if err := context.Cause(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}
