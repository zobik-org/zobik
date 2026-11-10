package dockerreader

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

// engine records the request that reaches it.
type engine struct{ got *http.Request }

func (e *engine) RoundTrip(r *http.Request) (*http.Response, error) {
	e.got = r
	rec := httptest.NewRecorder()
	rec.WriteString("[]")
	return rec.Result(), nil
}

func serve(t *testing.T, method, target string) (*httptest.ResponseRecorder, *http.Request) {
	t.Helper()
	e := &engine{}
	rec := httptest.NewRecorder()
	Handler("alpha", e).ServeHTTP(rec, httptest.NewRequest(method, target, nil))
	return rec, e.got
}

// Only GET on the closed list of routes passes, versioned or not; a read that
// returns a container's content does not (implementation §6).
func TestRoutes(t *testing.T) {
	for target, pass := range map[string]bool{
		"/_ping":                             true,
		"/v1.47/version":                     true,
		"/v1.47/containers/json":             true,
		"/events":                            true,
		"/v1.47/containers/abc/json":         false,
		"/containers/abc/archive?path=/":     false,
		"/v1.47/containers/abc/logs":         false,
		"/containers/abc/export":             false,
		"/images/json":                       false,
		"/v1.47/containers/json/../abc/json": false,
		"/containers%2Fjson":                 false,
	} {
		rec, got := serve(t, http.MethodGet, target)
		if pass != (got != nil) || pass != (rec.Code == http.StatusOK) {
			t.Errorf("GET %s: %d, reached the engine %v", target, rec.Code, got != nil)
		}
	}
	for _, method := range []string{http.MethodPost, http.MethodDelete, http.MethodHead} {
		if rec, got := serve(t, method, "/containers/json"); got != nil || rec.Code != http.StatusForbidden {
			t.Errorf("%s /containers/json: %d", method, rec.Code)
		}
	}
}

// The listing and the events carry the network's filter, joined to the
// request's in either of the engine's forms.
func TestNetworkFilter(t *testing.T) {
	for _, filters := range []string{
		"",
		`{"label":["org.zobik.topic"]}`,
		`{"label":{"org.zobik.topic":true},"status":["running"]}`,
		`{"label":["org.zobik.network=beta"]}`,
	} {
		for _, route := range []string{"/v1.47/containers/json", "/events"} {
			q := url.Values{"all": {"1"}}
			if filters != "" {
				q.Set("filters", filters)
			}
			_, got := serve(t, http.MethodGet, route+"?"+q.Encode())
			if got == nil {
				t.Fatalf("%s with %s did not reach the engine", route, filters)
			}
			var sets map[string]map[string]bool
			if err := json.Unmarshal([]byte(got.URL.Query().Get("filters")), &sets); err != nil {
				t.Fatal(err)
			}
			if !sets["label"]["org.zobik.network=alpha"] {
				t.Errorf("%s with %s: filters %v", route, filters, sets)
			}
			// What the request brought stays, so it can only narrow.
			var asked map[string]json.RawMessage
			_ = json.Unmarshal([]byte(filters), &asked)
			for k := range asked {
				if len(sets[k]) == 0 {
					t.Errorf("%s with %s lost %s", route, filters, k)
				}
			}
			if got.URL.Query().Get("all") != "1" {
				t.Errorf("%s lost the rest of the query", route)
			}
		}
	}
	if rec, got := serve(t, http.MethodGet, "/containers/json?filters=nope"); got != nil || rec.Code != http.StatusBadRequest {
		t.Errorf("malformed filters: %d", rec.Code)
	}
}
