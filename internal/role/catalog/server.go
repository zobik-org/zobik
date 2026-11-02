package catalog

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Catalog is the Specialty Catalog's service (implementation §1.2.11).
type Catalog struct {
	store    *store
	registry *registry
	scanner  Scanner
	// ruleset is the current ruleset_version, that of the role image catalog
	// and scanner share (implementation §1.2.19).
	ruleset string
	// capacity is the artifact storage capacity zobik init declares, in bytes
	// (implementation §1.2.16).
	capacity int64
	// adminHash is the hash of the administration credential.
	adminHash string

	// mu serializes the acts of registration, so two of the same digest, or two
	// that each fit alone, do not both pass.
	mu sync.Mutex
}

// The body ceilings: an entry carries whole interface documents in its slots.
const (
	maxEntryBody  = 4 << 20
	maxRecordBody = 1 << 20
)

// NewCredential mints a credential: an opaque secret, whose hash is what the
// Catalog stores (implementation §1.2.11).
func NewCredential() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// HashCredential is the hash the Catalog stores of a credential.
func HashCredential(credential string) string {
	sum := sha256.Sum256([]byte(credential))
	return hex.EncodeToString(sum[:])
}

// caller is who presents a credential: a network with a scope, or the
// administration.
type caller struct {
	network, scope string
	admin          bool
}

func (c *caller) can(scope string) bool { return !c.admin && scopeRank[c.scope] >= scopeRank[scope] }

func (c *Catalog) authenticate(r *http.Request) (*caller, error) {
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || token == "" {
		return nil, nil
	}
	hash := HashCredential(token)
	if subtle.ConstantTimeCompare([]byte(hash), []byte(c.adminHash)) == 1 {
		return &caller{admin: true}, nil
	}
	network, scope, ok, err := c.store.credential(r.Context(), hash)
	if err != nil || !ok {
		return nil, err
	}
	return &caller{network: network, scope: scope}, nil
}

// Handler is the /v1 service.
func (c *Catalog) Handler() http.Handler {
	mux := http.NewServeMux()
	route := func(pattern string, allowed func(*caller, *http.Request) bool, h func(*http.Request, *caller) (result, error)) {
		mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
			who, err := c.authenticate(r)
			if err != nil {
				log.Printf("catalog: authenticating: %v", err)
				write(w, respond(http.StatusServiceUnavailable, errorBody("the store does not answer")))
				return
			}
			if who == nil {
				w.Header().Set("WWW-Authenticate", "Bearer")
				write(w, respond(http.StatusUnauthorized, errorBody("a current credential is required")))
				return
			}
			if !allowed(who, r) {
				write(w, respond(http.StatusForbidden, errorBody("the credential does not enable this operation")))
				return
			}
			res, err := h(r, who)
			if err != nil {
				log.Printf("catalog: %s: %v", pattern, err)
				res = respond(http.StatusServiceUnavailable, errorBody(err.Error()))
			}
			write(w, res)
		})
	}
	scope := func(s string) func(*caller, *http.Request) bool {
		return func(who *caller, _ *http.Request) bool { return who.can(s) }
	}
	// The spawner credential of the network the path names.
	ownSpawner := func(who *caller, r *http.Request) bool {
		return who.can(ScopeSpawner) && who.network == r.PathValue("network")
	}
	admin := func(who *caller, _ *http.Request) bool { return who.admin }

	route("POST /v1/entries", scope(ScopeRegister), func(r *http.Request, who *caller) (result, error) {
		body, err := io.ReadAll(http.MaxBytesReader(nil, r.Body, maxEntryBody))
		if err != nil {
			return respond(http.StatusRequestEntityTooLarge, errorBody(err.Error())), nil
		}
		return c.register(r.Context(), who.network, body)
	})
	route("POST /v1/bundles", scope(ScopeRegister), func(r *http.Request, _ *caller) (result, error) {
		body, err := io.ReadAll(http.MaxBytesReader(nil, r.Body, maxEntryBody))
		if err != nil {
			return respond(http.StatusRequestEntityTooLarge, errorBody(err.Error())), nil
		}
		return c.registerBundle(r.Context(), body)
	})
	route("GET /v1/entries", scope(ScopeRead), func(r *http.Request, _ *caller) (result, error) {
		entries, err := c.store.entries(r.Context())
		if err != nil {
			return result{}, err
		}
		out := []json.RawMessage{}
		for _, e := range entries {
			v, err := e.view()
			if err != nil {
				return result{}, err
			}
			out = append(out, v)
		}
		return respond(http.StatusOK, map[string]any{"entries": out}), nil
	})
	route("GET /v1/entries/{digest}", scope(ScopeRead), func(r *http.Request, _ *caller) (result, error) {
		e, err := c.store.entry(r.Context(), r.PathValue("digest"))
		if err != nil || e == nil {
			return notFound(err)
		}
		v, err := e.view()
		return respond(http.StatusOK, v), err
	})
	route("GET /v1/bundles", scope(ScopeRead), func(r *http.Request, _ *caller) (result, error) {
		b, err := c.store.bundles(r.Context())
		return respond(http.StatusOK, map[string]any{"bundles": b}), err
	})
	route("GET /v1/bundles/{bundle_id}", scope(ScopeRead), func(r *http.Request, _ *caller) (result, error) {
		b, err := c.store.bundle(r.Context(), r.PathValue("bundle_id"))
		if err != nil || b == nil {
			return notFound(err)
		}
		return respond(http.StatusOK, b), nil
	})
	mark := func(on bool) func(*http.Request, *caller) (result, error) {
		return func(r *http.Request, _ *caller) (result, error) {
			c.mu.Lock()
			defer c.mu.Unlock()
			ok, err := c.store.setDeletionMark(r.Context(), r.PathValue("digest"), on)
			if err != nil || !ok {
				return notFound(err)
			}
			return result{status: http.StatusNoContent}, nil
		}
	}
	route("PUT /v1/entries/{digest}/deletion_mark", scope(ScopeSpawner), mark(true))
	route("DELETE /v1/entries/{digest}/deletion_mark", scope(ScopeSpawner), mark(false))
	route("GET /v1/status", scope(ScopeRead), func(r *http.Request, _ *caller) (result, error) {
		occupied, err := c.store.occupied(r.Context())
		return respond(http.StatusOK, Status{RulesetVersion: c.ruleset, Occupied: occupied, Capacity: c.capacity}), err
	})

	route("GET /v1/records/{network}/{key...}", scope(ScopeRead), func(r *http.Request, _ *caller) (result, error) {
		rec, err := c.store.record(r.Context(), r.PathValue("network"), r.PathValue("key"))
		if err != nil || rec == nil {
			return notFound(err)
		}
		return respond(http.StatusOK, rec.Value).withRevision(rec.Revision), nil
	})
	route("GET /v1/records", scope(ScopeRead), func(r *http.Request, _ *caller) (result, error) {
		q := r.URL.Query()
		recs, err := c.store.records(r.Context(), q.Get("prefix"), q.Get("network"))
		return respond(http.StatusOK, map[string]any{"records": recs}), err
	})
	route("PUT /v1/records/{network}/{key...}", ownSpawner, func(r *http.Request, _ *caller) (result, error) {
		cond, err := conditionOf(r)
		if err != nil {
			return respond(http.StatusBadRequest, errorBody(err.Error())), nil
		}
		var ttl time.Duration
		if v := r.URL.Query().Get("expires_in"); v != "" {
			s, err := strconv.ParseInt(v, 10, 64)
			if err != nil || s <= 0 {
				return respond(http.StatusBadRequest, errorBody("expires_in: a positive number of seconds")), nil
			}
			ttl = time.Duration(s) * time.Second
		}
		value, err := io.ReadAll(http.MaxBytesReader(nil, r.Body, maxRecordBody))
		if err != nil {
			return respond(http.StatusRequestEntityTooLarge, errorBody(err.Error())), nil
		}
		if !json.Valid(value) {
			return respond(http.StatusBadRequest, errorBody("a record is a JSON document")), nil
		}
		rev, err := c.store.putRecord(r.Context(), r.PathValue("network"), r.PathValue("key"), value, cond, ttl)
		if errors.Is(err, errPrecondition) {
			return respond(http.StatusPreconditionFailed, errorBody("the condition does not hold")), nil
		}
		if err != nil {
			return result{}, err
		}
		return result{status: http.StatusNoContent}.withRevision(rev), nil
	})
	route("DELETE /v1/records/{network}/{key...}", ownSpawner, func(r *http.Request, _ *caller) (result, error) {
		cond, err := conditionOf(r)
		if err != nil || cond.absent {
			return respond(http.StatusBadRequest, errorBody("a deletion is conditioned only by If-Match")), nil
		}
		err = c.store.deleteRecord(r.Context(), r.PathValue("network"), r.PathValue("key"), cond.revision)
		if errors.Is(err, errPrecondition) {
			return respond(http.StatusPreconditionFailed, errorBody("the condition does not hold")), nil
		}
		return result{status: http.StatusNoContent}, err
	})

	route("POST /v1/credentials", admin, func(r *http.Request, _ *caller) (result, error) {
		var req struct{ Network, Scope string }
		if err := json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&req); err != nil {
			return respond(http.StatusBadRequest, errorBody(err.Error())), nil
		}
		if !networkPattern.MatchString(req.Network) || scopeRank[req.Scope] == 0 {
			return respond(http.StatusBadRequest, errorBody("a network's name and a scope: read, register or spawner")), nil
		}
		cred, err := NewCredential()
		if err != nil {
			return result{}, err
		}
		if err := c.store.addCredential(r.Context(), HashCredential(cred), req.Network, req.Scope); err != nil {
			return result{}, err
		}
		return respond(http.StatusCreated, map[string]string{"credential": cred}), nil
	})
	route("DELETE /v1/credentials/{network}", admin, func(r *http.Request, _ *caller) (result, error) {
		return result{status: http.StatusNoContent}, c.store.revoke(r.Context(), r.PathValue("network"))
	})
	return mux
}

// withRevision answers the record's revision as its ETag.
func (r result) withRevision(rev int64) result {
	r.etag = strconv.Quote(strconv.FormatInt(rev, 10))
	return r
}

// conditionOf reads HTTP's conditional write: If-None-Match: * or If-Match
// with a revision (implementation §1.2.11).
func conditionOf(r *http.Request) (condition, error) {
	none, match := r.Header.Get("If-None-Match"), r.Header.Get("If-Match")
	switch {
	case none != "" && match != "":
		return condition{}, errors.New("If-None-Match and If-Match together")
	case none == "*":
		return condition{absent: true}, nil
	case none != "":
		return condition{}, errors.New("If-None-Match admits only *")
	case match != "":
		rev, err := strconv.ParseInt(strings.Trim(match, `"`), 10, 64)
		if err != nil || rev <= 0 {
			return condition{}, errors.New("If-Match carries a revision")
		}
		return condition{revision: rev}, nil
	}
	return condition{}, nil
}

func notFound(err error) (result, error) {
	if err != nil {
		return result{}, err
	}
	return respond(http.StatusNotFound, errorBody("not found")), nil
}

func errorBody(msg string) map[string]string { return map[string]string{"error": msg} }

func write(w http.ResponseWriter, res result) {
	if res.etag != "" {
		w.Header().Set("ETag", res.etag)
	}
	if res.body == nil {
		w.WriteHeader(res.status)
		return
	}
	var b []byte
	switch v := res.body.(type) {
	case json.RawMessage:
		b = v
	default:
		var err error
		if b, err = json.Marshal(v); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(res.status)
	_, _ = w.Write(b) // a client that left reads nothing
}

// serve runs srv until ctx ends.
func serve(ctx context.Context, srv *http.Server, serveFn func() error) error {
	go func() {
		<-ctx.Done()
		srv.Close()
	}()
	if err := serveFn(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
