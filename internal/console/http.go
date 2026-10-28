package console

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"zobik.org/zobik/internal/globalconfig"
	"zobik.org/zobik/web"
)

// The routes of the API.
const (
	pathSignIn  = "/signin"
	PathSession = "/api/session"
	PathConfig  = "/api/config"
	PathEvents  = "/api/events"
)

// CookieSession names the session cookie.
const CookieSession = "zobik_session"

// Handler serves the panel and the API on port, on localhost and 127.0.0.1.
func (s *Service) Handler(port string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+PathSession, s.postSession)
	mux.HandleFunc("GET "+PathSession, s.withSession(false, s.getSession))
	mux.HandleFunc("DELETE "+PathSession, s.withSession(true, s.deleteSession))
	mux.HandleFunc("GET "+pathSignIn, s.getSignIn)
	mux.HandleFunc("GET "+PathConfig, s.withSession(true, s.getConfig))
	mux.HandleFunc("POST "+PathConfig, s.withSession(true, s.postConfig))
	// The event stream is not activity (implementation §6, The operator console).
	mux.HandleFunc("GET "+PathEvents, s.withSession(false, s.getEvents))
	s.workRoutes(mux)
	s.secretRoutes(mux)
	mux.Handle("GET /", http.FileServerFS(web.FS))
	return addressedToItself(port, onlyOwnContent(mux))
}

// onlyOwnContent lets the panel load nothing from elsewhere, nor be framed:
// what it shows comes from the network and from the person's attachments.
func onlyOwnContent(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Security-Policy", "default-src 'self'; frame-ancestors 'none'")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		h.ServeHTTP(w, r)
	})
}

// addressedToItself rejects a Host other than the console's own address, and an
// Origin other than its own: a page of another site cannot use the session,
// nor reach the API under another name by DNS rebinding (implementation §6,
// The operator console).
func addressedToItself(port string, h http.Handler) http.Handler {
	hosts := map[string]bool{"localhost:" + port: true, "127.0.0.1:" + port: true}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !hosts[r.Host] {
			writeError(w, http.StatusMisdirectedRequest, "not addressed to the console")
			return
		}
		if o := r.Header.Get("Origin"); o != "" && !hosts[trimScheme(o)] {
			writeError(w, http.StatusForbidden, "another origin")
			return
		}
		h.ServeHTTP(w, r)
	})
}

func trimScheme(origin string) string {
	const scheme = "http://"
	if len(origin) > len(scheme) && origin[:len(scheme)] == scheme {
		return origin[len(scheme):]
	}
	return ""
}

// withSession admits only a request with an open session; activity marks it as
// the person's (implementation §6, The operator console).
func (s *Service) withSession(activity bool, h func(w http.ResponseWriter, r *http.Request, session string)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		c, err := r.Cookie(CookieSession)
		if err != nil || !s.touch(c.Value, activity) {
			writeError(w, http.StatusUnauthorized, "no session: sign in")
			return
		}
		h(w, r, c.Value)
	}
}

// Credentials are what signing in sends.
type Credentials struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

func (s *Service) postSession(w http.ResponseWriter, r *http.Request) {
	var c Credentials
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&c); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	token, err := s.signIn(c.Username, []byte(c.Password))
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	setSession(w, token)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Service) deleteSession(w http.ResponseWriter, _ *http.Request, session string) {
	s.signOut(session)
	http.SetCookie(w, &http.Cookie{Name: CookieSession, Path: "/", MaxAge: -1, HttpOnly: true, SameSite: http.SameSiteStrictMode}) //nolint:gosec // the panel is plain HTTP on localhost
	w.WriteHeader(http.StatusNoContent)
}

// getSignIn spends the single-use link: it leaves the cookie and goes to the
// panel without the token in the address.
func (s *Service) getSignIn(w http.ResponseWriter, r *http.Request) {
	session, ok := s.redeem(r.URL.Query().Get("token"))
	if !ok {
		writeError(w, http.StatusUnauthorized, "the link was used or expired: sign in")
		return
	}
	setSession(w, session)
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func setSession(w http.ResponseWriter, token string) {
	http.SetCookie(w, &http.Cookie{Name: CookieSession, Value: token, Path: "/", HttpOnly: true, SameSite: http.SameSiteStrictMode}) //nolint:gosec // the panel is plain HTTP on localhost
}

func (s *Service) getConfig(w http.ResponseWriter, r *http.Request, _ string) {
	op, err := s.operator(r.Context())
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	snap, err := op.Snapshot(r.Context())
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, SnapshotJSON{ConfigVersion: snap.ConfigVersion, PublishedAt: snap.PublishedAt, Values: snap.Values})
}

// postConfig opens a config_change on the head, or on the version the edit
// names, which the Config Store judges (architecture §3.14.4). An edit the
// schema rejects is not sent.
func (s *Service) postConfig(w http.ResponseWriter, r *http.Request, _ string) {
	var e configEdit
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&e); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	op, err := s.operator(r.Context())
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	snap, err := op.Snapshot(r.Context())
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	if e.BaseVersion != "" {
		snap.ConfigVersion = e.BaseVersion
	}
	opened, err := op.ConfigSet(r.Context(), snap, e.Assignments)
	var invalid *globalconfig.InvalidError
	switch {
	case errors.As(err, &invalid):
		writeError(w, http.StatusUnprocessableEntity, invalid.Error())
	case err != nil:
		writeError(w, http.StatusBadGateway, err.Error())
	default:
		writeJSON(w, http.StatusAccepted, opened)
	}
}

// getEvents is the event stream: what reaches the panel without being asked
// for, over Server-Sent Events, while the session is open.
func (s *Service) getEvents(w http.ResponseWriter, r *http.Request, session string) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "no streaming")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	st := s.hub.open(session)
	defer s.hub.close(st)
	// The comment tells the client the stream is open before anything is sent.
	if _, err := w.Write([]byte(": open\n\n")); err != nil {
		return
	}
	flusher.Flush()
	check := time.NewTicker(time.Second)
	defer check.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case f := <-st.frames:
			if _, err := w.Write(f); err != nil {
				return
			}
			flusher.Flush()
		case <-check.C:
			if !s.touch(session, false) {
				return
			}
		}
	}
}

// Problem is the body of an error the API returns.
type Problem struct {
	Error string `json:"error"`
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, Problem{Error: msg})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
