package console

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
)

// PathSecrets is where the panel and the command line write the secret behind
// a handle, PathSecrets/<handle>. The API never returns a value
// (implementation §6, The operator console).
const PathSecrets = "/api/secrets"

// maxSecret bounds a value: a key, a token or a listener's certificate.
const maxSecret = 64 << 10

// ErrHandle is a handle the Secret Store does not admit.
var ErrHandle = errors.New("console: not a handle")

// secretWrite is the body of a write.
type secretWrite struct {
	Value []byte `json:"value"`
}

func (s *Service) secretRoutes(mux *http.ServeMux) {
	mux.HandleFunc("PUT "+PathSecrets+"/{handle...}", s.withSession(true, func(w http.ResponseWriter, r *http.Request, _ string) {
		var body secretWrite
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxSecret)).Decode(&body); err != nil || len(body.Value) == 0 {
			writeError(w, http.StatusBadRequest, "the body carries the value")
			return
		}
		s.writeSecret(w, r, body.Value)
	}))
	mux.HandleFunc("DELETE "+PathSecrets+"/{handle...}", s.withSession(true, func(w http.ResponseWriter, r *http.Request, _ string) {
		s.writeSecret(w, r, nil)
	}))
}

// writeSecret performs the act one write at a time, since each rewrites the
// whole store.
func (s *Service) writeSecret(w http.ResponseWriter, r *http.Request, value []byte) {
	if s.o.WriteSecret == nil {
		writeError(w, http.StatusServiceUnavailable, "this console writes no secrets")
		return
	}
	s.secretsMu.Lock()
	err := s.o.WriteSecret(r.Context(), r.PathValue("handle"), value)
	s.secretsMu.Unlock()
	switch {
	case errors.Is(err, ErrHandle):
		writeError(w, http.StatusBadRequest, err.Error())
	case err != nil:
		writeError(w, http.StatusBadGateway, err.Error())
	default:
		w.WriteHeader(http.StatusNoContent)
	}
}

// SetSecret writes value behind handle.
func (c *Client) SetSecret(ctx context.Context, handle string, value []byte) error {
	resp, err := c.do(ctx, http.MethodPut, PathSecrets+"/"+escapeHandle(handle), secretWrite{Value: value})
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return problem(resp, http.StatusNoContent)
}

// DeleteSecret removes the value behind handle.
func (c *Client) DeleteSecret(ctx context.Context, handle string) error {
	resp, err := c.do(ctx, http.MethodDelete, PathSecrets+"/"+escapeHandle(handle), nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return problem(resp, http.StatusNoContent)
}

// escapeHandle escapes each segment of a handle, keeping its slashes.
func escapeHandle(handle string) string {
	return (&url.URL{Path: handle}).EscapedPath()
}
