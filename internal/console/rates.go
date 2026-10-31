package console

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/egress"
)

// PathRates is where the panel and the command line write a rate table,
// PathRates/<rate_source> (implementation §8.5).
const PathRates = "/api/rates"

// maxRates bounds a rate table: a few models with a few components each.
const maxRates = 1 << 20

// ratesLifetime bounds the identity the act writes the bucket with.
const ratesLifetime = 5 * time.Minute

func (s *Service) rateRoutes(mux *http.ServeMux) {
	mux.HandleFunc("PUT "+PathRates+"/{name...}", s.withSession(true, func(w http.ResponseWriter, r *http.Request, _ string) {
		name := r.PathValue("name")
		if !egress.ValidRateSource(name) {
			writeError(w, http.StatusBadRequest, "the name is not a key of the rates bucket")
			return
		}
		table, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxRates))
		if err != nil {
			writeError(w, http.StatusRequestEntityTooLarge, "the table exceeds 1 MiB")
			return
		}
		// The table is validated against its format before it is written
		// (implementation §8.5).
		if _, err := egress.ParseTable(table); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if s.o.WriteRates == nil {
			writeError(w, http.StatusServiceUnavailable, "this console writes no rate tables")
			return
		}
		creds, err := s.actCreds()
		if err != nil {
			writeError(w, http.StatusServiceUnavailable, err.Error())
			return
		}
		if err := s.o.WriteRates(r.Context(), creds, name, table); err != nil {
			writeError(w, http.StatusBadGateway, err.Error())
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
}

// actCreds is the console's platform identity for one act on the Bus, which the
// open session holds (implementation §6, The operator console).
func (s *Service) actCreds() ([]byte, error) {
	s.mu.Lock()
	keys := s.keys
	s.mu.Unlock()
	if keys == nil {
		return nil, errors.New("console: no session holds the keys")
	}
	token, err := keys.JWT(time.Now().Add(ratesLifetime))
	if err != nil {
		return nil, err
	}
	seed, err := keys.Console.Seed()
	if err != nil {
		return nil, err
	}
	return bus.Creds(token, seed)
}

// SetRates writes the rate table of name.
func (c *Client) SetRates(ctx context.Context, name string, table []byte) error {
	if !json.Valid(table) {
		return errors.New("console: the rate table is not JSON")
	}
	resp, err := c.do(ctx, http.MethodPut, PathRates+"/"+escapeHandle(name), json.RawMessage(table))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return problem(resp, http.StatusNoContent)
}
