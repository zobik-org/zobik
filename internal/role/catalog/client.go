package catalog

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// Client calls /v1 with one credential: the console's, to issue the networks'
// credentials and to register.
type Client struct {
	// URL is the service's root, https://host:port.
	URL        string
	Credential string
	HTTP       *http.Client
}

// Error is an answer other than the one expected; a rejected registration
// carries its Failure.
type Error struct {
	Status  int
	Failure Failure
	Message string
}

func (e *Error) Error() string {
	if e.Failure.Reason != "" {
		msg := fmt.Sprintf("catalog: %d %s", e.Status, e.Failure.Reason)
		if len(e.Failure.Findings) > 0 {
			msg += " (" + strings.Join(e.Failure.Findings, ", ") + ")"
		}
		return msg
	}
	return fmt.Sprintf("catalog: %d %s", e.Status, e.Message)
}

func (c *Client) do(ctx context.Context, method, path string, body, out any, want ...int) error {
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimSuffix(c.URL, "/")+path, r)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.Credential)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, maxEntryBody))
	if err != nil {
		return err
	}
	for _, s := range want {
		if resp.StatusCode == s {
			if out == nil || len(b) == 0 {
				return nil
			}
			return json.Unmarshal(b, out)
		}
	}
	e := &Error{Status: resp.StatusCode}
	if resp.StatusCode == http.StatusUnprocessableEntity {
		_ = json.Unmarshal(b, &e.Failure)
	}
	var msg struct{ Error string }
	if json.Unmarshal(b, &msg) == nil {
		e.Message = msg.Error
	}
	return e
}

// Register executes the act of registration, and returns the entry.
func (c *Client) Register(ctx context.Context, reg Registration) (json.RawMessage, error) {
	var out json.RawMessage
	err := c.do(ctx, http.MethodPost, "/v1/entries", reg, &out, http.StatusCreated, http.StatusOK)
	return out, err
}

// IssueCredential issues a credential of network with scope, with the
// administration credential.
func (c *Client) IssueCredential(ctx context.Context, network, scope string) (string, error) {
	var out struct{ Credential string }
	err := c.do(ctx, http.MethodPost, "/v1/credentials", map[string]string{"network": network, "scope": scope}, &out, http.StatusCreated)
	return out.Credential, err
}

// RevokeCredentials revokes every credential of network.
func (c *Client) RevokeCredentials(ctx context.Context, network string) error {
	return c.do(ctx, http.MethodDelete, "/v1/credentials/"+network, nil, nil, http.StatusNoContent)
}

// Status returns the current ruleset_version and the occupied space.
func (c *Client) Status(ctx context.Context) (*Status, error) {
	var out Status
	err := c.do(ctx, http.MethodGet, "/v1/status", nil, &out, http.StatusOK)
	return &out, err
}
