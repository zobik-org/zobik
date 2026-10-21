package console

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

// Client is a client of the console's API: the command line performs over it
// the same acts as the panel (implementation §6, The operator console).
type Client struct {
	base    string
	session *http.Cookie
}

// ErrNotRunning is a console that does not answer at its address.
var ErrNotRunning = errors.New("console: the console is not running: start it with zobik console")

// SignIn opens a session in the console at base, http://127.0.0.1:<port>.
func SignIn(ctx context.Context, base, user string, password []byte) (*Client, error) {
	c := &Client{base: base}
	resp, err := c.do(ctx, http.MethodPost, PathSession, Credentials{Username: user, Password: string(password)})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if err := problem(resp, http.StatusNoContent); err != nil {
		return nil, err
	}
	for _, ck := range resp.Cookies() {
		if ck.Name == CookieSession {
			c.session = ck
		}
	}
	if c.session == nil {
		return nil, errors.New("console: the sign-in returned no session")
	}
	return c, nil
}

// SignOut closes the session.
func (c *Client) SignOut(ctx context.Context) error {
	resp, err := c.do(ctx, http.MethodDelete, PathSession, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return problem(resp, http.StatusNoContent)
}

// Config reads the head of the Global Configuration.
func (c *Client) Config(ctx context.Context) (*SnapshotJSON, error) {
	resp, err := c.do(ctx, http.MethodGet, PathConfig, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if err := problem(resp, http.StatusOK); err != nil {
		return nil, err
	}
	var s SnapshotJSON
	return &s, json.NewDecoder(resp.Body).Decode(&s)
}

// NotSentError is an edit the console rejected before signing it.
type NotSentError struct{ Reason string }

func (e *NotSentError) Error() string { return "not sent: " + e.Reason }

// ConfigSet opens a config_change on base, or on the head if base is empty.
// Its outcome arrives over the event stream.
func (c *Client) ConfigSet(ctx context.Context, base string, assignments map[string]json.RawMessage) (*Opened, error) {
	resp, err := c.do(ctx, http.MethodPost, PathConfig, configEdit{BaseVersion: base, Assignments: assignments})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnprocessableEntity {
		var p Problem
		_ = json.NewDecoder(resp.Body).Decode(&p)
		return nil, &NotSentError{Reason: p.Error}
	}
	if err := problem(resp, http.StatusAccepted); err != nil {
		return nil, err
	}
	var o Opened
	return &o, json.NewDecoder(resp.Body).Decode(&o)
}

// Outcomes opens the event stream and returns the outcomes it brings, once the
// stream is open. The channel closes when the stream ends.
func (c *Client) Outcomes(ctx context.Context) (<-chan Outcome, error) {
	resp, err := c.do(ctx, http.MethodGet, PathEvents, nil) //nolint:bodyclose // the stream's goroutine closes it
	if err != nil {
		return nil, err
	}
	if err := problem(resp, http.StatusOK); err != nil {
		resp.Body.Close()
		return nil, err
	}
	r := bufio.NewReader(resp.Body)
	if l, err := r.ReadString('\n'); err != nil || !strings.HasPrefix(l, ":") {
		resp.Body.Close()
		if err == nil {
			err = fmt.Errorf("it began with %q", l)
		}
		return nil, fmt.Errorf("console: the event stream did not open: %w", err)
	}
	out := make(chan Outcome, 16)
	go func() {
		defer close(out)
		defer resp.Body.Close()
		var event string
		for {
			l, err := r.ReadString('\n')
			if err != nil {
				return
			}
			l = strings.TrimRight(l, "\r\n")
			switch {
			case strings.HasPrefix(l, "event: "):
				event = strings.TrimPrefix(l, "event: ")
			case strings.HasPrefix(l, "data: ") && event == "outcome":
				var o Outcome
				if json.Unmarshal([]byte(strings.TrimPrefix(l, "data: ")), &o) == nil {
					out <- o
				}
			case l == "":
				event = ""
			}
		}
	}()
	return out, nil
}

func (c *Client) do(ctx context.Context, method, path string, body any) (*http.Response, error) {
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			return nil, err
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, &buf)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.session != nil {
		req.AddCookie(c.session)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		if ctx.Err() == nil {
			return nil, fmt.Errorf("%w (%w)", ErrNotRunning, err)
		}
		return nil, err
	}
	return resp, nil
}

// problem returns the API's error when the status is not want.
func problem(resp *http.Response, want int) error {
	if resp.StatusCode == want {
		return nil
	}
	var p Problem
	if json.NewDecoder(resp.Body).Decode(&p) != nil || p.Error == "" {
		p.Error = resp.Status
	}
	return fmt.Errorf("console: %s", p.Error)
}
