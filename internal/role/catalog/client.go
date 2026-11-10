package catalog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Client calls /v1 with one credential: the console's, to issue the networks'
// credentials and to register, or a network's.
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
	_, err := c.call(ctx, method, path, nil, body, out, want...)
	return err
}

// call is do with request headers, and returns the answer's ETag.
func (c *Client) call(ctx context.Context, method, path string, header http.Header, body, out any, want ...int) (string, error) {
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return "", err
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimSuffix(c.URL, "/")+path, r)
	if err != nil {
		return "", err
	}
	for k, v := range header {
		req.Header[k] = v
	}
	req.Header.Set("Authorization", "Bearer "+c.Credential)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, maxEntryBody))
	if err != nil {
		return "", err
	}
	etag := resp.Header.Get("ETag")
	for _, s := range want {
		if resp.StatusCode == s {
			if out == nil || len(b) == 0 {
				return etag, nil
			}
			return etag, json.Unmarshal(b, out)
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
	return "", e
}

// IsStatus says whether err is an answer with status.
func IsStatus(err error, status int) bool {
	var e *Error
	return errors.As(err, &e) && e.Status == status
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

// Entry reads an entry by its digest; ok is false if it does not exist.
func (c *Client) Entry(ctx context.Context, digest string) (e *Entry, ok bool, err error) {
	err = c.do(ctx, http.MethodGet, "/v1/entries/"+url.PathEscape(digest), nil, &e, http.StatusOK)
	if IsStatus(err, http.StatusNotFound) {
		return nil, false, nil
	}
	return e, err == nil, err
}

// Entries lists every entry (implementation §1.2.11).
func (c *Client) Entries(ctx context.Context) ([]Entry, error) {
	var out struct{ Entries []Entry }
	err := c.do(ctx, http.MethodGet, "/v1/entries", nil, &out, http.StatusOK)
	return out.Entries, err
}

// Bundles lists every bundle (implementation §1.2.11).
func (c *Client) Bundles(ctx context.Context) ([]Bundle, error) {
	var out struct{ Bundles []Bundle }
	err := c.do(ctx, http.MethodGet, "/v1/bundles", nil, &out, http.StatusOK)
	return out.Bundles, err
}

// Topics lists the topics of the Topic Registry (architecture §3.7.6).
func (c *Client) Topics(ctx context.Context) ([]Topic, error) {
	var out struct{ Topics []Topic }
	err := c.do(ctx, http.MethodGet, "/v1/topics", nil, &out, http.StatusOK)
	return out.Topics, err
}

// Topic reads a topic of the Topic Registry; ok is false if it is not
// registered.
func (c *Client) Topic(ctx context.Context, name string) (t *Topic, ok bool, err error) {
	err = c.do(ctx, http.MethodGet, "/v1/topics/"+url.PathEscape(name), nil, &t, http.StatusOK)
	if IsStatus(err, http.StatusNotFound) {
		return nil, false, nil
	}
	return t, err == nil, err
}

// SetDeletionMark stamps or removes the deletion mark of the entry digest
// (architecture §3.13.8).
func (c *Client) SetDeletionMark(ctx context.Context, digest string, on bool) error {
	method := http.MethodDelete
	if on {
		method = http.MethodPut
	}
	return c.do(ctx, method, "/v1/entries/"+url.PathEscape(digest)+"/deletion_mark", nil, nil, http.StatusNoContent)
}

// Record reads the qualified record of network and key into out, and returns
// its revision; ok is false if it does not exist.
func (c *Client) Record(ctx context.Context, network, key string, out any) (revision int64, ok bool, err error) {
	etag, err := c.call(ctx, http.MethodGet, recordPath(network, key), nil, nil, out, http.StatusOK)
	if IsStatus(err, http.StatusNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	revision, err = strconv.ParseInt(strings.Trim(etag, `"`), 10, 64)
	return revision, true, err
}

// Records lists the records whose key starts with prefix, of network or, if
// it is empty, of every network.
func (c *Client) Records(ctx context.Context, prefix, network string) ([]Record, error) {
	q := url.Values{"prefix": {prefix}}
	if network != "" {
		q.Set("network", network)
	}
	var out struct{ Records []Record }
	err := c.do(ctx, http.MethodGet, "/v1/records?"+q.Encode(), nil, &out, http.StatusOK)
	return out.Records, err
}

// ErrPrecondition is a conditional write whose condition does not hold: 412.
var ErrPrecondition = errors.New("catalog: the condition does not hold")

// Write is how PutRecord writes: IfAbsent is the write-if-absent, and Expires,
// if not zero, the record's lifetime (implementation §1.2.11).
type Write struct {
	IfAbsent bool
	Expires  time.Duration
}

// PutRecord writes value as the record of network and key.
func (c *Client) PutRecord(ctx context.Context, network, key string, value any, w Write) error {
	path := recordPath(network, key)
	if w.Expires > 0 {
		path += "?expires_in=" + strconv.FormatInt(int64((w.Expires+time.Second-1)/time.Second), 10)
	}
	header := http.Header{}
	if w.IfAbsent {
		header.Set("If-None-Match", "*")
	}
	_, err := c.call(ctx, http.MethodPut, path, header, value, nil, http.StatusNoContent)
	if IsStatus(err, http.StatusPreconditionFailed) {
		return ErrPrecondition
	}
	return err
}

// DeleteRecord deletes the record of network and key if it is still at revision.
func (c *Client) DeleteRecord(ctx context.Context, network, key string, revision int64) error {
	header := http.Header{"If-Match": {strconv.Quote(strconv.FormatInt(revision, 10))}}
	_, err := c.call(ctx, http.MethodDelete, recordPath(network, key), header, nil, nil, http.StatusNoContent)
	if IsStatus(err, http.StatusPreconditionFailed) {
		return ErrPrecondition
	}
	return err
}

func recordPath(network, key string) string {
	return "/v1/records/" + url.PathEscape(network) + "/" + url.PathEscape(key)
}
