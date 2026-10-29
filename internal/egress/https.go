package egress

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
)

// The https adapter (implementation §8.3, §8.4): it builds the request from the
// operation's mapping and the call's fields, under the root of the entry's
// target, with the credential where the entry places it.

// maxJSONResponse bounds a response read whole, outside a body: what returns to
// the Logic Container fits in a gRPC message of the default 4 MiB.
const maxJSONResponse = 3 << 20

// HTTPClient is the adapter's client: it follows no redirect, which would take
// the request out of the entry's target (implementation §8.3). It trusts the
// system CAs.
func HTTPClient() *http.Client {
	return &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
}

// newRequest builds the request of a call, with body the copy of its body when
// the operation carries one.
func (e *Entry) newRequest(ctx context.Context, c *Call, body *os.File, credential []byte) (*http.Request, error) {
	m := e.Interface.Operations[c.Operation].HTTPS
	base, err := url.Parse(e.Target)
	if err != nil {
		return nil, err
	}

	// Each path field replaces its segment, encoded as a single segment.
	path := m.Path
	query := url.Values{}
	headers := http.Header{}
	for name, value := range m.Headers {
		headers.Set(name, value)
	}
	var doc map[string]any
	for name, fm := range m.Fields {
		v, ok := c.Fields[name]
		if !ok {
			continue // an absent optional field is not sent
		}
		switch fm.In {
		case "path":
			s := scalar(v)
			if s == "" {
				return nil, fmt.Errorf("%w: path field %s is empty", ErrInvalid, name)
			}
			path = strings.ReplaceAll(path, "{"+name+"}", segment(s))
		case "query":
			query.Add(fm.At, scalar(v))
		case "header":
			headers.Set(fm.At, scalar(v))
		case "json":
			if doc == nil {
				doc = map[string]any{}
			}
			setPointer(doc, fm.At, v)
		}
	}
	hasJSON := false
	for _, fm := range m.Fields {
		hasJSON = hasJSON || fm.In == "json"
	}

	// The path resolves under the root of the target (architecture §3.16.2).
	full := strings.TrimSuffix(base.EscapedPath(), "/") + path
	if p := e.CredentialPlacement; p != nil && p.Query != "" {
		query.Set(p.Query, string(credential))
	}
	raw := base.Scheme + "://" + base.Host + full
	if len(query) > 0 {
		raw += "?" + query.Encode()
	}
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("%w: the request's URL: %w", ErrInvalid, err)
	}

	var reader io.Reader
	var size int64
	switch {
	case body != nil:
		st, err := body.Stat()
		if err != nil {
			return nil, err
		}
		reader, size = body, st.Size()
	case hasJSON:
		if doc == nil {
			doc = map[string]any{}
		}
		b, err := json.Marshal(doc)
		if err != nil {
			return nil, err
		}
		reader, size = bytes.NewReader(b), int64(len(b))
	}
	req, err := http.NewRequestWithContext(ctx, m.Method, u.String(), reader)
	if err != nil {
		return nil, err
	}
	req.URL = u
	req.ContentLength = size
	req.Header = headers

	// The adapter places the credential, never an interface field (architecture §3.16.2).
	if p := e.CredentialPlacement; p != nil && p.Header != "" {
		switch p.Scheme {
		case "Bearer":
			req.Header.Set(p.Header, "Bearer "+string(credential))
		case "Basic":
			req.Header.Set(p.Header, "Basic "+base64.StdEncoding.EncodeToString(credential))
		default:
			req.Header.Set(p.Header, string(credential))
		}
	}
	return req, nil
}

// readResponse assembles the response: status, and each declared field from the
// JSON body or a header, copied without validating; a pointer that does not
// resolve leaves the field absent (implementation §8.3). With a response body,
// the HTTP body goes to bodyOut, bounded by max_body_size.
func (e *Entry) readResponse(op string, resp *http.Response, bodyOut io.Writer) (map[string]any, error) {
	o := e.Interface.Operations[op]
	out := map[string]any{statusField: statusNumber(resp.StatusCode)}
	var doc any
	if o.Body.Response {
		n, err := io.Copy(bodyOut, io.LimitReader(resp.Body, e.MaxBodySize+1))
		if err != nil {
			return nil, err
		}
		if n > e.MaxBodySize {
			return nil, fmt.Errorf("the response's body exceeds max_body_size, %d bytes", e.MaxBodySize)
		}
	} else {
		raw, err := io.ReadAll(io.LimitReader(resp.Body, maxJSONResponse+1))
		if err != nil {
			return nil, err
		}
		if len(raw) > maxJSONResponse {
			return nil, fmt.Errorf("the response exceeds %d bytes", maxJSONResponse)
		}
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		if dec.Decode(&doc) != nil {
			doc = nil // a body that is not JSON resolves no pointer
		}
	}
	for name, fm := range o.HTTPS.Response {
		switch fm.From {
		case "header":
			if v := resp.Header.Get(fm.At); v != "" {
				out[name] = v
			}
		case "json":
			if v, ok := getPointer(doc, fm.At); ok {
				out[name] = v
			}
		}
	}
	return out, nil
}

// scalar is a scalar field as it goes in a path, a query or a header.
func scalar(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case json.Number:
		return x.String()
	case bool:
		if x {
			return "true"
		}
		return "false"
	}
	b, _ := json.Marshal(v)
	return string(b)
}

// segment encodes a value as a single path segment: a / or a .. inside it does
// not change the route (implementation §8.3).
func segment(s string) string {
	escaped := url.PathEscape(s)
	if s == "." || s == ".." {
		escaped = strings.ReplaceAll(escaped, ".", "%2E")
	}
	return escaped
}

// tokens splits a JSON Pointer (RFC 6901) into its unescaped tokens.
func tokens(p string) []string {
	if p == "" {
		return nil
	}
	parts := strings.Split(p[1:], "/")
	for i, t := range parts {
		parts[i] = strings.ReplaceAll(strings.ReplaceAll(t, "~1", "/"), "~0", "~")
	}
	return parts
}

// setPointer writes v at p inside doc, creating the objects on the way.
func setPointer(doc map[string]any, p string, v any) {
	ts := tokens(p)
	cur := doc
	for _, t := range ts[:len(ts)-1] {
		next, ok := cur[t].(map[string]any)
		if !ok {
			next = map[string]any{}
			cur[t] = next
		}
		cur = next
	}
	cur[ts[len(ts)-1]] = v
}

// getPointer reads p inside doc; "" is the whole document.
func getPointer(doc any, p string) (any, bool) {
	cur := doc
	for _, t := range tokens(p) {
		switch x := cur.(type) {
		case map[string]any:
			v, ok := x[t]
			if !ok {
				return nil, false
			}
			cur = v
		case []any:
			var i int
			if _, err := fmt.Sscanf(t, "%d", &i); err != nil || i < 0 || i >= len(x) || fmt.Sprint(i) != t {
				return nil, false
			}
			cur = x[i]
		default:
			return nil, false
		}
	}
	return cur, cur != nil
}

// ErrNoTable is an entry whose rate_source the bucket does not have: it does
// not execute, because without a table there is no max_per_call (implementation §8.5).
var ErrNoTable = errors.New("egress: the rate table is not in the rates bucket")

// ErrNoResponse is a call that got no response: a connection, TLS or timeout
// failure. A 4xx or a 5xx is a response (implementation §8.3).
var ErrNoResponse = errors.New("egress: the call got no response")

// Executor executes the calls the rules allowed (architecture §3.16.4, step 5).
type Executor struct {
	HTTP *http.Client
	// Credential resolves a credential_handle.
	Credential func(ctx context.Context, handle string) ([]byte, error)
	Rates      RateSource
}

// Result is a call's response and its charge.
type Result struct {
	// Response is the response outside its body, with its status.
	Response map[string]any
	Charge   Charge
}

// Execute resolves the credential, builds the request, executes it and
// composes the charge with the billing_rule and the current rate. With a
// response body, it writes it to bodyOut. body is the copy of the call's body,
// or nil.
func (x *Executor) Execute(ctx context.Context, e *Entry, c *Call, body *os.File, bodyOut io.Writer) (*Result, error) {
	var table *Table
	if e.RateSource != "" {
		t, ok := x.Rates(e.RateSource)
		if !ok {
			return nil, fmt.Errorf("%w: %s", ErrNoTable, e.RateSource)
		}
		table = t
	}
	var credential []byte
	if e.CredentialHandle != "" {
		var err error
		if credential, err = x.Credential(ctx, e.CredentialHandle); err != nil {
			return nil, fmt.Errorf("egress: resolving %s: %w", e.CredentialHandle, err)
		}
	}
	req, err := e.newRequest(ctx, c, body, credential)
	if err != nil {
		return nil, err
	}
	resp, err := x.HTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrNoResponse, err)
	}
	defer resp.Body.Close()
	response, err := e.readResponse(c.Operation, resp, bodyOut)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrNoResponse, err)
	}
	return &Result{Response: response, Charge: e.Bill(c, response, table)}, nil
}
