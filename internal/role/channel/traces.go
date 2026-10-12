package channel

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"log"
	"strconv"
	"time"

	"github.com/google/uuid"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/signing"
	contextv1 "zobik.org/zobik/proto/zobik/context/v1"
	nriv1 "zobik.org/zobik/proto/zobik/nri/v1"
)

// signedActs are the entries whose TaskID the console mints, because the
// operator's proof covers it (architecture §3.14.2).
var signedActs = map[string]bool{
	"config_change": true, "node_catalog_publish": true, "node_provision": true,
	"node_create": true, "node_catalog_import": true, "node_retire": true,
}

// The portions the role writes in a root's section (implementation §3, Context Store and egress).
const (
	portionPayload = "zobik/payload"
	portionBodies  = "zobik/bodies/"
)

// errInvalid is a request the console sent malformed.
type errInvalid struct{ msg string }

func (e errInvalid) Error() string { return e.msg }

func invalid(format string, a ...any) error { return errInvalid{fmt.Sprintf(format, a...)} }

// trace is a trace being opened: the root's identifiers and its token.
type trace struct {
	id, entryTaskID, token string
}

// newTrace generates the TraceID and mints the root's token with the channel's
// credential: read under the trace, write in its own section (architecture §3.14.3).
func (c *channel) newTrace(entryTaskID string) (*trace, error) {
	b := make([]byte, 16)
	rand.Read(b)
	t := &trace{id: hex.EncodeToString(b), entryTaskID: entryTaskID}
	if t.entryTaskID == "" {
		t.entryTaskID = uuid.NewString()
	}
	var err error
	t.token, err = c.rt.Sign(signing.TypeScopeToken, signing.ScopeToken{
		Sub: c.rt.ID, Iat: time.Now().Unix(), TraceID: t.id, Section: t.id,
	}, c.rt.Credential)
	return t, err
}

// portion is what a write left in the Context Store.
type portion struct {
	ref    *nriv1.ContextRef
	size   int64
	digest string
}

// writer streams one portion into the root's section.
type writer struct {
	stream contextv1.ContextStore_WriteClient
	sum    hash.Hash
	size   int64
}

func (c *channel) newWriter(ctx context.Context, token, name string) (*writer, error) {
	s, err := c.context.Write(ctx)
	if err != nil {
		return nil, err
	}
	err = s.Send(&contextv1.StoreWrite{Body: &contextv1.StoreWrite_Header{Header: &contextv1.StoreWriteHeader{Token: token, Name: name}}})
	return &writer{stream: s, sum: sha256.New()}, err
}

func (w *writer) write(chunk []byte) error {
	w.size += int64(len(chunk))
	w.sum.Write(chunk)
	return w.stream.Send(&contextv1.StoreWrite{Body: &contextv1.StoreWrite_Chunk{Chunk: chunk}})
}

func (w *writer) close() (*portion, error) {
	ref, err := w.stream.CloseAndRecv()
	if err != nil {
		return nil, err
	}
	// The digest's form is that of implementation §8.1.
	return &portion{ref: ref, size: w.size, digest: "sha256:" + hex.EncodeToString(w.sum.Sum(nil))}, nil
}

// body is a body of the request as the console declared it.
type body struct {
	name, mediaType string
	context         bool
	portion         *portion
}

// announce assembles the entry subtask with what the console sent and the
// references to what the role wrote, persists the root and publishes the
// announcement (architecture §3.5.1, implementation §6, The service between the
// console and the channels).
func (c *channel) announce(ctx context.Context, t *trace, topic, workspace string, raw []byte, payload *portion, bodies []body) error {
	var data map[string]any
	if err := json.Unmarshal(raw, &data); err != nil || data == nil {
		return invalid("data is not a JSON object")
	}
	for _, k := range []string{"context_ref", "bodies", "current_attempts", "previous_attempt", "final_round"} {
		if _, ok := data[k]; ok {
			return invalid("data carries %s, which the role stamps", k)
		}
	}
	// The fields a signed act's proof covers come in the data the console
	// signed; the role checks them instead of overwriting them.
	for k, v := range map[string]string{
		"topic": topic, "requester_identity": c.rt.ID, "origin_audience": c.audience, "workspace": workspace,
	} {
		if v == "" {
			continue
		}
		if got, ok := data[k]; ok && got != v {
			return invalid("data.%s is %v, not %s", k, got, v)
		}
		data[k] = v
	}
	if payload != nil {
		data["context_ref"] = payload.ref
	}
	if len(bodies) > 0 {
		var list []map[string]any
		for _, b := range bodies {
			list = append(list, map[string]any{
				"ref": b.portion.ref, "size": b.portion.size, "digest": b.portion.digest,
				"name": b.name, "media_type": b.mediaType, "context": b.context,
			})
		}
		data["bodies"] = list
	}
	span := make([]byte, 8)
	rand.Read(span)
	head := c.head.Load()
	e, err := bus.NewEvent(bus.TypeAnnounced, c.rt.ID, t.entryTaskID, data)
	if err != nil {
		return err
	}
	// Pinning the configuration is part of opening the trace (architecture §3.5.1).
	e.TraceParent = "00-" + t.id + "-" + hex.EncodeToString(span) + "-01"
	e.ConfigVersion = head.ConfigVersion
	if err := e.Validate(); err != nil {
		return invalid("%v", err)
	}
	if err := e.ValidateTopic(); err != nil {
		return invalid("%v", err)
	}
	r := &root{
		TraceID: t.id, EntryTaskID: t.entryTaskID, EntryTopic: topic, Token: t.token,
		TraceParent: e.TraceParent, Version: e.ConfigVersion, OpenedAt: time.Now().UTC(),
	}
	// The root is kept before the announcement, so the entry subtask's terminal
	// event always finds it.
	if err := c.roots.save(r); err != nil {
		return err
	}
	c.mu.Lock()
	c.open[r.TraceID] = r
	c.byEntry[r.EntryTaskID] = r.TraceID
	c.mu.Unlock()
	if err := bus.Publish(ctx, c.js, bus.SubjectAnnounced(topic), e); err != nil {
		c.forget(r)
		return err
	}
	return nil
}

func (c *channel) forget(r *root) {
	c.mu.Lock()
	delete(c.open, r.TraceID)
	delete(c.byEntry, r.EntryTaskID)
	delete(c.byDelivery, r.Delivery)
	c.mu.Unlock()
	if err := c.roots.remove(r.TraceID); err != nil {
		log.Printf("channel: forgetting %s: %v", r.TraceID, err)
	}
}

// onTerminal handles the terminal event of a task the channel opened: the
// entry subtask's opens the delivery of the outcome, and the delivery's closes
// the root (architecture §3.5.4).
func (c *channel) onTerminal(ctx context.Context, e *bus.Event) error {
	c.mu.Lock()
	entryOf, isEntry := c.byEntry[e.Subject]
	deliveryOf, isDelivery := c.byDelivery[e.Subject]
	var r *root
	if isEntry {
		r = c.open[entryOf]
	} else if isDelivery {
		r = c.open[deliveryOf]
	}
	c.mu.Unlock()
	switch {
	case r == nil:
		return nil
	case isEntry && r.Outcome == nil:
		r.Outcome = e
		return c.deliver(ctx, r)
	case isDelivery && e.Subject == r.Delivery:
		return c.delivered(ctx, r, e)
	}
	return nil
}

// deliveryMode is result_delivery for the channel's audience (architecture §2.3).
func (c *channel) deliveryMode() string {
	var rd struct {
		Default   string            `json:"default"`
		Audiences map[string]string `json:"audiences"`
	}
	json.Unmarshal(c.head.Load().Values["result_delivery"], &rd)
	if m, ok := rd.Audiences[c.audience]; ok {
		return m
	}
	return rd.Default
}

// The options and the silence of each delivery mode (architecture §3.5.4).
var deliveryOptions = map[string]struct {
	options []string
	silence string
}{
	"none":       {},
	"ack":        {options: []string{"received"}, silence: "received"},
	"acceptance": {options: []string{"accept", "reject"}, silence: "accept"},
}

// deliver opens, from the root, the next attempt of the outcome's delivery on
// hitl_contact_<origin_audience>, which is the channel's own audience
// (architecture §3.5.4).
func (c *channel) deliver(ctx context.Context, r *root) error {
	out := r.Outcome
	var od map[string]any
	json.Unmarshal(out.Data, &od)
	details := map[string]any{"type": out.Type}
	rest := map[string]any{}
	for k, v := range od {
		switch k {
		case "failure_reason", "failure_code", "disposition", "context_ref":
			details[k] = v
		case "topic", "metrics", "requester_identity", "workspace", "origin_audience", "bodies":
		default:
			rest[k] = v
		}
	}
	if len(rest) > 0 {
		details["data"] = rest
	}
	var cost float64
	if m, ok := od["metrics"].(map[string]any); ok {
		cost, _ = m["cost"].(float64)
	}
	data := map[string]any{
		"topic": c.topic, "kind": "result_delivery", "scope": "trace",
		"since":              out.Time.UTC().Format(time.RFC3339Nano),
		"evidence":           map[string]any{"trace_id": r.TraceID, "task_id": r.EntryTaskID, "cost": cost},
		"details":            details,
		"requester_identity": c.rt.ID, "origin_audience": c.audience,
		"current_attempts": r.Attempts + 1,
	}
	if mode := deliveryOptions[c.deliveryMode()]; mode.options != nil {
		data["options"], data["outcome_if_silent"] = mode.options, mode.silence
	}
	if r.Delivery != "" {
		data["previous_attempt"] = r.Delivery
	}
	if r.FinalRound {
		data["final_round"] = true
	}
	e, err := bus.NewEvent(bus.TypeAnnounced, c.rt.ID, uuid.NewString(), data)
	if err != nil {
		return err
	}
	e.TraceParent, e.ConfigVersion = r.TraceParent, r.Version
	if err := e.ValidateTopic(); err != nil {
		return err
	}
	prev := r.Delivery
	r.Delivery, r.Attempts = e.Subject, r.Attempts+1
	if err := c.roots.save(r); err != nil {
		return err
	}
	c.mu.Lock()
	delete(c.byDelivery, prev)
	c.byDelivery[r.Delivery] = r.TraceID
	c.mu.Unlock()
	return bus.Publish(ctx, c.js, bus.SubjectAnnounced(c.topic), e)
}

// delivered reads how the delivery closed: an answer, or unanswered with its
// outcome_if_silent, delivered the outcome and the root closes; undelivered
// opens the next round, and once the final round closes without delivering,
// the root closes undelivered (architecture §3.5.4).
func (c *channel) delivered(ctx context.Context, r *root, e *bus.Event) error {
	if e.Type == bus.TypeCompleted {
		return c.closeRoot(ctx, r, bus.TypeCompleted, "", "")
	}
	var d struct {
		FailureCode   string `json:"failure_code"`
		FailureReason string `json:"failure_reason"`
	}
	json.Unmarshal(e.Data, &d)
	switch {
	case d.FailureCode == codeUnanswered:
		return c.closeRoot(ctx, r, bus.TypeCompleted, "", "")
	case r.FinalRound:
		return c.closeRoot(ctx, r, bus.TypeFailed, codeUndelivered,
			"the outcome was not delivered in any round, the final one included")
	}
	return c.deliver(ctx, r)
}

// closeRoot closes the trace with the terminal event whose subject is the
// TraceID, addressed to the channel itself on the entry subtask's topic
// (architecture §3.5.4, implementation §1.2.1).
func (c *channel) closeRoot(ctx context.Context, r *root, typ, code, reason string) error {
	data := map[string]any{
		"topic":   r.EntryTopic,
		"metrics": map[string]any{"tokens_in": 0, "tokens_out": 0, "cost": 0, "duration_ms": time.Since(r.OpenedAt).Milliseconds()},
	}
	if typ == bus.TypeFailed {
		data["failure_reason"], data["failure_code"] = reason, code
	}
	e, err := bus.NewEvent(typ, c.rt.ID, r.TraceID, data)
	if err != nil {
		return err
	}
	e.TraceParent, e.ConfigVersion = r.TraceParent, r.Version
	if err := bus.Publish(ctx, c.js, bus.SubjectTerminal(typ, c.rt.ID, r.EntryTopic), e); err != nil {
		return err
	}
	c.forget(r)
	return nil
}

// tokenOf is the scope token of a task the role holds: a query it retains or a
// root it opened (implementation §6, The service between the console and the channels).
func (c *channel) tokenOf(taskID string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if q, ok := c.queries[taskID]; ok {
		return q.a.Token, true
	}
	if r, ok := c.open[taskID]; ok {
		return r.Token, true
	}
	return "", false
}

func (c *channel) rootToken(traceID string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	r, ok := c.open[traceID]
	if !ok {
		return "", false
	}
	return r.Token, true
}

var errUnknownTask = errors.New("the role holds no such task")

// bodyName is where the i-th body of an Open goes.
func bodyName(i int) string { return portionBodies + strconv.Itoa(i) }
