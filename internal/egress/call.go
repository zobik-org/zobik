package egress

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"time"

	"zobik.org/zobik/internal/rules"
	"zobik.org/zobik/internal/schema"
)

// Call is a call validated against its entry's interface: the operation, its
// fields and the body's size and digest (architecture §3.16.4, step 3).
type Call struct {
	Operation string
	// Fields are the call's fields decoded with json.Decoder.UseNumber, from
	// which the adapter builds the request; native, as the conditions read them.
	Fields map[string]any
	native map[string]any
	// Body is the path of the body's copy, empty without a body, and BodySize
	// and BodyDigest are those of the copy.
	Body       string
	BodySize   int64
	BodyDigest string
}

// ErrInvalid is a call that does not validate against the entry: the
// Integration Sidecar rejects it, and does not complete or correct its fields
// (architecture §3.16.4, step 3).
var ErrInvalid = errors.New("egress: invalid call")

// NewCall validates a call's operation and fields against the entry, with the
// same validator the Config Store compiled the conditions against
// (implementation §8.2). hasBody says whether the call carries a body.
func (e *Entry) NewCall(op string, fields []byte, hasBody bool) (*Call, error) {
	if !slices.Contains(e.Operations, op) {
		return nil, fmt.Errorf("%w: the entry does not admit the operation %q", ErrInvalid, op)
	}
	o := e.Interface.Operations[op]
	if hasBody != o.Body.Request {
		if o.Body.Request {
			return nil, fmt.Errorf("%w: operation %s carries a body", ErrInvalid, op)
		}
		return nil, fmt.Errorf("%w: operation %s carries no body", ErrInvalid, op)
	}
	if len(fields) == 0 {
		fields = []byte("{}")
	}
	dec := json.NewDecoder(bytes.NewReader(fields))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil || dec.More() {
		return nil, fmt.Errorf("%w: the fields are not a JSON object", ErrInvalid)
	}
	if err := o.Fields.Validate(v); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	native, err := rules.Native(o.Fields, v)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	// The validator ignores the keys the schema does not declare (implementation
	// §10.3), and the call keeps only the declared ones: what is validated is
	// what is executed (architecture §3.16.4).
	declared := map[string]any{}
	for k, val := range v.(map[string]any) {
		if _, ok := o.Fields.Properties[k]; ok {
			declared[k] = val
		}
	}
	return &Call{Operation: op, Fields: declared, native: native.(map[string]any)}, nil
}

// Outcome is what the rules give a call, named by the rule that produced it
// (architecture §3.16.4).
type Outcome struct {
	// Then is allow, deny or an audience.
	Then string
	// Rule is the rule as the parser that evaluated it prints it.
	Rule string
	// Unevaluable is the evaluation error of a condition, which gives deny.
	Unevaluable error
}

func (o Outcome) String() string {
	if o.Unevaluable != nil {
		return fmt.Sprintf("deny: the rule %s could not be evaluated: %v", o.Rule, o.Unevaluable)
	}
	return o.Then + " by the rule " + o.Rule
}

// Lists is the value of egress_lists the conditions read. It is empty until the
// key enters: a condition that names a list does not evaluate, and gives deny
// (architecture §3.16.2).
var Lists = map[string][]any{}

// Decide evaluates the rules in order over the call, the lists and now: the
// first whose condition holds wins, and an evaluation error is deny, never
// passing to the next (architecture §3.16.2).
func (e *Entry) Decide(c *Call, lists map[string][]any, now time.Time) Outcome {
	vars := map[string]any{
		"operation": c.Operation, "body_size": c.BodySize, "body_digest": c.BodyDigest,
		"egress_lists": lists, "now": now, "call": c.native,
	}
	for f, v := range c.native {
		vars[f] = v
	}
	for _, r := range e.Rules {
		if r.When == nil {
			return Outcome{Then: r.Then, Rule: "then " + r.Then}
		}
		name := "when " + r.When.Text + " then " + r.Then
		holds, err := r.When.Eval(vars)
		if err != nil {
			return Outcome{Then: Deny, Rule: name, Unevaluable: err}
		}
		if holds {
			return Outcome{Then: r.Then, Rule: name}
		}
	}
	// The schema requires a last rule without when.
	return Outcome{Then: Deny, Rule: "no rule"}
}

// Charge is what a call cost, from its response and the current rate
// (implementation §8.5).
type Charge struct {
	Cost                float64
	TokensIn, TokensOut int64
	// BillingErrors is 1 when an expression could not be evaluated, and Errors
	// say which.
	BillingErrors int64
	Errors        []error
}

// Bill evaluates the operation's billing_rule over the call, the response and
// the table: an evaluation error of cost charges max_per_call, and one of a
// token count adds 0 (implementation §8.5). A cost that is not a non-negative
// number is an evaluation error too, because it would subtract spending.
func (e *Entry) Bill(c *Call, response map[string]any, t *Table) Charge {
	b := e.Billing[c.Operation]
	var maxPerCall float64
	if t != nil {
		maxPerCall = t.MaxPerCall
	}
	var ch Charge
	fail := func(err error) { ch.Errors = append(ch.Errors, err) }

	vars := map[string]any{"rate": t.rateMap(), "max_per_call": maxPerCall}
	for f, v := range c.native {
		vars[f] = v
	}
	native, err := e.nativeResponse(c.Operation, response)
	if err != nil {
		fail(fmt.Errorf("the response: %w", err))
		ch.Cost, ch.BillingErrors = maxPerCall, 1
		return ch
	}
	vars["response"] = native

	cost, err := b.Cost.Eval(vars)
	if f, ok := cost.(float64); err == nil && ok && nonNegative(f) && !math.IsNaN(f) {
		ch.Cost = f
	} else {
		if err == nil {
			err = fmt.Errorf("cost %v is not a non-negative number", cost)
		}
		fail(err)
		ch.Cost = maxPerCall
	}
	for _, tok := range []struct {
		expr *rules.Expr
		dst  *int64
	}{{b.TokensIn, &ch.TokensIn}, {b.TokensOut, &ch.TokensOut}} {
		if tok.expr == nil {
			continue
		}
		v, err := tok.expr.Eval(vars)
		if n, ok := v.(int64); err == nil && ok && n >= 0 {
			*tok.dst = n
			continue
		}
		if err == nil {
			err = fmt.Errorf("token count %v is not a non-negative integer", v)
		}
		fail(err)
	}
	// A call counts once, whatever expressions of it failed.
	if len(ch.Errors) > 0 {
		ch.BillingErrors = 1
	}
	return ch
}

// nativeResponse converts the response, as the adapter assembled it, to the
// value the billing_rule reads, with the operation's response shape.
func (e *Entry) nativeResponse(op string, response map[string]any) (any, error) {
	o := e.Interface.Operations[op]
	shape := &schema.Schema{Kind: schema.KindObject, Properties: map[string]*schema.Schema{}}
	if o.Response != nil {
		for k, v := range o.Response.Properties {
			shape.Properties[k] = v
		}
	}
	if e.Interface.Protocol == ProtocolHTTPS {
		shape.Properties[statusField] = &schema.Schema{Kind: schema.KindInteger}
	}
	return rules.Native(shape, response)
}

// statusNumber is the response's status as the adapter writes it.
func statusNumber(code int) json.Number { return json.Number(strconv.Itoa(code)) }
