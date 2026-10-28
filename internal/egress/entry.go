package egress

import (
	"encoding/json"
	"fmt"
	"maps"
	"net/textproto"
	"slices"
	"strings"

	"cel.dev/cel-go/cel"
	"cel.dev/cel-go/common/types"

	"zobik.org/zobik/internal/rules"
	"zobik.org/zobik/internal/schema"
)

// The values of grant_scope (architecture §3.16.3).
const (
	ScopeOpen      = "open"
	ScopeTopics    = "topics"
	ScopeProvision = "provision"
)

// The outcomes of a rule besides an audience (architecture §3.16.2).
const (
	Allow = "allow"
	Deny  = "deny"
)

// platformHandles is the prefix of the handles no entry names (implementation §1.2.18).
const platformHandles = "zobik/"

// Entry is a compiled egress_registry entry (architecture §3.16.2).
type Entry struct {
	Interface           *Interface
	Target              string
	Operations          []string
	GrantScope          string
	GranteeTopics       []string
	CredentialHandle    string
	CredentialPlacement *Placement
	Rules               []Rule
	// Billing has one entry per admitted operation (implementation §8.5).
	Billing      map[string]*Billing
	RateSource   string
	MaxBodySize  int64
	declarations *declarations
}

// Placement is where the credential goes in the request (implementation §8.4).
type Placement struct {
	Header string `json:"header,omitempty"`
	Scheme string `json:"scheme,omitempty"`
	Query  string `json:"query,omitempty"`
}

// Rule is a rule of the entry; When is nil in the last one.
type Rule struct {
	When *rules.Bool
	Then string
}

// Billing is the billing_rule of an operation; TokensIn and TokensOut are nil
// when the operation does not declare them.
type Billing struct {
	Cost, TokensIn, TokensOut *rules.Expr
}

// declarations are the CEL types an entry's expressions compile against.
type declarations struct {
	provider  *rules.Provider
	fields    []rules.Var
	responses map[string]*types.Type
}

// The types of the variables outside the fields (implementation §1.2.12, §8.5).
var (
	listsType = types.NewMapType(types.StringType, types.NewListType(types.DynType))
	rateType  = types.NewMapType(types.StringType, types.NewMapType(types.StringType, types.DoubleType))
)

// ParseEntry parses and compiles an egress_registry entry: its interface, its
// scope, its credential and the conditions and billing_rule it carries, against
// the interface's fields (architecture §3.14.4, §3.16.2; implementation §8).
func ParseEntry(raw json.RawMessage) (*Entry, error) {
	var doc struct {
		Interface           json.RawMessage `json:"interface"`
		Target              string          `json:"target"`
		Operations          []string        `json:"operations"`
		GrantScope          string          `json:"grant_scope"`
		GranteeTopics       []string        `json:"grantee_topics"`
		CredentialHandle    string          `json:"credential_handle"`
		CredentialPlacement *Placement      `json:"credential_placement"`
		Rules               []ruleDoc       `json:"rules"`
		BillingRule         map[string]struct {
			Cost      string  `json:"cost"`
			TokensIn  *string `json:"tokens_in"`
			TokensOut *string `json:"tokens_out"`
		} `json:"billing_rule"`
		RateSource  string `json:"rate_source"`
		MaxBodySize int64  `json:"max_body_size"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, err
	}
	in, err := ParseInterface(doc.Interface)
	if err != nil {
		return nil, err
	}
	if in.Direction != DirectionEgress {
		return nil, fmt.Errorf("interface: an egress entry implements an egress interface")
	}
	e := &Entry{
		Interface:           in,
		Target:              doc.Target,
		GrantScope:          doc.GrantScope,
		GranteeTopics:       doc.GranteeTopics,
		CredentialHandle:    doc.CredentialHandle,
		CredentialPlacement: doc.CredentialPlacement,
		RateSource:          doc.RateSource,
		MaxBodySize:         doc.MaxBodySize,
		Billing:             map[string]*Billing{},
	}

	// The operations are declared explicitly (architecture §3.16.2).
	for _, op := range doc.Operations {
		switch {
		case in.Operations[op] == nil:
			return nil, fmt.Errorf("operations: the interface has no operation %q", op)
		case slices.Contains(e.Operations, op):
			return nil, fmt.Errorf("operations: %s is listed twice", op)
		}
		e.Operations = append(e.Operations, op)
	}

	if e.GrantScope == "" {
		e.GrantScope = ScopeOpen
	}
	if (e.GrantScope == ScopeTopics) != (len(e.GranteeTopics) > 0) {
		return nil, fmt.Errorf("grantee_topics: only a topics entry lists them, and it lists at least one")
	}

	if err := e.checkCredential(); err != nil {
		return nil, err
	}

	// An entry whose interface declares a body bounds it (architecture §3.16.2).
	for _, op := range in.Operations {
		if (op.Body.Request || op.Body.Response) && e.MaxBodySize <= 0 {
			return nil, fmt.Errorf("max_body_size: the interface declares a body")
		}
	}

	if e.declarations, err = declare(in); err != nil {
		return nil, err
	}
	if err := e.compileRules(doc.Rules); err != nil {
		return nil, err
	}
	for _, op := range slices.Sorted(maps.Keys(doc.BillingRule)) {
		if !slices.Contains(e.Operations, op) {
			return nil, fmt.Errorf("billing_rule: %s is not an operation the entry admits", op)
		}
	}
	for _, op := range e.Operations {
		b, ok := doc.BillingRule[op]
		if !ok {
			return nil, fmt.Errorf("billing_rule: operation %s has no expression", op)
		}
		if e.Billing[op], err = e.compileBilling(op, b.Cost, b.TokensIn, b.TokensOut); err != nil {
			return nil, fmt.Errorf("billing_rule: %s: %w", op, err)
		}
	}
	return e, nil
}

// checkCredential checks the handle and where the credential goes
// (implementation §1.2.18, §8.4).
func (e *Entry) checkCredential() error {
	if strings.HasPrefix(e.CredentialHandle, platformHandles) {
		return fmt.Errorf("credential_handle: a handle that starts with %s is the platform's", platformHandles)
	}
	p := e.CredentialPlacement
	if p == nil {
		if e.CredentialHandle != "" && e.Interface.Protocol == ProtocolHTTPS {
			return fmt.Errorf("credential_placement: an https entry with a credential says where it goes")
		}
		return nil
	}
	switch {
	case e.CredentialHandle == "":
		return fmt.Errorf("credential_placement: the entry has no credential_handle")
	case e.Interface.Protocol != ProtocolHTTPS:
		return fmt.Errorf("credential_placement: only an https entry declares it")
	case (p.Header == "") == (p.Query == ""):
		return fmt.Errorf("credential_placement: the credential goes in a header or in a query parameter")
	case p.Scheme != "" && p.Header == "":
		return fmt.Errorf("credential_placement: a scheme goes with a header")
	case p.Scheme != "" && p.Scheme != "Bearer" && p.Scheme != "Basic":
		return fmt.Errorf("credential_placement: the scheme is Bearer or Basic")
	case p.Header != "" && forbiddenHeader(p.Header):
		return fmt.Errorf("credential_placement: the adapter sets %s", p.Header)
	}
	// No field of the interface goes where the credential goes.
	for _, name := range slices.Sorted(maps.Keys(e.Interface.Operations)) {
		headers, query := e.Interface.Operations[name].HTTPS.written()
		if p.Header != "" && slices.Contains(headers, textproto.CanonicalMIMEHeaderKey(p.Header)) {
			return fmt.Errorf("credential_placement: operation %s writes the header %s", name, p.Header)
		}
		if p.Query != "" && slices.Contains(query, p.Query) {
			return fmt.Errorf("credential_placement: operation %s writes the parameter %s", name, p.Query)
		}
	}
	return nil
}

// declare declares each field of the interface, with its single type, and the
// response of each operation, which with https also carries status
// (implementation §8.2, §8.3).
func declare(in *Interface) (*declarations, error) {
	p, err := rules.NewProvider()
	if err != nil {
		return nil, err
	}
	d := &declarations{provider: p, responses: map[string]*types.Type{}}
	seen := map[string]bool{}
	for _, name := range slices.Sorted(maps.Keys(in.Operations)) {
		op := in.Operations[name]
		for _, f := range slices.Sorted(maps.Keys(op.Fields.Properties)) {
			if seen[f] {
				continue
			}
			seen[f] = true
			t, err := p.Declare("zobik.field."+f, op.Fields.Properties[f])
			if err != nil {
				return nil, err
			}
			d.fields = append(d.fields, rules.Var{Name: f, Type: t})
		}
		response := &schema.Schema{Kind: schema.KindObject, Properties: map[string]*schema.Schema{}}
		if op.Response != nil {
			maps.Copy(response.Properties, op.Response.Properties)
		}
		if in.Protocol == ProtocolHTTPS {
			response.Properties[statusField] = &schema.Schema{Kind: schema.KindInteger}
		}
		if d.responses[name], err = p.Declare("zobik.response."+name, response); err != nil {
			return nil, err
		}
	}
	return d, nil
}

// conditionEnv is the environment of the conditions: the fields, the call and
// what they consult outside it (architecture §3.16.2, implementation §8.2).
func (e *Entry) conditionEnv() (*cel.Env, error) {
	vars := append(slices.Clone(e.declarations.fields),
		rules.Var{Name: "operation", Type: types.StringType},
		rules.Var{Name: "body_size", Type: types.IntType},
		rules.Var{Name: "body_digest", Type: types.StringType},
		rules.Var{Name: "egress_lists", Type: listsType},
		rules.Var{Name: "now", Type: types.TimestampType},
	)
	return rules.NewEnv(e.declarations.provider, vars...)
}

// billingEnv is the environment of an operation's billing_rule (implementation §8.5).
func (e *Entry) billingEnv(op string) (*cel.Env, error) {
	vars := append(slices.Clone(e.declarations.fields),
		rules.Var{Name: "response", Type: e.declarations.responses[op]},
		rules.Var{Name: "rate", Type: rateType},
		rules.Var{Name: "max_per_call", Type: types.DoubleType},
	)
	return rules.NewEnv(e.declarations.provider, vars...)
}

// ruleDoc is a rule as the entry writes it.
type ruleDoc struct {
	When *string `json:"when"`
	Then string  `json:"then"`
}

func (e *Entry) compileRules(list []ruleDoc) error {
	if len(list) == 0 {
		return fmt.Errorf("rules: the entry has no rules")
	}
	env, err := e.conditionEnv()
	if err != nil {
		return err
	}
	for i, r := range list {
		last := i == len(list)-1
		// The last rule carries no when, so every evaluation has an outcome
		// (architecture §3.16.2); one without when earlier would leave the rest
		// unreachable.
		switch {
		case last && r.When != nil:
			return fmt.Errorf("rules: the last rule carries no when")
		case !last && r.When == nil:
			return fmt.Errorf("rules: only the last rule goes without when")
		case r.Then != Allow && r.Then != Deny && !snakeCase(r.Then):
			return fmt.Errorf("rules: %q is not allow, deny or an audience", r.Then)
		}
		rule := Rule{Then: r.Then}
		if r.When != nil {
			if rule.When, err = rules.CompileBool(env, *r.When); err != nil {
				return fmt.Errorf("rules: %w", err)
			}
		}
		e.Rules = append(e.Rules, rule)
	}
	return nil
}

// free is the only cost of an entry without rate_source (implementation §8.5).
const free = "0.0"

func (e *Entry) compileBilling(op, cost string, tokensIn, tokensOut *string) (*Billing, error) {
	if e.RateSource == "" && strings.TrimSpace(cost) != free {
		return nil, fmt.Errorf("an entry without rate_source costs %s", free)
	}
	env, err := e.billingEnv(op)
	if err != nil {
		return nil, err
	}
	b := &Billing{}
	if b.Cost, err = rules.Compile(env, cost, types.DoubleType); err != nil {
		return nil, err
	}
	for _, t := range []struct {
		expr *string
		dst  **rules.Expr
	}{{tokensIn, &b.TokensIn}, {tokensOut, &b.TokensOut}} {
		if t.expr == nil {
			continue
		}
		if *t.dst, err = rules.Compile(env, *t.expr, types.IntType); err != nil {
			return nil, err
		}
	}
	return b, nil
}

// ParseRegistry parses and compiles every entry of an egress_registry value.
func ParseRegistry(raw json.RawMessage) (map[string]*Entry, error) {
	var entries map[string]json.RawMessage
	if err := json.Unmarshal(raw, &entries); err != nil {
		return nil, err
	}
	out := make(map[string]*Entry, len(entries))
	for _, key := range slices.Sorted(maps.Keys(entries)) {
		e, err := ParseEntry(entries[key])
		if err != nil {
			return nil, fmt.Errorf("%s: %w", key, err)
		}
		out[key] = e
	}
	return out, nil
}
