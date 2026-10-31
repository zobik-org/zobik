package egress

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"net/textproto"
	"regexp"
	"slices"
	"strings"
	"sync"

	"zobik.org/zobik/internal/digest"
	"zobik.org/zobik/internal/schema"
	"zobik.org/zobik/schemas"
)

// The directions and the protocols of an interface (architecture §3.16.2).
const (
	DirectionEgress  = "egress"
	DirectionIngress = "ingress"

	ProtocolHTTPS = "https"
)

// Interface is a parsed operation interface (implementation §8.1).
type Interface struct {
	Protocol   string
	Direction  string
	Operations map[string]*Operation
	// Digest identifies it: two interfaces are the same if their digests match.
	Digest string
}

// Operation is an operation of an interface.
type Operation struct {
	// Fields is an object schema with properties (implementation §8.2).
	Fields *schema.Schema
	// Response is the shape of the response, an object schema with properties;
	// nil if the operation declares none.
	Response *schema.Schema
	Body     Body
	HTTPS    *Mapping
}

// Body says whether the call and the response carry a body (architecture §3.16.2).
type Body struct {
	Request  bool `json:"request"`
	Response bool `json:"response"`
}

// Mapping is the declarative mapping of the https adapter (implementation §8.3).
type Mapping struct {
	Method   string                  `json:"method"`
	Path     string                  `json:"path"`
	Headers  map[string]string       `json:"headers"`
	Fields   map[string]FieldMapping `json:"fields"`
	Response map[string]FromMapping  `json:"response"`
}

// FieldMapping says where a field goes.
type FieldMapping struct {
	In string `json:"in"`
	At string `json:"at"`
}

// FromMapping says where a field of the response comes from.
type FromMapping struct {
	From string `json:"from"`
	At   string `json:"at"`
}

// The variables a condition and a billing_rule read besides the fields, which no
// field can be named like (implementation §8.2, §8.5).
var reserved = []string{
	"operation", "call", "body_size", "body_digest", "egress_lists", "now", "bodies",
	"response", "rate", "max_per_call",
}

// celReserved are CEL's reserved words, which cannot name a variable.
var celReserved = []string{
	"as", "break", "const", "continue", "else", "false", "for", "function", "if",
	"import", "in", "let", "loop", "null", "package", "namespace", "return", "true", "var",
}

var (
	// snakeCase is the form of an operation's name and of an audience a rule
	// names, the suffix of its hitl_contact_<audience> topic (architecture §3.5.3).
	snakeCase = regexp.MustCompile(`^[a-z][a-z0-9_]*$`).MatchString
	fieldName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`).MatchString
	pathParam = regexp.MustCompile(`\{([^{}]*)\}`)
)

// statusField is the field every https response carries without declaring it
// (implementation §8.3).
const statusField = "status"

// adapterHeaders are those the https adapter sets, which no mapping writes
// (implementation §8.3).
var adapterHeaders = []string{"Host", "Content-Length", "Transfer-Encoding"}

var formatSchema = sync.OnceValues(func() (*schema.Schema, error) {
	doc, err := schemas.FS.ReadFile("interface.schema.json")
	if err != nil {
		return nil, err
	}
	return schema.Compile(doc)
})

// ParseInterface parses an interface and checks it against its format: the
// format's schema, and what the binary checks beyond it (implementation §8.1–§8.3).
func ParseInterface(raw json.RawMessage) (*Interface, error) {
	format, err := formatSchema()
	if err != nil {
		return nil, err
	}
	if err := format.ValidateJSON(raw); err != nil {
		return nil, fmt.Errorf("interface: %w", err)
	}
	var doc struct {
		Protocol   string `json:"protocol"`
		Direction  string `json:"direction"`
		Operations map[string]struct {
			Fields   json.RawMessage `json:"fields"`
			Response json.RawMessage `json:"response"`
			Body     *Body           `json:"body"`
			Bodies   *bool           `json:"bodies"`
			Context  json.RawMessage `json:"context"`
			HTTPS    *Mapping        `json:"https"`
		} `json:"operations"`
		Verification json.RawMessage `json:"verification"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("interface: %w", err)
	}
	id, err := digest.JSON(raw)
	if err != nil {
		return nil, fmt.Errorf("interface: digest: %w", err)
	}
	in := &Interface{Protocol: doc.Protocol, Direction: doc.Direction, Operations: map[string]*Operation{}, Digest: id}
	if len(doc.Operations) == 0 {
		return nil, fmt.Errorf("interface: it declares no operation")
	}
	if doc.Direction == DirectionEgress && doc.Verification != nil {
		return nil, fmt.Errorf("interface: verification is of an ingress interface")
	}

	// A field name has a single type in the whole interface (implementation §8.2).
	types := map[string]string{}
	for _, name := range slices.Sorted(maps.Keys(doc.Operations)) {
		d := doc.Operations[name]
		fail := func(format string, args ...any) error {
			return fmt.Errorf("interface: operation %s: %s", name, fmt.Sprintf(format, args...))
		}
		if !snakeCase(name) {
			return nil, fail("an operation is named in snake_case")
		}
		op := &Operation{HTTPS: d.HTTPS}
		if op.Fields, err = objectSchema(d.Fields); err != nil {
			return nil, fail("fields: %v", err)
		}
		for _, f := range slices.Sorted(maps.Keys(op.Fields.Properties)) {
			switch {
			case !fieldName(f) || slices.Contains(celReserved, f):
				return nil, fail("field %q is not an identifier", f)
			case slices.Contains(reserved, f):
				return nil, fail("field %q is named like a variable the conditions read", f)
			}
			sig := signature(op.Fields.Properties[f])
			if prev, ok := types[f]; ok && prev != sig {
				return nil, fail("field %q has another type in another operation", f)
			}
			types[f] = sig
		}
		if d.Response != nil {
			if op.Response, err = objectSchema(d.Response); err != nil {
				return nil, fail("response: %v", err)
			}
		}
		switch doc.Direction {
		case DirectionEgress:
			if d.Bodies != nil || d.Context != nil {
				return nil, fail("bodies and context are of an ingress operation")
			}
			if d.Body != nil {
				op.Body = *d.Body
			}
		case DirectionIngress:
			if d.Body != nil {
				return nil, fail("body is of an egress operation")
			}
		}
		switch {
		case doc.Protocol == ProtocolHTTPS && op.HTTPS == nil:
			return nil, fail("an https operation declares its mapping")
		case doc.Protocol != ProtocolHTTPS && op.HTTPS != nil:
			return nil, fail("only an https operation declares a mapping")
		}
		if op.HTTPS != nil && doc.Direction == DirectionEgress {
			if err := op.checkEgressMapping(); err != nil {
				return nil, fail("https: %v", err)
			}
		}
		in.Operations[name] = op
	}
	return in, nil
}

// objectSchema compiles an object schema with properties.
func objectSchema(raw json.RawMessage) (*schema.Schema, error) {
	s, err := schema.Compile(raw)
	if err != nil {
		return nil, err
	}
	if s.Kind != schema.KindObject || s.Properties == nil {
		return nil, fmt.Errorf("an object schema with properties")
	}
	return s, nil
}

// checkEgressMapping checks the mapping of an egress operation against its
// fields and its response (implementation §8.3).
func (op *Operation) checkEgressMapping() error {
	m := op.HTTPS
	if !strings.HasPrefix(m.Path, "/") {
		return fmt.Errorf("the path starts with /")
	}
	// Each header is written once, by a constant or by a field.
	headers, _ := m.written()
	for i, name := range headers {
		if forbiddenHeader(name) {
			return fmt.Errorf("the adapter sets %s", name)
		}
		if slices.Contains(headers[:i], name) {
			return fmt.Errorf("the header %s is written twice", name)
		}
	}
	params := map[string]bool{}
	for _, p := range pathParam.FindAllStringSubmatch(m.Path, -1) {
		params[p[1]] = true
	}
	for f := range op.Fields.Properties {
		if _, ok := m.Fields[f]; !ok {
			return fmt.Errorf("field %s has no mapping", f)
		}
	}
	for _, f := range slices.Sorted(maps.Keys(m.Fields)) {
		fm := m.Fields[f]
		fs, ok := op.Fields.Properties[f]
		if !ok {
			return fmt.Errorf("the mapping names %s, which is not a field", f)
		}
		scalar := fs.Kind == schema.KindString || fs.Kind == schema.KindInteger ||
			fs.Kind == schema.KindNumber || fs.Kind == schema.KindBoolean
		switch fm.In {
		case "path":
			switch {
			case fm.At != "":
				return fmt.Errorf("path field %s goes in its {%s} segment, without at", f, f)
			case !params[f]:
				return fmt.Errorf("the path has no {%s} segment", f)
			case !slices.Contains(op.Fields.Required, f):
				return fmt.Errorf("path field %s is required", f)
			case fs.Kind != schema.KindString && fs.Kind != schema.KindInteger:
				return fmt.Errorf("path field %s is a string or an integer", f)
			}
			delete(params, f)
		case "query", "header":
			switch {
			case fm.At == "":
				return fmt.Errorf("field %s declares the name it goes in", f)
			case !scalar:
				return fmt.Errorf("field %s in %s is a scalar", f, fm.In)
			case fm.In == "header" && forbiddenHeader(fm.At):
				return fmt.Errorf("the adapter sets %s", fm.At)
			}
		case "json":
			switch {
			case op.Body.Request:
				return fmt.Errorf("an operation with a request body admits no json fields")
			case !pointer(fm.At) || fm.At == "":
				return fmt.Errorf("field %s goes at a JSON Pointer inside the body", f)
			}
		}
	}
	if len(params) > 0 {
		return fmt.Errorf("the path's {%s} segment is no path field", slices.Sorted(maps.Keys(params))[0])
	}

	var declared map[string]*schema.Schema
	if op.Response != nil {
		declared = op.Response.Properties
	}
	if _, ok := declared[statusField]; ok {
		return fmt.Errorf("the response carries status without declaring it")
	}
	for f := range declared {
		if _, ok := m.Response[f]; !ok {
			return fmt.Errorf("response field %s has no mapping", f)
		}
	}
	for _, f := range slices.Sorted(maps.Keys(m.Response)) {
		fm := m.Response[f]
		if _, ok := declared[f]; !ok {
			return fmt.Errorf("the response mapping names %s, which is not in the response", f)
		}
		switch fm.From {
		case "json":
			if op.Body.Response {
				return fmt.Errorf("with a response body, the response carries only header fields")
			}
			if !pointer(fm.At) {
				return fmt.Errorf("response field %s comes from a JSON Pointer", f)
			}
		case "header":
			if fm.At == "" {
				return fmt.Errorf("response field %s names its header", f)
			}
		}
	}
	return nil
}

// written returns the headers and the query parameters the mapping writes
// from a field, canonicalized, to compare them with the credential's place
// (implementation §8.4).
func (m *Mapping) written() (headers, query []string) {
	for name := range m.Headers {
		headers = append(headers, textproto.CanonicalMIMEHeaderKey(name))
	}
	for _, fm := range m.Fields {
		switch fm.In {
		case "header":
			headers = append(headers, textproto.CanonicalMIMEHeaderKey(fm.At))
		case "query":
			query = append(query, fm.At)
		}
	}
	return headers, query
}

func forbiddenHeader(name string) bool {
	return slices.Contains(adapterHeaders, textproto.CanonicalMIMEHeaderKey(name))
}

// pointer reports whether p is a JSON Pointer (RFC 6901): empty, or a sequence
// of /-prefixed tokens whose ~ escapes are ~0 or ~1.
func pointer(p string) bool {
	if p == "" {
		return true
	}
	if !strings.HasPrefix(p, "/") {
		return false
	}
	for i := 0; i < len(p); i++ {
		if p[i] == '~' && (i+1 == len(p) || (p[i+1] != '0' && p[i+1] != '1')) {
			return false
		}
	}
	return true
}

// signature is a schema's CEL type, written out, to compare two declarations of
// the same field.
func signature(s *schema.Schema) string {
	var b bytes.Buffer
	var write func(*schema.Schema)
	write = func(s *schema.Schema) {
		b.WriteString(string(s.Kind))
		switch {
		case s.DateTime:
			b.WriteString(":date-time")
		case s.Items != nil:
			b.WriteString("[")
			write(s.Items)
			b.WriteString("]")
		case s.AdditionalProperties != nil:
			b.WriteString("{*:")
			write(s.AdditionalProperties)
			b.WriteString("}")
		case s.Properties != nil:
			b.WriteString("{")
			for _, k := range slices.Sorted(maps.Keys(s.Properties)) {
				b.WriteString(k + ":")
				write(s.Properties[k])
				b.WriteString(",")
			}
			b.WriteString("}")
		}
	}
	write(s)
	return b.String()
}
