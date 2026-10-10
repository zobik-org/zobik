package schema

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
)

// The structural subset of JSON Schema 2020-12 (implementation §8.2): each node of
// the schema has a single known type, so each field has a CEL type. Compile rejects
// every keyword outside the subset.

// maxSafeInteger bounds integers so that every value survives a float64 (implementation §8.1).
const maxSafeInteger = 1<<53 - 1

// Kind is the single type of a node.
type Kind string

const (
	KindString  Kind = "string"
	KindInteger Kind = "integer"
	KindNumber  Kind = "number"
	KindBoolean Kind = "boolean"
	KindArray   Kind = "array"
	KindObject  Kind = "object"
	// KindOpaque is a node with opaque: true, which admits any JSON.
	KindOpaque Kind = "opaque"
)

// Schema is a compiled node.
type Schema struct {
	Kind Kind
	// DateTime is a string with format: date-time.
	DateTime bool

	Enum                 []string
	MinLength, MaxLength *int

	Minimum, Maximum, ExclusiveMinimum, ExclusiveMaximum *float64

	Items              *Schema
	MinItems, MaxItems *int

	// Properties and Required describe an object with fields; it ignores keys it
	// does not declare (implementation §10.3).
	Properties map[string]*Schema
	Required   []string
	// AdditionalProperties describes an object that is a map.
	AdditionalProperties *Schema
}

// The keywords each kind admits, besides type and description.
var admitted = map[Kind][]string{
	KindString:  {"enum", "minLength", "maxLength"},
	KindInteger: {"minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum"},
	KindNumber:  {"minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum"},
	KindBoolean: {},
	KindArray:   {"items", "minItems", "maxItems"},
	KindObject:  {"properties", "required", "additionalProperties"},
}

// rootOnly are identification keywords a document may carry at its root.
var rootOnly = []string{"$schema", "$id"}

// Compile parses a schema document and checks that it stays within the subset.
func Compile(doc []byte) (*Schema, error) {
	var raw map[string]json.RawMessage
	if err := strictUnmarshal(doc, &raw); err != nil {
		return nil, fmt.Errorf("schema: %w", err)
	}
	for _, k := range rootOnly {
		delete(raw, k)
	}
	return compile(raw, "")
}

func compile(raw map[string]json.RawMessage, path string) (*Schema, error) {
	at := func(format string, args ...any) error {
		where := path
		if where == "" {
			where = "(root)"
		}
		return fmt.Errorf("schema: %s: %s", where, fmt.Sprintf(format, args...))
	}
	keys := map[string]bool{}
	for k := range raw {
		keys[k] = true
	}
	delete(keys, "description")

	if op, ok := raw["opaque"]; ok {
		var opaque bool
		if err := json.Unmarshal(op, &opaque); err != nil || !opaque {
			return nil, at("opaque must be true")
		}
		delete(keys, "opaque")
		if len(keys) > 0 {
			return nil, at("an opaque node admits no other keyword: %s", sorted(keys))
		}
		return &Schema{Kind: KindOpaque}, nil
	}

	var kind Kind
	if err := json.Unmarshal(raw["type"], &kind); err != nil || raw["type"] == nil {
		return nil, at("type must be a single type name")
	}
	delete(keys, "type")
	allowed, ok := admitted[kind]
	if !ok {
		return nil, at("unknown type %q", kind)
	}
	s := &Schema{Kind: kind}

	if kind == KindString {
		if f, ok := raw["format"]; ok {
			var format string
			if err := json.Unmarshal(f, &format); err != nil || format != "date-time" {
				return nil, at("the only format is date-time")
			}
			delete(keys, "format")
			if len(keys) > 0 {
				return nil, at("a date-time admits no other keyword: %s", sorted(keys))
			}
			s.DateTime = true
			return s, nil
		}
	}
	for _, k := range allowed {
		delete(keys, k)
	}
	if len(keys) > 0 {
		return nil, at("keywords outside the subset for %s: %s", kind, sorted(keys))
	}

	var err error
	field := func(name string, dst any) {
		if v, ok := raw[name]; ok && err == nil {
			if e := strictUnmarshal(v, dst); e != nil {
				err = at("%s: %v", name, e)
			}
		}
	}
	switch kind {
	case KindString:
		field("enum", &s.Enum)
		field("minLength", &s.MinLength)
		field("maxLength", &s.MaxLength)
	case KindInteger, KindNumber:
		field("minimum", &s.Minimum)
		field("maximum", &s.Maximum)
		field("exclusiveMinimum", &s.ExclusiveMinimum)
		field("exclusiveMaximum", &s.ExclusiveMaximum)
		if kind == KindInteger && err == nil {
			for _, b := range []*float64{s.Minimum, s.Maximum, s.ExclusiveMinimum, s.ExclusiveMaximum} {
				if b != nil && math.Abs(*b) > maxSafeInteger {
					return nil, at("integer bounds go past ±(2^53 − 1)")
				}
			}
		}
	case KindArray:
		field("minItems", &s.MinItems)
		field("maxItems", &s.MaxItems)
		items, ok := raw["items"]
		if !ok {
			return nil, at("an array declares items")
		}
		if s.Items, err = compileChild(items, path+"[]"); err != nil {
			return nil, err
		}
	case KindObject:
		props, hasProps := raw["properties"]
		add, hasAdd := raw["additionalProperties"]
		switch {
		case hasProps && hasAdd:
			return nil, at("an object declares properties or additionalProperties, not both")
		case hasAdd:
			if _, ok := raw["required"]; ok {
				return nil, at("a map declares no required")
			}
			if s.AdditionalProperties, err = compileChild(add, path+"{}"); err != nil {
				return nil, err
			}
		case hasProps:
			var rawProps map[string]json.RawMessage
			if err := strictUnmarshal(props, &rawProps); err != nil {
				return nil, at("properties: %v", err)
			}
			s.Properties = map[string]*Schema{}
			for name, p := range rawProps {
				if s.Properties[name], err = compileChild(p, join(path, name)); err != nil {
					return nil, err
				}
			}
			field("required", &s.Required)
			for _, r := range s.Required {
				if _, ok := s.Properties[r]; !ok {
					return nil, at("required names %q, which is not a property", r)
				}
			}
		default:
			return nil, at("an object declares properties or additionalProperties")
		}
	}
	return s, err
}

func compileChild(doc json.RawMessage, path string) (*Schema, error) {
	var raw map[string]json.RawMessage
	if err := strictUnmarshal(doc, &raw); err != nil || raw == nil {
		return nil, fmt.Errorf("schema: %s: a node is an object", path)
	}
	return compile(raw, path)
}

func strictUnmarshal(data []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

func join(path, name string) string {
	if path == "" {
		return name
	}
	return path + "." + name
}

func sorted(keys map[string]bool) string {
	list := make([]string, 0, len(keys))
	for k := range keys {
		list = append(list, k)
	}
	sort.Strings(list)
	return strings.Join(list, ", ")
}
