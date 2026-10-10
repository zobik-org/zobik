package schema

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strconv"
	"time"
	"unicode/utf8"
)

// ValidationError names where a value fails its schema.
type ValidationError struct {
	Path, Reason string
}

func (e *ValidationError) Error() string {
	if e.Path == "" {
		return e.Reason
	}
	return e.Path + ": " + e.Reason
}

// ValidateJSON validates a JSON document against s.
func (s *Schema) ValidateJSON(doc []byte) error {
	dec := json.NewDecoder(bytes.NewReader(doc))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return &ValidationError{Reason: "invalid JSON: " + err.Error()}
	}
	if dec.More() {
		return &ValidationError{Reason: "invalid JSON: trailing data"}
	}
	return s.Validate(v)
}

// Validate validates a value decoded with json.Decoder.UseNumber.
func (s *Schema) Validate(v any) error {
	return s.validate(v, "")
}

func (s *Schema) validate(v any, path string) error {
	fail := func(format string, args ...any) error {
		return &ValidationError{Path: path, Reason: fmt.Sprintf(format, args...)}
	}
	if s.Kind == KindOpaque {
		return nil
	}
	// The subset has no null (implementation §8.2): an optional field is absent.
	if v == nil {
		return fail("null is not a value; omit the field")
	}
	switch s.Kind {
	case KindString:
		str, ok := v.(string)
		if !ok {
			return fail("expected string")
		}
		if s.DateTime {
			if _, err := time.Parse(time.RFC3339Nano, str); err != nil {
				return fail("expected an RFC 3339 date-time")
			}
			return nil
		}
		if len(s.Enum) > 0 && !slices.Contains(s.Enum, str) {
			return fail("%q is not one of %v", str, s.Enum)
		}
		n := utf8.RuneCountInString(str)
		if s.MinLength != nil && n < *s.MinLength {
			return fail("shorter than %d", *s.MinLength)
		}
		if s.MaxLength != nil && n > *s.MaxLength {
			return fail("longer than %d", *s.MaxLength)
		}
	case KindInteger, KindNumber:
		num, ok := v.(json.Number)
		if !ok {
			return fail("expected %s", s.Kind)
		}
		f, err := strconv.ParseFloat(string(num), 64)
		if err != nil {
			return fail("expected %s", s.Kind)
		}
		if s.Kind == KindInteger && (f != math.Trunc(f) || math.Abs(f) > maxSafeInteger) {
			return fail("expected an integer within ±(2^53 − 1)")
		}
		if s.Minimum != nil && f < *s.Minimum {
			return fail("below the minimum %v", *s.Minimum)
		}
		if s.Maximum != nil && f > *s.Maximum {
			return fail("above the maximum %v", *s.Maximum)
		}
		if s.ExclusiveMinimum != nil && f <= *s.ExclusiveMinimum {
			return fail("not above %v", *s.ExclusiveMinimum)
		}
		if s.ExclusiveMaximum != nil && f >= *s.ExclusiveMaximum {
			return fail("not below %v", *s.ExclusiveMaximum)
		}
	case KindBoolean:
		if _, ok := v.(bool); !ok {
			return fail("expected boolean")
		}
	case KindArray:
		list, ok := v.([]any)
		if !ok {
			return fail("expected array")
		}
		if s.MinItems != nil && len(list) < *s.MinItems {
			return fail("fewer than %d items", *s.MinItems)
		}
		if s.MaxItems != nil && len(list) > *s.MaxItems {
			return fail("more than %d items", *s.MaxItems)
		}
		for i, item := range list {
			if err := s.Items.validate(item, fmt.Sprintf("%s[%d]", path, i)); err != nil {
				return err
			}
		}
	case KindObject:
		obj, ok := v.(map[string]any)
		if !ok {
			return fail("expected object")
		}
		if s.AdditionalProperties != nil {
			for _, k := range sortedKeys(obj) {
				if err := s.AdditionalProperties.validate(obj[k], join(path, k)); err != nil {
					return err
				}
			}
			return nil
		}
		for _, r := range s.Required {
			if _, ok := obj[r]; !ok {
				return &ValidationError{Path: join(path, r), Reason: "required"}
			}
		}
		// Keys the schema does not declare are ignored (implementation §10.3).
		for _, k := range sortedKeys(obj) {
			if p, ok := s.Properties[k]; ok {
				if err := p.validate(obj[k], join(path, k)); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}
