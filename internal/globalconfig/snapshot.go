package globalconfig

import (
	"bytes"
	"encoding/json"
	"maps"
	"sort"

	"zobik.org/zobik/internal/rules"
)

// Values is a snapshot of the versioned region: the whole state, never a delta
// (architecture §2.3). A key without a value is absent.
type Values map[string]json.RawMessage

// InvalidError is an edit that does not validate against the schema, which the
// Config Store closes with request_invalid (implementation §10.1).
type InvalidError struct {
	Key, Reason string
}

func (e *InvalidError) Error() string {
	if e.Key == "" {
		return e.Reason
	}
	return e.Key + ": " + e.Reason
}

// V0 is the instance the defaults form (implementation §1.2.5).
func (s *Schema) V0() Values {
	v := Values{}
	for name, k := range s.Keys {
		if k.Storage == Versioned && k.Default != nil {
			v[name] = k.Default
		}
	}
	return v
}

// ApplyVersioned applies assignments over schema keys on head and returns the
// resulting snapshot. It rejects a key that does not exist or belongs to the
// mutable region, a value that does not validate, and an unsatisfied
// precondition of an assigned key over the result (architecture §3.14.4).
func (s *Schema) ApplyVersioned(head Values, assignments map[string]json.RawMessage) (Values, error) {
	if len(assignments) == 0 {
		return nil, &InvalidError{Reason: "the edit assigns no key"}
	}
	names := make([]string, 0, len(assignments))
	for name := range assignments {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		k, ok := s.Keys[name]
		if !ok {
			return nil, &InvalidError{Key: name, Reason: "no such key"}
		}
		if k.Storage != Versioned {
			return nil, &InvalidError{Key: name, Reason: "the key is in the mutable region"}
		}
		if err := k.Schema.ValidateJSON(assignments[name]); err != nil {
			return nil, &InvalidError{Key: name, Reason: err.Error()}
		}
	}
	result := maps.Clone(head)
	if result == nil {
		result = Values{}
	}
	maps.Copy(result, assignments)
	return s.check(result, names)
}

// check evaluates the preconditions declared on keys over values.
func (s *Schema) check(values Values, keys []string) (Values, error) {
	obj := map[string]any{}
	for name, raw := range values {
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		var v any
		if err := dec.Decode(&v); err != nil {
			return nil, &InvalidError{Key: name, Reason: err.Error()}
		}
		obj[name] = v
	}
	native, err := rules.Native(s.snapshot, obj)
	if err != nil {
		return nil, err
	}
	vars := map[string]any{"config": native}
	for _, name := range keys {
		k, ok := s.Keys[name]
		if !ok {
			continue
		}
		for i, r := range k.rules {
			ok, err := r.Eval(vars)
			if err != nil {
				return nil, &InvalidError{Key: name, Reason: err.Error()}
			}
			if !ok {
				return nil, &InvalidError{Key: name, Reason: k.Preconditions[i].Message}
			}
		}
	}
	return values, nil
}
