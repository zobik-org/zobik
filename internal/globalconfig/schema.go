// Package globalconfig is the Global Configuration's declared schema: its keys,
// the v0 their defaults form, and the validation of an edit against them
// (architecture §2.3, implementation §1.2.5). The Config Store validates with it
// and the console validates with it before signing.
package globalconfig

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"

	"zobik.org/zobik/internal/rules"
	"zobik.org/zobik/internal/schema"
)

// The query class and the region of a key (architecture §2.3).
const (
	Pinned  = "pinned"
	Current = "current"

	Versioned = "versioned"
	Mutable   = "mutable"
)

// Precondition is a condition the resulting snapshot has to meet, in CEL over the
// variable config (implementation §1.2.5).
type Precondition struct {
	Rule    string `json:"rule"`
	Message string `json:"message"`
}

// Key is a declared key.
type Key struct {
	Name   string
	Schema *schema.Schema
	// Default is the key's value in v0; nil for a key without a default.
	Default json.RawMessage
	Storage string
	// QueryClass is the key's class; a composite key declares one per field in
	// FieldClasses instead.
	QueryClass   string
	FieldClasses map[string]string

	Preconditions []Precondition
	rules         []*rules.Bool
}

// Schema is the assembled schema of the Global Configuration.
type Schema struct {
	Keys map[string]*Key
	// snapshot is the versioned region as one object, for the preconditions.
	snapshot *schema.Schema
}

// The annotations a key carries besides the subset's keywords (implementation §1.2.5).
const (
	annDefault       = "default"
	annStorage       = "storage"
	annQueryClass    = "query_class"
	annPreconditions = "preconditions"
)

// Assemble builds the schema from one fragment per piece of logic that consumes
// parameters (implementation §1.2.5). A fragment is an object whose properties are
// its keys; a key is declared in a single fragment.
func Assemble(fragments ...[]byte) (*Schema, error) {
	s := &Schema{Keys: map[string]*Key{}}
	for _, f := range fragments {
		var frag struct {
			Description string                     `json:"description"`
			Properties  map[string]json.RawMessage `json:"properties"`
		}
		if err := strict(f, &frag); err != nil {
			return nil, fmt.Errorf("globalconfig: fragment: %w", err)
		}
		for name, raw := range frag.Properties {
			if _, dup := s.Keys[name]; dup {
				return nil, fmt.Errorf("globalconfig: %s is declared in two fragments", name)
			}
			k, err := parseKey(name, raw)
			if err != nil {
				return nil, err
			}
			s.Keys[name] = k
		}
	}

	snapshot := &schema.Schema{Kind: schema.KindObject, Properties: map[string]*schema.Schema{}}
	for _, k := range s.Keys {
		if k.Storage == Versioned {
			snapshot.Properties[k.Name] = k.Schema
		}
	}
	s.snapshot = snapshot
	if err := s.compilePreconditions(); err != nil {
		return nil, err
	}

	// v0 has to be a valid configuration itself.
	if _, err := s.check(s.V0(), s.names()); err != nil {
		return nil, fmt.Errorf("globalconfig: v0: %w", err)
	}
	return s, nil
}

func parseKey(name string, raw json.RawMessage) (*Key, error) {
	fail := func(format string, args ...any) error {
		return fmt.Errorf("globalconfig: %s: %s", name, fmt.Sprintf(format, args...))
	}
	var node map[string]json.RawMessage
	if err := strict(raw, &node); err != nil {
		return nil, fail("%v", err)
	}
	k := &Key{Name: name}
	if d, ok := node[annDefault]; ok {
		k.Default = d
	}
	if err := annotation(node, annStorage, &k.Storage); err != nil {
		return nil, fail("%v", err)
	}
	if err := annotation(node, annQueryClass, &k.QueryClass); err != nil {
		return nil, fail("%v", err)
	}
	if p, ok := node[annPreconditions]; ok {
		if err := strict(p, &k.Preconditions); err != nil {
			return nil, fail("preconditions: %v", err)
		}
	}
	for _, a := range []string{annDefault, annStorage, annQueryClass, annPreconditions} {
		delete(node, a)
	}

	// A composite key declares its class per field (architecture §2.3).
	if k.QueryClass == "" {
		var props map[string]map[string]json.RawMessage
		if p, ok := node["properties"]; ok {
			if err := strict(p, &props); err != nil {
				return nil, fail("properties: %v", err)
			}
		}
		if len(props) == 0 {
			return nil, fail("a key declares its query_class, or one per field")
		}
		k.FieldClasses = map[string]string{}
		for field, fn := range props {
			var class string
			if err := annotation(fn, annQueryClass, &class); err != nil || class == "" {
				return nil, fail("field %s declares no query_class", field)
			}
			k.FieldClasses[field] = class
			delete(fn, annQueryClass)
		}
		stripped, err := json.Marshal(props)
		if err != nil {
			return nil, fail("%v", err)
		}
		node["properties"] = stripped
	}

	for _, class := range k.classes() {
		if class != Pinned && class != Current {
			return nil, fail("query_class %q", class)
		}
		// Only a current key can be mutable (architecture §2.3).
		if k.Storage == Mutable && class != Current {
			return nil, fail("a pinned key cannot be mutable")
		}
	}
	switch k.Storage {
	case Versioned:
	case Mutable:
		// There are no preconditions across regions (architecture §2.3).
		if len(k.Preconditions) > 0 {
			return nil, fail("a mutable key declares no preconditions")
		}
	default:
		return nil, fail("storage %q", k.Storage)
	}

	doc, err := json.Marshal(node)
	if err != nil {
		return nil, fail("%v", err)
	}
	if k.Schema, err = schema.Compile(doc); err != nil {
		return nil, fail("%v", err)
	}
	if k.Default != nil {
		if err := k.Schema.ValidateJSON(k.Default); err != nil {
			return nil, fail("default: %v", err)
		}
	}
	return k, nil
}

func (k *Key) classes() []string {
	if k.QueryClass != "" {
		return []string{k.QueryClass}
	}
	out := make([]string, 0, len(k.FieldClasses))
	for _, c := range k.FieldClasses {
		out = append(out, c)
	}
	return out
}

// snapshotType names the object type of the snapshot in the preconditions.
const snapshotType = "zobik.config"

func (s *Schema) compilePreconditions() error {
	p, err := rules.NewProvider()
	if err != nil {
		return err
	}
	t, err := p.Declare(snapshotType, s.snapshot)
	if err != nil {
		return err
	}
	env, err := rules.NewEnv(p, rules.Var{Name: "config", Type: t})
	if err != nil {
		return err
	}
	for _, k := range s.Keys {
		for _, pc := range k.Preconditions {
			b, err := rules.CompileBool(env, pc.Rule)
			if err != nil {
				return fmt.Errorf("globalconfig: %s: precondition: %w", k.Name, err)
			}
			k.rules = append(k.rules, b)
		}
	}
	return nil
}

func (s *Schema) names() []string {
	out := make([]string, 0, len(s.Keys))
	for n := range s.Keys {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

func annotation(node map[string]json.RawMessage, name string, dst *string) error {
	v, ok := node[name]
	if !ok {
		return nil
	}
	if err := json.Unmarshal(v, dst); err != nil {
		return fmt.Errorf("%s: %v", name, err)
	}
	return nil
}

func strict(data []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}
