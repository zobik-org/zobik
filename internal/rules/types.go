package rules

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"time"

	"cel.dev/cel-go/common/types"

	"zobik.org/zobik/internal/schema"
)

// Each node of the subset has a single CEL type (implementation §8.2), so a
// condition is compiled against the fields it reads.

// Provider is the CEL type provider of the objects a schema declares. It adds
// one object type per object node with properties to the standard types.
type Provider struct {
	*types.Registry
	objects map[string]map[string]*types.Type
}

// NewProvider returns a provider with only the standard types.
func NewProvider() (*Provider, error) {
	r, err := types.NewRegistry()
	if err != nil {
		return nil, err
	}
	return &Provider{Registry: r, objects: map[string]map[string]*types.Type{}}, nil
}

// Declare returns the CEL type of s, registering an object type for each object
// node with properties, named name and, below it, by the path of each field.
func (p *Provider) Declare(name string, s *schema.Schema) (*types.Type, error) {
	switch s.Kind {
	case schema.KindString:
		if s.DateTime {
			return types.TimestampType, nil
		}
		return types.StringType, nil
	case schema.KindInteger:
		return types.IntType, nil
	case schema.KindNumber:
		return types.DoubleType, nil
	case schema.KindBoolean:
		return types.BoolType, nil
	case schema.KindOpaque:
		return types.DynType, nil
	case schema.KindArray:
		item, err := p.Declare(name+"_item", s.Items)
		if err != nil {
			return nil, err
		}
		return types.NewListType(item), nil
	case schema.KindObject:
		if s.AdditionalProperties != nil {
			value, err := p.Declare(name+"_value", s.AdditionalProperties)
			if err != nil {
				return nil, err
			}
			return types.NewMapType(types.StringType, value), nil
		}
		if _, ok := p.objects[name]; ok {
			return nil, fmt.Errorf("rules: object type %s declared twice", name)
		}
		fields := map[string]*types.Type{}
		p.objects[name] = fields
		for field, fs := range s.Properties {
			t, err := p.Declare(name+"."+field, fs)
			if err != nil {
				return nil, err
			}
			fields[field] = t
		}
		return types.NewObjectType(name), nil
	}
	return nil, fmt.Errorf("rules: no CEL type for %s", s.Kind)
}

func (p *Provider) FindStructType(name string) (*types.Type, bool) {
	if _, ok := p.objects[name]; ok {
		return types.NewTypeTypeWithParam(types.NewObjectType(name)), true
	}
	return p.Registry.FindStructType(name)
}

func (p *Provider) FindStructFieldNames(name string) ([]string, bool) {
	fields, ok := p.objects[name]
	if !ok {
		return p.Registry.FindStructFieldNames(name)
	}
	names := make([]string, 0, len(fields))
	for f := range fields {
		names = append(names, f)
	}
	sort.Strings(names)
	return names, true
}

// FindStructFieldType gives the checker the field's type. It leaves the access
// to the interpreter, which reads the field from the map Native builds.
func (p *Provider) FindStructFieldType(name, field string) (*types.FieldType, bool) {
	fields, ok := p.objects[name]
	if !ok {
		return p.Registry.FindStructFieldType(name, field)
	}
	t, ok := fields[field]
	if !ok {
		return nil, false
	}
	return &types.FieldType{Type: t}, true
}

// Native converts a value that validates against s, decoded with
// json.Decoder.UseNumber, into the value CEL evaluates: an integer as int64, a
// number as float64 and a date-time as time.Time. An absent field stays absent,
// which is what has() tests.
func Native(s *schema.Schema, v any) (any, error) {
	switch s.Kind {
	case schema.KindInteger:
		n, ok := v.(json.Number)
		if !ok {
			return nil, fmt.Errorf("rules: expected integer")
		}
		return strconv.ParseInt(string(n), 10, 64)
	case schema.KindNumber:
		n, ok := v.(json.Number)
		if !ok {
			return nil, fmt.Errorf("rules: expected number")
		}
		return strconv.ParseFloat(string(n), 64)
	case schema.KindString:
		if s.DateTime {
			str, _ := v.(string)
			return time.Parse(time.RFC3339Nano, str)
		}
		return v, nil
	case schema.KindArray:
		list, _ := v.([]any)
		out := make([]any, len(list))
		for i, item := range list {
			var err error
			if out[i], err = Native(s.Items, item); err != nil {
				return nil, err
			}
		}
		return out, nil
	case schema.KindObject:
		obj, _ := v.(map[string]any)
		out := make(map[string]any, len(obj))
		for k, fv := range obj {
			fs := s.AdditionalProperties
			if fs == nil {
				if fs = s.Properties[k]; fs == nil {
					continue // a field the schema does not declare is not a field of the type
				}
			}
			var err error
			if out[k], err = Native(fs, fv); err != nil {
				return nil, err
			}
		}
		return out, nil
	}
	return v, nil
}
