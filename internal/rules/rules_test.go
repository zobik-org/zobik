package rules

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"zobik.org/zobik/internal/schema"
)

const doc = `{
  "type": "object",
  "properties": {
    "count": {"type": "integer"},
    "share": {"type": "number"},
    "mode": {"type": "string"},
    "model": {"type": "object", "required": ["name"], "properties": {"name": {"type": "string"}}},
    "lists": {"type": "object", "additionalProperties": {"type": "array", "items": {"type": "string"}}}
  }
}`

func setup(t *testing.T) (*Provider, *schema.Schema, Var) {
	t.Helper()
	s, err := schema.Compile([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewProvider()
	if err != nil {
		t.Fatal(err)
	}
	typ, err := p.Declare("test", s)
	if err != nil {
		t.Fatal(err)
	}
	return p, s, Var{Name: "config", Type: typ}
}

func native(t *testing.T, s *schema.Schema, value string) any {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader([]byte(value)))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		t.Fatal(err)
	}
	n, err := Native(s, v)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestCompileAndEval(t *testing.T) {
	p, s, v := setup(t)
	env, err := NewEnv(p, v)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		expr, value string
		want        bool
	}{
		{`config.count <= 3`, `{"count": 3}`, true},
		{`config.mode == "frozen" || has(config.model)`, `{"mode": "open"}`, false},
		{`config.mode == "frozen" || has(config.model)`, `{"mode": "open", "model": {"name": "x"}}`, true},
		{`double(config.count) <= config.share * 10.0`, `{"count": 2, "share": 0.25}`, true},
		{`"a" in config.lists["allowed"]`, `{"lists": {"allowed": ["a", "b"]}}`, true},
	}
	for _, c := range cases {
		b, err := CompileBool(env, c.expr)
		if err != nil {
			t.Fatalf("%s: %v", c.expr, err)
		}
		got, err := b.Eval(map[string]any{"config": native(t, s, c.value)})
		if err != nil {
			t.Fatalf("%s over %s: %v", c.expr, c.value, err)
		}
		if got != c.want {
			t.Errorf("%s over %s = %v, want %v", c.expr, c.value, got, c.want)
		}
	}
}

func TestCompileRejects(t *testing.T) {
	p, _, v := setup(t)
	env, err := NewEnv(p, v)
	if err != nil {
		t.Fatal(err)
	}
	for _, expr := range []string{
		`config.missing == 1`,                         // no such field
		`config.count <= config.share * config.count`, // double * int has no overload
		`config.count`,                                // not a bool
		`config.mode == 1`,                            // string against int
	} {
		if _, err := CompileBool(env, expr); err == nil {
			t.Errorf("%s compiled", expr)
		}
	}
}

func TestEvalErrorIsNotFalse(t *testing.T) {
	p, s, v := setup(t)
	env, _ := NewEnv(p, v)
	b, err := CompileBool(env, `config.count > 1`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Eval(map[string]any{"config": native(t, s, `{}`)}); err == nil || !strings.Contains(err.Error(), "count") {
		t.Errorf("reading an absent field gave %v", err)
	}
}
