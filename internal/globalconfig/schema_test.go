package globalconfig

import (
	"strings"
	"testing"
)

// What Assemble rejects in a fragment (architecture §2.3, implementation §1.2.5).
func TestAssembleRejects(t *testing.T) {
	cases := []struct {
		name, fragment, reject string
	}{
		{"no query class",
			`{"properties": {"k": {"type": "integer", "default": 1, "storage": "versioned"}}}`,
			"query_class"},
		{"unknown query class",
			`{"properties": {"k": {"type": "integer", "default": 1, "storage": "versioned", "query_class": "live"}}}`,
			"query_class"},
		{"no storage",
			`{"properties": {"k": {"type": "integer", "default": 1, "query_class": "current"}}}`,
			"storage"},
		{"pinned and mutable",
			`{"properties": {"k": {"type": "array", "items": {"type": "string"}, "default": [], "query_class": "pinned", "storage": "mutable"}}}`,
			"cannot be mutable"},
		{"a composite field without a class",
			`{"properties": {"k": {"type": "object", "properties": {"a": {"type": "string", "query_class": "current"}, "b": {"type": "string"}}, "storage": "versioned"}}}`,
			"field b"},
		{"a keyword outside the subset",
			`{"properties": {"k": {"type": "string", "pattern": "x", "default": "x", "query_class": "current", "storage": "versioned"}}}`,
			"outside the subset"},
		{"a default that does not validate",
			`{"properties": {"k": {"type": "integer", "maximum": 5, "default": 9, "query_class": "current", "storage": "versioned"}}}`,
			"default"},
		{"a precondition that does not compile",
			`{"properties": {"k": {"type": "integer", "default": 1, "query_class": "current", "storage": "versioned", "preconditions": [{"rule": "config.k == \"1\"", "message": "m"}]}}}`,
			"precondition"},
		{"a v0 that breaks its precondition",
			`{"properties": {"k": {"type": "integer", "default": 1, "query_class": "current", "storage": "versioned", "preconditions": [{"rule": "config.k > 1", "message": "k is above one"}]}}}`,
			"k is above one"},
		{"a precondition across regions",
			`{"properties": {"k": {"type": "array", "items": {"type": "string"}, "default": [], "query_class": "current", "storage": "mutable", "preconditions": [{"rule": "true", "message": "m"}]}}}`,
			"no preconditions"},
	}
	for _, c := range cases {
		_, err := Assemble([]byte(c.fragment))
		if err == nil || !strings.Contains(err.Error(), c.reject) {
			t.Errorf("%s: got %v, want an error with %q", c.name, err, c.reject)
		}
	}
	dup := `{"properties": {"k": {"type": "integer", "default": 1, "query_class": "current", "storage": "versioned"}}}`
	if _, err := Assemble([]byte(dup), []byte(dup)); err == nil || !strings.Contains(err.Error(), "two fragments") {
		t.Errorf("a key in two fragments: %v", err)
	}
}

// The mutable region stays out of v0's snapshot.
func TestMutableOutsideSnapshot(t *testing.T) {
	s, err := Assemble([]byte(`{"properties": {
		"k": {"type": "integer", "default": 1, "query_class": "current", "storage": "versioned"},
		"lists": {"type": "array", "items": {"type": "string"}, "default": [], "query_class": "current", "storage": "mutable"}
	}}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := s.V0()["lists"]; ok {
		t.Error("a mutable key is in the snapshot")
	}
}
