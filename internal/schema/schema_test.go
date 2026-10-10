package schema

import (
	"strings"
	"testing"
)

func TestCompileRejectsOutsideTheSubset(t *testing.T) {
	cases := map[string]string{
		"$ref":                     `{"type":"object","properties":{"a":{"$ref":"#/x"}}}`,
		"oneOf":                    `{"oneOf":[{"type":"string"},{"type":"integer"}]}`,
		"anyOf":                    `{"type":"string","anyOf":[]}`,
		"allOf":                    `{"type":"string","allOf":[]}`,
		"not":                      `{"type":"string","not":{}}`,
		"if":                       `{"type":"string","if":{}}`,
		"pattern":                  `{"type":"string","pattern":"^a"}`,
		"type list":                `{"type":["string","null"]}`,
		"no type":                  `{"description":"x"}`,
		"null type":                `{"type":"null"}`,
		"other format":             `{"type":"string","format":"email"}`,
		"date-time with enum":      `{"type":"string","format":"date-time","enum":["x"]}`,
		"keyword of another kind":  `{"type":"integer","maxLength":3}`,
		"properties and map":       `{"type":"object","properties":{},"additionalProperties":{"type":"string"}}`,
		"additionalProperties off": `{"type":"object","properties":{},"additionalProperties":false}`,
		"bare object":              `{"type":"object"}`,
		"required not a property":  `{"type":"object","properties":{},"required":["a"]}`,
		"array without items":      `{"type":"array"}`,
		"opaque with type":         `{"opaque":true,"type":"string"}`,
		"unsafe integer bound":     `{"type":"integer","maximum":9007199254740992}`,
		"$id below the root":       `{"type":"object","properties":{"a":{"$id":"x","type":"string"}}}`,
	}
	for name, doc := range cases {
		if _, err := Compile([]byte(doc)); err == nil {
			t.Errorf("%s: compiled", name)
		}
	}
}

const sample = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "sample",
  "type": "object",
  "description": "a sample",
  "required": ["name", "count"],
  "properties": {
    "name":   {"type": "string", "minLength": 1, "maxLength": 4},
    "level":  {"type": "string", "enum": ["low", "high"]},
    "count":  {"type": "integer", "minimum": 0},
    "ratio":  {"type": "number", "exclusiveMaximum": 1},
    "on":     {"type": "boolean"},
    "since":  {"type": "string", "format": "date-time"},
    "tags":   {"type": "array", "items": {"type": "string"}, "maxItems": 2},
    "labels": {"type": "object", "additionalProperties": {"type": "integer"}},
    "schema": {"opaque": true}
  }
}`

func TestValidate(t *testing.T) {
	s, err := Compile([]byte(sample))
	if err != nil {
		t.Fatal(err)
	}
	valid := []string{
		`{"name":"a","count":0}`,
		`{"name":"ñañá","count":3}`,
		`{"name":"a","count":2.0,"ratio":0.5,"on":true,"since":"2026-10-10T12:00:00Z","tags":["x"],"labels":{"a":1},"schema":null}`,
		`{"name":"a","count":1,"unknown":{"anything":[1]}}`,
	}
	for _, doc := range valid {
		if err := s.ValidateJSON([]byte(doc)); err != nil {
			t.Errorf("%s: %v", doc, err)
		}
	}
	invalid := map[string]string{
		`{"count":1}`:                                 "name: required",
		`{"name":"","count":1}`:                       "name: shorter",
		`{"name":"abcde","count":1}`:                  "name: longer",
		`{"name":"a","count":1,"level":"mid"}`:        "level:",
		`{"name":"a","count":1.5}`:                    "count: expected an integer",
		`{"name":"a","count":9007199254740992}`:       "count: expected an integer",
		`{"name":"a","count":-1}`:                     "count: below",
		`{"name":"a","count":1,"ratio":1}`:            "ratio: not below",
		`{"name":"a","count":"1"}`:                    "count: expected integer",
		`{"name":"a","count":1,"since":"yesterday"}`:  "since:",
		`{"name":"a","count":1,"tags":["x","y","z"]}`: "tags: more than",
		`{"name":"a","count":1,"tags":[1]}`:           "tags[0]: expected string",
		`{"name":"a","count":1,"labels":{"a":"x"}}`:   "labels.a:",
		`{"name":"a","count":1,"on":null}`:            "on: null",
		`[]`:                                          "expected object",
	}
	for doc, want := range invalid {
		err := s.ValidateJSON([]byte(doc))
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: got %v, want %q", doc, err, want)
		}
	}
}
