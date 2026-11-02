package deploy

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"zobik.org/zobik/internal/egress"
	"zobik.org/zobik/internal/role/spawner"
	"zobik.org/zobik/internal/signing"
)

// iface is an interface with a read and a write.
const iface = `{"protocol":"https","direction":"egress","operations":{
  "read":{"fields":{"type":"object","properties":{}},"https":{"method":"GET","path":"/r","fields":{},"response":{}}},
  "write":{"fields":{"type":"object","properties":{}},"https":{"method":"POST","path":"/w","fields":{},"response":{}}}}}`

func registryOf(t *testing.T, entries map[string]map[string]any) map[string]*egress.Entry {
	t.Helper()
	raw := map[string]any{}
	for k, extra := range entries {
		e := map[string]any{
			"interface": json.RawMessage(iface), "target": "https://api.example.com", "operations": []string{"read", "write"},
			"rules": []map[string]string{{"then": "allow"}},
		}
		for f, v := range extra {
			e[f] = v
		}
		billing := map[string]any{}
		for _, op := range e["operations"].([]string) {
			billing[op] = map[string]string{"cost": "0.0"}
		}
		e["billing_rule"] = billing
		raw[k] = e
	}
	b, _ := json.Marshal(raw)
	reg, err := egress.ParseRegistry(b)
	if err != nil {
		t.Fatal(err)
	}
	return reg
}

// Without the request's association, a slot is associated with the single open
// or topics entry that implements its interface, admits its operations and
// reaches the node, and otherwise the node is not provisioned (architecture §3.16.3).
func TestAssociate(t *testing.T) {
	slots, ifaces, err := parseSlots(json.RawMessage(`[{"name":"api","interface":` + iface + `,"operations":["read"]}]`))
	if err != nil {
		t.Fatal(err)
	}
	readOnly := map[string]any{"operations": []string{"read"}}
	cases := []struct {
		name     string
		registry map[string]map[string]any
		entry    string // the associated entry; empty when refused
		reason   string
	}{
		{"a single open entry", map[string]map[string]any{"a": nil}, "a", ""},
		{"a topics entry of the node's topic", map[string]map[string]any{"a": {"grant_scope": "topics", "grantee_topics": []string{"echo"}}}, "a", ""},
		{"a topics entry of another topic", map[string]map[string]any{"a": {"grant_scope": "topics", "grantee_topics": []string{"other"}}}, "", "no entry"},
		{"an entry that admits fewer operations", map[string]map[string]any{"a": {"operations": []string{"write"}}}, "", "no entry"},
		{"two candidates", map[string]map[string]any{"a": nil, "b": readOnly}, "", "operator's"},
		{"only a provision entry", map[string]map[string]any{"a": {"grant_scope": "provision"}}, "", "provision entry"},
		{"no entry", map[string]map[string]any{}, "", "no entry"},
	}
	for _, c := range cases {
		grants, _, err := associate(slots, ifaces, registryOf(t, c.registry), ProvisionRequest{Topic: "echo"})
		if c.entry != "" {
			if err != nil || len(grants) != 1 || grants[0].Entry != c.entry || grants[0].Slot != "api" || grants[0].Interface != ifaces[0].Digest {
				t.Errorf("%s: %v %v", c.name, grants, err)
			}
			continue
		}
		var pe *spawner.Refusal
		if !errors.As(err, &pe) || pe.Code != codeEgressSlotUnbound || !strings.Contains(pe.Reason, c.reason) {
			t.Errorf("%s: %v, want %s with %q", c.name, err, codeEgressSlotUnbound, c.reason)
		}
	}
}

// The request's association governs its slot, within what the entry serves; a
// provision entry needs the proof to carry the association (architecture §3.16.3).
func TestAssociateByRequest(t *testing.T) {
	slots, ifaces, err := parseSlots(json.RawMessage(`[{"name":"api","interface":` + iface + `,"operations":["read"]}]`))
	if err != nil {
		t.Fatal(err)
	}
	registry := registryOf(t, map[string]map[string]any{
		"a": nil, "b": nil,
		"other":     {"grant_scope": "topics", "grantee_topics": []string{"other"}},
		"write":     {"operations": []string{"write"}},
		"provision": {"grant_scope": "provision"},
	})
	carried := []signing.Association{{Slot: "api", Entry: "provision"}}
	cases := []struct {
		name         string
		associations map[string]string
		grants       []signing.Association
		code         string // empty when associated
	}{
		{"one of two open entries", map[string]string{"api": "b"}, nil, ""},
		{"a provision entry the proof carries", map[string]string{"api": "provision"}, carried, ""},
		{"a provision entry the proof does not carry", map[string]string{"api": "provision"}, nil, codeEgressSlotUnbound},
		{"an entry of another topic", map[string]string{"api": "other"}, nil, codeEgressSlotUnbound},
		{"an entry that admits fewer operations", map[string]string{"api": "write"}, nil, codeEgressSlotUnbound},
		{"an entry the registry lacks", map[string]string{"api": "missing"}, nil, codeEgressSlotUnbound},
		{"a slot the blueprint lacks", map[string]string{"api": "a", "db": "b"}, nil, codeRequestInvalid},
	}
	for _, c := range cases {
		grants, provisioned, err := associate(slots, ifaces, registry, ProvisionRequest{Topic: "echo", Associations: c.associations, ProvisionGrants: c.grants})
		if c.code == "" {
			if err != nil || len(grants) != 1 || grants[0].Entry != c.associations["api"] || provisioned != (c.grants != nil) {
				t.Errorf("%s: %v %v %v", c.name, grants, provisioned, err)
			}
			continue
		}
		var r *spawner.Refusal
		if !errors.As(err, &r) || r.Code != c.code {
			t.Errorf("%s: %v, want %s", c.name, err, c.code)
		}
	}
}

func TestParseSlotsRejects(t *testing.T) {
	for _, raw := range []string{
		`{}`,
		`[{"name":"","interface":` + iface + `,"operations":["read"]}]`,
		`[{"name":"a","interface":` + iface + `,"operations":["read"]},{"name":"a","interface":` + iface + `,"operations":["read"]}]`,
		`[{"name":"a","interface":{"protocol":"https"},"operations":["read"]}]`,
		`[{"name":"a","interface":` + iface + `,"operations":[]}]`,
		`[{"name":"a","interface":` + iface + `,"operations":["delete"]}]`,
	} {
		_, _, err := parseSlots(json.RawMessage(raw))
		var pe *spawner.Refusal
		if !errors.As(err, &pe) || pe.Code != codeRequestInvalid {
			t.Errorf("%s: %v", raw, err)
		}
	}
}
