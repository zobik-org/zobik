package deploy

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"zobik.org/zobik/internal/egress"
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

// zobik node up associates a slot with the single open or topics entry that
// implements its interface, admits its operations and reaches the node, and
// otherwise does not provision (architecture §3.16.3).
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
		grants, err := associate(slots, ifaces, registryOf(t, c.registry), "echo")
		if c.entry != "" {
			if err != nil || len(grants) != 1 || grants[0].Entry != c.entry || grants[0].Slot != "api" || grants[0].Interface != ifaces[0].Digest {
				t.Errorf("%s: %v %v", c.name, grants, err)
			}
			continue
		}
		var pe *ProvisionError
		if !errors.As(err, &pe) || pe.Code != codeEgressSlotUnbound || !strings.Contains(pe.Reason, c.reason) {
			t.Errorf("%s: %v, want %s with %q", c.name, err, codeEgressSlotUnbound, c.reason)
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
		var pe *ProvisionError
		if !errors.As(err, &pe) || pe.Code != codeRequestInvalid {
			t.Errorf("%s: %v", raw, err)
		}
	}
}
