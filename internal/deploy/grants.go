package deploy

import (
	"encoding/json"
	"maps"
	"slices"

	"zobik.org/zobik/internal/egress"
	"zobik.org/zobik/internal/signing"
)

// slot is an egress slot of a blueprint: the name the Logic Container calls it
// by, the interface it requires and the operations it uses (implementation §1.2.11).
type slot struct {
	Name       string          `json:"name"`
	Interface  json.RawMessage `json:"interface"`
	Operations []string        `json:"operations"`
}

// parseSlots reads egress_required: each slot with a unique name, an egress
// interface and operations of it.
func parseSlots(raw json.RawMessage) ([]slot, []*egress.Interface, error) {
	var slots []slot
	if err := json.Unmarshal(raw, &slots); err != nil {
		return nil, nil, refuse(codeRequestInvalid, "egress_required is not a list of slots")
	}
	ifaces := make([]*egress.Interface, len(slots))
	seen := map[string]bool{}
	for i, s := range slots {
		if s.Name == "" || seen[s.Name] {
			return nil, nil, refuse(codeRequestInvalid, "egress_required: slot names are unique and not empty")
		}
		seen[s.Name] = true
		in, err := egress.ParseInterface(s.Interface)
		if err != nil {
			return nil, nil, refuse(codeRequestInvalid, "egress_required: slot %s: %v", s.Name, err)
		}
		if in.Direction != egress.DirectionEgress {
			return nil, nil, refuse(codeRequestInvalid, "egress_required: slot %s requires an egress interface", s.Name)
		}
		if len(s.Operations) == 0 {
			return nil, nil, refuse(codeRequestInvalid, "egress_required: slot %s uses no operation", s.Name)
		}
		for _, op := range s.Operations {
			if in.Operations[op] == nil {
				return nil, nil, refuse(codeRequestInvalid, "egress_required: slot %s uses %s, which its interface lacks", s.Name, op)
			}
		}
		ifaces[i] = in
	}
	return slots, ifaces, nil
}

// associate associates each slot by the Spawner's rule: with the single open or
// topics entry that implements its interface, admits its operations and reaches
// the node; with none or several, the blueprint is not provisioned
// (architecture §3.16.3). A provision entry does not count, because only a
// node_provision associates it.
func associate(slots []slot, ifaces []*egress.Interface, registry map[string]*egress.Entry, topic string) ([]signing.Grant, error) {
	var grants []signing.Grant
	for i, s := range slots {
		var candidates []string
		provision := false
		for _, key := range slices.Sorted(maps.Keys(registry)) {
			e := registry[key]
			if e.Interface.Digest != ifaces[i].Digest || !admits(e, s.Operations) {
				continue
			}
			switch e.GrantScope {
			case egress.ScopeOpen:
				candidates = append(candidates, key)
			case egress.ScopeTopics:
				if slices.Contains(e.GranteeTopics, topic) {
					candidates = append(candidates, key)
				}
			case egress.ScopeProvision:
				provision = true
			}
		}
		switch {
		case len(candidates) == 1:
			grants = append(grants, signing.Grant{Slot: s.Name, Entry: candidates[0], Interface: ifaces[i].Digest})
		case len(candidates) > 1:
			return nil, refuse(codeEgressSlotUnbound, "slot %s: entries %v implement its interface; choosing among them is the operator's", s.Name, candidates)
		case provision:
			return nil, refuse(codeEgressSlotUnbound, "slot %s: only a provision entry implements its interface, which the operator associates", s.Name)
		default:
			return nil, refuse(codeEgressSlotUnbound, "slot %s: no entry implements its interface, admits its operations and reaches %s", s.Name, topic)
		}
	}
	return grants, nil
}

func admits(e *egress.Entry, ops []string) bool {
	for _, op := range ops {
		if !slices.Contains(e.Operations, op) {
			return false
		}
	}
	return true
}
