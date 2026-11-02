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

// associate associates each slot: with the entry the request names for it,
// which implements its interface, admits its operations and reaches the node,
// or else by the Spawner's rule, with the single open or topics entry that
// does. A provision entry only the request associates, and only with its proof
// carrying the association (architecture §3.16.3). provisioned says whether a
// grant is of a provision entry.
func associate(slots []slot, ifaces []*egress.Interface, registry map[string]*egress.Entry, req ProvisionRequest) (grants []signing.Grant, provisioned bool, err error) {
	known := map[string]bool{}
	for _, s := range slots {
		known[s.Name] = true
	}
	for _, name := range slices.Sorted(maps.Keys(req.Associations)) {
		if !known[name] {
			return nil, false, refuse(codeRequestInvalid, "the request associates slot %s, which egress_required lacks", name)
		}
	}
	for i, s := range slots {
		if key, ok := req.Associations[s.Name]; ok {
			e := registry[key]
			switch {
			case e == nil:
				return nil, false, refuse(codeEgressSlotUnbound, "slot %s: the request associates %s, which egress_registry lacks", s.Name, key)
			case e.Interface.Digest != ifaces[i].Digest || !admits(e, s.Operations):
				return nil, false, refuse(codeEgressSlotUnbound, "slot %s: %s does not implement its interface or admit its operations", s.Name, key)
			case e.GrantScope == egress.ScopeTopics && !slices.Contains(e.GranteeTopics, req.Topic):
				return nil, false, refuse(codeEgressSlotUnbound, "slot %s: %s does not reach %s", s.Name, key, req.Topic)
			case e.GrantScope == egress.ScopeProvision:
				if !slices.Contains(req.ProvisionGrants, signing.Association{Slot: s.Name, Entry: key}) {
					return nil, false, refuse(codeEgressSlotUnbound, "slot %s: the proof does not carry its association with %s", s.Name, key)
				}
				provisioned = true
			}
			grants = append(grants, signing.Grant{Slot: s.Name, Entry: key, Interface: ifaces[i].Digest})
			continue
		}
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
				if slices.Contains(e.GranteeTopics, req.Topic) {
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
			return nil, false, refuse(codeEgressSlotUnbound, "slot %s: entries %v implement its interface; choosing among them is the operator's", s.Name, candidates)
		case provision:
			return nil, false, refuse(codeEgressSlotUnbound, "slot %s: only a provision entry implements its interface, which the operator associates", s.Name)
		default:
			return nil, false, refuse(codeEgressSlotUnbound, "slot %s: no entry implements its interface, admits its operations and reaches %s", s.Name, req.Topic)
		}
	}
	return grants, provisioned, nil
}

func admits(e *egress.Entry, ops []string) bool {
	for _, op := range ops {
		if !slices.Contains(e.Operations, op) {
			return false
		}
	}
	return true
}
