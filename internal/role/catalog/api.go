package catalog

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"

	"zobik.org/zobik/internal/signing"
)

// The scopes of a credential (implementation §1.2.11): read only reads,
// register also registers, and spawner also writes the records of its network
// and the deletion mark.
const (
	ScopeRead     = "read"
	ScopeRegister = "register"
	ScopeSpawner  = "spawner"
)

// scopeRank orders the scopes: each one enables what the previous ones do.
var scopeRank = map[string]int{ScopeRead: 1, ScopeRegister: 2, ScopeSpawner: 3}

// The reasons a registration fails with (implementation §1.2.11).
const (
	ReasonScanFailed        = "scan_failed"
	ReasonUnresolvable      = "unresolvable"
	ReasonNRIVersionMissing = "nri_version_missing"
	ReasonStorageFull       = "storage_full"
	// ReasonUnregisteredMember is a bundle that names a digest without an entry.
	ReasonUnregisteredMember = "unregistered_member"
)

// The provenances an act of registration carries in its acquisition row
// (implementation §1.2.11); derived is the Spawner's to write.
const (
	ProvenanceSeeded    = "seeded"
	ProvenanceImported  = "imported"
	ProvenancePurchased = "purchased"
)

// Registration is the body of POST /v1/entries: the half without a qualifier
// and, optionally, the acquisition row of the registering network
// (implementation §1.2.11).
type Registration struct {
	Topic           string                     `json:"topic"`
	ArtifactRef     string                     `json:"artifact_ref"`
	CapabilityText  string                     `json:"capability_text"`
	AuthorID        string                     `json:"author_id"`
	AttemptWindow   *signing.AttemptWindow     `json:"attempt_window,omitempty"`
	TopicsAnnounced []TopicAnnounced           `json:"topics_announced,omitempty"`
	Requires        map[string]json.RawMessage `json:"requires"`
	Acquisition     *Acquisition               `json:"acquisition,omitempty"`
}

// TopicAnnounced is a domain topic the blueprint announces, with the fixed
// texts it announces it with, if any.
type TopicAnnounced struct {
	Topic      string   `json:"topic"`
	FixedTexts []string `json:"fixed_texts,omitempty"`
}

// Acquisition is the acquisition row the act of registration writes as the
// record acquisition:<digest> of the credential's network.
type Acquisition struct {
	Provenance string    `json:"provenance"`
	Cost       float64   `json:"cost"`
	AsOf       time.Time `json:"as_of"`
}

// ScanResult is the verdict the Catalog stores on an entry (architecture
// §3.13.3): findings only on a fail.
type ScanResult struct {
	Verdict        string    `json:"verdict"`
	RulesetVersion string    `json:"ruleset_version"`
	Findings       []string  `json:"findings,omitempty"`
	AsOf           time.Time `json:"as_of"`
}

// Entry is an entry as the reads return it: the half it was registered with,
// without acquisition, and what the Catalog adds (implementation §1.2.11).
type Entry struct {
	Registration
	ScanResult   ScanResult `json:"scan_result"`
	NRIVersion   string     `json:"nri_version"`
	ArtifactSize int64      `json:"artifact_size"`
	DeletionMark bool       `json:"deletion_mark"`
}

// Failure is the body of a rejected registration: 422 with its reason.
type Failure struct {
	Reason         string   `json:"reason"`
	RulesetVersion string   `json:"ruleset_version,omitempty"`
	Findings       []string `json:"findings,omitempty"`
	Member         string   `json:"member,omitempty"`
}

// Status is the body of GET /v1/status.
type Status struct {
	RulesetVersion string `json:"ruleset_version"`
	Occupied       int64  `json:"occupied"`
	Capacity       int64  `json:"capacity"`
}

// Bundle is a bundle as POST /v1/bundles registers it and GET returns it.
type Bundle struct {
	BundleID    string   `json:"bundle_id,omitempty"`
	PurposeText string   `json:"purpose_text"`
	Members     []string `json:"members"`
}

// Record is a qualified record as the listing returns it.
type Record struct {
	Network  string          `json:"network"`
	Key      string          `json:"key"`
	Value    json.RawMessage `json:"value"`
	Revision int64           `json:"revision"`
}

// The shapes a topic name and a network name take: a topic is a snake_case
// slug (architecture §3.6.5), and a network's name prefixes engine resources.
var (
	topicPattern   = regexp.MustCompile(`^[a-z][a-z0-9]*(_[a-z0-9]+)*$`)
	networkPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,62}$`)
	digestPattern  = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
)

// validate checks the shape of a registration: what the Catalog reads of it,
// and the keys of requires it knows. An unknown key of requires is the
// Spawner's to judge (architecture §3.13.3), so it passes.
func (r *Registration) validate() error {
	switch {
	case !topicPattern.MatchString(r.Topic) || len(r.Topic) > 64:
		return fmt.Errorf("topic %q is not a topic name", r.Topic)
	case !digestPattern.MatchString(r.ArtifactRef):
		return fmt.Errorf("artifact_ref %q is not a sha256 digest", r.ArtifactRef)
	case r.CapabilityText == "":
		return errors.New("capability_text is required")
	case r.AuthorID == "":
		return errors.New("author_id is required")
	case r.Requires == nil:
		return errors.New("requires is required")
	}
	if w := r.AttemptWindow; w != nil && w.From != nil && w.To != nil && *w.From > *w.To {
		return errors.New("attempt_window: from is above to")
	}
	for _, t := range r.TopicsAnnounced {
		if !topicPattern.MatchString(t.Topic) {
			return fmt.Errorf("topics_announced: %q is not a topic name", t.Topic)
		}
	}
	if raw, ok := r.Requires["memory_required"]; ok {
		var mb int64
		if err := json.Unmarshal(raw, &mb); err != nil || mb <= 0 {
			return errors.New("requires.memory_required: a positive integer of MB")
		}
	}
	for _, list := range []string{"egress_required", "ingress_required"} {
		raw, ok := r.Requires[list]
		if !ok {
			continue
		}
		var slots []struct {
			Name       string          `json:"name"`
			Interface  json.RawMessage `json:"interface"`
			Operations []string        `json:"operations"`
		}
		if err := json.Unmarshal(raw, &slots); err != nil {
			return fmt.Errorf("requires.%s: %w", list, err)
		}
		seen := map[string]bool{}
		for _, s := range slots {
			if s.Name == "" || seen[s.Name] || len(s.Interface) == 0 || len(s.Operations) == 0 {
				return fmt.Errorf("requires.%s: each slot has a unique name, its interface and its operations", list)
			}
			seen[s.Name] = true
		}
	}
	if a := r.Acquisition; a != nil {
		switch a.Provenance {
		case ProvenanceSeeded, ProvenanceImported, ProvenancePurchased:
		default:
			return fmt.Errorf("acquisition.provenance %q: seeded, imported or purchased", a.Provenance)
		}
		if a.Cost < 0 || a.AsOf.IsZero() {
			return errors.New("acquisition: a cost not below zero and its as_of")
		}
	}
	return nil
}
