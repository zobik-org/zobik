package taskbroker

import (
	"strings"

	"zobik.org/zobik/internal/signing"
)

// topicIntake is the entry topic, the only fixed-convention one an emergent
// role claims (architecture §3.7.7).
const topicIntake = "intake"

// structuralTopics are the fixed-convention topics with a structural claimant
// (architecture §3.7.7): admission there is by credential, without similarity
// (architecture §3.7.5, step 4).
var structuralTopics = map[string]bool{
	"config_change":        true,
	"trace_query":          true,
	"node_review":          true,
	"node_query":           true,
	"node_catalog_query":   true,
	"node_catalog_publish": true,
	"node_catalog_import":  true,
	"node_provision":       true,
	"node_create":          true,
	"node_retire":          true,
	"topic_catalog_query":  true,
}

func isHITL(topic string) bool { return strings.HasPrefix(topic, signing.HITLPrefix) }

// The values of network_admission (architecture §3.18).
const (
	admissionOpen   = "open"
	admissionDrain  = "drain"
	admissionFrozen = "frozen"
)

// admits is the network pause's predicate over a topic identifier: drain
// rejects intake, and frozen also every emergent topic; what stays admitted is
// the table of architecture §3.7.7 minus intake (architecture §3.18). An unknown
// value admits nothing, which is where the schema's enum leaves a snapshot
// this binary cannot read.
func admits(admission, topic string) bool {
	switch admission {
	case admissionOpen:
		return true
	case admissionDrain:
		return topic != topicIntake
	case admissionFrozen:
		return structuralTopics[topic] || isHITL(topic)
	}
	return false
}
