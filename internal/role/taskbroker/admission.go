package taskbroker

import (
	"strings"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/signing"
)

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
		return topic != bus.TopicIntake
	case admissionFrozen:
		return bus.StructuralTopics[topic] || isHITL(topic)
	}
	return false
}
