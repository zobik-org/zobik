package bus

import (
	"encoding/json"
	"fmt"
	"strings"
)

// The subject space (implementation §1.2.1): the event's type, then what whoever
// receives it filters by. Every subject has a fixed number of tokens, because a
// topic is a single term.

func SubjectAnnounced(topic string) string { return "task.announced." + topic }
func SubjectProposed(identity, topic string) string {
	return "prop." + identity + "." + topic
}
func SubjectAddressed(typ, identity string) string { return typ + "." + identity }
func SubjectTerminal(typ, identity, topic string) string {
	return typ + "." + identity + "." + topic
}
func SubjectNotice(topic string) string { return "notice." + topic }

// subjectShape returns the literal prefix tokens of typ's subject and the
// position of the topic among the tokens, or -1 if the subject carries none.
func subjectShape(typ string) (prefix []string, tokens, topicAt int, err error) {
	switch typ {
	case TypeAnnounced:
		return []string{"task", "announced"}, 3, 2, nil
	case TypeProposed:
		return []string{"prop"}, 3, 2, nil
	case TypeAssigned, TypeRejected:
		return strings.Split(typ, "."), 3, -1, nil
	case TypeCompleted, TypeFailed, TypeAborted:
		return strings.Split(typ, "."), 4, 3, nil
	case TypeNotice:
		return []string{"notice"}, 2, 1, nil
	}
	return nil, 0, 0, fmt.Errorf("bus: unknown type %q", typ)
}

// checkSubject checks that subject has the shape of e's type, that each variable
// token is a single term, and that it names the same topic as data.topic.
func checkSubject(subject string, e *Event) error {
	prefix, n, topicAt, err := subjectShape(e.Type)
	if err != nil {
		return err
	}
	tokens := strings.Split(subject, ".")
	if len(tokens) != n {
		return fmt.Errorf("bus: subject %q: a %s travels on %d tokens", subject, e.Type, n)
	}
	for i, t := range tokens {
		if i < len(prefix) {
			if t != prefix[i] {
				return fmt.Errorf("bus: subject %q is not that of a %s", subject, e.Type)
			}
			continue
		}
		if t == "" || strings.ContainsAny(t, "*> \t\r\n") {
			return fmt.Errorf("bus: subject %q: %q is not a single term", subject, t)
		}
	}
	if topicAt < 0 {
		return nil
	}
	var d struct {
		Topic *string `json:"topic"`
	}
	if err := json.Unmarshal(e.Data, &d); err != nil {
		return err
	}
	if d.Topic != nil && *d.Topic != tokens[topicAt] {
		return fmt.Errorf("bus: subject %q names topic %q, data.topic is %q", subject, tokens[topicAt], *d.Topic)
	}
	return nil
}
