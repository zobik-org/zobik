package networkmonitor

import (
	"encoding/json"
	"log"
	"slices"
	"sort"
	"strings"
	"time"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/signing"
)

// families are the streams the role observes (implementation §1.2.8).
var families = []string{"task", "prop", "notice"}

// Consumers are the role's durable consumers, one per family it observes; the
// one of task.> also brings the cycle of node_query (implementation §1.2.8).
func Consumers(id string) []bus.Consumer {
	out := make([]bus.Consumer, 0, len(families))
	for _, f := range families {
		out = append(out, bus.Consumer{Stream: f, Name: id, Filters: []string{f + ".>"}})
	}
	return out
}

// mark is how far the role applied a family: the last stream sequence and when
// the stream stored it. A later sequence that skips one is an observation gap
// (implementation §1.2.8).
type mark struct {
	Seq    uint64    `json:"seq"`
	Stored time.Time `json:"stored"`
}

// eventData are the fields of data the measurement reads.
type eventData struct {
	Topic             string   `json:"topic"`
	RequesterIdentity string   `json:"requester_identity"`
	PreviousAttempt   string   `json:"previous_attempt"`
	Kind              string   `json:"kind"`
	Similarity        float64  `json:"similarity"`
	Metrics           *metrics `json:"metrics"`
}

type metrics struct {
	TokensIn   float64 `json:"tokens_in"`
	TokensOut  float64 `json:"tokens_out"`
	Cost       float64 `json:"cost"`
	DurationMs float64 `json:"duration_ms"`
}

// delta is what an event moves: copies of the keys it changes, applied
// together once every key is written.
type delta struct {
	family  string
	seq     uint64
	nodes   map[string]*Aggregate
	reviews map[string]*Review
	state   *state
}

// review returns the copy of the open review taskID the event changes, or nil
// if it is not one or its close was already observed.
func (d *delta) review(taskID string) *Review {
	if r, ok := d.reviews[taskID]; ok {
		return r
	}
	r := d.state.reviews[taskID]
	if r == nil || r.Closed {
		return nil
	}
	c := *r
	d.reviews[taskID] = &c
	return &c
}

// touch returns the copy of id's key the event changes, or nil if the node is
// not in the roster or the key already applied the event.
func (d *delta) touch(id string) *Aggregate {
	if c, ok := d.nodes[id]; ok {
		return c
	}
	a := d.state.nodes[id]
	if a == nil || a.Applied[d.family] >= d.seq {
		return nil
	}
	c := a.clone()
	d.nodes[id] = c
	return c
}

// ofTopic are the roster's nodes on topic.
func (s *state) ofTopic(topic string) []string {
	var ids []string
	for id, a := range s.nodes {
		if a.Topic == topic {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}

// holder is the node of topic that holds taskID, or "".
func (s *state) holder(topic, taskID string) string {
	for _, id := range s.ofTopic(topic) {
		if h := s.nodes[id].Holding; h != nil && h.TaskID == taskID {
			return id
		}
	}
	return ""
}

// observe computes what the event at seq of family moves, gap included.
func (s *state) observe(family string, seq uint64, stored time.Time, e *bus.Event, subject string, p Params) *delta {
	d := &delta{family: family, seq: seq, nodes: map[string]*Aggregate{}, reviews: map[string]*Review{}, state: s}
	if m := s.marks[family]; m.Seq != 0 && seq > m.Seq+1 {
		// The stream no longer reaches the point where the role stopped: the
		// events in between were lost (architecture §3.12.1).
		log.Printf("network_monitor: observation gap in %s from %s to %s", family, m.Stored.Format(time.RFC3339), stored.Format(time.RFC3339))
		for id := range s.nodes {
			if a := d.touch(id); a != nil {
				a.dropBefore(stored)
			}
		}
		// A review whose close the gap could have hidden is closed without a
		// snooze: the damage is a redundant review (architecture §3.12.1).
		if family == "task" {
			for id := range s.reviews {
				if r := d.review(id); r != nil {
					r.Closed = true
				}
			}
		}
	}
	if e == nil {
		return d
	}
	var data eventData
	_ = json.Unmarshal(e.Data, &data) // a field of another type reads as absent
	tokens := strings.Split(subject, ".")
	switch {
	case family == "task" && e.Type == bus.TypeAnnounced:
		s.announced(d, e, data, stored)
	case family == "task" && e.Type == bus.TypeAssigned && len(tokens) == 3:
		s.assigned(d, e, data, tokens[2], stored, p)
	case family == "task" && (e.Type == bus.TypeCompleted || e.Type == bus.TypeFailed || e.Type == bus.TypeAborted) && len(tokens) == 4:
		s.closed(d, e, data, tokens[2], tokens[3], stored, p)
	case family == "prop" && e.Type == bus.TypeProposed && len(tokens) == 3:
		if a := d.touch(tokens[1]); a != nil {
			a.proposed(e.Subject, stored, p)
		}
	case family == "notice" && e.Type == bus.TypeNotice:
		// A notice from a node of the roster is a contract violation
		// (architecture §3.12.2).
		if a := d.touch(e.Source); a != nil {
			a.Notices = append(a.Notices, stored)
			a.trimNotices(stored, p)
		}
	}
	return d
}

// announced reads a task.announced: the re-announcement of a held task frees
// its holder (architecture §3.1), a reopen classifies the attempt it names
// (architecture §3.12.2), and a subtask of a node counts on the task it holds.
func (s *state) announced(d *delta, e *bus.Event, data eventData, stored time.Time) {
	if id := s.holder(data.Topic, e.Subject); id != "" {
		if a := d.touch(id); a != nil {
			a.Holding = nil
		}
	}
	if data.PreviousAttempt != "" {
		for _, id := range s.ofTopic(data.Topic) {
			i := slices.IndexFunc(s.nodes[id].Pending, func(o Observation) bool {
				return o.TaskID == data.PreviousAttempt && o.Requester == data.RequesterIdentity
			})
			if i < 0 {
				continue
			}
			if a := d.touch(id); a != nil {
				o := a.Pending[i]
				a.Pending = slices.Delete(a.Pending, i, i+1)
				o.Reopened = true
				a.classify(o)
			}
		}
	}
	r := s.nodes[data.RequesterIdentity]
	if r == nil || r.Holding == nil || r.Holding.TraceID != bus.TraceID(e.TraceParent) || r.Holding.Subtasks[e.Subject] {
		return
	}
	a := d.touch(data.RequesterIdentity)
	if a == nil {
		return
	}
	h := a.Holding
	if h.Subtasks == nil {
		h.Subtasks = map[string]bool{}
	}
	h.Subtasks[e.Subject] = true
	if data.PreviousAttempt != "" {
		return
	}
	h.Lineages++
	// The queries it opened, by origin (architecture §3.12.2).
	if strings.HasPrefix(data.Topic, signing.HITLPrefix) {
		if data.Kind == "gate_approval" {
			h.Gates++
		} else {
			h.Queries++
		}
	}
}

// assigned reads a task.assigned: an assignment on the topic of the nodes that
// cover it (architecture §3.12.2), and the task its winner holds from then.
func (s *state) assigned(d *delta, e *bus.Event, data eventData, winner string, stored time.Time, p Params) {
	for _, id := range s.ofTopic(data.Topic) {
		a := d.touch(id)
		if a == nil {
			continue
		}
		outcome := outcomeIdle
		switch {
		case id == winner:
			outcome = outcomeWon
		case a.Holding != nil:
			outcome = outcomeBusy
		}
		_, proposed := a.Proposals[e.Subject]
		delete(a.Proposals, e.Subject)
		a.Usage = append(a.Usage, Assignment{TaskID: e.Subject, At: stored, Outcome: outcome, Proposed: proposed || id == winner})
		if over := len(a.Usage) - max(p.UsageWindow, 1); over > 0 {
			a.Usage = slices.Delete(a.Usage, 0, over)
		}
		a.SinceRoster++
		a.DemandFrom = stored
	}
	if a := d.touch(winner); a != nil {
		a.Holding = &Holding{
			TaskID: e.Subject, TraceID: bus.TraceID(e.TraceParent), Requester: data.RequesterIdentity,
			Assigned: stored, Similarity: data.Similarity,
		}
	}
}

// closed reads a terminal event: the close of the task a node holds, and a
// subtask whose subtree adds to the task its requester holds (architecture
// §3.9.1, §3.12.2).
func (s *state) closed(d *delta, e *bus.Event, data eventData, requester, topic string, stored time.Time, p Params) {
	var m metrics
	if data.Metrics != nil {
		m = *data.Metrics
	}
	subtree := m.Cost
	if id := s.holder(topic, e.Subject); id != "" {
		h := s.nodes[id].Holding
		subtree += h.ChildrenCost
		if a := d.touch(id); a != nil {
			o := Observation{
				TaskID: e.Subject, Requester: h.Requester, Closed: stored, Similarity: h.Similarity,
				Cost: m.Cost, SubtreeCost: subtree, DurationMs: m.DurationMs, TokensIn: m.TokensIn, TokensOut: m.TokensOut,
				Attempts: len(h.Subtasks), Lineages: h.Lineages, Queries: h.Queries, Gates: h.Gates,
			}
			a.Holding = nil
			switch e.Type {
			case bus.TypeCompleted:
				a.Pending = append(a.Pending, o)
			case bus.TypeFailed:
				// A task.failed did not close clean (architecture §3.12.2).
				o.Failed = true
				a.classify(o)
			}
			// A snooze that waits for observations counts this one
			// (implementation §5.9).
			if sn := a.Snooze; sn != nil && sn.Action == ActionSnoozed && e.Type != bus.TypeAborted {
				sn.Observations--
			}
			// A task.aborted is not an observation (architecture §3.12.1).
			a.trimObservations(stored, p)
		}
	}
	if requester == s.self && topic == TopicReview {
		s.reviewClosed(d, e, stored)
	}
	r := s.nodes[requester]
	if r == nil || r.Holding == nil || r.Holding.TaskID == e.Subject || r.Holding.TraceID != bus.TraceID(e.TraceParent) {
		return
	}
	if a := d.touch(requester); a != nil {
		a.Holding.ChildrenCost += subtree
	}
}

// proposed records a proposal of the node, on the assignment of its auction if
// the role already saw it.
func (a *Aggregate) proposed(taskID string, stored time.Time, p Params) {
	for i := range a.Usage {
		if a.Usage[i].TaskID == taskID {
			a.Usage[i].Proposed = true
			return
		}
	}
	if a.Proposals == nil {
		a.Proposals = map[string]time.Time{}
	}
	a.Proposals[taskID] = stored
	// What waits for its assignment is bounded: a proposal whose auction did not
	// award goes once newer ones push it out.
	for limit := 2 * max(p.UsageWindow, 1); len(a.Proposals) > limit; {
		oldest := ""
		for id, t := range a.Proposals {
			if oldest == "" || t.Before(a.Proposals[oldest]) {
				oldest = id
			}
		}
		delete(a.Proposals, oldest)
	}
}

// classify adds a classified close in the order it closed.
func (a *Aggregate) classify(o Observation) {
	i, _ := slices.BinarySearchFunc(a.Observations, o.Closed, func(x Observation, t time.Time) int { return x.Closed.Compare(t) })
	a.Observations = slices.Insert(a.Observations, i, o)
}

// trimObservations keeps what the windows ask for: two windows, within
// evidence_staleness (architecture §3.12.1, implementation §5.2).
func (a *Aggregate) trimObservations(now time.Time, p Params) {
	from := now.Add(-p.EvidenceStaleness)
	a.Observations = slices.DeleteFunc(a.Observations, func(o Observation) bool { return o.Closed.Before(from) })
	if over := len(a.Observations) - 2*max(p.QualityWindow, 1); over > 0 {
		a.Observations = slices.Delete(a.Observations, 0, over)
	}
}

func (a *Aggregate) trimNotices(now time.Time, p Params) {
	from := now.Add(-p.EvidenceStaleness)
	a.Notices = slices.DeleteFunc(a.Notices, func(t time.Time) bool { return t.Before(from) })
}

// dropBefore applies an observation gap that ends at end: what came before it
// leaves the windows, the classifications in flight across it are discarded,
// and the periods count again from its end (architecture §3.12.1,
// implementation §1.2.8). A held task whose close the gap could have hidden is
// released.
func (a *Aggregate) dropBefore(end time.Time) {
	before := func(t time.Time) bool { return t.Before(end) }
	a.Observations = slices.DeleteFunc(a.Observations, func(o Observation) bool { return before(o.Closed) })
	a.Pending = slices.DeleteFunc(a.Pending, func(o Observation) bool { return before(o.Closed) })
	a.Usage = slices.DeleteFunc(a.Usage, func(u Assignment) bool { return before(u.At) })
	for id, t := range a.Proposals {
		if before(t) {
			delete(a.Proposals, id)
		}
	}
	if a.Holding != nil && before(a.Holding.Assigned) {
		a.Holding = nil
	}
	a.SinceRoster = 0
	if before(a.DemandFrom) {
		a.DemandFrom = end
	}
}

// clone is a deep copy, which the event changes before its keys are written.
func (a *Aggregate) clone() *Aggregate {
	b, _ := json.Marshal(a)
	var c Aggregate
	_ = json.Unmarshal(b, &c)
	if c.Applied == nil {
		c.Applied = map[string]uint64{}
	}
	return &c
}
