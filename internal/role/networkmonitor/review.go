package networkmonitor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/claim"
	"zobik.org/zobik/internal/signing"
)

// TopicReview is the topic of the reviews the role emits toward the Spawner
// (architecture §3.12.3).
const TopicReview = "node_review"

// The keys of the open reviews and of the emissions of the last hour
// (implementation §1.2.8, §5.7).
const (
	keyReview    = "review."
	keyEmissions = "emissions"
)

// The data.action values of a review's close the snooze reads
// (architecture §3.13.5).
const (
	ActionSnoozed  = "snoozed"
	ActionNoAction = "no_action"
	ActionCapped   = "capped"
)

// Snooze is what a review's close snoozes the node for: data.snooze_until is an
// amount, in observations with snoozed and in milliseconds of admission_clock
// with no_action, anchored at the close; capped does not expire
// (implementation §5.9).
type Snooze struct {
	Action string `json:"action"`
	// Observations are the node's observations still to come, with snoozed.
	Observations int `json:"observations,omitempty"`
	// From is the close it is anchored at, and Admission the admission_clock
	// it waits for from there, with no_action.
	From      time.Time     `json:"from,omitzero"`
	Admission time.Duration `json:"admission,omitempty"`
}

// active says whether the snooze still holds at now.
func (s *Snooze) active(now time.Time, clock *admission) bool {
	if s == nil {
		return false
	}
	switch s.Action {
	case ActionSnoozed:
		return s.Observations > 0
	case ActionNoAction:
		return clock.at(now)-clock.at(s.From) < s.Admission
	case ActionCapped:
		return true
	}
	return false
}

// silences says whether the snooze keeps reason from going out: snoozed and
// capped silence every trigger, no_action only routine (architecture §3.13.5).
func (s *Snooze) silences(reason string, now time.Time, clock *admission) bool {
	if !s.active(now, clock) {
		return false
	}
	return s.Action != ActionNoAction || reason == ReasonRoutine
}

// snoozeOf reads the snooze a review's close carries, anchored at the close.
func snoozeOf(data json.RawMessage, closed time.Time) *Snooze {
	var d struct {
		Action      string   `json:"action"`
		SnoozeUntil *float64 `json:"snooze_until"`
	}
	_ = json.Unmarshal(data, &d) // a field of another type reads as absent
	switch {
	case d.Action == ActionCapped:
		return &Snooze{Action: ActionCapped}
	case d.SnoozeUntil == nil || *d.SnoozeUntil <= 0:
		return nil
	case d.Action == ActionSnoozed:
		return &Snooze{Action: ActionSnoozed, Observations: int(math.Ceil(*d.SnoozeUntil))}
	case d.Action == ActionNoAction:
		return &Snooze{Action: ActionNoAction, From: closed, Admission: time.Duration(*d.SnoozeUntil) * time.Millisecond}
	}
	return nil
}

// Review is a review the role announced and whose trace's root it holds: a key
// of its own, because the review that archives a node closes once the node's
// key has left with it (implementation §1.2.8).
type Review struct {
	TaskID string     `json:"task_id"`
	NodeID string     `json:"node_id"`
	Reason string     `json:"reason"`
	Root   claim.Root `json:"root"`
	// Announcement is kept until the Bus takes it, so a restart publishes it
	// again with the same id.
	Announcement *bus.Event `json:"announcement"`
	Published    bool       `json:"published"`
	// Released is a gap that could have hidden the review's close: its node
	// can be reviewed again, and the root still waits for the close.
	Released bool `json:"released,omitempty"`
	// Closed is the review's close observed: what is left is closing the root.
	Closed bool `json:"closed"`
}

// emission is a review emitted, which counts toward the hour's cap.
type emission struct {
	At      time.Time `json:"at"`
	Reserve bool      `json:"reserve"`
}

// reserved says whether reason goes out by the exploration_share reserve
// (implementation §5.7).
func reserved(reason string) bool { return reason == ReasonRoutine || reason == ReasonNoDemand }

// rank is the class order outside the reserve (implementation §5.7).
var rank = map[string]int{
	ReasonNoticeProvenance: 0,
	ReasonMemoryExhausted:  1,
	ReasonLowScore:         2,
	ReasonOuterGradient:    3,
	ReasonHighCost:         4,
	ReasonHighLatency:      4,
	ReasonLowUsage:         5,
}

// waiting is a node in the queue, with the condition it holds now.
type waiting struct {
	id     string
	reason string
	since  time.Time
	spend  float64
	v      Vector
}

// underReview are the nodes with a review open.
func (s *state) underReview() map[string]bool {
	out := map[string]bool{}
	for _, r := range s.reviews {
		if !r.Closed && !r.Released {
			out[r.NodeID] = true
		}
	}
	return out
}

// reviewClosed reads the close of a review: the snooze it carries goes to the
// node, if it is still in the roster, and the root is left to close. A close
// without the Spawner's outcome—a task.failed or a task.aborted—snoozes it like
// snoozed, for node_review_snooze observations (implementation §5.9).
func (s *state) reviewClosed(d *delta, e *bus.Event, stored time.Time, p Params) {
	r := d.review(e.Subject)
	if r == nil {
		return
	}
	r.Closed = true
	a := d.touch(r.NodeID)
	switch {
	case a == nil:
	case e.Type == bus.TypeCompleted:
		a.Snooze = snoozeOf(e.Data, stored)
	case p.NodeReviewSnooze > 0:
		a.Snooze = &Snooze{Action: ActionSnoozed, Observations: p.NodeReviewSnooze}
	}
}

// emit runs the emission queue: it brings each node's place up to date, and
// emits what the hour's cap and the reserve let out, in the order of
// implementation §5.7. now is how far the task family has been read: a fact
// still unread would otherwise be missed.
func (m *monitor) emit(ctx context.Context, now time.Time) error {
	var queue []waiting
	reviewing := m.st.underReview()
	for _, id := range m.st.ids() {
		a := m.st.nodes[id]
		reason, v := "", Vector{}
		// No node_review on a hitl_contact_* channel, and none while one is open
		// (architecture §3.12.3, implementation §5.9).
		if !strings.HasPrefix(a.Topic, signing.HITLPrefix) && !reviewing[id] {
			reason, v = a.condition(m.params, now, &m.clock)
			if a.Snooze.silences(reason, now, &m.clock) {
				reason = ""
			}
		}
		switch {
		case reason == "" && !a.QueuedAt.IsZero():
			c := a.clone()
			c.QueuedAt = time.Time{}
			if err := m.putNode(ctx, c); err != nil {
				return err
			}
			continue
		case reason == "":
			continue
		case a.QueuedAt.IsZero():
			c := a.clone()
			c.QueuedAt = now
			if err := m.putNode(ctx, c); err != nil {
				return err
			}
			a = c
		}
		w := waiting{id: id, reason: reason, since: a.QueuedAt, v: v}
		if v.SpendWindow != nil {
			w.spend = *v.SpendWindow
		}
		queue = append(queue, w)
	}

	wall := time.Now().UTC()
	m.st.emissions = slices.DeleteFunc(m.st.emissions, func(e emission) bool { return wall.Sub(e.At) >= time.Hour })
	reserve, evidence := split(queue)
	for _, w := range pick(reserve, evidence, m.st.emissions, m.params) {
		if err := m.announce(ctx, w, now, wall); err != nil {
			return err
		}
	}
	return nil
}

// pick is what goes out now, given the emissions of the last hour: at most
// review_emission_rate in the hour, of which the reserve's at most the
// fraction exploration_share; the evidence takes the rest, and the reserve's
// share while no node waits in it (implementation §5.7).
func pick(reserve, evidence []waiting, emitted []emission, p Params) []waiting {
	rate := p.ReviewEmissionRate
	share := min(int(math.Floor(p.ExplorationShare*float64(rate))), rate)
	var inReserve, inEvidence int
	for _, e := range emitted {
		if e.Reserve {
			inReserve++
		} else {
			inEvidence++
		}
	}
	var out []waiting
	for inReserve+inEvidence < rate {
		switch {
		case len(reserve) > 0 && inReserve < share:
			out, reserve = append(out, reserve[0]), reserve[1:]
			inReserve++
		case len(evidence) > 0 && (inEvidence < rate-share || len(reserve) == 0):
			out, evidence = append(out, evidence[0]), evidence[1:]
			inEvidence++
		default:
			return out
		}
	}
	return out
}

// split orders the queue: the reserve, no_demand before routine; outside it,
// by class, and within each class by waiting time, except high_cost, by
// spend_window (implementation §5.7).
func split(queue []waiting) (reserve, evidence []waiting) {
	for _, w := range queue {
		if reserved(w.reason) {
			reserve = append(reserve, w)
		} else {
			evidence = append(evidence, w)
		}
	}
	sort.SliceStable(reserve, func(i, j int) bool {
		if a, b := reserve[i].reason == ReasonNoDemand, reserve[j].reason == ReasonNoDemand; a != b {
			return a
		}
		return reserve[i].since.Before(reserve[j].since)
	})
	sort.SliceStable(evidence, func(i, j int) bool {
		a, b := evidence[i], evidence[j]
		if rank[a.reason] != rank[b.reason] {
			return rank[a.reason] < rank[b.reason]
		}
		if (a.reason == ReasonHighCost) != (b.reason == ReasonHighCost) {
			return a.reason == ReasonHighCost
		}
		if a.reason == ReasonHighCost && a.spend != b.spend {
			return a.spend > b.spend
		}
		return a.since.Before(b.since)
	})
	return reserve, evidence
}

// announce opens the review's trace and announces the review as a subtask of
// its root (architecture §3.5.1, §3.12.3); now is the instant its vector was
// read at, and at the emission's. The emission, the review's key and the node
// out of the queue are written before publishing, so a restart publishes it
// again instead of losing it, and the emission counts even if it never went
// out.
func (m *monitor) announce(ctx context.Context, w waiting, now, at time.Time) error {
	a := m.st.nodes[w.id]
	root := claim.OpenRoot(m.version, TopicReview)
	data := map[string]any{
		"topic": TopicReview, "requester_identity": m.id,
		"node_id": w.id, "target_topic": a.Topic,
		"n_observations": w.v.NObservations, "trigger_reason": w.reason,
		"signals": w.v, "reopens_by_requester": a.reopens(m.params, now),
	}
	for k, v := range root.Inherited() {
		data[k] = v
	}
	e, err := bus.NewEvent(bus.TypeAnnounced, m.id, uuid.NewString(), data)
	if err != nil {
		return err
	}
	root.Announce(e)
	if err := e.Validate(); err != nil {
		return fmt.Errorf("the review of %s: %w", w.id, err)
	}
	if err := e.ValidateTopic(); err != nil {
		return fmt.Errorf("the review of %s: %w", w.id, err)
	}
	emissions := append(slices.Clone(m.st.emissions), emission{At: at, Reserve: reserved(w.reason)})
	if err := m.put(ctx, keyEmissions, emissions); err != nil {
		return err
	}
	m.st.emissions = emissions
	r := &Review{TaskID: e.Subject, NodeID: w.id, Reason: w.reason, Root: root, Announcement: e}
	if err := m.put(ctx, keyReview+r.TaskID, r); err != nil {
		return err
	}
	m.st.reviews[r.TaskID] = r
	c := a.clone()
	c.QueuedAt = time.Time{}
	if err := m.putNode(ctx, c); err != nil {
		return err
	}
	log.Printf("network_monitor: reviewing %s for %s", w.id, w.reason)
	return m.publishReview(ctx, r)
}

// publishReview publishes a review's announcement and records that the Bus
// took it.
func (m *monitor) publishReview(ctx context.Context, r *Review) error {
	if err := bus.Publish(ctx, m.js, bus.SubjectAnnounced(TopicReview), r.Announcement); err != nil {
		return err
	}
	c := *r
	c.Published = true
	if err := m.put(ctx, keyReview+r.TaskID, &c); err != nil {
		return err
	}
	m.st.reviews[r.TaskID] = &c
	return nil
}

// settle publishes the announcements a restart left unpublished and closes the
// root of every review whose close was observed (architecture §3.5.1).
func (m *monitor) settle(ctx context.Context) error {
	for _, id := range m.st.reviewIDs() {
		r := m.st.reviews[id]
		switch {
		case r.Closed:
			if err := r.Root.Close(ctx, m.js, m.id); err != nil {
				return err
			}
			dctx, cancel := context.WithTimeout(ctx, opTimeout)
			err := m.kv.Delete(dctx, keyReview+id)
			cancel()
			if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
				return err
			}
			delete(m.st.reviews, id)
		case !r.Published:
			if err := m.publishReview(ctx, r); err != nil {
				return err
			}
		}
	}
	return nil
}

// reopens are the node's reopens in the quality window, by requester
// (architecture §3.12.3).
func (a *Aggregate) reopens(p Params, now time.Time) []map[string]any {
	by := map[string]int{}
	for _, o := range a.window(p, now) {
		if o.Reopened {
			by[o.Requester]++
		}
	}
	out := []map[string]any{}
	for _, r := range sortedKeys(by) {
		out = append(out, map[string]any{"requester_identity": r, "reopens": by[r]})
	}
	return out
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// ids are the roster's nodes, in a fixed order.
func (s *state) ids() []string { return sortedKeys(s.nodes) }

// reviewIDs are the open reviews, in a fixed order.
func (s *state) reviewIDs() []string { return sortedKeys(s.reviews) }

// putNode writes a node's key and keeps it in memory.
func (m *monitor) putNode(ctx context.Context, a *Aggregate) error {
	if err := m.put(ctx, keyNode+a.NodeID, a); err != nil {
		return err
	}
	m.st.nodes[a.NodeID] = a
	return nil
}
