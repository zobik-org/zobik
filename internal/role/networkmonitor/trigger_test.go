package networkmonitor

import (
	"encoding/json"
	"testing"
	"time"

	"zobik.org/zobik/internal/bus"
)

var triggerParams = func() Params {
	p := params
	p.MaxIdleShare, p.NoDemandRetention = 0.8, 7*24*time.Hour
	p.CostDriftThreshold, p.DurationDriftThreshold = 0.3, 0.5
	p.OuterReopenThreshold, p.OuterCostThreshold, p.OuterDurationThreshold = 0.05, 0.5, 0.5
	p.ReviewEmissionRate, p.ExplorationShare = 12, 0.25
	return p
}()

// openClock is an admission_clock open since well before t0.
func openClock() *admission {
	var a admission
	a.add(t0.Add(-365*24*time.Hour), true)
	return &a
}

// healthy is a node with a window of clean closes at the same cost, close to
// its center, assigned a moment ago.
func healthy(n int) *Aggregate {
	a := &Aggregate{DemandFrom: t0}
	for i := range n {
		a.classify(obs(i, 0.9, 1, false, false))
	}
	return a
}

// Each trigger_reason goes out with its condition, and a node that meets none
// and is evaluable goes to routine (implementation §5).
func TestConditions(t *testing.T) {
	now := t0.Add(time.Hour)
	cases := []struct {
		name string
		node func() *Aggregate
		want string
	}{
		{"healthy and evaluable", func() *Aggregate { return healthy(4) }, ReasonRoutine},
		{"below min_observations", func() *Aggregate { return healthy(1) }, ""},
		{"a notice", func() *Aggregate { a := healthy(1); a.Notices = []time.Time{t0}; return a }, ReasonNoticeProvenance},
		{"no assignment for no_demand_retention", func() *Aggregate {
			a := healthy(1)
			a.DemandFrom = now.Add(-8 * 24 * time.Hour)
			return a
		}, ReasonNoDemand},
		{"free while others win", func() *Aggregate {
			a := healthy(1)
			a.Usage = []Assignment{{Outcome: outcomeIdle}, {Outcome: outcomeIdle}, {Outcome: outcomeIdle}, {Outcome: outcomeIdle}, {Outcome: outcomeIdle}}
			a.SinceRoster = 4
			return a
		}, ReasonLowUsage},
		{"free, with the window from before the roster changed", func() *Aggregate {
			a := healthy(1)
			a.Usage = []Assignment{{Outcome: outcomeIdle}, {Outcome: outcomeIdle}, {Outcome: outcomeIdle}, {Outcome: outcomeIdle}}
			a.SinceRoster = 3
			return a
		}, ""},
		{"reopened", func() *Aggregate {
			a := healthy(3)
			a.classify(obs(3, 0.9, 1, false, true))
			return a
		}, ReasonLowScore},
		{"worse at the edge", func() *Aggregate {
			a := healthy(6)
			a.classify(obs(6, 0.6, 3, false, false))
			a.classify(obs(7, 0.6, 3, false, false))
			return a
		}, ReasonOuterGradient},
		{"cost up from the previous window", func() *Aggregate {
			a := &Aggregate{DemandFrom: t0}
			for i := range 8 {
				a.classify(obs(i, 0.9, 1, false, false))
			}
			for i := 8; i < 16; i++ {
				o := obs(i, 0.9, 2, false, false)
				o.DurationMs = 1000
				a.classify(o)
			}
			return a
		}, ReasonHighCost},
		{"duration up from the previous window", func() *Aggregate {
			a := &Aggregate{DemandFrom: t0}
			for i := range 16 {
				o := obs(i, 0.9, 1, false, false)
				if i >= 8 {
					o.DurationMs = 2000
				}
				a.classify(o)
			}
			return a
		}, ReasonHighLatency},
		{"cost up from zero", func() *Aggregate {
			a := &Aggregate{DemandFrom: t0}
			for i := range 16 {
				o := obs(i, 0.9, 0, false, false)
				o.DurationMs = 1000
				if i >= 8 {
					o.Cost = 0.01
				}
				a.classify(o)
			}
			return a
		}, ReasonHighCost},
		// Several hold at once: the precedence of implementation §5.6 chooses.
		{"a notice on a node that violates the constraint", func() *Aggregate {
			a := healthy(3)
			a.classify(obs(3, 0.9, 1, true, false))
			a.Notices = []time.Time{t0}
			return a
		}, ReasonNoticeProvenance},
		{"no demand on a node that violates the constraint", func() *Aggregate {
			a := healthy(3)
			a.classify(obs(3, 0.9, 1, true, false))
			a.DemandFrom = now.Add(-8 * 24 * time.Hour)
			return a
		}, ReasonNoDemand},
	}
	for _, c := range cases {
		if got, _ := c.node().condition(triggerParams, now, openClock()); got != c.want {
			t.Errorf("%s: %q, want %q", c.name, got, c.want)
		}
	}
}

// The no_demand period is counted in admission_clock: a pause does not count
// (architecture §3.12.1).
func TestNoDemandGatedByAdmission(t *testing.T) {
	var clock admission
	clock.add(t0.Add(-365*24*time.Hour), true)
	clock.add(t0, false) // paused since t0
	a := healthy(1)
	a.DemandFrom = t0.Add(-24 * time.Hour)
	if got, _ := a.condition(triggerParams, t0.Add(30*24*time.Hour), &clock); got == ReasonNoDemand {
		t.Error("no_demand counted the pause")
	}
}

// snoozed and capped silence every trigger; no_action only routine; snoozed
// is used up in observations and no_action in admission_clock
// (architecture §3.13.5, implementation §5.9).
func TestSnooze(t *testing.T) {
	clock := openClock()
	snoozed := snoozeOf(json.RawMessage(`{"action":"snoozed","snooze_until":2}`), t0)
	noAction := snoozeOf(json.RawMessage(`{"action":"no_action","snooze_until":86400000}`), t0)
	capped := snoozeOf(json.RawMessage(`{"action":"capped"}`), t0)
	at := t0.Add(time.Hour)
	for _, c := range []struct {
		name   string
		s      *Snooze
		reason string
		want   bool
	}{
		{"snoozed, low_score", snoozed, ReasonLowScore, true},
		{"capped, notice_provenance", capped, ReasonNoticeProvenance, true},
		{"no_action, routine", noAction, ReasonRoutine, true},
		{"no_action, high_cost", noAction, ReasonHighCost, false},
	} {
		if got := c.s.silences(c.reason, at, clock); got != c.want {
			t.Errorf("%s: %v", c.name, got)
		}
	}
	if noAction.silences(ReasonRoutine, t0.Add(25*time.Hour), clock) {
		t.Error("no_action did not expire with its admission_clock")
	}
	snoozed.Observations -= 2
	if snoozed.silences(ReasonLowScore, at, clock) {
		t.Error("snoozed did not expire with its observations")
	}
	if snoozeOf(json.RawMessage(`{"action":"archived"}`), t0) != nil {
		t.Error("an outcome without a snooze snoozed")
	}
}

// A burst respects the hour's cap and lets the reserve through; the evidence
// takes the reserve's share while no node waits in it, and the reserve never
// takes more than its share (implementation §5.7).
func TestPick(t *testing.T) {
	many := func(reason string, n int) []waiting {
		var out []waiting
		for i := range n {
			out = append(out, waiting{id: string(rune('a' + i)), reason: reason, since: t0.Add(time.Duration(i) * time.Minute)})
		}
		return out
	}
	count := func(ws []waiting) (reserve, evidence int) {
		for _, w := range ws {
			if reserved(w.reason) {
				reserve++
			} else {
				evidence++
			}
		}
		return
	}
	reserve, evidence := split(append(many(ReasonHighCost, 20), many(ReasonRoutine, 5)...))
	if r, e := count(pick(reserve, evidence, nil, triggerParams)); r != 3 || e != 9 {
		t.Errorf("burst: %d of the reserve and %d of evidence", r, e)
	}
	if r, e := count(pick(nil, many(ReasonLowScore, 20), nil, triggerParams)); r != 0 || e != 12 {
		t.Errorf("without a reserve waiting: %d and %d", r, e)
	}
	if r, e := count(pick(many(ReasonRoutine, 20), nil, nil, triggerParams)); r != 3 || e != 0 {
		t.Errorf("only the reserve: %d and %d", r, e)
	}
	// What the hour already emitted counts.
	hour := []emission{{At: t0, Reserve: true}, {At: t0}, {At: t0}}
	if r, e := count(pick(reserve, evidence, hour, triggerParams)); r != 2 || e != 7 {
		t.Errorf("after three: %d and %d", r, e)
	}
}

// The queue goes out by class, and within high_cost by spend_window; in the
// reserve, no_demand before routine (implementation §5.7).
func TestOrder(t *testing.T) {
	reserve, evidence := split([]waiting{
		{id: "routine", reason: ReasonRoutine, since: t0},
		{id: "usage", reason: ReasonLowUsage, since: t0},
		{id: "cheap", reason: ReasonHighCost, since: t0, spend: 1},
		{id: "latency", reason: ReasonHighLatency, since: t0},
		{id: "dear", reason: ReasonHighCost, since: t0.Add(time.Hour), spend: 9},
		{id: "score", reason: ReasonLowScore, since: t0.Add(time.Hour)},
		{id: "demand", reason: ReasonNoDemand, since: t0.Add(time.Hour)},
		{id: "notice", reason: ReasonNoticeProvenance, since: t0.Add(2 * time.Hour)},
	})
	ids := func(ws []waiting) (out []string) {
		for _, w := range ws {
			out = append(out, w.id)
		}
		return
	}
	if got, want := ids(evidence), []string{"notice", "score", "dear", "cheap", "latency", "usage"}; !equal(got, want) {
		t.Errorf("evidence %v, want %v", got, want)
	}
	if got, want := ids(reserve), []string{"demand", "routine"}; !equal(got, want) {
		t.Errorf("reserve %v, want %v", got, want)
	}
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// A gap in task.> releases the node of an open review, which can be reviewed
// again, and the review waits for its close to close the root
// (implementation §1.2.8).
func TestGapReleasesReview(t *testing.T) {
	s := &state{
		self: "UMONITOR", nodes: map[string]*Aggregate{},
		marks:   map[string]mark{"task": {Seq: 1, Stored: t0}},
		reviews: map[string]*Review{"T": {TaskID: "T", NodeID: "UNODE"}},
	}
	d := s.observe("task", 5, t0.Add(time.Hour), nil, "", params)
	r := d.reviews["T"]
	if r == nil || !r.Released || r.Closed {
		t.Fatalf("after the gap %+v", r)
	}
	s.reviews["T"] = r
	if s.underReview()["UNODE"] {
		t.Error("the released node is still under review")
	}
}

// A review closed without the Spawner's outcome snoozes its node for
// node_review_snooze observations (implementation §5.9).
func TestFailedReviewSnoozes(t *testing.T) {
	p := params
	p.NodeReviewSnooze = 6
	s := &state{
		self:    "UMONITOR",
		nodes:   map[string]*Aggregate{"UNODE": {NodeID: "UNODE", Applied: map[string]uint64{}}},
		marks:   map[string]mark{},
		reviews: map[string]*Review{"T": {TaskID: "T", NodeID: "UNODE"}},
	}
	e := &bus.Event{Type: bus.TypeFailed, Subject: "T", Data: json.RawMessage(`{"topic":"node_review","failure_reason":"x"}`)}
	d := s.observe("task", 1, t0, e, "task.failed.UMONITOR.node_review", p)
	if r := d.reviews["T"]; r == nil || !r.Closed {
		t.Fatalf("the review %+v", r)
	}
	if sn := d.nodes["UNODE"].Snooze; sn == nil || sn.Action != ActionSnoozed || sn.Observations != 6 {
		t.Errorf("the snooze %+v", sn)
	}
}
