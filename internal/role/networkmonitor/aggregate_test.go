package networkmonitor

import (
	"testing"
	"time"
)

var t0 = time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)

// The admission_clock advances only while the network is open, and is a pure
// function of the sequence of versions (architecture §3.12.1).
func TestAdmissionClock(t *testing.T) {
	var a admission
	a.add(t0, false)                  // v0, frozen
	a.add(t0.Add(time.Hour), true)    // opened
	a.add(t0.Add(3*time.Hour), false) // paused
	a.add(t0.Add(5*time.Hour), true)  // opened again
	for _, c := range []struct {
		at   time.Duration
		want time.Duration
	}{
		{0, 0},
		{time.Hour, 0},
		{2 * time.Hour, time.Hour},
		{3 * time.Hour, 2 * time.Hour},
		{4 * time.Hour, 2 * time.Hour},
		{6 * time.Hour, 3 * time.Hour},
	} {
		if got := a.at(t0.Add(c.at)); got != c.want {
			t.Errorf("at +%s = %s, want %s", c.at, got, c.want)
		}
	}
}

var params = Params{
	QualityWindow: 8, MinObservations: 2, UsageWindow: 4, EvidenceStaleness: 30 * 24 * time.Hour,
	GracePeriod: time.Hour, ScoreFloor: 0.5, W1: 5, OuterShare: 0.25,
}

func obs(i int, sim, cost float64, failed, reopened bool) Observation {
	return Observation{
		TaskID: string(rune('a' + i)), Closed: t0.Add(time.Duration(i) * time.Minute), Similarity: sim,
		Cost: cost, SubtreeCost: cost * 2, DurationMs: 1000 * cost, Failed: failed, Reopened: reopened,
	}
}

// The quality axis counts as unresolved every task.failed and every reopened
// task.completed (architecture §3.12.2); the radial axis splits the window by
// similarity, and the usage axis is absent until its window fills.
func TestVector(t *testing.T) {
	a := &Aggregate{}
	for _, o := range []Observation{
		obs(0, 0.95, 1, false, false), obs(1, 0.9, 1, false, false), obs(2, 0.88, 1, false, false),
		obs(3, 0.86, 1, false, false), obs(4, 0.85, 1, false, true), obs(5, 0.84, 1, false, false),
		obs(6, 0.61, 4, true, false), obs(7, 0.62, 4, false, true),
	} {
		a.classify(o)
	}
	v := a.Vector(params, t0.Add(time.Hour))
	if v.NObservations != 8 || *v.ReopenRate != 3.0/8 || *v.Score != 0 || *v.Margin != -0.5 {
		t.Fatalf("quality %d %v %v %v", v.NObservations, *v.ReopenRate, *v.Score, *v.Margin)
	}
	if *v.CostOwnP50 != 1 || *v.CostOwnP90 != 4 || *v.CostDispersion != 4 || *v.CostSubtreeP50 != 2 {
		t.Errorf("cost %v %v %v %v", *v.CostOwnP50, *v.CostOwnP90, *v.CostDispersion, *v.CostSubtreeP50)
	}
	// Five clean closes over the window's whole cost (architecture §3.12.2).
	if *v.SpendWindow != 14 || *v.CostPerResolutionOwn != 14.0/5 || *v.CostPerResolutionSubtree != 28.0/5 {
		t.Errorf("resolution %v %v %v", *v.SpendWindow, *v.CostPerResolutionOwn, *v.CostPerResolutionSubtree)
	}
	// The outer quarter is the two farthest tasks.
	if *v.ReopenRateOuter != 1 || *v.ReopenRateInner != 1.0/6 || *v.CostOwnP50Outer != 4 || *v.CostOwnP50Inner != 1 {
		t.Errorf("radial %v %v %v %v", *v.ReopenRateOuter, *v.ReopenRateInner, *v.CostOwnP50Outer, *v.CostOwnP50Inner)
	}
	if v.UsageShare != nil || v.IdleShare != nil || v.WinRate != nil {
		t.Error("the usage axis with an incomplete window is not absent")
	}
	// Evidence older than evidence_staleness leaves the window: the node is not
	// evaluable, not badly scored (architecture §3.12.1).
	if v := a.Vector(params, t0.Add(31*24*time.Hour)); v.NObservations != 0 || v.Score != nil {
		t.Errorf("stale window %+v", v)
	}
}

func TestUsage(t *testing.T) {
	a := &Aggregate{Usage: []Assignment{
		{Outcome: outcomeIdle, Proposed: true}, {Outcome: outcomeWon, Proposed: true},
		{Outcome: outcomeBusy}, {Outcome: outcomeIdle}, {Outcome: outcomeWon, Proposed: true},
	}}
	v := a.Vector(params, t0)
	if *v.UsageShare != 0.5 || *v.IdleShare != 0.25 || *v.WinRate != 1 {
		t.Errorf("usage %v %v %v", *v.UsageShare, *v.IdleShare, *v.WinRate)
	}
}

// A gap leaves out what came before it and counts the periods again from its
// end (architecture §3.12.1).
func TestDropBefore(t *testing.T) {
	end := t0.Add(5 * time.Minute)
	a := &Aggregate{
		Observations: []Observation{obs(1, 0.9, 1, false, false), obs(7, 0.9, 1, false, false)},
		Pending:      []Observation{obs(2, 0.9, 1, false, false)},
		Usage:        []Assignment{{At: t0}, {At: end.Add(time.Second)}},
		Holding:      &Holding{Assigned: t0},
		SinceRoster:  9,
		DemandFrom:   t0,
	}
	a.dropBefore(end)
	if len(a.Observations) != 1 || len(a.Pending) != 0 || len(a.Usage) != 1 || a.Holding != nil || a.SinceRoster != 0 || !a.DemandFrom.Equal(end) {
		t.Errorf("after the gap %+v", a)
	}
}
