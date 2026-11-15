package networkmonitor

import (
	"encoding/json"
	"math"
	"slices"
	"sort"
	"time"

	"zobik.org/zobik/internal/globalconfig"
)

// Aggregate is the working aggregate of a node of the roster: the evidence of
// its windows, its deferred classifications in flight and the point of each
// family up to which it observed (architecture §3.12.1). It is a key of the
// role's bucket (implementation §1.2.8).
type Aggregate struct {
	NodeID string `json:"node_id"`
	Topic  string `json:"topic"`
	// Applied is, per family, the stream sequence of the last event applied to
	// this key: a redelivered event is recognized by it (implementation §1.2.8).
	Applied map[string]uint64 `json:"applied"`
	// Since is when the node entered the roster.
	Since time.Time `json:"since"`
	// Holding is the task the node works on: a node works one at a time
	// (architecture §3.6.1).
	Holding *Holding `json:"holding,omitempty"`
	// Pending are its task.completed waiting for their classification
	// (architecture §3.12.1).
	Pending []Observation `json:"pending,omitempty"`
	// Observations are its classified closes, in the order they closed; the
	// windows read the last ones within evidence_staleness. It keeps two
	// windows: the drift of implementation §5.2 compares them.
	Observations []Observation `json:"observations,omitempty"`
	// Usage are the last assignments granted on its topic, with the node's state
	// at each one (architecture §3.12.2).
	Usage []Assignment `json:"usage,omitempty"`
	// SinceRoster counts the assignments on its topic since the last change to
	// the topic's roster (implementation §5.4).
	SinceRoster int `json:"since_roster"`
	// Proposals are its proposals whose assignment it has not yet seen, by
	// TaskID: the families are read separately, so an assignment can arrive
	// before the proposals of its auction.
	Proposals map[string]time.Time `json:"proposals,omitempty"`
	// Notices are the notices it emitted (architecture §3.12.2).
	Notices []time.Time `json:"notices,omitempty"`
	// DemandFrom is the instant no_demand's period counts from: the last
	// assignment on its topic, its entry into the roster or the end of an
	// observation gap (architecture §3.12.1).
	DemandFrom time.Time `json:"demand_from"`
}

// Holding is the task a node holds, with what its subtasks add up while it
// holds it.
type Holding struct {
	TaskID    string    `json:"task_id"`
	TraceID   string    `json:"trace_id"`
	Requester string    `json:"requester"`
	Assigned  time.Time `json:"assigned"`
	// Similarity is the one the Task Broker awarded with, which places the task
	// on the radial axis (architecture §3.12.2).
	Similarity float64 `json:"similarity"`
	// ChildrenCost is the subtree cost of the subtasks closed so far
	// (architecture §3.9.1).
	ChildrenCost float64 `json:"children_cost"`
	// Subtasks are the TaskIDs it announced: a re-announcement is the same one.
	Subtasks map[string]bool `json:"subtasks,omitempty"`
	// Lineages are the subtasks that open a lineage, without previous_attempt.
	Lineages int `json:"lineages"`
	Queries  int `json:"queries"`
	Gates    int `json:"gates"`
}

// Observation is a close of the node.
type Observation struct {
	TaskID    string    `json:"task_id"`
	Requester string    `json:"requester"`
	Closed    time.Time `json:"closed"`
	// Failed is a task.failed; Reopened, a task.completed its requester reopened.
	Failed      bool    `json:"failed,omitempty"`
	Reopened    bool    `json:"reopened,omitempty"`
	Similarity  float64 `json:"similarity"`
	Cost        float64 `json:"cost"`
	SubtreeCost float64 `json:"subtree_cost"`
	DurationMs  float64 `json:"duration_ms"`
	TokensIn    float64 `json:"tokens_in"`
	TokensOut   float64 `json:"tokens_out"`
	Attempts    int     `json:"attempts"`
	Lineages    int     `json:"lineages"`
	Queries     int     `json:"queries"`
	Gates       int     `json:"gates"`
}

// clean says whether the close counts as resolved (architecture §3.12.2).
func (o Observation) clean() bool { return !o.Failed && !o.Reopened }

// The node's state at an assignment on its topic (architecture §3.12.2).
const (
	outcomeWon  = "won"
	outcomeBusy = "busy"
	outcomeIdle = "idle"
)

// Assignment is an assignment granted on the node's topic.
type Assignment struct {
	TaskID   string    `json:"task_id"`
	At       time.Time `json:"at"`
	Outcome  string    `json:"outcome"`
	Proposed bool      `json:"proposed,omitempty"`
}

// Params are the Global Configuration keys the measurement reads, from the
// head in the role's cycle (architecture §2.3).
type Params struct {
	QualityWindow     int
	MinObservations   int
	UsageWindow       int
	EvidenceStaleness time.Duration
	GracePeriod       time.Duration
	ScoreFloor        float64
	W1                float64
	OuterShare        float64
}

// ReadParams reads the parameters from a published version.
func ReadParams(rec *globalconfig.Record) Params {
	num := func(key string) float64 {
		var v float64
		_ = json.Unmarshal(rec.Values[key], &v)
		return v
	}
	return Params{
		QualityWindow:     int(num("quality_window")),
		MinObservations:   int(num("min_observations")),
		UsageWindow:       int(num("usage_window")),
		EvidenceStaleness: rec.Duration("evidence_staleness"),
		GracePeriod:       rec.Duration("grace_period"),
		ScoreFloor:        num("score_floor"),
		W1:                num("w1"),
		OuterShare:        num("outer_share"),
	}
}

// Vector is the node's signals (architecture §3.12.2). A signal without enough
// evidence is absent, never zero.
type Vector struct {
	NObservations            int      `json:"n_observations"`
	ReopenRate               *float64 `json:"reopen_rate,omitempty"`
	Score                    *float64 `json:"score,omitempty"`
	Margin                   *float64 `json:"margin,omitempty"`
	CostOwnP50               *float64 `json:"cost_own_p50,omitempty"`
	CostOwnP90               *float64 `json:"cost_own_p90,omitempty"`
	CostSubtreeP50           *float64 `json:"cost_subtree_p50,omitempty"`
	CostSubtreeP90           *float64 `json:"cost_subtree_p90,omitempty"`
	CostPerResolutionOwn     *float64 `json:"cost_per_resolution_own,omitempty"`
	CostPerResolutionSubtree *float64 `json:"cost_per_resolution_subtree,omitempty"`
	AvgAttempts              *float64 `json:"avg_attempts,omitempty"`
	CostDispersion           *float64 `json:"cost_dispersion,omitempty"`
	SpendWindow              *float64 `json:"spend_window,omitempty"`
	DurationP50              *float64 `json:"duration_p50,omitempty"`
	DurationP90              *float64 `json:"duration_p90,omitempty"`
	DurationDispersion       *float64 `json:"duration_dispersion,omitempty"`
	TokensInP50              *float64 `json:"tokens_in_p50,omitempty"`
	TokensOutP50             *float64 `json:"tokens_out_p50,omitempty"`
	RequesterQueriesPerTask  *float64 `json:"requester_queries_per_task,omitempty"`
	EgressGatesPerTask       *float64 `json:"egress_gates_per_task,omitempty"`
	ReopenRateInner          *float64 `json:"reopen_rate_inner,omitempty"`
	ReopenRateOuter          *float64 `json:"reopen_rate_outer,omitempty"`
	CostOwnP50Inner          *float64 `json:"cost_own_p50_inner,omitempty"`
	CostOwnP50Outer          *float64 `json:"cost_own_p50_outer,omitempty"`
	DurationP50Inner         *float64 `json:"duration_p50_inner,omitempty"`
	DurationP50Outer         *float64 `json:"duration_p50_outer,omitempty"`
	UsageShare               *float64 `json:"usage_share,omitempty"`
	IdleShare                *float64 `json:"idle_share,omitempty"`
	WinRate                  *float64 `json:"win_rate,omitempty"`
	NoticeProvenance         int      `json:"notice_provenance"`
}

// window is the last quality_window classified closes within
// evidence_staleness (architecture §3.12.1).
func (a *Aggregate) window(p Params, now time.Time) []Observation {
	from := now.Add(-p.EvidenceStaleness)
	var w []Observation
	for _, o := range a.Observations {
		if !o.Closed.Before(from) {
			w = append(w, o)
		}
	}
	if len(w) > p.QualityWindow {
		w = w[len(w)-p.QualityWindow:]
	}
	return w
}

// Vector computes the node's signals at now.
func (a *Aggregate) Vector(p Params, now time.Time) Vector {
	w := a.window(p, now)
	v := Vector{NObservations: len(w)}
	if n := len(w); n > 0 {
		rr := nonClean(w)
		score := 1 - math.Min(math.Max(p.W1*rr, 0), 1)
		v.ReopenRate, v.Score, v.Margin = &rr, &score, ptr(score-p.ScoreFloor)
		own, subtree, duration := field(w, func(o Observation) float64 { return o.Cost }),
			field(w, func(o Observation) float64 { return o.SubtreeCost }),
			field(w, func(o Observation) float64 { return o.DurationMs })
		v.CostOwnP50, v.CostOwnP90 = ptr(percentile(own, 0.5)), ptr(percentile(own, 0.9))
		v.CostSubtreeP50, v.CostSubtreeP90 = ptr(percentile(subtree, 0.5)), ptr(percentile(subtree, 0.9))
		v.DurationP50, v.DurationP90 = ptr(percentile(duration, 0.5)), ptr(percentile(duration, 0.9))
		v.CostDispersion = ratio(*v.CostOwnP90, *v.CostOwnP50)
		v.DurationDispersion = ratio(*v.DurationP90, *v.DurationP50)
		v.TokensInP50 = ptr(percentile(field(w, func(o Observation) float64 { return o.TokensIn }), 0.5))
		v.TokensOutP50 = ptr(percentile(field(w, func(o Observation) float64 { return o.TokensOut }), 0.5))
		var spend, spendSubtree float64
		var cleanCount, attempts, lineages, queries, gates int
		for _, o := range w {
			spend += o.Cost
			spendSubtree += o.SubtreeCost
			attempts += o.Attempts
			lineages += o.Lineages
			queries += o.Queries
			gates += o.Gates
			if o.clean() {
				cleanCount++
			}
		}
		v.SpendWindow = &spend
		// Undefined, not infinite, below min_observations (architecture §3.12.2).
		if cleanCount >= p.MinObservations && cleanCount > 0 {
			v.CostPerResolutionOwn = ptr(spend / float64(cleanCount))
			v.CostPerResolutionSubtree = ptr(spendSubtree / float64(cleanCount))
		}
		if lineages > 0 {
			v.AvgAttempts = ptr(float64(attempts) / float64(lineages))
		}
		v.RequesterQueriesPerTask = ptr(float64(queries) / float64(n))
		v.EgressGatesPerTask = ptr(float64(gates) / float64(n))
		a.radial(&v, w, p)
	}
	a.usage(&v, p)
	from := now.Add(-p.EvidenceStaleness)
	for _, t := range a.Notices {
		if !t.Before(from) {
			v.NoticeProvenance++
		}
	}
	return v
}

// radial splits the window by distance: the outer group is the fraction
// outer_share with the lowest similarity (architecture §3.12.2).
func (a *Aggregate) radial(v *Vector, w []Observation, p Params) {
	k := int(math.Floor(p.OuterShare * float64(len(w))))
	if k < p.MinObservations || k == 0 {
		return
	}
	sorted := slices.Clone(w)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].Similarity < sorted[j].Similarity })
	outer, inner := sorted[:k], sorted[k:]
	v.ReopenRateOuter, v.ReopenRateInner = ptr(nonClean(outer)), ptr(nonClean(inner))
	cost := func(o Observation) float64 { return o.Cost }
	duration := func(o Observation) float64 { return o.DurationMs }
	v.CostOwnP50Outer, v.CostOwnP50Inner = ptr(percentile(field(outer, cost), 0.5)), ptr(percentile(field(inner, cost), 0.5))
	v.DurationP50Outer, v.DurationP50Inner = ptr(percentile(field(outer, duration), 0.5)), ptr(percentile(field(inner, duration), 0.5))
}

// usage computes the usefulness axis over the last usage_window assignments on
// the topic; an incomplete window is absent (architecture §3.12.2).
func (a *Aggregate) usage(v *Vector, p Params) {
	if p.UsageWindow <= 0 || len(a.Usage) < p.UsageWindow {
		return
	}
	u := a.Usage[len(a.Usage)-p.UsageWindow:]
	var won, idle, proposed int
	for _, e := range u {
		switch e.Outcome {
		case outcomeWon:
			won++
		case outcomeIdle:
			idle++
		}
		if e.Proposed {
			proposed++
		}
	}
	n := float64(len(u))
	v.UsageShare, v.IdleShare = ptr(float64(won)/n), ptr(float64(idle)/n)
	if proposed > 0 {
		v.WinRate = ptr(float64(won) / float64(proposed))
	}
}

// nonClean is the fraction of the closes that did not close clean.
func nonClean(w []Observation) float64 {
	n := 0
	for _, o := range w {
		if !o.clean() {
			n++
		}
	}
	return float64(n) / float64(len(w))
}

func field(w []Observation, f func(Observation) float64) []float64 {
	out := make([]float64, len(w))
	for i, o := range w {
		out[i] = f(o)
	}
	return out
}

// percentile is the nearest-rank percentile: the window is a few tens of
// observations, so it is kept whole (architecture §3.12.1).
func percentile(xs []float64, p float64) float64 {
	s := slices.Clone(xs)
	slices.Sort(s)
	i := int(math.Ceil(p*float64(len(s)))) - 1
	return s[max(i, 0)]
}

// ratio is p90/p50, absent when the median is zero.
func ratio(p90, p50 float64) *float64 {
	if p50 == 0 {
		return nil
	}
	return ptr(p90 / p50)
}

func ptr[T any](v T) *T { return &v }
