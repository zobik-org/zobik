package networkmonitor

import (
	"time"
)

// The trigger_reason values (architecture §3.12.3).
const (
	ReasonLowScore         = "low_score"
	ReasonNoticeProvenance = "notice_provenance"
	ReasonMemoryExhausted  = "memory_exhausted"
	ReasonHighCost         = "high_cost"
	ReasonHighLatency      = "high_latency"
	ReasonOuterGradient    = "outer_gradient"
	ReasonLowUsage         = "low_usage"
	ReasonNoDemand         = "no_demand"
	ReasonRoutine          = "routine"
)

// condition is the trigger_reason the node meets at now, by the precedence of
// implementation §5.6, with the vector it was read from; "" if it meets none.
// routine asks only that the node be evaluable (implementation §5.6).
func (a *Aggregate) condition(p Params, now time.Time, clock *admission) (string, Vector) {
	v := a.Vector(p, now)
	switch {
	case v.NoticeProvenance > 0:
		return ReasonNoticeProvenance, v
	case a.noDemand(p, now, clock):
		return ReasonNoDemand, v
	case v.IdleShare != nil && *v.IdleShare > p.MaxIdleShare && a.SinceRoster >= p.UsageWindow:
		return ReasonLowUsage, v
	case v.NObservations >= p.MinObservations && v.Score != nil && *v.Score < p.ScoreFloor:
		return ReasonLowScore, v
	case outerGradient(v, p):
		return ReasonOuterGradient, v
	}
	if cost, duration := a.drift(p, now); cost {
		return ReasonHighCost, v
	} else if duration {
		return ReasonHighLatency, v
	}
	if v.NObservations >= p.MinObservations {
		return ReasonRoutine, v
	}
	return "", v
}

// noDemand is no_demand_retention of admission_clock passed without an
// assignment on the topic (implementation §5.5).
func (a *Aggregate) noDemand(p Params, now time.Time, clock *admission) bool {
	return p.NoDemandRetention > 0 && clock.at(now)-clock.at(a.DemandFrom) >= p.NoDemandRetention
}

// outerGradient is the edge performing worse than the center, with the outer
// group at min_observations, which the vector carries only then
// (implementation §5.3).
func outerGradient(v Vector, p Params) bool {
	if v.ReopenRateOuter == nil {
		return false
	}
	return *v.ReopenRateOuter-*v.ReopenRateInner > p.OuterReopenThreshold ||
		exceeds(*v.CostOwnP50Outer, *v.CostOwnP50Inner, p.OuterCostThreshold) ||
		exceeds(*v.DurationP50Outer, *v.DurationP50Inner, p.OuterDurationThreshold)
}

// drift compares the medians of the current window with those of the previous
// one; with either below min_observations it is not evaluable
// (implementation §5.2).
func (a *Aggregate) drift(p Params, now time.Time) (cost, duration bool) {
	current, previous := a.windows(p, now)
	if len(current) < p.MinObservations || len(previous) < p.MinObservations || len(current) == 0 || len(previous) == 0 {
		return false, false
	}
	median := func(w []Observation, f func(Observation) float64) float64 { return percentile(field(w, f), 0.5) }
	own := func(o Observation) float64 { return o.Cost }
	took := func(o Observation) float64 { return o.DurationMs }
	return exceeds(median(current, own), median(previous, own), p.CostDriftThreshold),
		exceeds(median(current, took), median(previous, took), p.DurationDriftThreshold)
}

// exceeds is x / reference > 1 + threshold; from a zero reference, any positive
// x (implementation §5.2, §5.3).
func exceeds(x, reference, threshold float64) bool {
	if reference == 0 {
		return x > 0
	}
	return x/reference > 1+threshold
}
