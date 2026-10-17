package tracingcollector

import (
	"sort"
	"time"

	"zobik.org/zobik/internal/bus"
)

// The outcomes a span ends with (implementation §1.2.6).
const (
	outcomeCompleted    = "completed"
	outcomeFailed       = "failed"
	outcomeAborted      = "aborted"
	outcomeLeaseExpired = "lease_expired"
)

var outcomes = map[string]string{
	bus.TypeCompleted: outcomeCompleted,
	bus.TypeFailed:    outcomeFailed,
	bus.TypeAborted:   outcomeAborted,
}

// span is a task's span as the role rebuilds it from the trace's facts
// (architecture §3.2, implementation §1.2.6).
type span struct {
	ID, Parent string
	// Task is its TaskID; in the root, the TraceID.
	Task string
	Root bool
	// Announcement is the one its task's attributes come from, if observed.
	Announcement *fact
	// Assigned opens it; nil in the root and in a task closed without assignment.
	Assigned *fact
	// Terminal closes it; nil if it is open or its lease expired.
	Terminal   *fact
	Start, End time.Time
	Outcome    string
	// Settle is when, by the server's clock, the last event that closes or
	// names the span was stored: once every reader passes it, nothing
	// more is expected for it. Zero while it is not exportable.
	Settle time.Time
	Events []*fact
}

func (s *span) closed() bool { return s.Outcome != "" }

// spans rebuilds the trace's spans from its facts. It depends only on the set
// of facts, not on the order they arrived in: each event names its parent, and
// the server's clock orders the events of one task (architecture §3.2).
func (t *trace) spans() []*span {
	facts := make([]*fact, len(t.Facts))
	for i := range t.Facts {
		facts[i] = &t.Facts[i]
	}
	sort.SliceStable(facts, func(i, j int) bool { return facts[i].Stored.Before(facts[j].Stored) })

	byTask := map[string][]*fact{}
	var order []string
	var notices []*fact
	var rootClose *fact
	for _, f := range facts {
		switch {
		case f.Type == bus.TypeNotice:
			notices = append(notices, f)
		case f.Task == t.ID:
			// The root's own events: its close (architecture §3.5.4).
			if outcomes[f.Type] != "" && rootClose == nil {
				rootClose = f
			}
		default:
			if byTask[f.Task] == nil {
				order = append(order, f.Task)
			}
			byTask[f.Task] = append(byTask[f.Task], f)
		}
	}

	var out []*span
	for _, task := range order {
		out = append(out, taskSpans(task, byTask[task])...)
	}
	if root := rootSpan(t.ID, rootClose, out, facts); root != nil {
		out = append(out, root)
	}

	// A notice is an annotation of the span its traceparent names (architecture §3.2).
	byID := map[string]*span{}
	for _, s := range out {
		byID[s.ID] = s
	}
	for _, n := range notices {
		if s := byID[n.Parent]; s != nil {
			s.Events = append(s.Events, n)
		}
	}
	return out
}

// taskSpans are the spans of one task: one per assignment, and one of its own
// for a close by a source that holds none of them (implementation §1.2.6).
func taskSpans(task string, facts []*fact) []*span {
	var (
		spans []*span
		open  *span
		// pending are the announcements and proposals since the last award:
		// they belong to the span the next award opens.
		pending []*fact
		// proposedIn is the span of each proposer's last proposal.
		proposedIn = map[string]*span{}
		pendingBy  = map[string]bool{}
		first      *fact
	)
	for _, f := range facts {
		switch {
		case f.Type == bus.TypeAnnounced:
			if first == nil {
				first = f
			}
			pending = append(pending, f)
		case f.Type == bus.TypeProposed:
			pending = append(pending, f)
			pendingBy[f.Node] = true
		case f.Type == bus.TypeAssigned:
			// A new award closes the previous one, which received no terminal event.
			if open != nil {
				open.End, open.Outcome = f.Time, outcomeLeaseExpired
				open = nil
			}
			s := &span{ID: bus.SpanID(f.ID), Parent: f.Parent, Task: task, Assigned: f, Start: f.Time, Events: pending}
			for node := range pendingBy {
				proposedIn[node] = s
			}
			pending, pendingBy = nil, map[string]bool{}
			spans = append(spans, s)
			open = s
		case f.Type == bus.TypeRejected:
			// It answers the proposal: it goes where that one went.
			if s := proposedIn[f.Node]; s != nil && !pendingBy[f.Node] {
				s.Events = append(s.Events, f)
			} else {
				pending = append(pending, f)
			}
		case outcomes[f.Type] != "":
			var held *span
			for _, s := range spans {
				if s.Assigned != nil && s.Assigned.Node == f.Source {
					held = s
				}
			}
			switch {
			case held != nil && !held.closed():
				held.Terminal, held.End, held.Outcome, held.Settle = f, f.Time, outcomes[f.Type], f.Stored
				if held == open {
					open = nil
				}
			case held != nil:
				// The terminal event of an assignee whose span already ended.
				held.Events = append(held.Events, f)
			default:
				// Closed by a source that holds no award: the vacancy (architecture §3.2).
				start := f.Time
				for _, p := range pending {
					if p.Type == bus.TypeAnnounced {
						start = p.Time
						break
					}
				}
				if open != nil {
					open.End, open.Outcome = f.Time, outcomeLeaseExpired
					open = nil
				}
				s := &span{ID: bus.SpanID(f.ID), Parent: f.Parent, Task: task, Terminal: f,
					Start: start, End: f.Time, Outcome: outcomes[f.Type], Settle: f.Stored, Events: pending}
				pending, pendingBy = nil, map[string]bool{}
				spans = append(spans, s)
			}
		}
	}
	// The span of an expired lease waits for its task's close, whose late
	// terminal event it may still receive.
	var final time.Time
	for _, s := range spans {
		if s.Terminal != nil && s.Settle.After(final) {
			final = s.Settle
		}
	}
	for _, s := range spans {
		s.Announcement = first
		if s.Outcome == outcomeLeaseExpired {
			s.Settle = final
		}
	}
	return spans
}

// rootSpan is the trace's root: its span is the one the entry subtask's
// announcement names as its parent, from that announcement to the channel's
// close (architecture §3.2, §3.5.1). Before the close, it is the parent span
// no award of the trace opened.
func rootSpan(traceID string, close *fact, tasks []*span, facts []*fact) *span {
	opened := map[string]bool{}
	for _, s := range tasks {
		opened[s.ID] = true
	}
	id := ""
	if close != nil {
		id = close.Parent
	}
	var first *fact
	for _, f := range facts {
		if f.Type != bus.TypeAnnounced || f.Task == traceID {
			continue
		}
		if (id == "" && !opened[f.Parent]) || f.Parent == id {
			id, first = f.Parent, f
			break
		}
	}
	if id == "" {
		return nil
	}
	s := &span{ID: id, Task: traceID, Root: true}
	if first != nil {
		s.Start = first.Time
		// The trace's workspace and config_version are those of its opening.
		s.Announcement = &fact{ConfigVersion: first.ConfigVersion, Data: factData{Workspace: first.Data.Workspace}}
	}
	if close != nil {
		if first == nil {
			s.Start = close.Time
			s.Announcement = &fact{ConfigVersion: close.ConfigVersion}
		}
		s.Terminal, s.End, s.Outcome, s.Settle = close, close.Time, outcomes[close.Type], close.Stored
	}
	return s
}
