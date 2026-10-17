package tracingcollector

import (
	"fmt"
	"math/rand/v2"
	"sort"
	"strings"
	"testing"
	"time"

	"zobik.org/zobik/internal/bus"
)

// script writes a trace's facts in the order the server stores them.
type script struct {
	trace string
	at    time.Time
	facts []fact
}

func newScript() *script {
	return &script{trace: strings.Repeat("ab", 16), at: time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)}
}

func (s *script) add(typ, task, source, node, parent string, d factData) fact {
	s.at = s.at.Add(time.Second)
	f := fact{ID: fmt.Sprintf("e%d", len(s.facts)), Type: typ, Stored: s.at, Time: s.at, Task: task,
		Source: source, Node: node, Parent: parent, ConfigVersion: "v0", Data: d}
	s.facts = append(s.facts, f)
	return f
}

func str(v string) *string { return &v }

// summary describes the spans by what the export depends on, in a fixed order.
func summary(spans []*span) []string {
	var out []string
	for _, s := range spans {
		var events []string
		for _, e := range s.Events {
			events = append(events, e.Type+"/"+e.Node+e.Source)
		}
		sort.Strings(events)
		out = append(out, fmt.Sprintf("%s task=%s parent=%s root=%v outcome=%s start=%s end=%s settle=%s events=%v",
			s.ID, s.Task, s.Parent, s.Root, s.Outcome, s.Start.Format(time.TimeOnly), s.End.Format(time.TimeOnly),
			s.Settle.Format(time.TimeOnly), events))
	}
	sort.Strings(out)
	return out
}

func byTask(spans []*span, task string) []*span {
	var out []*span
	for _, s := range spans {
		if s.Task == task {
			out = append(out, s)
		}
	}
	return out
}

// The reconstruction of a config_change, a subtask whose lease expired and a
// vacancy depends only on which events were observed, not on the order they
// were read in.
func TestSpansIgnoreArrivalOrder(t *testing.T) {
	s := newScript()
	root := "00000000000000aa"
	ann := factData{Topic: str("config_change"), TaskText: str("set x")}
	s.add(bus.TypeAnnounced, "entry", "UCHANNEL", "", root, ann)
	s.add(bus.TypeProposed, "entry", "UCONFIG", "UCONFIG", root, factData{})
	s.add(bus.TypeProposed, "entry", "UOTHER", "UOTHER", root, factData{})
	won := s.add(bus.TypeAssigned, "entry", "UBROKER", "UCONFIG", root, factData{})
	entrySpan := bus.SpanID(won.ID)
	s.add(bus.TypeNotice, "", "UCONFIG", "", entrySpan, factData{Kind: str("progress")})

	// A subtask whose first assignee lost the lease and closed late.
	s.add(bus.TypeAnnounced, "sub", "UCONFIG", "", entrySpan, factData{Topic: str("echo")})
	first := s.add(bus.TypeAssigned, "sub", "UBROKER", "UC", entrySpan, factData{})
	s.add(bus.TypeAnnounced, "sub", "UBROKER", "", entrySpan, factData{Topic: str("echo")})
	second := s.add(bus.TypeAssigned, "sub", "UBROKER", "UD", entrySpan, factData{})
	done := s.add(bus.TypeCompleted, "sub", "UD", "", entrySpan, factData{})
	s.add(bus.TypeCompleted, "sub", "UC", "", entrySpan, factData{})

	// A vacancy: closed by a source that holds no award.
	vacant := s.add(bus.TypeAnnounced, "vacant", "UCONFIG", "", entrySpan, factData{Topic: str("nobody")})
	closeVacant := s.add(bus.TypeFailed, "vacant", "USPAWNER", "", entrySpan, factData{})

	closeEntry := s.add(bus.TypeCompleted, "entry", "UCONFIG", "", root, factData{})
	// The Task Broker's rejection, stored after a fast close.
	s.add(bus.TypeRejected, "entry", "UBROKER", "UOTHER", root, factData{})
	closeRoot := s.add(bus.TypeCompleted, s.trace, "UCHANNEL", "", root, factData{})

	tr := &trace{ID: s.trace, Facts: s.facts}
	want := summary(tr.spans())
	for i := 0; i < 50; i++ {
		shuffled := append([]fact(nil), s.facts...)
		rand.Shuffle(len(shuffled), func(a, b int) { shuffled[a], shuffled[b] = shuffled[b], shuffled[a] })
		got := summary((&trace{ID: s.trace, Facts: shuffled}).spans())
		if strings.Join(got, "\n") != strings.Join(want, "\n") {
			t.Fatalf("order %d:\n%s\nwant\n%s", i, strings.Join(got, "\n"), strings.Join(want, "\n"))
		}
	}

	spans := tr.spans()
	entry := byTask(spans, "entry")
	if len(entry) != 1 || entry[0].ID != entrySpan || entry[0].Parent != root || entry[0].Outcome != outcomeCompleted ||
		!entry[0].Settle.Equal(closeEntry.Stored) {
		t.Fatalf("entry spans %v", summary(entry))
	}
	// The announcement, both proposals, the rejection and the notice.
	if len(entry[0].Events) != 5 {
		t.Errorf("entry events %v", summary(entry))
	}

	sub := byTask(spans, "sub")
	if len(sub) != 2 {
		t.Fatalf("sub spans %v", summary(sub))
	}
	expired, completed := sub[0], sub[1]
	if expired.ID != bus.SpanID(first.ID) || expired.Outcome != outcomeLeaseExpired || !expired.End.Equal(second.Time) {
		t.Errorf("expired span %v", summary(sub))
	}
	// It waits for its task's close, and keeps the late terminal event.
	if !expired.Settle.Equal(done.Stored) || len(expired.Events) != 2 || expired.Events[1].Source != "UC" {
		t.Errorf("expired span %v", summary(sub))
	}
	if completed.ID != bus.SpanID(second.ID) || completed.Outcome != outcomeCompleted || len(completed.Events) != 1 {
		t.Errorf("second span %v", summary(sub))
	}

	v := byTask(spans, "vacant")
	if len(v) != 1 || v[0].ID != bus.SpanID(closeVacant.ID) || v[0].Outcome != outcomeFailed ||
		!v[0].Start.Equal(vacant.Time) || v[0].Assigned != nil {
		t.Errorf("vacant span %v", summary(v))
	}

	r := byTask(spans, s.trace)
	if len(r) != 1 || !r[0].Root || r[0].ID != root || !r[0].Settle.Equal(closeRoot.Stored) || !r[0].Start.Equal(s.facts[0].Time) {
		t.Errorf("root span %v", summary(r))
	}
}

// Before the channel closes it, the root is the parent no award of the trace opened.
func TestOpenRoot(t *testing.T) {
	s := newScript()
	root := "00000000000000aa"
	s.add(bus.TypeAnnounced, "entry", "UCHANNEL", "", root, factData{Topic: str("intake")})
	won := s.add(bus.TypeAssigned, "entry", "UBROKER", "UN", root, factData{})
	s.add(bus.TypeAnnounced, "sub", "UN", "", bus.SpanID(won.ID), factData{Topic: str("echo")})
	tr := &trace{ID: s.trace, Facts: s.facts}
	r := byTask(tr.spans(), s.trace)
	if len(r) != 1 || r[0].ID != root || r[0].closed() {
		t.Fatalf("root %v", summary(r))
	}
	if !tr.awardOpen() {
		t.Error("the entry's award is open")
	}
}

// A lease that expired before a vacancy close ends at that close, and the
// vacancy's span starts at the announcement after the award.
func TestVacancyAfterExpiredLease(t *testing.T) {
	s := newScript()
	parent := "00000000000000aa"
	s.add(bus.TypeAnnounced, "task", "UREQ", "", parent, factData{Topic: str("echo")})
	won := s.add(bus.TypeAssigned, "task", "UBROKER", "UE", parent, factData{})
	again := s.add(bus.TypeAnnounced, "task", "UBROKER", "", parent, factData{Topic: str("echo")})
	closed := s.add(bus.TypeFailed, "task", "USPAWNER", "", parent, factData{})
	spans := byTask((&trace{ID: s.trace, Facts: s.facts}).spans(), "task")
	if len(spans) != 2 {
		t.Fatalf("spans %v", summary(spans))
	}
	if spans[0].ID != bus.SpanID(won.ID) || spans[0].Outcome != outcomeLeaseExpired || !spans[0].End.Equal(closed.Time) || !spans[0].Settle.Equal(closed.Stored) {
		t.Errorf("expired %v", summary(spans))
	}
	if spans[1].ID != bus.SpanID(closed.ID) || !spans[1].Start.Equal(again.Time) || spans[1].Outcome != outcomeFailed {
		t.Errorf("vacancy %v", summary(spans))
	}
}
