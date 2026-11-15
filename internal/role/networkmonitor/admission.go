package networkmonitor

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/globalconfig"
)

// admission is the admission_clock: wall-clock time that advances only while
// network_admission = open (architecture §3.12.1). It is a pure function of the
// Config Store's history, which the role reads from its first version and then
// follows, version by version (implementation §1.2.8).
type admission struct {
	// next is the next sequence of the history to read.
	next  uint64
	steps []step
}

// step is a published version: from at, the network is open or not.
type step struct {
	at   time.Time
	open bool
}

// follow reads the versions published since the last read.
func (a *admission) follow(ctx context.Context, js jetstream.JetStream) error {
	s, err := js.Stream(ctx, bus.StreamHistory)
	if err != nil {
		return err
	}
	info, err := s.Info(ctx)
	if err != nil {
		return err
	}
	if a.next == 0 {
		a.next = 1
	}
	for ; a.next <= info.State.LastSeq; a.next++ {
		m, err := s.GetMsg(ctx, a.next)
		if err != nil {
			return fmt.Errorf("history %d: %w", a.next, err)
		}
		var rec globalconfig.Record
		if err := json.Unmarshal(m.Data, &rec); err != nil {
			return fmt.Errorf("history %d: %w", a.next, err)
		}
		var state string
		_ = json.Unmarshal(rec.Values["network_admission"], &state)
		a.add(rec.PublishedAt, state == "open")
	}
	return nil
}

func (a *admission) add(at time.Time, open bool) {
	a.steps = append(a.steps, step{at: at, open: open})
}

// at is the clock's reading at t: the open time of every version up to t.
func (a *admission) at(t time.Time) time.Duration {
	var d time.Duration
	for i, s := range a.steps {
		if !s.open || !s.at.Before(t) {
			continue
		}
		end := t
		if i+1 < len(a.steps) && a.steps[i+1].at.Before(end) {
			end = a.steps[i+1].at
		}
		if end.After(s.at) {
			d += end.Sub(s.at)
		}
	}
	return d
}
