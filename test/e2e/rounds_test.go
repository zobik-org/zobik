//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"zobik.org/zobik/internal/console"
)

// Las rondas de las consultas: the assistant's gate goes out while nobody is
// at the panel. Its round 1 goes vacant and the Spawner closes it; round 2 is
// outside the panel's window and goes vacant too, so the next round is the
// final one, which the panel's channel retains until the person comes back.
// Unanswered there, the gate's silence is deny: the call does not run, and the
// outcome is the task's failure (architecture §3.5.2, §3.13.2).
func TestRounds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	z, p, text := assistantNetwork(t, ctx)
	z.run(t, ctx, credentials, "config", "set", "hitl_response_window=120000")

	var conv console.Conversation
	p.call(t, ctx, http.MethodPost, console.PathConversations, nil, http.StatusCreated, &conv)
	request := p.send(t, ctx, conv.ID, text)
	p.call(t, ctx, http.MethodDelete, console.PathSession, nil, http.StatusNoContent, nil)

	// Each vacant round closes on its first re-announcement, at
	// reannounce_backoff_base: the final round opens well within 40 seconds,
	// and hitl_response_window holds it past them.
	time.Sleep(40 * time.Second)
	user, password, _ := strings.Cut(strings.TrimSuffix(credentials, "\n"), "\n")
	p.call(t, ctx, http.MethodPost, console.PathSession, console.Credentials{Username: user, Password: password}, http.StatusNoContent, nil)
	changed := p.events(t, ctx)

	gate := p.wait(t, ctx, changed, conv.ID, func(m *console.Message) bool {
		return m.Kind == console.KindQuery && m.TraceID == request.TraceID && m.State == console.StateAwaiting
	})
	var d struct {
		Kind       string `json:"kind"`
		Attempts   int    `json:"current_attempts"`
		FinalRound bool   `json:"final_round"`
	}
	if err := json.Unmarshal(gate.Data, &d); err != nil {
		t.Fatalf("the gate's data: %v: %s", err, gate.Data)
	}
	if d.Kind != "gate_approval" || !d.FinalRound || d.Attempts != 3 {
		t.Fatalf("the gate after two vacant rounds: %s", gate.Data)
	}

	// Nobody answers within hitl_response_window: the gate expires and the
	// call does not run.
	p.wait(t, ctx, changed, conv.ID, func(m *console.Message) bool {
		return m.Kind == console.KindQuery && m.TaskID == gate.TaskID && m.State == console.StateExpired
	})
	outcome := p.wait(t, ctx, changed, conv.ID, func(m *console.Message) bool {
		return m.Kind == console.KindOutcome && m.TraceID == request.TraceID && m.State == console.StateAwaiting
	})
	if outcome.Outcome != "task.failed" {
		t.Errorf("the outcome after the gate's silence: %s %q", outcome.Outcome, outcome.FailureReason)
	}
	p.accept(t, ctx, conv.ID, outcome)
}
