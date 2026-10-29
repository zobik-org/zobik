package sidecar

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/claim"
	"zobik.org/zobik/internal/egress"
	"zobik.org/zobik/internal/signing"
	nriv1 "zobik.org/zobik/proto/zobik/nri/v1"
)

// The answers of gate_approval (architecture §3.16.4).
const (
	decisionApprove = "approve"
	decisionDeny    = "deny"
)

// gated is a call whose rule ended in an audience: what was validated and
// decided in steps 2 to 4 of architecture §3.16.4.
type gated struct {
	key     string
	entry   *egress.Entry
	call    *egress.Call
	outcome egress.Outcome
	at      time.Time
	version string
}

// gate asks the audience the rule names whether the call runs, and waits for
// its answer while the task's lease is renewed. Once approved, it runs steps 2
// to 4 again against the current registry, and returns the entry and the call
// to execute only if the outcome is allow or the same audience and the
// request's digest is the approved one (architecture §3.16.4, steps 6 and 7).
// The wait ends with the Logic Container's call or with the task.
func (n *node) gate(ctx context.Context, a *claim.Assignment, req *nriv1.EgressRequest, g *gated) (*egress.Entry, *egress.Call, error) {
	audience, err := audienceOf(g.outcome.Then, a)
	if err != nil {
		return nil, nil, err
	}
	approved, err := g.entry.Digest(g.key, g.call)
	if err != nil {
		return nil, nil, status.Error(codes.Internal, err.Error())
	}
	evidence := map[string]any{"trace_id": bus.TraceID(a.Assigned.TraceParent), "task_id": a.TaskID}
	data := map[string]any{
		"kind": "gate_approval", "scope": "call", "since": g.at.Format(time.RFC3339Nano),
		"options": []string{decisionApprove, decisionDeny}, "outcome_if_silent": decisionDeny,
		"evidence": evidence,
		"details": map[string]any{
			"entry": g.key, "rule": g.outcome.Rule, "evaluated_at": g.at.Format(time.RFC3339Nano),
			"config_version": g.version, "call": g.entry.Rendered(g.call), "digest": approved,
			"topic": n.cred.Topic, "node_id": n.id,
		},
	}
	l, err := n.openGate(a.TaskID, signing.HITLPrefix+audience, data)
	if err != nil {
		return nil, nil, err
	}

	var answer *bus.Event
	select {
	case answer = <-l.gate:
	case <-l.stop:
		return nil, nil, status.Error(codes.FailedPrecondition, "the task closed while its call waited for "+audience)
	case <-ctx.Done():
		n.mu.Lock()
		n.forgetLineage(l)
		n.mu.Unlock()
		return nil, nil, status.FromContextError(ctx.Err()).Err()
	}
	if answer.Type != bus.TypeCompleted {
		return nil, nil, status.Errorf(codes.Aborted, "the query to %s ended with %s: the call does not run", audience, answer.Type)
	}
	var d struct {
		Decision string `json:"decision"`
	}
	_ = json.Unmarshal(answer.Data, &d) // a field of another type reads as absent
	switch d.Decision {
	case decisionApprove:
	case decisionDeny:
		return nil, nil, status.Errorf(codes.PermissionDenied, "%s denied the call", audience)
	default:
		// An answer outside options is silence (architecture §3.5.2).
		return nil, nil, status.Errorf(codes.PermissionDenied, "%s answered %q, outside the options: silence is deny", audience, d.Decision)
	}

	// Step 7: the association, the interface and the rules belong to the
	// current class, and the body is the copy that was approved.
	key, e, version, err := n.served(req.GetSlot())
	if err != nil {
		return nil, nil, err
	}
	call, err := e.NewCall(req.GetOperation(), req.GetFields(), g.call.BodyDigest != "")
	if err != nil {
		return nil, nil, status.Error(codes.InvalidArgument, err.Error())
	}
	call.BodySize, call.BodyDigest = g.call.BodySize, g.call.BodyDigest
	if call.BodyDigest != "" && call.BodySize > e.MaxBodySize {
		return nil, nil, status.Errorf(codes.InvalidArgument, "body: %d bytes exceed max_body_size %d", call.BodySize, e.MaxBodySize)
	}
	now := time.Now().UTC()
	o := e.Decide(call, egress.Lists, now)
	if o.Then != egress.Allow {
		again, err := audienceOf(o.Then, a)
		if o.Then == egress.Deny || err != nil || again != audience {
			return nil, nil, status.Errorf(codes.PermissionDenied, "approved by %s, but %s", audience, evaluated(o, now, version))
		}
	}
	if current, err := e.Digest(key, call); err != nil || current != approved {
		return nil, nil, status.Errorf(codes.PermissionDenied, "approved by %s, but the request changed during the wait: its digest is %s, not %s", audience, current, approved)
	}
	return e, call, nil
}

// audienceOf resolves the outcome of a rule to an audience: requester is the
// trace's origin audience (architecture §3.5.3).
func audienceOf(then string, a *claim.Assignment) (string, error) {
	if then != egress.Requester {
		return then, nil
	}
	audience, _ := a.Inherited()["origin_audience"].(string)
	if audience == "" {
		return "", status.Error(codes.FailedPrecondition, "the rule ends in requester and the task's trace has no origin_audience")
	}
	return audience, nil
}

// openGate publishes the gate_approval on topic from the task taskID and
// returns its lineage, whose terminal event the node delivers to the waiting
// call instead of the Logic Container.
func (n *node) openGate(taskID, topic string, data map[string]any) (*lineage, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	t := n.tasks[taskID]
	if t == nil || t.closing {
		return nil, status.Error(codes.FailedPrecondition, "the node holds no task "+taskID+" open")
	}
	for k, v := range t.a.Inherited() {
		data[k] = v
	}
	data["topic"], data["requester_identity"] = topic, n.id
	id := uuid.NewString()
	// The Integration Sidecar validates what it opens itself (implementation §10.2).
	if err := n.emit(t, id, data, false, true); err != nil {
		return nil, err
	}
	l := &lineage{
		parent: taskID, topic: topic, known: id, last: id, attempts: 1, data: data,
		gate: make(chan *bus.Event, 1), stop: make(chan struct{}),
	}
	n.subtasks[id] = l
	return l, nil
}

// endGates ends the waits of the gates opened from the task parent, or of
// every gate when parent is empty: their calls return without executing.
// Called with n.mu held.
func (n *node) endGates(parent string) {
	for _, l := range n.subtasks {
		if l.gate != nil && (parent == "" || l.parent == parent) {
			n.forgetLineage(l)
			close(l.stop)
		}
	}
}

// forgetLineage forgets every attempt of l: a terminal event that arrives
// later reaches no one. Called with n.mu held.
func (n *node) forgetLineage(l *lineage) {
	for id, other := range n.subtasks {
		if other == l {
			delete(n.subtasks, id)
		}
	}
}
