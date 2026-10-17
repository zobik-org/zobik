package sidecar

import (
	"encoding/json"
	"errors"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/signing"
	nriv1 "zobik.org/zobik/proto/zobik/nri/v1"
)

// codeStaleVectorSpace is the failure of a subtask left outside the current
// vector space, which the Integration Sidecar reopens itself (architecture §3.3).
const codeStaleVectorSpace = "stale_vector_space"

// lineage is a subtask the node opened from one of its tasks, with its attempts
// (architecture §3.3, §3.6.1).
type lineage struct {
	// parent is the task it was opened from.
	parent string
	topic  string
	ask    bool
	// known is the TaskID the Logic Container names the current attempt by, and
	// last the attempt's own: they differ when the Integration Sidecar reopened
	// it because of a stale space (implementation §3, The publication requests).
	known, last string
	attempts    int
	// data is the last attempt's, before its task_embedding; embed, whether
	// its announcements carry one.
	data  map[string]any
	embed bool
	// terminal is the type of the last attempt's terminal event, once received.
	terminal string
}

// opening is a subtask request: an AnnounceTask, or an AskRequester when ask.
type opening struct {
	taskID, topic string
	data          []byte
	previous      string
	ask           bool
}

// stampedOnAnnouncement are the keys of an announcement's data the Integration
// Sidecar writes itself (implementation §3, The publication requests).
var stampedOnAnnouncement = []string{"requester_identity", "task_embedding", "model_id"}

// open publishes the task.announced of a subtask of one of the node's tasks,
// or of a new attempt at one, and returns the TaskID the Logic Container knows
// it by.
func (n *node) open(o opening) (string, error) {
	data, err := payload(o.data)
	if err != nil {
		return "", status.Error(codes.InvalidArgument, err.Error())
	}
	for _, k := range stampedOnAnnouncement {
		if _, ok := data[k]; ok {
			return "", status.Errorf(codes.InvalidArgument, "data carries %s, which the Integration Sidecar stamps", k)
		}
	}
	text, _ := data["task_text"].(string)
	if !o.ask {
		switch {
		case o.topic == "":
			return "", status.Error(codes.InvalidArgument, "an announcement names its topic")
		// The Logic Container reaches a person only through AskRequester
		// (architecture §3.6.1).
		case strings.HasPrefix(o.topic, signing.HITLPrefix):
			return "", status.Errorf(codes.InvalidArgument, "%s is a topic of the hitl_contact_* family: a query goes through AskRequester", o.topic)
		// Without a task_text there is no task_embedding, and on a topic
		// awarded by similarity nobody could win it (architecture §3.7.4).
		case text == "" && !bus.StructuralTopics[o.topic]:
			return "", status.Error(codes.InvalidArgument, "an announcement carries its task_text")
		}
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	t := n.tasks[o.taskID]
	if t == nil || t.closing {
		return "", status.Error(codes.FailedPrecondition, "the node holds no task "+o.taskID+" open")
	}
	inherited := t.a.Inherited()
	if o.ask {
		audience, _ := inherited["origin_audience"].(string)
		if audience == "" {
			return "", status.Error(codes.FailedPrecondition, "the task's trace has no origin_audience to ask")
		}
		o.topic = signing.HITLPrefix + audience
	}
	var l *lineage
	if o.previous != "" {
		if l, err = n.reopenable(o); err != nil {
			return "", err
		}
		data["current_attempts"] = l.attempts + 1
		data["previous_attempt"] = l.last
	}
	for k, v := range inherited {
		data[k] = v
	}
	data["topic"], data["requester_identity"] = o.topic, n.id

	// The task_embedding is what similarity is measured against, so a topic
	// awarded without it does not wait for one (architecture §3.7.5, step 4).
	embed := text != "" && !o.ask && !bus.StructuralTopics[o.topic]
	taskID := uuid.NewString()
	if err := n.emit(t, taskID, data, embed, !o.ask); err != nil {
		return "", err
	}
	if l == nil {
		l = &lineage{parent: o.taskID, topic: o.topic, ask: o.ask, embed: embed}
	}
	l.known, l.last, l.attempts, l.data, l.terminal = taskID, taskID, l.attempts+1, data, ""
	n.subtasks[taskID] = l
	return taskID, nil
}

// reopenable returns the lineage a reopen continues, if the attempt it names
// meets the conditions of architecture §3.3. Called with n.mu held.
func (n *node) reopenable(o opening) (*lineage, error) {
	l := n.subtasks[o.previous]
	reject := func(why string) (*lineage, error) {
		return nil, status.Errorf(codes.FailedPrecondition, "%s cannot be reopened: %s", o.previous, why)
	}
	switch {
	case l == nil || l.parent != o.taskID:
		return reject("it is not a subtask the node opened from " + o.taskID)
	case l.known != o.previous:
		return reject("it is not the last attempt of its lineage")
	case l.terminal == "":
		return reject("its terminal event has not arrived")
	case l.topic != o.topic:
		return reject("a reopen keeps the topic " + l.topic)
	case l.ask && l.terminal == bus.TypeCompleted:
		return reject("the query was answered")
	}
	return l, nil
}

// emit puts the announcement taskID with data in the outbox, on the trace and
// the configuration of the task t it is opened from. embed asks for its
// task_embedding, which the outbox generates when sending it; schema validates
// the payload against its topic's, which a query of the Logic Container's has
// not: its shape is the blueprint's (architecture §3.5.2).
func (n *node) emit(t *task, taskID string, data map[string]any, embed, schema bool) error {
	e, err := bus.NewEvent(bus.TypeAnnounced, n.id, taskID, data)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	e.TraceParent, e.ConfigVersion = t.a.ChildTraceParent(), t.a.Assigned.ConfigVersion
	if err := e.Validate(); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	// On a fixed-convention topic the payload has its schema (implementation §3).
	if schema {
		if err := e.ValidateTopic(); err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}
	}
	topic, _ := data["topic"].(string)
	if _, err := n.out.put(&pending{Subject: bus.SubjectAnnounced(topic), Event: e, Embed: embed}); err != nil {
		return outboxError(err)
	}
	return nil
}

// onResult takes the terminal event of a subtask the node opened: it reopens
// one left outside the vector space, and delivers any other to the Logic
// Container under the TaskID it knows (architecture §3.3, §3.6.1). It reports
// whether it holds m until the Logic Container confirms it.
func (n *node) onResult(e *bus.Event, m jetstream.Msg) bool {
	n.mu.Lock()
	l := n.subtasks[e.Subject]
	s := n.session
	// A terminal event of an attempt the node no longer waits on, or of a task
	// it no longer holds, has no one to reach.
	if l == nil || l.last != e.Subject || l.terminal != "" || s == nil {
		n.mu.Unlock()
		return false
	}
	var d struct {
		FailureCode string `json:"failure_code"`
	}
	_ = json.Unmarshal(e.Data, &d) // a field of another type reads as absent
	if e.Type == bus.TypeFailed && d.FailureCode == codeStaleVectorSpace {
		err := n.restale(l)
		n.mu.Unlock()
		if err != nil {
			log.Printf("sidecar: reopening %s: %v", e.Subject, err)
			_ = m.NakWithDelay(time.Second) // the Bus delivers it again
			return true
		}
		n.settle(e.ID, m)
		return true
	}
	l.terminal = e.Type
	known := l.known
	n.hold(e.ID, l.parent, m, s.done)
	n.mu.Unlock()
	s.send(&nriv1.SidecarMessage{Body: &nriv1.SidecarMessage_Result{Result: &nriv1.Delivery{
		DeliveryId: e.ID, Event: result(e, known),
	}}})
	return true
}

// restale reopens the last attempt of l with its topic and payload, under the
// current vector space (architecture §3.3). Called with n.mu held.
func (n *node) restale(l *lineage) error {
	t := n.tasks[l.parent]
	if t == nil {
		return nil
	}
	data := map[string]any{}
	for k, v := range l.data {
		data[k] = v
	}
	data["current_attempts"] = l.attempts + 1
	data["previous_attempt"] = l.last
	taskID := uuid.NewString()
	if err := n.emit(t, taskID, data, l.embed, !l.ask); err != nil {
		return err
	}
	n.subtasks[taskID] = l
	l.last, l.attempts, l.data = taskID, l.attempts+1, data
	return nil
}

// result is a terminal event as the Logic Container receives it: without
// source, because the requester does not know who resolved its subtask, and
// on the TaskID it knows (architecture §3.3, implementation §3, The session).
func result(e *bus.Event, known string) []byte {
	c := *e
	c.Subject = known
	var doc map[string]json.RawMessage
	b, _ := json.Marshal(c)
	_ = json.Unmarshal(b, &doc)
	delete(doc, "source")
	out, _ := json.Marshal(doc)
	return out
}

// forgetSubtasks forgets the lineages opened from the task parent, which
// closed. Called with n.mu held.
func (n *node) forgetSubtasks(parent string) {
	for id, l := range n.subtasks {
		if l.parent == parent {
			delete(n.subtasks, id)
		}
	}
}

// outboxError is the status of an event the outbox did not take.
func outboxError(err error) error {
	if errors.Is(err, errTooLarge) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	return status.Error(codes.Unavailable, err.Error())
}
