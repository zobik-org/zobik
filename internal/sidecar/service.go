package sidecar

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/signing"
	contextv1 "zobik.org/zobik/proto/zobik/context/v1"
	nriv1 "zobik.org/zobik/proto/zobik/nri/v1"
)

// session is the Logic Container's Attach (implementation §3, The session).
type session struct {
	out  chan *nriv1.SidecarMessage
	done chan struct{}
	// unbind ends the Attach and unbinds the node from its consumer; the cut
	// calls it.
	unbind context.CancelFunc
}

func (s *session) send(m *nriv1.SidecarMessage) {
	select {
	case s.out <- m:
	case <-s.done:
	}
}

// service is the NodeRuntime service (implementation §3). What enters with a
// later stage answers UNIMPLEMENTED.
type service struct {
	nriv1.UnimplementedNodeRuntimeServer
	n *node
	// store is the Context Store, which the Integration Sidecar reaches on the
	// Logic Container's behalf (architecture §3.6.1).
	store contextv1.ContextStoreClient
	// speaks is the range the binary speaks, and declared the version the
	// Spawner passed in the startup configuration.
	speaks   Range
	declared Version
	// fatal ends the process: a Logic Container whose version it does not
	// serve (implementation §3, The interface version).
	fatal func(error)
}

// Attach opens the session: a Hello of a version it serves is answered with
// Accepted, and only then the node binds to its consumer.
func (sv *service) Attach(stream nriv1.NodeRuntime_AttachServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	hello := first.GetHello()
	if hello == nil {
		return status.Error(codes.InvalidArgument, "the session opens with Hello")
	}
	v := Version{Major: 1, Minor: hello.GetMinor()}
	switch {
	case !sv.speaks.Fits(v):
		err = fmt.Errorf("the Logic Container speaks NRI %s, outside %v", v, sv.speaks)
	case v != sv.declared:
		err = fmt.Errorf("the Logic Container speaks NRI %s, its image declares %s", v, sv.declared)
	}
	if err != nil {
		sv.fatal(err)
		return status.Error(codes.FailedPrecondition, err.Error())
	}

	ctx, unbind := context.WithCancel(stream.Context())
	s := &session{out: make(chan *nriv1.SidecarMessage, 64), done: make(chan struct{}), unbind: unbind}
	if err := sv.n.attach(s); err != nil {
		unbind()
		return err
	}
	defer func() {
		unbind()
		close(s.done)
		sv.n.cut(s)
	}()
	if err := stream.Send(&nriv1.SidecarMessage{Body: &nriv1.SidecarMessage_Accepted{Accepted: &nriv1.Accepted{}}}); err != nil {
		return err
	}
	go sv.n.bind(ctx)

	recvErr := make(chan error, 1)
	go func() {
		for {
			m, err := stream.Recv()
			if err != nil {
				recvErr <- err
				return
			}
			if r := m.GetReceived(); r != nil {
				sv.n.received(r.GetDeliveryId())
			}
		}
	}()
	for {
		select {
		case m := <-s.out:
			if err := stream.Send(m); err != nil {
				return err
			}
		case err := <-recvErr:
			return err
		case <-ctx.Done():
			return status.Error(codes.Unavailable, "the session was cut")
		}
	}
}

// reserved are the keys of data the Integration Sidecar stamps itself, and those
// the request carries as fields of its own (implementation §3, The publication requests).
var reserved = []string{
	"topic", "metrics", "workspace", "origin_audience",
	"current_attempts", "previous_attempt", "final_round", "bodies",
}

// The failure codes a channel closes a query with (architecture §3.5.2).
var channelFailureCodes = map[string]bool{"undelivered": true, "unanswered": true}

// Publish emits what the Logic Container decides on one of its tasks: a
// subtask, a query or the close. It returns once the event is in the outbox
// (implementation §3, The publication requests).
func (sv *service) Publish(ctx context.Context, req *nriv1.PublishRequest) (*nriv1.PublishAck, error) {
	var o *opening
	switch body := req.GetBody().(type) {
	case *nriv1.PublishRequest_Announce:
		o = &opening{topic: body.Announce.GetTopic(), data: body.Announce.GetData(), previous: body.Announce.GetPreviousAttempt()}
	case *nriv1.PublishRequest_Ask:
		o = &opening{ask: true, data: body.Ask.GetData(), previous: body.Ask.GetPreviousAttempt()}
	}
	if o != nil {
		o.taskID = req.GetTaskId()
		taskID, err := sv.n.open(*o)
		if err != nil {
			return nil, err
		}
		return &nriv1.PublishAck{TaskId: taskID}, nil
	}
	t := sv.n.begin(req.GetTaskId())
	if t == nil {
		return nil, status.Error(codes.FailedPrecondition, "the node holds no task "+req.GetTaskId()+" open to close")
	}
	ack, err := sv.close(t, req)
	if err != nil {
		sv.n.abort(t)
	}
	return ack, err
}

func (sv *service) close(t *task, req *nriv1.PublishRequest) (*nriv1.PublishAck, error) {
	var typ string
	var data map[string]any
	var err error
	switch body := req.GetBody().(type) {
	case *nriv1.PublishRequest_Complete:
		typ = bus.TypeCompleted
		if body.Complete.GetDeliveryId() != "" {
			return nil, status.Error(codes.FailedPrecondition, "no Inbound was delivered with "+body.Complete.GetDeliveryId())
		}
		data, err = payload(body.Complete.GetData())
	case *nriv1.PublishRequest_Fail:
		typ = bus.TypeFailed
		data, err = sv.failure(body.Fail)
	default:
		return nil, status.Error(codes.InvalidArgument, "the request names no act")
	}
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	t.calls.Wait()
	e, subject, err := sv.n.claimant.Terminal(t.a, typ, data)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	sent, err := sv.n.out.put(&pending{Subject: subject, Event: e})
	if err != nil {
		return nil, outboxError(err)
	}
	sv.n.closed(t, sent)
	return &nriv1.PublishAck{}, nil
}

// failure is the data of a FailTask. Only a channel's query carries a
// failure_code, undelivered or unanswered; the network stamps the others
// (implementation §3, The traces a channel opens).
func (sv *service) failure(f *nriv1.FailTask) (map[string]any, error) {
	data, err := payload(f.GetData())
	if err != nil {
		return nil, err
	}
	for _, k := range []string{"failure_reason", "failure_code"} {
		if _, ok := data[k]; ok {
			return nil, fmt.Errorf("data carries %s, which goes in its own field", k)
		}
	}
	if f.GetFailureReason() == "" {
		return nil, errors.New("a failure carries its failure_reason")
	}
	data["failure_reason"] = f.GetFailureReason()
	code := f.GetFailureCode()
	switch {
	case sv.n.channel && !channelFailureCodes[code]:
		return nil, fmt.Errorf("a query fails with undelivered or unanswered, not %q", code)
	case !sv.n.channel && code != "":
		return nil, fmt.Errorf("failure_code %q is stamped by the network, not by the node", code)
	}
	if code != "" {
		data["failure_code"] = code
	}
	return data, nil
}

// payload reads the JSON object of a request's data, empty if it brings none,
// and rejects the keys the Integration Sidecar stamps.
func payload(raw []byte) (map[string]any, error) {
	data := map[string]any{}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &data); err != nil || data == nil {
			return nil, errors.New("data is not a JSON object")
		}
	}
	for _, k := range reserved {
		if _, ok := data[k]; ok {
			return nil, fmt.Errorf("data carries %s, which the Integration Sidecar stamps", k)
		}
	}
	return data, nil
}

// credential verifies the node's capability credential against the root.
func credential(token, root string) (*signing.Capability, error) {
	v, err := signing.VerifyCapability(token, root)
	if err != nil {
		return nil, fmt.Errorf("sidecar: credential: %w", err)
	}
	return &v.Claims, nil
}
