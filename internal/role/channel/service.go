package channel

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"zobik.org/zobik/internal/globalconfig"
	"zobik.org/zobik/internal/rpcauth"
	channelv1 "zobik.org/zobik/proto/zobik/channel/v1"
	contextv1 "zobik.org/zobik/proto/zobik/context/v1"
	nriv1 "zobik.org/zobik/proto/zobik/nri/v1"
)

// onlyIdentity admits only the console's platform identity (implementation §6,
// The service between the console and the channels).
func onlyIdentity(console string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		if err := isConsole(ctx, console); err != nil {
			return nil, err
		}
		return h(ctx, req)
	}
}

func onlyIdentityStream(console string) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, h grpc.StreamHandler) error {
		if err := isConsole(ss.Context(), console); err != nil {
			return err
		}
		return h(srv, ss)
	}
}

func isConsole(ctx context.Context, console string) error {
	info, ok := rpcauth.FromContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "unauthenticated connection")
	}
	if info.Identity != console {
		return status.Error(codes.PermissionDenied, "only the console reaches a channel")
	}
	return nil
}

// service is the Channel service the console calls.
type service struct {
	channelv1.UnimplementedChannelServer
	c *channel
}

// Attach is the console's session: the role sends what it retains and what it
// wins, and receives what the panel showed and the expired it confirms.
func (s *service) Attach(stream channelv1.Channel_AttachServer) error {
	sess := s.c.attach()
	defer s.c.detach(sess)
	errs := make(chan error, 1)
	go func() {
		for {
			m, err := stream.Recv()
			if err != nil {
				errs <- err
				return
			}
			switch b := m.Body.(type) {
			case *channelv1.ConsoleMessage_Shown:
				s.c.shown(b.Shown.GetTaskId())
			case *channelv1.ConsoleMessage_Received:
				s.c.received(b.Received.GetDeliveryId())
			}
		}
	}()
	for {
		select {
		case m := <-sess.out:
			if err := stream.Send(m); err != nil {
				return err
			}
		case err := <-errs:
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		case <-stream.Context().Done():
			return nil
		}
	}
}

// Open opens a trace: the payload and the bodies go to the root's section, and
// the entry subtask is announced on the entry the console names.
func (s *service) Open(stream channelv1.Channel_OpenServer) error {
	c := s.c
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	h := first.GetHeader()
	switch {
	case h == nil:
		return status.Error(codes.InvalidArgument, "the first message is the header")
	case !entries[c.audience][h.GetTopic()]:
		return status.Errorf(codes.InvalidArgument, "%q is not an entry of the %s channel", h.GetTopic(), c.audience)
	case signedActs[h.GetTopic()] && h.GetTaskId() == "":
		return status.Error(codes.InvalidArgument, "a signed act carries the TaskID the proof covers")
	}
	ctx, cancel := context.WithTimeout(stream.Context(), time.Minute)
	defer cancel()
	t, err := c.newTrace(h.GetTaskId())
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	var payload *portion
	var bodies []body
	var w *writer
	finish := func() error {
		if w == nil {
			return nil
		}
		p, err := w.close()
		w = nil
		if err != nil {
			return err
		}
		if len(bodies) == 0 {
			payload = p
		} else {
			bodies[len(bodies)-1].portion = p
		}
		return nil
	}
	for {
		m, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		switch part := m.Part.(type) {
		case *channelv1.OpenTrace_Chunk:
			if w == nil {
				name := portionPayload
				if len(bodies) > 0 {
					name = bodyName(len(bodies) - 1)
				}
				if w, err = c.newWriter(ctx, t.token, name); err != nil {
					return status.Error(codes.Unavailable, err.Error())
				}
			}
			if err := w.write(part.Chunk); err != nil {
				return status.Error(codes.Unavailable, err.Error())
			}
		case *channelv1.OpenTrace_Body:
			if err := finish(); err != nil {
				return status.Error(codes.Unavailable, err.Error())
			}
			if len(bodies) > 0 && bodies[len(bodies)-1].portion == nil {
				return status.Error(codes.InvalidArgument, "a body without content")
			}
			bodies = append(bodies, body{name: part.Body.GetName(), mediaType: part.Body.GetMediaType(), context: part.Body.GetContext()})
		default:
			return status.Error(codes.InvalidArgument, "the header goes once, first")
		}
	}
	if err := finish(); err != nil {
		return status.Error(codes.Unavailable, err.Error())
	}
	if len(bodies) > 0 && bodies[len(bodies)-1].portion == nil {
		return status.Error(codes.InvalidArgument, "a body without content")
	}
	err = c.announce(ctx, t, h.GetTopic(), h.GetWorkspace(), h.GetData(), payload, bodies)
	var bad errInvalid
	if errors.As(err, &bad) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if err != nil {
		return status.Error(codes.Unavailable, err.Error())
	}
	return stream.SendAndClose(&nriv1.OpenAck{TraceId: t.id, EntryTaskId: t.entryTaskID})
}

func (s *service) Answer(ctx context.Context, req *channelv1.AnswerRequest) (*channelv1.AnswerAck, error) {
	err := s.c.answer(ctx, req.GetTaskId(), req.GetData())
	switch {
	case errors.Is(err, errNotRetained):
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	case err != nil:
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &channelv1.AnswerAck{}, nil
}

// ReadContext reads with the token of the task it names: a query the role
// retains or a root it opened.
func (s *service) ReadContext(req *nriv1.ContextRead, stream channelv1.Channel_ReadContextServer) error {
	token, ok := s.c.tokenOf(req.GetTaskId())
	if !ok {
		return status.Error(codes.FailedPrecondition, errUnknownTask.Error())
	}
	r, err := s.c.context.Read(stream.Context(), &contextv1.StoreRead{Token: token, Ref: req.GetRef()})
	if err != nil {
		return err
	}
	for {
		chunk, err := r.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(chunk); err != nil {
			return err
		}
	}
}

// Purge deletes a trace the console opened, with its root's token (architecture §3.4).
func (s *service) Purge(ctx context.Context, req *nriv1.PurgeRequest) (*nriv1.PurgeAck, error) {
	token, ok := s.c.rootToken(req.GetTaskId())
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "not a root the role opened")
	}
	if _, err := s.c.context.Purge(ctx, &contextv1.StorePurge{Token: token}); err != nil {
		return nil, err
	}
	return &nriv1.PurgeAck{}, nil
}

// ReadConfig returns the head of the Global Configuration (implementation §6,
// The service between the console and the channels).
func (s *service) ReadConfig(ctx context.Context, _ *channelv1.ReadConfigRequest) (*channelv1.ConfigSnapshot, error) {
	ctx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	head, _, err := globalconfig.Head(ctx, s.c.js)
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	values, err := json.Marshal(head.Values)
	if err != nil {
		return nil, err
	}
	return &channelv1.ConfigSnapshot{
		ConfigVersion: head.ConfigVersion,
		PublishedAt:   head.PublishedAt.UTC().Format(time.RFC3339Nano),
		Values:        values,
	}, nil
}
