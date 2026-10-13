package sidecar

import (
	"context"
	"errors"
	"io"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	contextv1 "zobik.org/zobik/proto/zobik/context/v1"
	nriv1 "zobik.org/zobik/proto/zobik/nri/v1"
)

// reservedPortions prefixes the portion names the Integration Sidecar writes
// itself (implementation §3, Context Store and egress).
const reservedPortions = "zobik/"

// token is the scope token of a task the node holds (architecture §3.14.3).
func (n *node) token(taskID string) (string, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	t := n.tasks[taskID]
	if t == nil {
		return "", status.Error(codes.FailedPrecondition, "the node holds no task "+taskID)
	}
	return t.a.Token, nil
}

// ReadContext reads a portion of the task's trace with the task's token, and
// returns it as the Context Store sends it.
func (sv *service) ReadContext(req *nriv1.ContextRead, stream nriv1.NodeRuntime_ReadContextServer) error {
	token, err := sv.n.token(req.GetTaskId())
	if err != nil {
		return err
	}
	in, err := sv.store.Read(stream.Context(), &contextv1.StoreRead{Token: token, Ref: req.GetRef()})
	if err != nil {
		return err
	}
	for {
		c, err := in.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(c); err != nil {
			return err
		}
	}
}

// WriteContext writes a portion in the section of the task the first message
// names, with that task's token.
func (sv *service) WriteContext(stream nriv1.NodeRuntime_WriteContextServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	header := first.GetHeader()
	if header == nil {
		return status.Error(codes.InvalidArgument, "the first message is the header")
	}
	if strings.HasPrefix(header.GetName(), reservedPortions) {
		return status.Errorf(codes.InvalidArgument, "the names under %s are the Integration Sidecar's", reservedPortions)
	}
	token, err := sv.n.token(header.GetTaskId())
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	out, err := sv.store.Write(ctx)
	if err != nil {
		return err
	}
	if err := out.Send(&contextv1.StoreWrite{Body: &contextv1.StoreWrite_Header{Header: &contextv1.StoreWriteHeader{
		Token: token, Name: header.GetName(),
	}}}); err != nil {
		return storeSendError(out, err)
	}
	for {
		m, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err // cancelling the write leaves nothing in the Context Store
		}
		if m.GetHeader() != nil {
			return status.Error(codes.InvalidArgument, "a second header")
		}
		if err := out.Send(&contextv1.StoreWrite{Body: &contextv1.StoreWrite_Chunk{Chunk: m.GetChunk()}}); err != nil {
			return storeSendError(out, err)
		}
	}
	ref, err := out.CloseAndRecv()
	if err != nil {
		return err
	}
	return stream.SendAndClose(ref)
}

// storeSendError is the Context Store's answer to a write it ended: a failed
// Send reports only io.EOF, and the status comes on receiving.
func storeSendError(out contextv1.ContextStore_WriteClient, err error) error {
	if errors.Is(err, io.EOF) {
		if _, rerr := out.CloseAndRecv(); rerr != nil {
			return rerr
		}
	}
	return err
}

// ListContext lists the portions of a section of the task's trace.
func (sv *service) ListContext(ctx context.Context, req *nriv1.ContextList) (*nriv1.ContextListing, error) {
	token, err := sv.n.token(req.GetTaskId())
	if err != nil {
		return nil, err
	}
	return sv.store.List(ctx, &contextv1.StoreList{Token: token, TaskId: req.GetSection()})
}
