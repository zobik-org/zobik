package contextstore

import (
	"context"
	"errors"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"zobik.org/zobik/internal/rpcauth"
	"zobik.org/zobik/internal/signing"
	contextv1 "zobik.org/zobik/proto/zobik/context/v1"
	nriv1 "zobik.org/zobik/proto/zobik/nri/v1"
)

// chunkSize is the largest chunk a read sends (implementation §3).
const chunkSize = 1 << 20

// Server is the Context Store's service (implementation §1.2.9). Each request
// carries its scope token, and everything the path needs comes from it.
type Server struct {
	contextv1.UnimplementedContextStoreServer
	Store *Store
	// Root is the deployment root every token verifies against (architecture §3.14.3).
	Root string
}

// authorize verifies the token against the root and requires the connection it
// arrived over to be that of the subject it names (architecture §3.14.3).
func (s *Server) authorize(ctx context.Context, token string) (*signing.ScopeToken, error) {
	info, ok := rpcauth.FromContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "unauthenticated connection")
	}
	v, err := signing.VerifyScopeToken(token, s.Root)
	if err != nil {
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}
	if v.Claims.Sub != info.Identity {
		return nil, status.Error(codes.PermissionDenied, "the token is not the connection's")
	}
	return &v.Claims, nil
}

func (s *Server) Read(req *contextv1.StoreRead, stream contextv1.ContextStore_ReadServer) error {
	tok, err := s.authorize(stream.Context(), req.GetToken())
	if err != nil {
		return err
	}
	ref := req.GetRef()
	// A read reaches the whole trace (architecture §3.14.3).
	if ref.GetTraceId() != tok.TraceID {
		return status.Error(codes.PermissionDenied, "the portion is not of the token's trace")
	}
	f, err := s.Store.Read(ref.GetTraceId(), ref.GetTaskId(), ref.GetName())
	if err != nil {
		return storeError(err)
	}
	defer f.Close()
	buf := make([]byte, chunkSize)
	for {
		n, err := f.Read(buf)
		if n > 0 {
			if err := stream.Send(&nriv1.Chunk{Data: buf[:n]}); err != nil {
				return err
			}
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func (s *Server) Write(stream contextv1.ContextStore_WriteServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	header := first.GetHeader()
	if header == nil {
		return status.Error(codes.InvalidArgument, "the first message is the header")
	}
	tok, err := s.authorize(stream.Context(), header.GetToken())
	if err != nil {
		return err
	}
	// A write reaches only the section of the token's TaskID (architecture §3.14.3).
	w, err := s.Store.Create(tok.TraceID, tok.Section, header.GetName())
	if err != nil {
		return storeError(err)
	}
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			w.Abort()
			return err
		}
		if msg.GetHeader() != nil {
			w.Abort()
			return status.Error(codes.InvalidArgument, "a second header")
		}
		if err := w.Write(msg.GetChunk()); err != nil {
			w.Abort()
			return storeError(err)
		}
	}
	if err := w.Commit(); err != nil {
		return storeError(err)
	}
	return stream.SendAndClose(&nriv1.ContextRef{TraceId: tok.TraceID, TaskId: tok.Section, Name: header.GetName()})
}

func (s *Server) List(ctx context.Context, req *contextv1.StoreList) (*nriv1.ContextListing, error) {
	tok, err := s.authorize(ctx, req.GetToken())
	if err != nil {
		return nil, err
	}
	names, err := s.Store.List(tok.TraceID, req.GetTaskId())
	if err != nil {
		return nil, storeError(err)
	}
	out := &nriv1.ContextListing{}
	for _, n := range names {
		out.Refs = append(out.Refs, &nriv1.ContextRef{TraceId: tok.TraceID, TaskId: req.GetTaskId(), Name: n})
	}
	return out, nil
}

func (s *Server) Purge(ctx context.Context, req *contextv1.StorePurge) (*nriv1.PurgeAck, error) {
	tok, err := s.authorize(ctx, req.GetToken())
	if err != nil {
		return nil, err
	}
	// Purging reaches the whole trace, so it requires a root's token, whose
	// section is its TraceID (architecture §3.14.3).
	if tok.Section != tok.TraceID {
		return nil, status.Error(codes.PermissionDenied, "purging requires the root's token")
	}
	if err := s.Store.Purge(tok.TraceID); err != nil {
		return nil, storeError(err)
	}
	return &nriv1.PurgeAck{}, nil
}

func storeError(err error) error {
	switch {
	case errors.Is(err, ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, ErrExhausted):
		return status.Error(codes.ResourceExhausted, err.Error())
	case errors.Is(err, ErrInvalid):
		return status.Error(codes.InvalidArgument, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}
