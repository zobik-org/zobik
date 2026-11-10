package claim

import (
	"context"
	"fmt"

	"zobik.org/zobik/internal/bus"
	contextv1 "zobik.org/zobik/proto/zobik/context/v1"
)

// chunkSize bounds each message of a write, well under gRPC's default limit.
const chunkSize = 64 << 10

// Answer is an outcome a structural claimant answers by Claim-Check: it
// validates raw against the schema of its portion, writes it as the portion
// name of the task's section and returns the context_ref its task.completed
// carries (architecture §3.4, implementation §10.1).
func Answer(ctx context.Context, store contextv1.ContextStoreClient, a *Assignment, name, schema string, raw []byte) (map[string]any, error) {
	s, err := bus.Schema(schema)
	if err != nil {
		return nil, err
	}
	if err := s.ValidateJSON(raw); err != nil {
		return nil, fmt.Errorf("the portion %s: %w", name, err)
	}
	if err := WritePortion(ctx, store, a.Token, name, raw); err != nil {
		return nil, err
	}
	return map[string]any{"trace_id": bus.TraceID(a.Announcement.TraceParent), "task_id": a.TaskID, "name": name}, nil
}

// WritePortion writes raw as the portion name of the section the scope token
// names (architecture §3.14.3).
func WritePortion(ctx context.Context, store contextv1.ContextStoreClient, token, name string, raw []byte) error {
	w, err := store.Write(ctx)
	if err != nil {
		return err
	}
	if err := w.Send(&contextv1.StoreWrite{Body: &contextv1.StoreWrite_Header{Header: &contextv1.StoreWriteHeader{Token: token, Name: name}}}); err != nil {
		return err
	}
	for len(raw) > 0 {
		n := min(len(raw), chunkSize)
		if err := w.Send(&contextv1.StoreWrite{Body: &contextv1.StoreWrite_Chunk{Chunk: raw[:n]}}); err != nil {
			return err
		}
		raw = raw[n:]
	}
	_, err = w.CloseAndRecv()
	return err
}
