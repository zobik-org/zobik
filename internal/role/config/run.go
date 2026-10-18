package config

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/claim"
	"zobik.org/zobik/internal/configschema"
	"zobik.org/zobik/internal/globalconfig"
	"zobik.org/zobik/internal/role"
	contextv1 "zobik.org/zobik/proto/zobik/context/v1"
	nriv1 "zobik.org/zobik/proto/zobik/nri/v1"
)

// Topic is the fixed-convention topic the Config Store claims (architecture §3.7.7).
const Topic = "config_change"

// opTimeout bounds each read and write on the Bus and on the Context Store.
const opTimeout = 5 * time.Second

// Options are where the role reaches what it reads besides the Bus.
type Options struct {
	// ContextAddr is the Context Store's address (implementation §6, The Docker network).
	ContextAddr string
}

// Run is the config role, the Config Store (implementation §1.2.5). It is ready
// once it reads a current version of the Global Configuration and binds its
// consumer (implementation §6, The health of the roles).
func Run(ctx context.Context, rt *role.Runtime, nc *nats.Conn, o Options) error {
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	schema, err := configschema.Load()
	if err != nil {
		return err
	}
	l := rt.MainLoop()
	// A publication that stopped between the history and the head is finished
	// before serving: the history is the record.
	var head *globalconfig.Record
	if err := role.Retry(ctx, l, opTimeout, func(ctx context.Context) error {
		head, _, err = globalconfig.RollForward(ctx, js)
		return err
	}); err != nil {
		return err
	}
	consumer, err := role.AwaitConsumer(ctx, l, js, rt, "task")
	if err != nil {
		return err
	}
	conn, err := grpc.NewClient(o.ContextAddr, grpc.WithTransportCredentials(rt.RPCCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()

	s := &store{
		rt: rt, js: js, schema: schema,
		claimant: claim.New(rt.ID, rt.Credential, js, nc, Topic),
		context:  contextv1.NewContextStoreClient(conn),
	}
	s.leaseTTL.Store(int64(head.Duration("task_lease_ttl")))
	rt.SetReady(true)
	return s.serve(ctx, l, consumer)
}

type store struct {
	rt       *role.Runtime
	js       jetstream.JetStream
	schema   *globalconfig.Schema
	claimant *claim.Claimant
	context  contextv1.ContextStoreClient
	// leaseTTL is task_lease_ttl as of the last head read, in nanoseconds.
	leaseTTL atomic.Int64
}

// serve pulls the announcements of config_change and the answers to its
// proposals. Edits are applied one at a time, because each one is judged
// against the head the previous one left.
func (s *store) serve(ctx context.Context, l *role.Loop, c jetstream.Consumer) error {
	role.Consume(ctx, l, c, func(m jetstream.Msg) { s.handle(ctx, m) })
	return nil
}

func (s *store) handle(ctx context.Context, m jetstream.Msg) {
	var e bus.Event
	if err := json.Unmarshal(m.Data(), &e); err != nil || e.Validate() != nil || e.Subject == "" {
		m.Ack()
		return
	}
	opCtx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	switch e.Type {
	case bus.TypeAnnounced:
		// Deterministic routing: the only instance always proposes (architecture §3.7.7).
		if err := s.claimant.Propose(opCtx, &e, nil); err != nil {
			log.Printf("config: proposing on %s: %v", e.Subject, err)
			m.NakWithDelay(time.Second)
			return
		}
	case bus.TypeRejected:
		s.claimant.Rejected(&e)
	case bus.TypeAssigned:
		if a, ok := s.claimant.Assigned(&e); ok {
			s.apply(ctx, a)
		}
	}
	m.Ack()
}

// apply runs an edit while it keeps its lease, and closes it. A failure of the
// Bus or of the Context Store leaves the task without closing: the lease
// expires and the task comes back with its re-announcement.
func (s *store) apply(ctx context.Context, a *claim.Assignment) {
	stop := s.claimant.KeepLease(a, func() time.Duration { return time.Duration(s.leaseTTL.Load()) })
	defer stop()
	version, rej, err := s.validateAndPublish(ctx, a)
	opCtx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	switch {
	case err != nil:
		log.Printf("config: %s left to its re-announcement: %v", a.TaskID, err)
		return
	case rej != nil:
		log.Printf("config: %s rejected, %s: %s", a.TaskID, rej.code, rej.reason)
		err = s.claimant.Fail(opCtx, a, rej.code, rej.reason)
	default:
		log.Printf("config: %s published %s", a.TaskID, version)
		err = s.claimant.Complete(opCtx, a, map[string]any{"config_version": version})
	}
	if err != nil {
		log.Printf("config: closing %s: %v", a.TaskID, err)
	}
}

// rejection is a request the Store closes with task.failed (architecture §3.14.4).
type rejection struct{ code, reason string }

// The failure codes of config_change (implementation §10.1).
const (
	codeProofInvalid     = "proof_invalid"
	codeBaseVersionStale = "base_version_stale"
	codeRequestInvalid   = "request_invalid"
)

func reject(code, format string, a ...any) *rejection {
	return &rejection{code: code, reason: fmt.Sprintf(format, a...)}
}

// announcementData is what the Store reads from the announcement.
type announcementData struct {
	ContextRef struct {
		TraceID string `json:"trace_id"`
		TaskID  string `json:"task_id"`
		Name    string `json:"name"`
	} `json:"context_ref"`
	OperatorProof string `json:"operator_proof"`
}

// payload is the edit, by Claim-Check (architecture §3.14.4).
type payload struct {
	BaseVersion string                     `json:"base_version"`
	Assignments map[string]json.RawMessage `json:"assignments"`
}

// validateAndPublish validates in layers—the request's shape, the proof, the
// payload, base_version against the head and the assignments against the
// schema—and publishes the resulting snapshot (architecture §3.14.4).
func (s *store) validateAndPublish(ctx context.Context, a *claim.Assignment) (string, *rejection, error) {
	ann := a.Announcement
	if err := ann.ValidateTopic(); err != nil {
		return "", reject(codeRequestInvalid, "%v", err), nil
	}
	var d announcementData
	if err := json.Unmarshal(ann.Data, &d); err != nil {
		return "", reject(codeRequestInvalid, "data: %v", err), nil
	}
	proof, rej := verifyProof(s.rt.Root, a.TaskID, ann.Data, d.OperatorProof)
	if rej != nil {
		return "", rej, nil
	}

	ref := &nriv1.ContextRef{TraceId: d.ContextRef.TraceID, TaskId: d.ContextRef.TaskID, Name: d.ContextRef.Name}
	raw, err := s.read(ctx, a.Token, ref)
	if code := status.Code(err); code == codes.NotFound || code == codes.PermissionDenied || code == codes.InvalidArgument || errors.Is(err, errTooLarge) {
		return "", reject(codeRequestInvalid, "the edit in context_ref: %v", err), nil
	}
	if err != nil {
		return "", nil, fmt.Errorf("reading the edit: %w", err)
	}
	if rej := checkPayloadDigest(proof, raw); rej != nil {
		return "", rej, nil
	}
	payloadSchema, err := bus.Schema("config_change.payload.schema.json")
	if err != nil {
		return "", nil, err
	}
	if err := payloadSchema.ValidateJSON(raw); err != nil {
		return "", reject(codeRequestInvalid, "the edit: %v", err), nil
	}
	var p payload
	if err := json.Unmarshal(raw, &p); err != nil {
		return "", reject(codeRequestInvalid, "the edit: %v", err), nil
	}

	// A head that moves between the read and the CAS is read again.
	for {
		opCtx, cancel := context.WithTimeout(ctx, opTimeout)
		version, rej, err := s.publish(opCtx, a.TaskID, p)
		cancel()
		if !errors.Is(err, globalconfig.ErrHeadMoved) || ctx.Err() != nil {
			return version, rej, err
		}
	}
}

// publish judges the edit against the head and publishes it.
func (s *store) publish(ctx context.Context, taskID string, p payload) (string, *rejection, error) {
	head, rev, err := globalconfig.RollForward(ctx, s.js)
	if err != nil {
		return "", nil, err
	}
	s.leaseTTL.Store(int64(head.Duration("task_lease_ttl")))
	// The edit is already the head: a previous attempt of this same task
	// published it and did not get to close.
	if head.TaskID == taskID {
		return head.ConfigVersion, nil, nil
	}
	if p.BaseVersion != head.ConfigVersion {
		return "", reject(codeBaseVersionStale, "base_version %s is not the head, %s", p.BaseVersion, head.ConfigVersion), nil
	}
	values, err := s.schema.ApplyVersioned(head.Values, p.Assignments)
	var invalid *globalconfig.InvalidError
	if errors.As(err, &invalid) {
		return "", reject(codeRequestInvalid, "%v", invalid), nil
	}
	if err != nil {
		return "", nil, err
	}
	version, err := globalconfig.NextVersion(head.ConfigVersion)
	if err != nil {
		return "", nil, err
	}
	err = globalconfig.Publish(ctx, s.js, rev, globalconfig.Record{
		ConfigVersion: version,
		PublishedAt:   time.Now().UTC(),
		AuthorizedBy:  globalconfig.AuthorizedByOperator,
		TaskID:        taskID,
		Values:        values,
	})
	return version, nil, err
}

// maxEdit bounds what the Store reads of an edit: assignments over the keys of
// the schema fit in far less.
const maxEdit = 1 << 20

var errTooLarge = errors.New("the edit exceeds 1 MiB")

// read reads a portion from the Context Store with the task's scope token.
func (s *store) read(ctx context.Context, token string, ref *nriv1.ContextRef) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	stream, err := s.context.Read(ctx, &contextv1.StoreRead{Token: token, Ref: ref})
	if err != nil {
		return nil, err
	}
	var out []byte
	for {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		out = append(out, chunk.GetData()...)
		if len(out) > maxEdit {
			return nil, errTooLarge
		}
	}
}
