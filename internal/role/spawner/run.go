package spawner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/claim"
	"zobik.org/zobik/internal/globalconfig"
	"zobik.org/zobik/internal/role"
	"zobik.org/zobik/internal/role/catalog"
	"zobik.org/zobik/internal/signing"
)

// opTimeout bounds each read and write on the Bus and on the Catalog.
const opTimeout = 10 * time.Second

// actTimeout bounds one act: pulling the images and the embedding's retries
// (architecture §3.13.8) fit in it.
const actTimeout = 15 * time.Minute

// Options are what the role reaches besides the Bus.
type Options struct {
	// Network is the network's name, the qualifier of its Catalog records.
	Network string
	Driver  Driver
	// Catalog returns the Catalog client, or ErrNoCatalog.
	Catalog func(ctx context.Context) (Catalog, error)
	// IdentityLifetime is how long a node's platform identity is valid; the
	// Spawner renews it at a third of it (implementation §1.2.4).
	IdentityLifetime time.Duration
	Registry         Registry
}

// Run is the spawner role, the Spawner (implementation §1.2.4). It is ready
// once it reads the Global Configuration and binds its consumer. Its single
// instance owns every topic (implementation thesis 3).
func Run(ctx context.Context, rt *role.Runtime, nc *nats.Conn, o Options) error {
	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}
	l := rt.MainLoop()
	head, err := role.AwaitHead(ctx, l, js)
	if err != nil {
		return err
	}
	consumer, err := role.AwaitConsumer(ctx, l, js, rt, "task")
	if err != nil {
		return err
	}
	s := &spawner{
		rt: rt, js: js, o: o,
		claimants: map[string]*claim.Claimant{},
		acts:      make(chan act, 64),
		coming:    map[string]int{},
		renewed:   map[string]time.Time{},
	}
	for _, topic := range []string{TopicProvision, TopicRetire} {
		s.claimants[topic] = claim.New(rt.ID, rt.Credential, js, nc, topic)
	}
	s.head.Store(head)
	go s.serveActs(ctx, rt.Loop("acts", actTimeout+time.Minute))
	go s.serveRenewal(ctx, rt.Loop("renewal", 5*time.Minute))
	rt.SetReady(true)
	role.Consume(ctx, l, consumer, func(m jetstream.Msg) { s.handle(ctx, m) })
	return nil
}

type spawner struct {
	rt *role.Runtime
	js jetstream.JetStream
	o  Options
	// claimants are its cycles on the operator's requests, by topic.
	claimants map[string]*claim.Claimant
	head      atomic.Pointer[globalconfig.Record]
	// acts are the requests it won, which it executes one at a time: admission
	// counts the fleet serially (implementation §1.2.4).
	acts chan act
	// fleet is held by whatever changes the fleet: an act, or the renewal when
	// it archives what is being deleted.
	fleet sync.Mutex
	// coming are the entries whose node an act is bringing up, by digest: the
	// renewal keeps their presence marks although the fleet does not list them yet.
	mu     sync.Mutex
	coming map[string]int
	// renewed is when each node's identity was last renewed; only the renewal
	// reads and writes it.
	renewed map[string]time.Time
}

// act is a request it won, with the claimant of its topic.
type act struct {
	topic string
	c     *claim.Claimant
	a     *claim.Assignment
}

// handle proposes on each announcement and takes what it is awarded. The
// routing by target_topic is trivial: the single instance owns every topic
// (architecture §3.13.1).
func (s *spawner) handle(ctx context.Context, m jetstream.Msg) {
	var e bus.Event
	if err := json.Unmarshal(m.Data(), &e); err != nil || e.Validate() != nil || e.Subject == "" {
		m.Ack()
		return
	}
	opCtx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	switch e.Type {
	case bus.TypeAnnounced:
		c := s.claimants[strings.TrimPrefix(m.Subject(), bus.SubjectAnnounced(""))]
		if c == nil {
			break
		}
		if err := c.Propose(opCtx, &e, nil); err != nil {
			log.Printf("spawner: proposing on %s: %v", e.Subject, err)
			m.NakWithDelay(time.Second)
			return
		}
	case bus.TypeRejected:
		for _, c := range s.claimants {
			c.Rejected(&e)
		}
	case bus.TypeAssigned:
		for topic, c := range s.claimants {
			a, ok := c.Assigned(&e)
			if !ok {
				continue
			}
			select {
			case s.acts <- act{topic, c, a}:
			default:
				// Without room the lease expires and the request comes back.
				log.Printf("spawner: %s left to its re-announcement: too many acts waiting", a.TaskID)
			}
		}
	}
	m.Ack()
}

func (s *spawner) serveActs(ctx context.Context, l *role.Loop) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			l.Mark()
		case a := <-s.acts:
			l.Mark()
			s.execute(ctx, a)
			l.Mark()
		}
	}
}

// outcome is a request's close, which the request's key in the Catalog stamps
// (architecture §3.13.8).
type outcome struct {
	Type string         `json:"type"`
	Data map[string]any `json:"data"`
}

func failed(err error) outcome {
	var r *Refusal
	if !errors.As(err, &r) {
		r = &Refusal{Reason: err.Error()}
	}
	data := map[string]any{"failure_reason": r.Reason}
	if r.Code != "" {
		data["failure_code"] = r.Code
	}
	return outcome{Type: bus.TypeFailed, Data: data}
}

// execute runs a request while it keeps its lease, and closes it. A failure of
// the Bus, of the Catalog or of the engine leaves it without closing: the lease
// expires and the request comes back with its re-announcement.
func (s *spawner) execute(ctx context.Context, x act) {
	a := x.a
	stop := x.c.KeepLease(a, func() time.Duration { return s.head.Load().Duration("task_lease_ttl") })
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, actTimeout)
	defer cancel()
	if head, _, err := globalconfig.Head(ctx, s.js); err == nil {
		s.head.Store(head)
	}

	s.fleet.Lock()
	out, err := s.request(ctx, x.topic, a)
	s.fleet.Unlock()
	if err != nil {
		log.Printf("spawner: %s left to its re-announcement: %v", a.TaskID, err)
		return
	}
	opCtx, cancelOp := context.WithTimeout(ctx, opTimeout)
	defer cancelOp()
	if out.Type == bus.TypeFailed {
		log.Printf("spawner: %s rejected, %v: %v", a.TaskID, out.Data["failure_code"], out.Data["failure_reason"])
	} else {
		log.Printf("spawner: %s closed: %v", a.TaskID, out.Data)
	}
	if err := x.c.Close(opCtx, a, out.Type, out.Data); err != nil {
		log.Printf("spawner: closing %s: %v", a.TaskID, err)
	}
}

// request returns the outcome stamped under the request's key, or executes
// the request and stamps it: the first stamp wins, and a duplicated delivery
// returns it (architecture §3.13.8).
func (s *spawner) request(ctx context.Context, topic string, a *claim.Assignment) (outcome, error) {
	cat, err := s.o.Catalog(ctx)
	if errors.Is(err, ErrNoCatalog) {
		return failed(Refuse(CodeRequestInvalid, "%v", err)), nil
	}
	if err != nil {
		return outcome{}, err
	}
	key := "operator_request:" + a.TaskID
	var stamped outcome
	if _, ok, err := cat.Record(ctx, s.o.Network, key, &stamped); err != nil || ok {
		return stamped, err
	}
	var data map[string]any
	if topic == TopicRetire {
		data, err = s.retire(ctx, cat, a)
	} else {
		data, err = s.provision(ctx, cat, a)
	}
	var r *Refusal
	out := outcome{Type: bus.TypeCompleted, Data: data}
	switch {
	case errors.As(err, &r):
		out = failed(r)
	case err != nil:
		return outcome{}, err
	}
	err = cat.PutRecord(ctx, s.o.Network, key, out, catalog.Write{IfAbsent: true})
	if errors.Is(err, catalog.ErrPrecondition) {
		_, _, err = cat.Record(ctx, s.o.Network, key, &stamped)
		return stamped, err
	}
	return out, err
}

// request is what the Spawner reads of a node_provision.
type request struct {
	TargetTopic        string                `json:"target_topic"`
	ArtifactRef        string                `json:"artifact_ref"`
	EgressAssociations []signing.Association `json:"egress_associations"`
	EntryTopic         string                `json:"entry_topic"`
	OperatorProof      string                `json:"operator_proof"`
}

// provision validates a node_provision in layers—its shape, the proof, the
// entry it names and the network's state—and provisions the node; the close
// names it (architecture §3.13.8). Every layer is a predicate over identifiers.
func (s *spawner) provision(ctx context.Context, cat Catalog, a *claim.Assignment) (map[string]any, error) {
	node, err := s.instantiate(ctx, cat, a)
	if err != nil {
		return nil, err
	}
	return map[string]any{"node_id": node.ID}, nil
}

func (s *spawner) instantiate(ctx context.Context, cat Catalog, a *claim.Assignment) (*Node, error) {
	ann := a.Announcement
	if err := ann.ValidateTopic(); err != nil {
		return nil, Refuse(CodeRequestInvalid, "%v", err)
	}
	var req request
	if err := json.Unmarshal(ann.Data, &req); err != nil {
		return nil, Refuse(CodeRequestInvalid, "data: %v", err)
	}
	proof, err := verifyProof(s.rt.Root, TopicProvision, a.TaskID, ann.Data, req.OperatorProof)
	if err != nil {
		return nil, err
	}
	if proof.ArtifactRef != req.ArtifactRef {
		return nil, Refuse(CodeProofInvalid, "the proof is of artifact_ref %s", proof.ArtifactRef)
	}
	// The channels a node_provision brings up enter with the ingress.
	switch {
	case strings.HasPrefix(req.TargetTopic, signing.HITLPrefix):
		return nil, Refuse(CodeRequestInvalid, "this Spawner does not bring up %s channels", req.TargetTopic)
	case req.EntryTopic != "":
		return nil, Refuse(CodeRequestInvalid, "this Spawner does not grant entry_topic")
	}
	associations := map[string]string{}
	for _, as := range req.EgressAssociations {
		if _, dup := associations[as.Slot]; dup {
			return nil, Refuse(CodeRequestInvalid, "egress_associations names slot %s twice", as.Slot)
		}
		associations[as.Slot] = as.Entry
	}

	entry, ok, err := cat.Entry(ctx, req.ArtifactRef)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, Refuse(CodeRequestInvalid, "the Catalog has no entry %s", req.ArtifactRef)
	}
	// A target_topic other than the entry's sends the request to a partition
	// that does not own the niche.
	if entry.Topic != req.TargetTopic {
		return nil, Refuse(CodeRequestInvalid, "the entry's topic is %s, not %s", entry.Topic, req.TargetTopic)
	}
	if _, vetoed, err := cat.Record(ctx, s.o.Network, "veto:"+entry.ArtifactRef, nil); err != nil {
		return nil, err
	} else if vetoed {
		return nil, Refuse(CodeArtifactVetoed, "the artifact %s is vetoed in this network", entry.ArtifactRef)
	}
	status, err := cat.Status(ctx)
	if err != nil {
		return nil, err
	}
	switch scan := entry.ScanResult; {
	case scan.Verdict != "pass":
		return nil, Refuse(CodeAdmissionFailed, "the Admission Scanner gave %s: %s", scan.Verdict, strings.Join(scan.Findings, ", "))
	case scan.RulesetVersion != status.RulesetVersion:
		return nil, Refuse(CodeRequestInvalid, "the entry's analysis is of an earlier ruleset and is being redone")
	}
	var memory struct {
		MemoryRequired int64 `json:"memory_required"`
	}
	if _, _, err := cat.Record(ctx, s.o.Network, "memory:"+entry.ArtifactRef, &memory); err != nil {
		return nil, err
	}
	floor, err := s.floor(ctx, cat, entry.ArtifactRef)
	if err != nil {
		return nil, err
	}

	// The presence mark goes before the deletion mark is read, and before the
	// node comes up (architecture §3.13.3, §3.13.8).
	defer s.bringingUp(entry.ArtifactRef)()
	if err := s.markPresence(ctx, cat, entry.ArtifactRef); err != nil {
		return nil, err
	}
	if entry, ok, err = cat.Entry(ctx, req.ArtifactRef); err != nil {
		return nil, err
	}
	if !ok || entry.DeletionMark {
		return nil, Refuse(CodeRequestInvalid, "the artifact %s is being deleted", req.ArtifactRef)
	}

	p := Provisioning{
		Topic: entry.Topic, Entry: entry, Associations: associations,
		Proof: req.OperatorProof, ProvisionGrants: proof.ProvisionGrants, MemoryMB: memory.MemoryRequired,
		Floor: floor,
	}
	node, err := s.provisionRetrying(ctx, p)
	if err != nil {
		return nil, err
	}
	// The network's row of the entry (architecture §3.13.1, §3.13.3).
	if err := cat.PutRecord(ctx, s.o.Network, "embedding:"+entry.ArtifactRef,
		map[string]string{"embedding": node.Embedding, "model_id": node.ModelID}, catalog.Write{}); err != nil {
		return nil, err
	}
	if memory.MemoryRequired == 0 {
		err := cat.PutRecord(ctx, s.o.Network, "memory:"+entry.ArtifactRef,
			map[string]int64{"memory_required": node.MemoryMB}, catalog.Write{IfAbsent: true})
		if err != nil && !errors.Is(err, catalog.ErrPrecondition) {
			return nil, err
		}
	}
	return node, nil
}

// provisionRetrying runs the act, and retries a failed embedding generation by
// embedding_regeneration_backoff before closing with it (architecture §3.13.8).
func (s *spawner) provisionRetrying(ctx context.Context, p Provisioning) (*Node, error) {
	var backoff struct {
		Initial  int64 `json:"initial"`
		Attempts int   `json:"attempts"`
	}
	if err := json.Unmarshal(s.head.Load().Values["embedding_regeneration_backoff"], &backoff); err != nil {
		return nil, fmt.Errorf("embedding_regeneration_backoff: %w", err)
	}
	wait := time.Duration(backoff.Initial) * time.Millisecond
	for attempt := 1; ; attempt++ {
		node, err := s.o.Driver.Provision(ctx, p)
		var r *Refusal
		if !errors.As(err, &r) || r.Code != CodeEmbeddingUnavailable || attempt >= backoff.Attempts {
			return node, err
		}
		log.Printf("spawner: generating the embedding of %s, attempt %d: %s", p.Entry.ArtifactRef, attempt, r.Reason)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(wait):
		}
		wait *= 2
	}
}

// bringingUp counts an act bringing up a node of digest, until the returned
// function is called.
func (s *spawner) bringingUp(digest string) func() {
	s.mu.Lock()
	s.coming[digest]++
	s.mu.Unlock()
	return func() {
		s.mu.Lock()
		if s.coming[digest]--; s.coming[digest] == 0 {
			delete(s.coming, digest)
		}
		s.mu.Unlock()
	}
}
