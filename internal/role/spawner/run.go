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
		rt: rt, js: js, nc: nc, o: o,
		claimants: map[string]*claim.Claimant{},
		acts:      make(chan act, 64),
		coming:    map[string]int{},
		renewed:   map[string]time.Time{},
		pinned:    map[string]int{},
		obs:       newObserver(),
		vacants:   map[string]*claim.Claimant{},
		pending:   map[string]time.Time{},
		niches:    map[string]niche{},
		sweep:     make(chan struct{}, 1),
	}
	for _, topic := range []string{TopicProvision, TopicRetire} {
		s.claimants[topic] = claim.New(rt.ID, rt.Credential, js, nc, topic)
	}
	s.head.Store(head)
	// What it did not observe while down it cannot read elsewhere: every node
	// counts as busy until it has had a lease's time to renew (implementation
	// §1.2.4).
	s.obs.hold(time.Now().Add(s.leaseTTL()))
	leases, err := nc.Subscribe("lease.>", func(m *nats.Msg) { s.obs.renewed(m.Subject, time.Now(), s.leaseTTL()) })
	if err != nil {
		return err
	}
	defer func() { _ = leases.Unsubscribe() }()
	s.loadNiches(ctx)
	go s.serveSweep(ctx)
	go s.serveActs(ctx, rt.Loop("acts", actTimeout+time.Minute))
	go s.serveRenewal(ctx, rt.Loop("renewal", 5*time.Minute))
	rt.SetReady(true)
	role.Consume(ctx, l, consumer, func(m jetstream.Msg) { s.handle(ctx, m) })
	return nil
}

type spawner struct {
	rt *role.Runtime
	js jetstream.JetStream
	nc *nats.Conn
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
	// pinned caches similarity_precision_decimals by config_version, and
	// evictions are the moments of the evictions within eviction_pressure_window.
	pinned    map[string]int
	evictions []time.Time

	// obs is what it derives by observing the stream (architecture §3.13.1).
	obs *observer
	// vacants are its cycles on the vacant tasks, by topic; pending, the
	// vacant tasks it proposed on or is resolving; and niches, the niche
	// records it knows, which the observation resets (architecture §3.13.2).
	vmu     sync.Mutex
	vacants map[string]*claim.Claimant
	pending map[string]time.Time
	niches  map[string]niche
	// sweep asks for the regeneration of every stale vector (architecture §3.13.7).
	sweep chan struct{}
}

// leaseTTL is task_lease_ttl.
func (s *spawner) leaseTTL() time.Duration {
	return s.head.Load().Duration("task_lease_ttl")
}

// act is a request or a vacant task it won, with the claimant of its topic.
type act struct {
	topic  string
	c      *claim.Claimant
	a      *claim.Assignment
	vacant bool
}

// handle observes each event of the task family, proposes on the operator's
// requests and on the vacant tasks, and takes what it is awarded. The routing
// by target_topic is trivial: the single instance owns every topic
// (architecture §3.13.1).
func (s *spawner) handle(ctx context.Context, m jetstream.Msg) {
	var e bus.Event
	if err := json.Unmarshal(m.Data(), &e); err != nil || e.Validate() != nil || e.Subject == "" {
		m.Ack()
		return
	}
	opCtx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	now := time.Now()
	switch e.Type {
	case bus.TypeAnnounced:
		topic := strings.TrimPrefix(m.Subject(), bus.SubjectAnnounced(""))
		if c := s.claimants[topic]; c != nil {
			if err := c.Propose(opCtx, &e, nil); err != nil {
				log.Printf("spawner: proposing on %s: %v", e.Subject, err)
				m.NakWithDelay(time.Second)
				return
			}
			break
		}
		rounds := s.obs.announced(&e, topic, now)
		if err := s.observeVacancy(opCtx, &e, topic, rounds); err != nil {
			log.Printf("spawner: proposing on the vacant %s: %v", e.Subject, err)
		}
	case bus.TypeRejected:
		for _, c := range s.claimants {
			c.Rejected(&e)
		}
		s.vacancyRejected(&e)
	case bus.TypeAssigned:
		assignee := strings.TrimPrefix(m.Subject(), bus.TypeAssigned+".")
		topic := s.obs.assigned(e.Subject, assignee, now, s.leaseTTL())
		if assignee != s.rt.ID {
			if topic != "" {
				s.covered(ctx, topic)
			}
			break
		}
		for topic, c := range s.claimants {
			a, ok := c.Assigned(&e)
			if !ok {
				continue
			}
			select {
			case s.acts <- act{topic: topic, c: c, a: a}:
			default:
				// Without room the lease expires and the request comes back.
				log.Printf("spawner: %s left to its re-announcement: too many acts waiting", a.TaskID)
			}
		}
		s.vacancyAssigned(&e)
	case bus.TypeCompleted, bus.TypeFailed, bus.TypeAborted:
		s.obs.closed(&e)
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
	if x.vacant {
		s.fleet.Lock()
		defer s.fleet.Unlock()
		s.vacancy(ctx, x)
		return
	}
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
	if _, vetoed, err := cat.Record(ctx, s.o.Network, vetoPrefix+entry.ArtifactRef, nil); err != nil {
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
	return s.bringUp(ctx, cat, upRequest{
		entry: *entry, associations: associations, proof: req.OperatorProof, grants: proof.ProvisionGrants,
	})
}

// upRequest is what brings up a node of an entry: the egress slots and the
// proof of an operator's request, which a vacancy does not carry.
type upRequest struct {
	entry        catalog.Entry
	associations map[string]string
	proof        string
	grants       []signing.Association
}

// bringUp provisions a node of the entry, after the validation of whoever
// requests it: the act of architecture §3.13.1, shared by node_provision and
// the vacancy. It writes the presence mark before reading the deletion mark,
// makes room by evicting, and writes the network's row of the entry and, for
// the first node of an emergent topic, the Topic Registry's.
func (s *spawner) bringUp(ctx context.Context, cat Catalog, r upRequest) (*Node, error) {
	entry := &r.entry
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
	entry, ok, err := cat.Entry(ctx, entry.ArtifactRef)
	if err != nil {
		return nil, err
	}
	if !ok || entry.DeletionMark {
		return nil, Refuse(CodeRequestInvalid, "the artifact %s is being deleted", r.entry.ArtifactRef)
	}

	p := Provisioning{
		Topic: entry.Topic, Entry: entry, Associations: r.associations,
		Proof: r.proof, ProvisionGrants: r.grants, MemoryMB: memory.MemoryRequired,
		Floor: floor,
	}
	if err := s.makeRoom(ctx, cat, p); err != nil {
		return nil, err
	}
	node, err := s.provisionRetrying(ctx, p)
	if err != nil {
		return nil, err
	}
	// The network's row of the entry (architecture §3.13.1, §3.13.3).
	if err := cat.PutRecord(ctx, s.o.Network, embeddingPrefix+entry.ArtifactRef,
		vectorRecord{Embedding: node.Embedding, ModelID: node.ModelID}, catalog.Write{}); err != nil {
		return nil, err
	}
	if memory.MemoryRequired == 0 {
		err := cat.PutRecord(ctx, s.o.Network, "memory:"+entry.ArtifactRef,
			map[string]int64{"memory_required": node.MemoryMB}, catalog.Write{IfAbsent: true})
		if err != nil && !errors.Is(err, catalog.ErrPrecondition) {
			return nil, err
		}
	}
	if err := s.registerTopic(ctx, cat, entry.Topic, node, entry.CapabilityText); err != nil {
		log.Printf("spawner: registering the topic %s: %v", entry.Topic, err)
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
