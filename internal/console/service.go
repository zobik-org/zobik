package console

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"zobik.org/zobik/internal/bus"
	"zobik.org/zobik/internal/globalconfig"
	channelv1 "zobik.org/zobik/proto/zobik/channel/v1"
)

// Options configure the console's service.
type Options struct {
	// Open opens the keys with the operator's username and password, and fails
	// with other ones.
	Open func(user string, password []byte) (*Keys, error)
	// Channels are the panel's channels the network runs.
	Channels []Channel
	// IdleTimeout is session_idle_timeout (implementation §6, The deployment parameters).
	IdleTimeout time.Duration
	// JWTLifetime bounds the console identity's JWT; the console renews the
	// connections at half of it. Zero is an hour.
	JWTLifetime time.Duration
	// LinkLifetime bounds the single-use link. Zero is five minutes.
	LinkLifetime time.Duration
	// ConversationsDir is where the work area keeps the conversations, which
	// the key a sign-in opens encrypts (implementation §6, The operator console).
	ConversationsDir string
	// TracingAddr is the address of the tracing backend's query, whose UI the
	// console serves behind the session (implementation §1.2.17); empty is none.
	TracingAddr string
	// WriteSecret sets the value behind a handle in the Secret Store, or removes
	// it with a nil value: an act of deployment (implementation §1.2.18).
	WriteSecret func(ctx context.Context, handle string, value []byte) error
	// WriteRates writes a rate table, already validated, to the rates bucket
	// with the console's identity for the act in creds: an act of deployment
	// (implementation §8.5).
	WriteRates func(ctx context.Context, creds []byte, name string, table []byte) error
}

// Service is the console's local API: the session, and the acts the panel and
// the command line perform over it (implementation §6, The operator console).
type Service struct {
	o   Options
	hub hub

	mu       sync.Mutex
	sessions map[string]*session
	links    map[string]oneTime
	// While some session is open: the keys, the links with the channels, and
	// the deliveries already passed on, by TaskID.
	keys    *Keys
	stop    context.CancelFunc
	channel map[string]*link
	seen    map[string]bool
	work    *workArea
	traces  *traceProxy
	// tracePort is the port where ServeTraces serves the tracing backend's UI.
	tracePort string
	// secretsMu holds the Secret Store's writes one at a time.
	secretsMu sync.Mutex
}

type session struct{ last time.Time }

type oneTime struct {
	session string
	expires time.Time
}

// New returns the service, with no session open.
func New(o Options) *Service {
	if o.JWTLifetime == 0 {
		o.JWTLifetime = time.Hour
	}
	if o.LinkLifetime == 0 {
		o.LinkLifetime = 5 * time.Minute
	}
	return &Service{o: o, sessions: map[string]*session{}, links: map[string]oneTime{}}
}

// ErrSignIn is a username or a password that does not open the root.
var ErrSignIn = errors.New("console: wrong username or password")

// signIn opens a session: the first one opens the keys and the links with the
// channels, and the following ones verify the credentials the same way.
func (s *Service) signIn(user string, password []byte) (string, error) {
	keys, err := s.o.Open(user, password)
	if err != nil {
		return "", ErrSignIn
	}
	token := randomToken()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.keys != nil {
		keys.Close()
	} else {
		s.unlock(keys)
	}
	s.sessions[token] = &session{last: time.Now()}
	return token, nil
}

// unlock keeps the keys and attaches to each channel; s.mu is held.
func (s *Service) unlock(keys *Keys) {
	ctx, stop := context.WithCancel(context.Background())
	s.keys, s.stop = keys, stop
	s.channel, s.seen = map[string]*link{}, map[string]bool{}
	for _, ch := range s.o.Channels {
		s.channel[ch.Audience] = &link{ch: ch, keys: keys, lifetime: s.o.JWTLifetime, receive: s.receive}
	}
	if l := s.channel[AudienceUser]; l != nil && s.o.ConversationsDir != "" && keys.Conversations != nil {
		w, err := s.openWorkArea(l, keys.Conversations)
		if err != nil {
			log.Printf("console: opening the conversations: %v", err)
		}
		s.work = w
	}
	if keys.Tracing != nil && s.o.TracingAddr != "" {
		s.traces = newTraceProxy(s.o.TracingAddr, keys.Tracing)
	}
	for _, l := range s.channel {
		go l.run(ctx)
	}
}

func (s *Service) openWorkArea(l *link, key []byte) (*workArea, error) {
	st, err := openStore(s.o.ConversationsDir, key)
	if err != nil {
		return nil, err
	}
	return newWorkArea(st, l, s.hub.notify)
}

// signOut closes a session, and with the last one discards the keys.
func (s *Service) signOut(token string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, token)
	s.lockIfIdle()
}

// lockIfIdle discards the keys and detaches from the channels when no session
// is open; s.mu is held.
func (s *Service) lockIfIdle() {
	if len(s.sessions) > 0 || s.keys == nil {
		return
	}
	s.stop()
	if s.traces != nil {
		s.traces.close()
	}
	s.keys.Close()
	s.keys, s.stop, s.channel, s.seen, s.work, s.traces = nil, nil, nil, nil, nil, nil
	s.hub.reset()
}

// touch marks activity in a session, and says whether it is open.
func (s *Service) touch(token string, activity bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	ss, ok := s.sessions[token]
	if ok && activity {
		ss.last = time.Now()
	}
	return ok
}

// expire closes the sessions without activity for IdleTimeout.
func (s *Service) expire(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for token, ss := range s.sessions {
		if now.Sub(ss.last) >= s.o.IdleTimeout {
			delete(s.sessions, token)
		}
	}
	for token, l := range s.links {
		if now.After(l.expires) {
			delete(s.links, token)
		}
	}
	s.lockIfIdle()
}

// Link signs in and returns the path of a single-use link that leaves the
// browser in that session (implementation §6, Installation and zobik init).
func (s *Service) Link(user string, password []byte) (string, error) {
	session, err := s.signIn(user, password)
	if err != nil {
		return "", err
	}
	token := randomToken()
	s.mu.Lock()
	s.links[token] = oneTime{session: session, expires: time.Now().Add(s.o.LinkLifetime)}
	s.mu.Unlock()
	return pathSignIn + "?token=" + token, nil
}

// redeem spends a single-use link and returns its session.
func (s *Service) redeem(token string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	l, ok := s.links[token]
	delete(s.links, token)
	if !ok || time.Now().After(l.expires) {
		return "", false
	}
	ss, ok := s.sessions[l.session]
	if ok {
		ss.last = time.Now()
	}
	return l.session, ok
}

// Serve serves the API on ln until ctx ends.
func (s *Service) Serve(ctx context.Context, ln net.Listener) error {
	_, port, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		return err
	}
	srv := &http.Server{Handler: s.Handler(port), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		srv.Close()
	}()
	go func() {
		every := max(s.o.IdleTimeout/10, 100*time.Millisecond)
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-t.C:
				s.expire(now)
			}
		}
	}()
	err = srv.Serve(ln)
	s.mu.Lock()
	clear(s.sessions)
	s.lockIfIdle()
	s.mu.Unlock()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// receive takes what a channel sends over the Attach. The deliveries of an
// outcome go to the event stream, and the console acks each one with received
// once some stream has it: showing the outcome is delivering it
// (implementation §6, The service between the console and the channels).
func (s *Service) receive(l *link, m *channelv1.ChannelMessage) {
	if l.ch.Audience == AudienceUser {
		if w := s.workArea(); w != nil {
			w.receive(m)
		}
		return
	}
	c := m.GetConsultation()
	if c == nil {
		// An expired is confirmed once the panel shows it.
		return
	}
	var assigned, announced bus.Event
	if json.Unmarshal(c.GetEvent(), &assigned) != nil || json.Unmarshal(c.GetAnnouncement(), &announced) != nil {
		return
	}
	var d struct {
		Kind     string `json:"kind"`
		Evidence struct {
			TaskID string `json:"task_id"`
		} `json:"evidence"`
		Details json.RawMessage `json:"details"`
	}
	if json.Unmarshal(announced.Data, &d) != nil || d.Kind != "result_delivery" {
		return
	}
	// A query comes again with each Attach; the console recognizes it by its TaskID.
	taskID := assigned.Subject
	s.mu.Lock()
	if s.seen == nil || s.seen[taskID] {
		s.mu.Unlock()
		return
	}
	s.seen[taskID] = true
	s.mu.Unlock()
	s.hub.outcome(Outcome{
		Channel: l.ch.Audience, TraceID: bus.TraceID(assigned.TraceParent),
		EntryTaskID: d.Evidence.TaskID, Details: d.Details,
	}, func() { s.ack(l, taskID) })
}

// ack answers a delivery with received. One that fails is passed on again when
// the channel sends it again.
func (s *Service) ack(l *link, taskID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := l.send(&channelv1.ConsoleMessage{Body: &channelv1.ConsoleMessage_Shown{Shown: &channelv1.Shown{TaskId: taskID}}})
	if err == nil {
		var c channelv1.ChannelClient
		if c, err = l.client(); err == nil {
			_, err = c.Answer(ctx, &channelv1.AnswerRequest{TaskId: taskID, Data: []byte(`{"decision":"received"}`)})
		}
	}
	if err != nil {
		log.Printf("console: acking the delivery %s: %v", taskID, err)
		s.mu.Lock()
		if s.seen != nil {
			delete(s.seen, taskID)
		}
		s.mu.Unlock()
	}
}

// workArea returns the work area while some session is open.
func (s *Service) workArea() *workArea {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.work
}

// operator returns the console's half of the Operator Channel, once it holds
// the session with the channel.
func (s *Service) operator(ctx context.Context) (*Operator, error) {
	s.mu.Lock()
	l, keys := s.channel[AudienceOperator], s.keys
	s.mu.Unlock()
	if l == nil {
		return nil, errDetached
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	c, err := l.waitAttached(ctx)
	if err != nil {
		return nil, err
	}
	return &Operator{Root: keys.Root, ChannelID: l.ch.ID, Client: c}, nil
}

// The audiences of the panel's channels (implementation §6, The operator console).
const (
	AudienceOperator = "operator"
	AudienceUser     = "user"
)

// The fields of the API that carry a ConfigSet.
type configEdit struct {
	// BaseVersion is the version edited on; empty is the head.
	BaseVersion string                     `json:"base_version"`
	Assignments map[string]json.RawMessage `json:"assignments"`
}

// SnapshotJSON is the head as the API returns it.
type SnapshotJSON struct {
	ConfigVersion string              `json:"config_version"`
	PublishedAt   string              `json:"published_at"`
	Values        globalconfig.Values `json:"values"`
}

func randomToken() string {
	b := make([]byte, 32)
	rand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)
}
