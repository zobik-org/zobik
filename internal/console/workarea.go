package console

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"slices"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"zobik.org/zobik/internal/bus"
	channelv1 "zobik.org/zobik/proto/zobik/channel/v1"
	nriv1 "zobik.org/zobik/proto/zobik/nri/v1"
)

// workArea is the Entry Node's interface in the console: the conversations,
// the requests they open on intake, and the outcomes and queries the user
// channel brings to the person (implementation §6, The operator console).
type workArea struct {
	st   *store
	link *link
	// notify passes an event to the open event streams.
	notify func(event string, v any)

	mu      sync.Mutex
	convs   map[string]*Conversation
	byTrace map[string]string // TraceID → conversation
	// deleted are the traces of deleted conversations whose outcome has not
	// arrived yet: the console receives it without showing it and purges the
	// trace again.
	deleted map[string]bool
	// expired are the notices the panel has not shown yet, by delivery_id.
	expired map[string]ExpiredNotice
}

// ExpiredNotice is a query or a delivery the role closed without an answer,
// as the panel shows it on sign-in.
type ExpiredNotice struct {
	DeliveryID     string `json:"delivery_id"`
	ConversationID string `json:"conversation_id"`
	MessageID      string `json:"message_id"`
	Kind           string `json:"kind"`
	FailureCode    string `json:"failure_code"`
}

// The events the work area passes to the event stream.
const (
	eventConversation = "conversation" // a conversation changed: {"id": …}
	eventExpired      = "expired"      // a new expired notice
)

const (
	topicIntake        = "intake"
	kindResultDelivery = "result_delivery"
	// opTimeout bounds each call to the channel.
	opTimeout = 30 * time.Second
	chunkSize = 256 << 10
)

func newWorkArea(st *store, l *link, notify func(string, any)) (*workArea, error) {
	convs, err := st.loadConversations()
	if err != nil {
		return nil, err
	}
	deleted, err := st.loadDeleted()
	if err != nil {
		return nil, err
	}
	w := &workArea{st: st, link: l, notify: notify, convs: convs, byTrace: map[string]string{}, deleted: deleted, expired: map[string]ExpiredNotice{}}
	for _, c := range convs {
		for _, m := range c.Messages {
			if m.Kind == KindRequest {
				w.byTrace[m.TraceID] = c.ID
			}
		}
	}
	return w, nil
}

// receive takes what the user channel sends over the Attach.
func (w *workArea) receive(msg *channelv1.ChannelMessage) {
	if ex := msg.GetExpired(); ex != nil {
		w.onExpired(ex)
		return
	}
	c := msg.GetConsultation()
	var assigned, announced bus.Event
	if c == nil || json.Unmarshal(c.GetEvent(), &assigned) != nil || json.Unmarshal(c.GetAnnouncement(), &announced) != nil {
		return
	}
	var d struct {
		Kind     string          `json:"kind"`
		Options  json.RawMessage `json:"options"`
		Silent   string          `json:"outcome_if_silent"`
		Previous string          `json:"previous_attempt"`
		Final    bool            `json:"final_round"`
		Evidence struct {
			TaskID string `json:"task_id"`
		} `json:"evidence"`
		Details struct {
			Type          string   `json:"type"`
			FailureReason string   `json:"failure_reason"`
			FailureCode   string   `json:"failure_code"`
			ContextRef    *jsonRef `json:"context_ref"`
		} `json:"details"`
	}
	_ = json.Unmarshal(announced.Data, &d) // a field of another type reads as absent
	var options []string
	_ = json.Unmarshal(d.Options, &options) // the panel's convention: a list of strings, or none (implementation §6)
	taskID, traceID := assigned.Subject, bus.TraceID(assigned.TraceParent)

	w.mu.Lock()
	convID, ok := w.byTrace[traceID]
	deleted := w.deleted[traceID]
	w.mu.Unlock()
	if !ok {
		// A conversation the person deleted: its outcome is received without
		// showing it, and its queries are let go.
		if d.Kind == kindResultDelivery {
			go w.dismiss(traceID, taskID, options, d.Silent, deleted)
		} else if deleted {
			go w.release(taskID)
		}
		return
	}
	if d.Kind != kindResultDelivery {
		w.update(convID, func(c *Conversation) {
			m := find(c, func(m *Message) bool {
				return m.Kind == KindQuery && (m.TaskID == taskID || d.Previous != "" && m.TaskID == d.Previous)
			})
			if m == nil {
				m = &Message{ID: uuid.NewString(), Kind: KindQuery, Time: time.Now().UTC(), TraceID: traceID}
				c.Messages = append(c.Messages, m)
			}
			m.TaskID, m.Options, m.Data, m.State, m.Expired, m.Answer = taskID, options, announced.Data, StateAwaiting, "", nil
		})
		return
	}
	// The same outcome comes in a new attempt of its delivery after one that
	// did not reach the person (architecture §3.5.4).
	again := false
	w.update(convID, func(c *Conversation) {
		if m := find(c, func(m *Message) bool { return m.Kind == KindOutcome && m.EntryTaskID == d.Evidence.TaskID }); m != nil {
			m.TaskID, m.Options, m.State, m.Expired, m.Answer, m.FinalRound = taskID, options, StateAwaiting, "", nil, d.Final
			again = true
		}
	})
	if again {
		return
	}
	m := &Message{
		ID: uuid.NewString(), Kind: KindOutcome, Time: time.Now().UTC(), TraceID: traceID,
		EntryTaskID: d.Evidence.TaskID, Outcome: d.Details.Type,
		FailureReason: d.Details.FailureReason, FailureCode: d.Details.FailureCode,
		TaskID: taskID, Options: options, State: StateAwaiting, FinalRound: d.Final,
	}
	if d.Details.ContextRef != nil {
		if err := w.readResult(convID, taskID, d.Details.ContextRef.proto(), m); err != nil {
			// The role sends the delivery again with the next Attach.
			log.Printf("console: reading the result of %s: %v", traceID, err)
			return
		}
	}
	w.update(convID, func(c *Conversation) {
		if find(c, func(o *Message) bool { return o.Kind == KindOutcome && o.EntryTaskID == m.EntryTaskID }) == nil {
			c.Messages = append(c.Messages, m)
		}
	})
}

// jsonRef is a ContextRef as the data carries it, with the proto's field names.
type jsonRef struct {
	TraceID string `json:"trace_id"`
	TaskID  string `json:"task_id"`
	Name    string `json:"name"`
}

func (r *jsonRef) proto() *nriv1.ContextRef {
	return &nriv1.ContextRef{TraceId: r.TraceID, TaskId: r.TaskID, Name: r.Name}
}

// readResult reads the outcome's result with the delivery's token: as text if
// it is UTF-8, and otherwise as an attachment the panel offers to download.
func (w *workArea) readResult(convID, taskID string, ref *nriv1.ContextRef, m *Message) error {
	c, err := w.client()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	stream, err := c.ReadContext(ctx, &nriv1.ContextRead{TaskId: taskID, Ref: ref})
	if err != nil {
		return err
	}
	var b []byte
	for {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		b = append(b, chunk.GetData()...)
	}
	if utf8.Valid(b) {
		m.Text = string(b)
		return nil
	}
	a := Attachment{ID: uuid.NewString(), Name: ref.GetName(), MediaType: "application/octet-stream", Size: int64(len(b))}
	if err := w.st.write(w.st.attachmentName(convID, a.ID), b); err != nil {
		return err
	}
	m.Attachments = []Attachment{a}
	return nil
}

// dismiss receives, without showing it, the outcome of a trace whose
// conversation was deleted, with the decision the delivery fixes for silence,
// and purges the trace again.
func (w *workArea) dismiss(traceID, taskID string, options []string, silent string, deleted bool) {
	c, err := w.client()
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()
	if deleted {
		if _, err := c.Purge(ctx, &nriv1.PurgeRequest{TaskId: traceID}); err != nil {
			log.Printf("console: purging %s: %v", traceID, err)
		}
	}
	data := map[string]any{}
	if options != nil {
		data["decision"] = silent
	}
	raw, _ := json.Marshal(data)
	if _, err := c.Answer(ctx, &channelv1.AnswerRequest{TaskId: taskID, Data: raw}); err != nil {
		log.Printf("console: receiving the outcome of %s: %v", traceID, err)
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.deleted[traceID] {
		delete(w.deleted, traceID)
		if err := w.st.saveDeleted(w.deleted); err != nil {
			log.Printf("console: %v", err)
		}
	}
}

// onExpired marks the query or the delivery the role closed without an answer,
// and keeps the notice until the panel shows it; one of a conversation the
// console does not have is confirmed now.
func (w *workArea) onExpired(ex *channelv1.Expired) {
	var notice *ExpiredNotice
	var convToNotify string
	w.mu.Lock()
	if _, seen := w.expired[ex.GetDeliveryId()]; seen {
		w.mu.Unlock()
		return
	}
	if convID, ok := w.byTrace[ex.GetTraceId()]; ok {
		c := w.convs[convID]
		if m := find(c, func(m *Message) bool { return m.TaskID == ex.GetTaskId() }); m != nil {
			if m.State == StateAwaiting {
				m.State, m.Expired = StateExpired, ex.GetFailureCode()
				c.UpdatedAt = time.Now().UTC()
				if err := w.st.saveConversation(c); err != nil {
					log.Printf("console: %v", err)
				}
			}
			// An outcome that did not reach the person comes back in the next
			// round, so it has no notice; past the final round it does not.
			if m.Kind != KindOutcome || ex.GetFailureCode() != "undelivered" || m.FinalRound {
				notice = &ExpiredNotice{DeliveryID: ex.GetDeliveryId(), ConversationID: convID, MessageID: m.ID, Kind: m.Kind, FailureCode: ex.GetFailureCode()}
				w.expired[notice.DeliveryID] = *notice
			}
			convToNotify = convID
		}
	}
	w.mu.Unlock()
	if convToNotify != "" {
		w.notify(eventConversation, map[string]string{"id": convToNotify})
	}
	if notice == nil {
		w.confirm(ex.GetDeliveryId())
		return
	}
	w.notify(eventExpired, notice)
}

func (w *workArea) confirm(deliveryID string) {
	if err := w.link.send(&channelv1.ConsoleMessage{Body: &channelv1.ConsoleMessage_Received{Received: &nriv1.Received{DeliveryId: deliveryID}}}); err != nil {
		log.Printf("console: confirming the expired %s: %v", deliveryID, err)
	}
}

// update applies f to a conversation, saves it and tells the event streams.
func (w *workArea) update(convID string, f func(c *Conversation)) {
	w.mu.Lock()
	c, ok := w.convs[convID]
	if !ok {
		w.mu.Unlock()
		return
	}
	f(c)
	c.UpdatedAt = time.Now().UTC()
	err := w.st.saveConversation(c)
	w.mu.Unlock()
	if err != nil {
		log.Printf("console: saving the conversation %s: %v", convID, err)
	}
	w.notify(eventConversation, map[string]string{"id": convID})
}

func find(c *Conversation, f func(m *Message) bool) *Message {
	for _, m := range c.Messages {
		if f(m) {
			return m
		}
	}
	return nil
}

func (w *workArea) client() (channelv1.ChannelClient, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return w.link.waitAttached(ctx)
}

// Summary is a conversation in the list.
type Summary struct {
	ID        string    `json:"id"`
	Title     string    `json:"title"`
	UpdatedAt time.Time `json:"updated_at"`
	// Awaiting counts the outcomes and queries waiting for the person.
	Awaiting int `json:"awaiting"`
}

func (w *workArea) list() []Summary {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]Summary, 0, len(w.convs))
	for _, c := range w.convs {
		s := Summary{ID: c.ID, Title: c.Title, UpdatedAt: c.UpdatedAt}
		for _, m := range c.Messages {
			if m.State == StateAwaiting {
				s.Awaiting++
			}
		}
		out = append(out, s)
	}
	slices.SortFunc(out, func(a, b Summary) int { return b.UpdatedAt.Compare(a.UpdatedAt) })
	return out
}

func (w *workArea) create() (*Conversation, error) {
	now := time.Now().UTC()
	c := &Conversation{ID: uuid.NewString(), CreatedAt: now, UpdatedAt: now, Messages: []*Message{}}
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.st.saveConversation(c); err != nil {
		return nil, err
	}
	w.convs[c.ID] = c
	return c, nil
}

// get returns a copy of the conversation, as JSON.
func (w *workArea) get(id string) ([]byte, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	c, ok := w.convs[id]
	if !ok {
		return nil, false
	}
	b, err := json.Marshal(c)
	return b, err == nil
}

// attachment returns an attachment of a conversation with its content.
func (w *workArea) attachment(convID, id string) (*Attachment, []byte, error) {
	w.mu.Lock()
	c, ok := w.convs[convID]
	var a *Attachment
	if ok {
		for _, m := range c.Messages {
			for i := range m.Attachments {
				if m.Attachments[i].ID == id {
					a = &m.Attachments[i]
				}
			}
		}
	}
	w.mu.Unlock()
	if a == nil {
		return nil, nil, errNotFound
	}
	b, err := w.st.read(w.st.attachmentName(convID, id))
	return a, b, err
}

var (
	errNotFound = errors.New("console: no such conversation, message or attachment")
	errNoAnswer = errors.New("console: the answer does not fit the query")
)

// File is an attachment as the person sends it.
type File struct {
	Name, MediaType string
	Content         []byte
}

// send opens a trace on intake with the request and the conversation's
// messages as its context, and adds the request to the conversation
// (implementation §6, The service between the console and the channels).
func (w *workArea) send(ctx context.Context, convID, text string, files []File) (*Message, error) {
	w.mu.Lock()
	c, ok := w.convs[convID]
	var bodies []openBody
	if ok {
		for _, f := range files {
			bodies = append(bodies, openBody{name: f.Name, mediaType: f.MediaType, content: f.Content})
		}
		var err error
		if bodies, err = w.context(c, bodies); err != nil {
			w.mu.Unlock()
			return nil, err
		}
	}
	w.mu.Unlock()
	if !ok {
		return nil, errNotFound
	}
	data, err := json.Marshal(map[string]string{"task_text": text})
	if err != nil {
		return nil, err
	}
	client, err := w.client()
	if err != nil {
		return nil, err
	}
	ack, err := open(ctx, client, &channelv1.OpenHeader{Topic: topicIntake, Data: data}, nil, bodies)
	if err != nil {
		return nil, err
	}
	m := &Message{ID: uuid.NewString(), Kind: KindRequest, Time: time.Now().UTC(), TraceID: ack.GetTraceId(), EntryTaskID: ack.GetEntryTaskId(), Text: text}
	for _, f := range files {
		a := Attachment{ID: uuid.NewString(), Name: f.Name, MediaType: f.MediaType, Size: int64(len(f.Content))}
		if err := w.st.write(w.st.attachmentName(convID, a.ID), f.Content); err != nil {
			return nil, err
		}
		m.Attachments = append(m.Attachments, a)
	}
	w.mu.Lock()
	w.byTrace[m.TraceID] = convID
	w.mu.Unlock()
	w.update(convID, func(c *Conversation) {
		if c.Title == "" {
			c.Title = title(text, files)
		}
		c.Messages = append(c.Messages, m)
	})
	return m, nil
}

func title(text string, files []File) string {
	if text == "" && len(files) > 0 {
		return files[0].Name
	}
	r := []rune(text)
	if len(r) > 80 {
		return string(r[:80]) + "…"
	}
	return string(r)
}

// contextMessage is a previous message as the context carries it: a JSON body
// followed by its attachments (implementation §6, The service between the
// console and the channels).
type contextMessage struct {
	From        string   `json:"from"`
	Text        string   `json:"text"`
	Decision    string   `json:"decision,omitempty"`
	Attachments []string `json:"attachments"`
}

// context appends to bodies the messages the person has in view, from the
// oldest to the newest, each answer right after what it answers; w.mu is held.
func (w *workArea) context(c *Conversation, bodies []openBody) ([]openBody, error) {
	for _, m := range c.Messages {
		cm := contextMessage{Text: m.Text, Attachments: []string{}}
		switch m.Kind {
		case KindRequest:
			cm.From = "person"
		case KindOutcome:
			cm.From = "network"
			if m.Outcome != bus.TypeCompleted {
				cm.Text = m.FailureReason
			}
		case KindQuery:
			cm.From, cm.Text = "network", queryText(m.Data)
		}
		var atts []openBody
		for _, a := range m.Attachments {
			b, err := w.st.read(w.st.attachmentName(c.ID, a.ID))
			if err != nil {
				return nil, err
			}
			cm.Attachments = append(cm.Attachments, a.Name)
			atts = append(atts, openBody{name: a.Name, mediaType: a.MediaType, context: true, content: b})
		}
		var err error
		if bodies, err = appendMessage(bodies, cm); err != nil {
			return nil, err
		}
		bodies = append(bodies, atts...)
		if a, ok := personAnswer(m); ok {
			if bodies, err = appendMessage(bodies, a); err != nil {
				return nil, err
			}
		}
	}
	return bodies, nil
}

func appendMessage(bodies []openBody, cm contextMessage) ([]openBody, error) {
	b, err := json.Marshal(cm)
	if err != nil {
		return nil, err
	}
	return append(bodies, openBody{name: "message.json", mediaType: "application/json", context: true, content: b}), nil
}

// queryText is what a query asks: its task_text, or its data as JSON without
// one (implementation §6, The operator console).
func queryText(data json.RawMessage) string {
	var d struct {
		TaskText *string `json:"task_text"`
	}
	if json.Unmarshal(data, &d) == nil && d.TaskText != nil {
		return *d.TaskText
	}
	return string(data)
}

// personAnswer is the person's answer to an outcome with a choice or to a query.
func personAnswer(m *Message) (contextMessage, bool) {
	if m.State != StateAnswered || m.Kind == KindRequest || m.Kind == KindOutcome && !slices.Contains(m.Options, "accept") {
		return contextMessage{}, false
	}
	var d struct {
		Decision      string `json:"decision"`
		FailureReason string `json:"failure_reason"`
		Answer        string `json:"answer"`
	}
	_ = json.Unmarshal(m.Answer, &d) // the console wrote it
	return contextMessage{From: "person", Decision: d.Decision, Text: d.FailureReason + d.Answer, Attachments: []string{}}, true
}

type openBody struct {
	name, mediaType string
	context         bool
	content         []byte
}

// open streams an Open: the header, the payload and each body, each with at
// least one chunk so that an empty one also has content.
func open(ctx context.Context, c channelv1.ChannelClient, h *channelv1.OpenHeader, payload []byte, bodies []openBody) (*nriv1.OpenAck, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	s, err := c.Open(ctx)
	if err != nil {
		return nil, err
	}
	// A Send the channel ended returns io.EOF, and CloseAndRecv carries its status.
	sendErr := func(m *channelv1.OpenTrace) bool {
		err := s.Send(m)
		return err != nil
	}
	chunks := func(b []byte) bool {
		for first := true; first || len(b) > 0; first = false {
			n := min(len(b), chunkSize)
			if sendErr(&channelv1.OpenTrace{Part: &channelv1.OpenTrace_Chunk{Chunk: b[:n]}}) {
				return true
			}
			b = b[n:]
		}
		return false
	}
	failed := sendErr(&channelv1.OpenTrace{Part: &channelv1.OpenTrace_Header{Header: h}})
	if !failed && payload != nil {
		failed = chunks(payload)
	}
	for _, b := range bodies {
		if failed {
			break
		}
		failed = sendErr(&channelv1.OpenTrace{Part: &channelv1.OpenTrace_Body{Body: &channelv1.Body{Name: b.name, MediaType: b.mediaType, Context: b.context}}}) || chunks(b.content)
	}
	return s.CloseAndRecv()
}

// Answer is what the person answers to an outcome or a query.
type Answer struct {
	TaskID string `json:"task_id"`
	// Decision is one of the options; FailureReason goes with a reject.
	Decision      string `json:"decision,omitempty"`
	FailureReason string `json:"failure_reason,omitempty"`
	// Text is the free-text answer of a query without options.
	Text string `json:"text,omitempty"`
}

// answer closes an outcome's delivery or a query with what the person
// answered (architecture §3.5.2, §3.5.4; implementation §6, The operator console).
func (w *workArea) answer(ctx context.Context, a Answer) error {
	convID, m := w.awaiting(a.TaskID)
	if m == nil {
		return errNotFound
	}
	data := map[string]any{}
	switch {
	case m.Kind == KindQuery && m.Options == nil:
		data["answer"] = a.Text
	case !slices.Contains(m.Options, a.Decision):
		return errNoAnswer
	case a.Decision == "reject" && a.FailureReason == "":
		// A reject without its reason is silence (architecture §3.5.4).
		return fmt.Errorf("%w: a reject carries its reason", errNoAnswer)
	default:
		data["decision"] = a.Decision
		if a.Decision == "reject" {
			data["failure_reason"] = a.FailureReason
		}
	}
	return w.close(ctx, convID, a.TaskID, data)
}

// close answers a task the role retains and records the answer.
func (w *workArea) close(ctx context.Context, convID, taskID string, data map[string]any) error {
	raw, err := json.Marshal(data)
	if err != nil {
		return err
	}
	c, err := w.client()
	if err != nil {
		return err
	}
	if _, err := c.Answer(ctx, &channelv1.AnswerRequest{TaskId: taskID, Data: raw}); err != nil {
		return err
	}
	w.update(convID, func(c *Conversation) {
		if m := find(c, func(m *Message) bool { return m.TaskID == taskID }); m != nil {
			m.State, m.Answer = StateAnswered, raw
		}
	})
	return nil
}

// awaiting finds the message waiting for an answer under taskID.
func (w *workArea) awaiting(taskID string) (string, *Message) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for id, c := range w.convs {
		if m := find(c, func(m *Message) bool { return m.TaskID == taskID && m.State == StateAwaiting }); m != nil {
			cp := *m
			return id, &cp
		}
	}
	return "", nil
}

// shown tells the role the panel showed these tasks to the person. An outcome
// without a choice is delivered by showing it: the console answers it then
// (implementation §6, The service between the console and the channels).
func (w *workArea) shown(ctx context.Context, taskIDs []string) {
	for _, id := range taskIDs {
		convID, m := w.awaiting(id)
		if m == nil {
			continue
		}
		if err := w.link.send(&channelv1.ConsoleMessage{Body: &channelv1.ConsoleMessage_Shown{Shown: &channelv1.Shown{TaskId: id}}}); err != nil {
			log.Printf("console: showing %s: %v", id, err)
			continue
		}
		if m.Kind != KindOutcome {
			continue
		}
		var data map[string]any
		switch {
		case m.Options == nil:
			data = map[string]any{}
		case slices.Equal(m.Options, []string{"received"}):
			data = map[string]any{"decision": "received"}
		default:
			continue
		}
		if err := w.close(ctx, convID, id, data); err != nil {
			log.Printf("console: delivering %s: %v", id, err)
		}
	}
}

// notices returns the expired notices the panel has not shown yet.
func (w *workArea) notices() []ExpiredNotice {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]ExpiredNotice, 0, len(w.expired))
	for _, n := range w.expired {
		out = append(out, n)
	}
	return out
}

// seen confirms the expired notices the panel showed.
func (w *workArea) seen(deliveryIDs []string) {
	for _, id := range deliveryIDs {
		w.mu.Lock()
		_, ok := w.expired[id]
		delete(w.expired, id)
		w.mu.Unlock()
		if ok {
			w.confirm(id)
		}
	}
}

// remove deletes a conversation: it purges each of its traces, remembers those
// whose outcome has not arrived yet, receives without showing the outcomes
// waiting for the person and lets go the queries, so that no trace waits for
// hitl_response_window to close (architecture §3.4; implementation §6, The
// service between the console and the channels).
func (w *workArea) remove(ctx context.Context, id string) error {
	w.mu.Lock()
	c, ok := w.convs[id]
	var traces []string
	var outcomes, queries []Message
	pending := map[string]bool{}
	if ok {
		for _, m := range c.Messages {
			switch {
			case m.Kind == KindRequest:
				traces = append(traces, m.TraceID)
				pending[m.TraceID] = true
			case m.State != StateAwaiting:
			case m.Kind == KindOutcome:
				outcomes = append(outcomes, *m)
			case m.Kind == KindQuery:
				queries = append(queries, *m)
			}
		}
		for _, m := range c.Messages {
			if closesRoot(m) {
				delete(pending, m.TraceID)
			}
		}
	}
	w.mu.Unlock()
	if !ok {
		return errNotFound
	}
	client, err := w.client()
	if err != nil {
		return err
	}
	for _, t := range traces {
		_, err := client.Purge(ctx, &nriv1.PurgeRequest{TaskId: t})
		// A trace past its grace is already gone (architecture §3.4).
		if err != nil && !isPrecondition(err) {
			return err
		}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for t := range pending {
		w.deleted[t] = true
	}
	if err := w.st.saveDeleted(w.deleted); err != nil {
		return err
	}
	for _, t := range traces {
		delete(w.byTrace, t)
	}
	delete(w.convs, id)
	for k, n := range w.expired {
		if n.ConversationID == id {
			delete(w.expired, k)
			go w.confirm(k)
		}
	}
	for _, m := range outcomes {
		go w.dismiss(m.TraceID, m.TaskID, m.Options, silence(m.Options), true) //nolint:gosec // it outlives the request on purpose: the trace closes after the answer to the delete
	}
	for _, m := range queries {
		go w.release(m.TaskID)
	}
	if err := w.st.removeConversation(id); err != nil {
		return err
	}
	go w.notify(eventConversation, map[string]string{"id": id})
	return nil
}

// silence is the decision a delivery applies when nobody answers (architecture §3.5.4).
func silence(options []string) string {
	switch {
	case slices.Contains(options, "accept"):
		return "accept"
	case slices.Contains(options, "received"):
		return "received"
	}
	return ""
}

// release lets go a query the panel will not show again (implementation §6,
// The service between the console and the channels).
func (w *workArea) release(taskID string) {
	if err := w.link.send(&channelv1.ConsoleMessage{Body: &channelv1.ConsoleMessage_Release{Release: &channelv1.Release{TaskId: taskID}}}); err != nil {
		log.Printf("console: letting go %s: %v", taskID, err)
	}
}

// closesRoot says whether the answer to an outcome closed its trace: any answer
// but a reject, or the silence of one that reached the person (architecture §3.5.4).
func closesRoot(m *Message) bool {
	if m.Kind != KindOutcome {
		return false
	}
	switch m.State {
	case StateAnswered:
		var d struct {
			Decision string `json:"decision"`
		}
		_ = json.Unmarshal(m.Answer, &d)
		return d.Decision != "reject"
	case StateExpired:
		return m.Expired == "unanswered"
	}
	return false
}

func isPrecondition(err error) bool { return status.Code(err) == codes.FailedPrecondition }
