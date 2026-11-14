package console

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"slices"
	"sort"
	"sync"
	"time"

	"zobik.org/zobik/internal/bus"
	channelv1 "zobik.org/zobik/proto/zobik/channel/v1"
	nriv1 "zobik.org/zobik/proto/zobik/nri/v1"
)

// The paths of the queries of the operator audience: GET lists them with the
// expired notices, and the others answer, mark as shown and confirm
// (implementation §6, The operator console).
const (
	PathOperatorQueries = "/api/operator/queries"
	PathOperatorAnswers = "/api/operator/answers"
	PathOperatorShown   = "/api/operator/shown"
	PathOperatorExpired = "/api/operator/expired"
)

// eventOperator tells the panel the operator's queries changed.
const eventOperator = "operator"

// OperatorQuery is a query the Operator Channel retains for the person. The
// panel renders every kind with one renderer: the enumeration, the facts that
// support it and the element chosen (architecture §3.17.1).
type OperatorQuery struct {
	TaskID  string    `json:"task_id"`
	TraceID string    `json:"trace_id"`
	Kind    string    `json:"kind,omitempty"`
	Options []string  `json:"options,omitempty"`
	Silent  string    `json:"outcome_if_silent,omitempty"`
	Final   bool      `json:"final_round,omitempty"`
	Arrived time.Time `json:"arrived"`
	// Data is the query's data as it arrived.
	Data json.RawMessage `json:"data"`
}

// OperatorExpired is a query the Operator Channel closed without an answer.
type OperatorExpired struct {
	DeliveryID  string `json:"delivery_id"`
	TaskID      string `json:"task_id"`
	TraceID     string `json:"trace_id"`
	FailureCode string `json:"failure_code"`
	// Kind is the query's, when the console saw it before it expired.
	Kind string `json:"kind,omitempty"`
}

// OperatorQueries is the answer of a GET to PathOperatorQueries.
type OperatorQueries struct {
	Queries []OperatorQuery   `json:"queries"`
	Expired []OperatorExpired `json:"expired"`
}

// OperatorAnswer is the body of a POST to PathOperatorAnswers: one of the
// query's options, or the text of a query without options.
type OperatorAnswer struct {
	TaskID   string `json:"task_id"`
	Decision string `json:"decision,omitempty"`
	Text     string `json:"text,omitempty"`
}

// operatorArea holds, while some session is open, what the Operator Channel
// sends that is not the outcome of an act. The channel retains each query and
// sends it again with each Attach, so nothing of it is saved.
type operatorArea struct {
	link   *link
	notify func(event string, v any)

	mu      sync.Mutex
	queries map[string]*OperatorQuery
	expired map[string]OperatorExpired
	// deliveries are the outcome deliveries the channel sent, by TaskID, each
	// with whether it is in the final round.
	deliveries map[string]bool
}

func newOperatorArea(l *link, notify func(string, any)) *operatorArea {
	return &operatorArea{link: l, notify: notify, queries: map[string]*OperatorQuery{}, expired: map[string]OperatorExpired{}, deliveries: map[string]bool{}}
}

// query keeps a query of the operator audience the channel sent.
func (o *operatorArea) query(assigned, announced *bus.Event) {
	var d struct {
		Kind    string          `json:"kind"`
		Options json.RawMessage `json:"options"`
		Silent  string          `json:"outcome_if_silent"`
		Final   bool            `json:"final_round"`
	}
	_ = json.Unmarshal(announced.Data, &d) // a field of another type reads as absent
	var options []string
	_ = json.Unmarshal(d.Options, &options)
	q := &OperatorQuery{
		TaskID: assigned.Subject, TraceID: bus.TraceID(assigned.TraceParent), Kind: d.Kind,
		Options: options, Silent: d.Silent, Final: d.Final, Arrived: time.Now().UTC(), Data: announced.Data,
	}
	o.mu.Lock()
	if prev, ok := o.queries[q.TaskID]; ok {
		q.Arrived = prev.Arrived
	}
	o.queries[q.TaskID] = q
	o.mu.Unlock()
	o.notify(eventOperator, map[string]string{"task_id": q.TaskID})
}

// delivery remembers an outcome delivery the channel sent, so its expiry is
// told apart from a query's.
func (o *operatorArea) delivery(taskID string, final bool) {
	o.mu.Lock()
	o.deliveries[taskID] = final
	o.mu.Unlock()
}

// onExpired keeps the notice of a query or a delivery the channel closed
// without an answer. An outcome that did not reach the person comes back in
// the next round, so it has no notice; past the final round it does
// (implementation §6, The service between the console and the channels).
func (o *operatorArea) onExpired(ex *channelv1.Expired) {
	n := OperatorExpired{DeliveryID: ex.GetDeliveryId(), TaskID: ex.GetTaskId(), TraceID: ex.GetTraceId(), FailureCode: ex.GetFailureCode()}
	o.mu.Lock()
	if q, ok := o.queries[n.TaskID]; ok {
		n.Kind = q.Kind
		delete(o.queries, n.TaskID)
	}
	final, delivery := o.deliveries[n.TaskID]
	delete(o.deliveries, n.TaskID)
	if delivery {
		n.Kind = kindResultDelivery
		if n.FailureCode == "undelivered" && !final {
			o.mu.Unlock()
			o.confirm(n.DeliveryID)
			return
		}
	}
	o.expired[n.DeliveryID] = n
	o.mu.Unlock()
	o.notify(eventOperator, map[string]string{"delivery_id": n.DeliveryID})
}

func (o *operatorArea) list() OperatorQueries {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := OperatorQueries{Queries: []OperatorQuery{}, Expired: []OperatorExpired{}}
	for _, q := range o.queries {
		out.Queries = append(out.Queries, *q)
	}
	for _, n := range o.expired {
		out.Expired = append(out.Expired, n)
	}
	sort.Slice(out.Queries, func(i, j int) bool { return out.Queries[i].Arrived.Before(out.Queries[j].Arrived) })
	sort.Slice(out.Expired, func(i, j int) bool { return out.Expired[i].DeliveryID < out.Expired[j].DeliveryID })
	return out
}

// answer closes a query with the decision or the text the person chose. The
// console carries the choice: an option outside the enumeration is refused
// before it reaches the channel.
func (o *operatorArea) answer(ctx context.Context, a OperatorAnswer) error {
	o.mu.Lock()
	q, ok := o.queries[a.TaskID]
	o.mu.Unlock()
	if !ok {
		return errNotFound
	}
	data := map[string]any{}
	switch {
	case len(q.Options) == 0:
		data["answer"] = a.Text
	case !slices.Contains(q.Options, a.Decision):
		return errNoAnswer
	default:
		data["decision"] = a.Decision
	}
	raw, err := json.Marshal(data)
	if err != nil {
		return err
	}
	c, err := o.link.client()
	if err != nil {
		return err
	}
	if _, err := c.Answer(ctx, &channelv1.AnswerRequest{TaskId: a.TaskID, Data: raw}); err != nil {
		return err
	}
	o.mu.Lock()
	delete(o.queries, a.TaskID)
	o.mu.Unlock()
	o.notify(eventOperator, map[string]string{"task_id": a.TaskID})
	return nil
}

// shown tells the channel the panel showed these queries to the person, which
// separates unanswered from undelivered (implementation §6).
func (o *operatorArea) shown(taskIDs []string) {
	for _, id := range taskIDs {
		o.mu.Lock()
		_, ok := o.queries[id]
		o.mu.Unlock()
		if !ok {
			continue
		}
		if err := o.link.send(&channelv1.ConsoleMessage{Body: &channelv1.ConsoleMessage_Shown{Shown: &channelv1.Shown{TaskId: id}}}); err != nil {
			log.Printf("console: showing %s: %v", id, err)
		}
	}
}

// seen confirms the expired notices the panel showed.
func (o *operatorArea) seen(deliveryIDs []string) {
	for _, id := range deliveryIDs {
		o.mu.Lock()
		_, ok := o.expired[id]
		delete(o.expired, id)
		o.mu.Unlock()
		if ok {
			o.confirm(id)
		}
	}
}

func (o *operatorArea) confirm(deliveryID string) {
	if err := o.link.send(&channelv1.ConsoleMessage{Body: &channelv1.ConsoleMessage_Received{Received: &nriv1.Received{DeliveryId: deliveryID}}}); err != nil {
		log.Printf("console: confirming the expired %s: %v", deliveryID, err)
	}
}

func (s *Service) operatorRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET "+PathOperatorQueries, s.withOperatorArea(func(w http.ResponseWriter, r *http.Request, o *operatorArea) {
		writeJSON(w, http.StatusOK, o.list())
	}))
	mux.HandleFunc("POST "+PathOperatorAnswers, s.withOperatorArea(func(w http.ResponseWriter, r *http.Request, o *operatorArea) {
		var a OperatorAnswer
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&a); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if err := o.answer(r.Context(), a); err != nil {
			writeCallError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	mux.HandleFunc("POST "+PathOperatorShown, s.withOperatorArea(func(w http.ResponseWriter, r *http.Request, o *operatorArea) {
		var ids TaskIDs
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&ids); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		o.shown(ids.TaskIDs)
		w.WriteHeader(http.StatusNoContent)
	}))
	mux.HandleFunc("POST "+PathOperatorExpired, s.withOperatorArea(func(w http.ResponseWriter, r *http.Request, o *operatorArea) {
		var ids DeliveryIDs
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&ids); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		o.seen(ids.DeliveryIDs)
		w.WriteHeader(http.StatusNoContent)
	}))
}

// withOperatorArea admits a request with an open session and an Operator
// Channel; one that carries HeaderRefresh is not activity, as in the work area.
func (s *Service) withOperatorArea(h func(w http.ResponseWriter, r *http.Request, o *operatorArea)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s.withSession(r.Header.Get(HeaderRefresh) == "", func(w http.ResponseWriter, r *http.Request, _ string) {
			s.mu.Lock()
			o := s.ops
			s.mu.Unlock()
			if o == nil {
				writeError(w, http.StatusServiceUnavailable, "the network runs no Operator Channel")
				return
			}
			h(w, r, o)
		})(w, r)
	}
}
