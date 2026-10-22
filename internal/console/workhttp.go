package console

import (
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The routes of the work area (implementation §6, The operator console).
const (
	PathConversations = "/api/conversations"
	PathAnswers       = "/api/answers"
	PathShown         = "/api/shown"
	PathExpired       = "/api/expired"
)

// HeaderRefresh marks a request the panel makes on its own, after an event:
// it is not the person's activity (implementation §6, The operator console).
const HeaderRefresh = "Zobik-Refresh"

// maxRequest bounds a request with its attachments, which the console holds
// in memory: it writes nothing unencrypted to disk.
const maxRequest = 64 << 20

func (s *Service) workRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET "+PathConversations, s.withWork(func(w http.ResponseWriter, r *http.Request, wa *workArea) {
		writeJSON(w, http.StatusOK, wa.list())
	}))
	mux.HandleFunc("POST "+PathConversations, s.withWork(func(w http.ResponseWriter, r *http.Request, wa *workArea) {
		c, err := wa.create()
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusCreated, c)
	}))
	mux.HandleFunc("GET "+PathConversations+"/{id}", s.withWork(func(w http.ResponseWriter, r *http.Request, wa *workArea) {
		b, ok := wa.get(r.PathValue("id"))
		if !ok {
			writeError(w, http.StatusNotFound, errNotFound.Error())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(b)
	}))
	mux.HandleFunc("DELETE "+PathConversations+"/{id}", s.withWork(func(w http.ResponseWriter, r *http.Request, wa *workArea) {
		if err := wa.remove(r.Context(), r.PathValue("id")); err != nil {
			writeCallError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	mux.HandleFunc("POST "+PathConversations+"/{id}/messages", s.withWork(s.postMessage))
	mux.HandleFunc("GET "+PathConversations+"/{id}/attachments/{attachment}", s.withWork(func(w http.ResponseWriter, r *http.Request, wa *workArea) {
		a, b, err := wa.attachment(r.PathValue("id"), r.PathValue("attachment"))
		if err != nil {
			writeCallError(w, err)
			return
		}
		// An attachment is never rendered under the console's origin.
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": a.Name}))
		w.Header().Set("Content-Security-Policy", "sandbox")
		_, _ = w.Write(b)
	}))
	mux.HandleFunc("POST "+PathAnswers, s.withWork(func(w http.ResponseWriter, r *http.Request, wa *workArea) {
		var a Answer
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&a); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if err := wa.answer(r.Context(), a); err != nil {
			writeCallError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	mux.HandleFunc("POST "+PathShown, s.withWork(func(w http.ResponseWriter, r *http.Request, wa *workArea) {
		var ids TaskIDs
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&ids); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		wa.shown(r.Context(), ids.TaskIDs)
		w.WriteHeader(http.StatusNoContent)
	}))
	mux.HandleFunc("GET "+PathExpired, s.withWork(func(w http.ResponseWriter, r *http.Request, wa *workArea) {
		writeJSON(w, http.StatusOK, wa.notices())
	}))
	mux.HandleFunc("POST "+PathExpired, s.withWork(func(w http.ResponseWriter, r *http.Request, wa *workArea) {
		var ids DeliveryIDs
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&ids); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		wa.seen(ids.DeliveryIDs)
		w.WriteHeader(http.StatusNoContent)
	}))
}

// TaskIDs are the outcomes and queries the panel showed.
type TaskIDs struct {
	TaskIDs []string `json:"task_ids"`
}

// DeliveryIDs are the expired notices the panel showed.
type DeliveryIDs struct {
	DeliveryIDs []string `json:"delivery_ids"`
}

// withWork admits a request with an open session and a work area.
func (s *Service) withWork(h func(w http.ResponseWriter, r *http.Request, wa *workArea)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s.withSession(r.Header.Get(HeaderRefresh) == "", func(w http.ResponseWriter, r *http.Request, _ string) {
			wa := s.workArea()
			if wa == nil {
				writeError(w, http.StatusServiceUnavailable, "the network runs no Entry Node, or its conversations did not open")
				return
			}
			h(w, r, wa)
		})(w, r)
	}
}

// postMessage sends a request: a multipart form with its text and its
// attachments, read in memory.
func (s *Service) postMessage(w http.ResponseWriter, r *http.Request, wa *workArea) {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequest)
	reader, err := r.MultipartReader()
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	var text string
	var files []File
	for {
		p, err := reader.NextPart()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			writeError(w, http.StatusRequestEntityTooLarge, err.Error())
			return
		}
		b, err := io.ReadAll(p)
		if err != nil {
			writeError(w, http.StatusRequestEntityTooLarge, err.Error())
			return
		}
		switch {
		case p.FormName() == "text":
			text = string(b)
		case p.FormName() == "file" && p.FileName() != "":
			mt := p.Header.Get("Content-Type")
			if mt == "" {
				mt = "application/octet-stream"
			}
			files = append(files, File{Name: p.FileName(), MediaType: mt, Content: b})
		}
	}
	m, err := wa.send(r.Context(), r.PathValue("id"), text, files)
	if err != nil {
		writeCallError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, m)
}

// writeCallError answers with the status that matches what the channel said.
func writeCallError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errNotFound):
		writeError(w, http.StatusNotFound, err.Error())
		return
	case errors.Is(err, errNoAnswer):
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	st, ok := status.FromError(err)
	if !ok {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	code := http.StatusBadGateway
	switch st.Code() {
	case codes.InvalidArgument:
		code = http.StatusUnprocessableEntity
	case codes.FailedPrecondition:
		code = http.StatusConflict
	case codes.ResourceExhausted:
		code = http.StatusRequestEntityTooLarge
	case codes.Unavailable:
		code = http.StatusServiceUnavailable
	}
	writeError(w, code, st.Message())
}
