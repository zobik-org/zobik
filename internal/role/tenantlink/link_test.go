package tenantlink

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHandlerOnlyReads(t *testing.T) {
	h := Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	for method, want := range map[string]int{
		http.MethodGet:    http.StatusOK,
		http.MethodHead:   http.StatusOK,
		http.MethodPost:   http.StatusForbidden,
		http.MethodPut:    http.StatusForbidden,
		http.MethodPatch:  http.StatusForbidden,
		http.MethodDelete: http.StatusForbidden,
	} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(method, "/v2/artifacts/manifests/x", nil))
		if w.Code != want {
			t.Errorf("%s: %d, want %d", method, w.Code, want)
		}
	}
}
