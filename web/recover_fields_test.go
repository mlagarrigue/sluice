package web

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// A handler that panics before writing is answered with a 500, and the
// fields it set for the answer it never gave — a Cache-Control: public, a
// Content-Encoding — went out with it: WriteJSON replaces Content-Type and
// Content-Length only. The 500 carries the fields set before the handler
// ran, an outer middleware's, and none the handler added.
func TestRecoverDropsThePanickedHandlersFields(t *testing.T) {
	inner := Recover(func(any) {}, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Cache-Control", "public, max-age=3600")
		w.Header().Set("Content-Encoding", "gzip")
		panic("boom")
	}))
	outer := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Outer", "kept")
		inner.ServeHTTP(w, r)
	})
	rec := httptest.NewRecorder()
	outer.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status %d, want 500", rec.Code)
	}
	if cc, ce := rec.Header().Get("Cache-Control"), rec.Header().Get("Content-Encoding"); cc == "public, max-age=3600" || ce != "" {
		t.Errorf("the 500 left with the panicked handler's fields: Cache-Control %q, Content-Encoding %q", cc, ce)
	}
	if got := rec.Header().Get("X-Outer"); got != "kept" {
		t.Errorf("X-Outer = %q, want the outer middleware's %q", got, "kept")
	}
}

// BenchmarkRecoverPassThrough is a request that does not panic, through
// Recover, behind a middleware that set two fields first: the case where
// Recover copies the fields it would put back.
func BenchmarkRecoverPassThrough(b *testing.B) {
	h := Recover(func(any) {}, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusNoContent)
	}))
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	b.ReportAllocs()
	for b.Loop() {
		w := httptest.NewRecorder()
		w.Header().Set("Strict-Transport-Security", "max-age=63072000")
		w.Header().Set("X-Request-Id", "1")
		h.ServeHTTP(w, r)
	}
}
