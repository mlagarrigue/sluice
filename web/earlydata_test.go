package web_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mlagarrigue/sluice/web"
)

// A request an intermediary received in early data carries Early-Data (RFC
// 8470 §5.1). Unless allow accepts it, it is answered 425 with a report the
// client can match on, and the handler never runs; a request without the
// field, or one allow accepts, goes through untouched. Any value counts.
func TestEarlyDataAnswers425UnlessAllowed(t *testing.T) {
	getOnly := func(r *http.Request) bool { return r.Method == http.MethodGet }
	for _, tc := range []struct {
		name    string
		allow   func(*http.Request) bool
		method  string
		field   string // Early-Data value; empty means absent
		handled bool
	}{
		{"no field, no predicate", nil, http.MethodPost, "", true},
		{"field, no predicate", nil, http.MethodGet, "1", false},
		{"field with an invalid value", nil, http.MethodGet, "yes", false},
		{"field, predicate refuses", getOnly, http.MethodPost, "1", false},
		{"field, predicate accepts", getOnly, http.MethodGet, "1", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handled := false
			h := web.EarlyData(tc.allow, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				handled = true
				w.WriteHeader(http.StatusNoContent)
			}))
			r := httptest.NewRequest(tc.method, "/orders/7", http.NoBody)
			if tc.field != "" {
				r.Header.Set("Early-Data", tc.field)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, r)
			if handled != tc.handled {
				t.Fatalf("handler ran = %v, want %v", handled, tc.handled)
			}
			if tc.handled {
				return
			}
			if rec.Code != http.StatusTooEarly {
				t.Errorf("status = %d, want 425", rec.Code)
			}
			var report web.Report
			if err := json.Unmarshal(rec.Body.Bytes(), &report); err != nil {
				t.Fatalf("body %q: %v", rec.Body.Bytes(), err)
			}
			if len(report.Problems) != 1 || report.Problems[0].Code != web.CodeTooEarly {
				t.Errorf("report = %+v, want one %s", report, web.CodeTooEarly)
			}
		})
	}
}

func TestEarlyDataRequiresAHandler(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("EarlyData accepted a nil handler")
		}
	}()
	web.EarlyData(nil, nil)
}
