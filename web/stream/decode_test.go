package stream

import (
	"strings"
	"testing"

	"github.com/mlagarrigue/sluice/net/httpstream"
	"github.com/mlagarrigue/sluice/web"
)

func withBody(body string) Exchange {
	e := exchangeFor("PATCH", "/orders/1")
	e.Request.Body = []byte(body)
	e.Request.Headers = append(e.Request.Headers, httpstream.Header{Name: []byte("Content-Type"), Value: []byte("application/json")})
	return e
}

// acceptPatchOf is the Accept-Patch field an exchange's answer carries, or
// "".
func acceptPatchOf(e Exchange) string {
	for _, h := range e.Headers {
		if strings.EqualFold(string(h.Name), "accept-patch") {
			return string(h.Value)
		}
	}
	return ""
}

func TestDecodeJSON(t *testing.T) {
	type patch struct {
		SaleQty int64 `json:"saleQty"`
	}

	t.Run("decodes", func(t *testing.T) {
		e := withBody(`{"saleQty": 4}`)
		var p patch
		if !DecodeJSON(&e, 1<<10, &p) {
			t.Fatalf("refused: %v", e.Diagnostics)
		}
		if p.SaleQty != 4 {
			t.Errorf("saleQty = %d, want 4", p.SaleQty)
		}
	})

	refusals := []struct {
		name, body string
		status     int
		code       string
	}{
		{"malformed", `{"saleQty":`, 400, web.CodeMalformedBody},
		{"an unknown field", `{"saleQty": 4, "price": 1}`, 400, web.CodeMalformedBody},
		{"a second value behind the first", `{"saleQty": 4}{"saleQty": 9}`, 400, web.CodeMalformedBody},
		{"null", `null`, 400, web.CodeMalformedBody},
		{"null behind whitespace", " \r\n\tnull ", 400, web.CodeMalformedBody},
		{"past the route's limit", `{"saleQty": 400000000000000000}`, 413, web.CodeBodyTooLarge},
	}
	for _, tc := range refusals {
		t.Run(tc.name, func(t *testing.T) {
			e := withBody(tc.body)
			var p patch
			limit := int64(1 << 10)
			if tc.status == 413 {
				limit = 8
			}
			if DecodeJSON(&e, limit, &p) {
				t.Fatal("decoded")
			}
			if e.Status != tc.status {
				t.Errorf("status = %d, want %d", e.Status, tc.status)
			}
			if len(e.Diagnostics) != 1 || e.Diagnostics[0].Code != tc.code {
				t.Errorf("diagnostics = %v, want one %s", e.Diagnostics, tc.code)
			}
		})
	}

	// The label rule is web's: one Content-Type naming JSON, checked before
	// the body is read.
	labels := []struct {
		name   string
		values []string
		ok     bool
	}{
		{"json, case and parameters aside", []string{"Application/JSON; charset=utf-8"}, true},
		{"a +json subtype", []string{"application/merge-patch+json"}, true},
		{"no label", nil, false},
		{"text/plain", []string{"text/plain"}, false},
		{"a form", []string{"application/x-www-form-urlencoded"}, false},
		{"a near miss", []string{"application/jsonx"}, false},
		{"two labels", []string{"application/json", "application/json"}, false},
	}
	for _, tc := range labels {
		t.Run(tc.name, func(t *testing.T) {
			e := withBody(`{"saleQty": 4}`)
			e.Request.Headers = nil
			for _, v := range tc.values {
				e.Request.Headers = append(e.Request.Headers, httpstream.Header{Name: []byte("content-type"), Value: []byte(v)})
			}
			var p patch
			if DecodeJSON(&e, 1<<10, &p) != tc.ok {
				t.Fatalf("decoded = %v, want %v (%v)", !tc.ok, tc.ok, e.Diagnostics)
			}
			if tc.ok {
				return
			}
			// RFC 9110 §15.5.16: a format the endpoint does not read is
			// 415, which tells the client to change the label, not the
			// document; on a PATCH, RFC 5789 §2.2 has the refusal name the
			// patch formats accepted.
			if e.Status != 415 || len(e.Diagnostics) != 1 || e.Diagnostics[0].MessageID != "RequestBodyMediaType" ||
				e.Diagnostics[0].Code != web.CodeUnsupportedMediaType {
				t.Errorf("status %d, diagnostics %v, want 415 and one RequestBodyMediaType under %s", e.Status, e.Diagnostics, web.CodeUnsupportedMediaType)
			}
			if got := acceptPatchOf(e); got != "application/merge-patch+json" {
				t.Errorf("a refused PATCH carries Accept-Patch %q, want application/merge-patch+json", got)
			}
			if p.SaleQty != 0 {
				t.Errorf("refused, yet the body was decoded: %+v", p)
			}
		})
	}

	// Accept-Patch belongs to a PATCH: a POST refused for its label gets the
	// 415 alone.
	t.Run("a POST refused for its label", func(t *testing.T) {
		e := exchangeFor("POST", "/orders")
		e.Request.Body = []byte(`{"saleQty": 4}`)
		e.Request.Headers = append(e.Request.Headers, httpstream.Header{Name: []byte("Content-Type"), Value: []byte("text/plain")})
		var p patch
		if DecodeJSON(&e, 1<<10, &p) {
			t.Fatal("a text/plain POST was decoded")
		}
		if e.Status != 415 {
			t.Errorf("status %d, want 415", e.Status)
		}
		if got := acceptPatchOf(e); got != "" {
			t.Errorf("a refused POST carries Accept-Patch %q", got)
		}
	})

	// The rules are web's exactly, including this one: the decoder's error
	// text names the server's Go types and never travels. See
	// web.MalformedBodyArgs.
	t.Run("the decoder error stays server-side", func(t *testing.T) {
		e := withBody(`{"saleQty": "four"}`)
		var p patch
		if DecodeJSON(&e, 1<<10, &p) {
			t.Fatal("a mistyped field was decoded")
		}
		d := e.Diagnostics[0]
		if _, found := d.Args["detail"]; found {
			t.Errorf("the raw decoder error travels in the args: %v", d.Args)
		}
		if d.Args["field"] != "saleQty" {
			t.Errorf("args = %v, want the JSON field named", d.Args)
		}
		for k, v := range d.Args {
			if s, ok := v.(string); ok && (strings.Contains(s, "int64") || strings.Contains(s, "patch")) {
				t.Errorf("args[%q] = %q leaks a Go type name", k, s)
			}
		}
		if d.Unwrap() == nil {
			t.Error("the raw error was not kept on the diagnostic's cause")
		}
	})

	t.Run("a refused exchange is stepped over", func(t *testing.T) {
		e := withBody(`{"saleQty": 4}`)
		e.Status = 401
		var p patch
		if DecodeJSON(&e, 1<<10, &p) {
			t.Error("a refused exchange was decoded")
		}
		if len(e.Diagnostics) != 0 {
			t.Errorf("a refused exchange gained diagnostics: %v", e.Diagnostics)
		}
	})

	t.Run("no limit is a caller bug", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Error("no panic for a zero limit")
			}
		}()
		e := withBody(`{}`)
		var p patch
		DecodeJSON(&e, 0, &p)
	})
}
