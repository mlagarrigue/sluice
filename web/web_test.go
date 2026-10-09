package web_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mlagarrigue/sluice/diagnostics"
	"github.com/mlagarrigue/sluice/web"
)

type amendment struct {
	OrderID int64 `json:"orderId"`
	SaleQty int64 `json:"saleQty"`
}

func post(body string) *http.Request {
	return httptest.NewRequest(http.MethodPost, "/orders", strings.NewReader(body))
}

func TestDecodeJSON(t *testing.T) {
	var a amendment
	if d, ok := web.DecodeJSON(post(`{"orderId":42,"saleQty":9}`), 1<<16, &a); !ok {
		t.Fatalf("a well-formed body was refused: %+v", d)
	}
	if a.OrderID != 42 || a.SaleQty != 9 {
		t.Errorf("decoded %+v", a)
	}
}

// Too large and malformed need different fixes. A caller that conflates them
// tells the client to check its JSON when the JSON was fine.
func TestDecodeJSONDistinguishesTooLargeFromMalformed(t *testing.T) {
	var a amendment

	big := `{"orderId":42,"saleQty":` + strings.Repeat("9", 200) + `}`
	d, ok := web.DecodeJSON(post(big), 32, &a)
	if ok {
		t.Fatal("an oversized body was accepted")
	}
	if d.Code != web.CodeBodyTooLarge {
		t.Errorf("code = %q, want %q", d.Code, web.CodeBodyTooLarge)
	}
	if d.Args["limit"] != int64(32) {
		t.Errorf("the diagnostic does not carry the limit that was exceeded: %v", d.Args)
	}

	d, ok = web.DecodeJSON(post(`{"orderId":`), 1<<16, &a)
	if ok {
		t.Fatal("a truncated body was accepted")
	}
	if d.Code != web.CodeMalformedBody {
		t.Errorf("code = %q, want %q", d.Code, web.CodeMalformedBody)
	}
}

// The decoder's error text names Go types and struct fields — the server's
// internals, not the client's document. Only position information travels;
// the raw error stays on the diagnostic's cause, for the log.
func TestDecodeJSONKeepsTheDecoderErrorServerSide(t *testing.T) {
	assertOpaque := func(t *testing.T, d diagnostics.Diagnostic) {
		t.Helper()
		if _, found := d.Args["detail"]; found {
			t.Errorf("the raw decoder error travels in the args: %v", d.Args)
		}
		for k, v := range d.Args {
			if s, ok := v.(string); ok && (strings.Contains(s, "int64") || strings.Contains(s, "amendment")) {
				t.Errorf("args[%q] = %q leaks a Go type name", k, s)
			}
		}
		if d.Unwrap() == nil {
			t.Error("the raw error was not kept on the diagnostic's cause")
		}
	}

	t.Run("type mismatch", func(t *testing.T) {
		var a amendment
		d, ok := web.DecodeJSON(post(`{"orderId":"forty-two"}`), 1<<16, &a)
		if ok {
			t.Fatal("a mistyped field was accepted")
		}
		assertOpaque(t, d)
		if d.Args["field"] != "orderId" {
			t.Errorf("args = %v, want the JSON field named", d.Args)
		}
		if _, found := d.Args["offset"]; !found {
			t.Errorf("args = %v, want the offset", d.Args)
		}
	})

	t.Run("syntax error", func(t *testing.T) {
		var a amendment
		d, ok := web.DecodeJSON(post(`{"orderId":42,,}`), 1<<16, &a)
		if ok {
			t.Fatal("a syntax error was accepted")
		}
		assertOpaque(t, d)
		if _, found := d.Args["offset"]; !found {
			t.Errorf("args = %v, want the offset", d.Args)
		}
	})

	t.Run("unknown field", func(t *testing.T) {
		var a amendment
		d, ok := web.DecodeJSON(post(`{"orderId":1,"saleQtyy":9}`), 1<<16, &a)
		if ok {
			t.Fatal("an unknown field was accepted")
		}
		assertOpaque(t, d)
		if d.Args["field"] != "saleQtyy" {
			t.Errorf("args = %v, want the client's own field quoted back", d.Args)
		}
	})
}

// A second value behind the first is a smuggled request, not a stray byte.
func TestDecodeJSONRefusesTrailingContent(t *testing.T) {
	var a amendment
	d, ok := web.DecodeJSON(post(`{"orderId":1,"saleQty":1}{"orderId":666,"saleQty":0}`), 1<<16, &a)
	if ok {
		t.Fatal("a body carrying two documents was accepted")
	}
	if d.Code != web.CodeMalformedBody {
		t.Errorf("code = %q, want %q", d.Code, web.CodeMalformedBody)
	}
}

// An unknown field is a typo or a client talking to the wrong version, and
// silently dropping it is how a request appears to succeed while doing
// something other than what was asked.
func TestDecodeJSONRefusesUnknownFields(t *testing.T) {
	var a amendment
	if _, ok := web.DecodeJSON(post(`{"orderId":1,"saleQty":1,"saleQtyy":9}`), 1<<16, &a); ok {
		t.Fatal("an unknown field was silently ignored")
	}
}

// An unbounded read from a socket is not a default anyone should get by
// forgetting an argument.
func TestDecodeJSONRefusesAnUnboundedLimit(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("DecodeJSON accepted a limit of zero")
		}
	}()
	var a amendment
	_, _ = web.DecodeJSON(post(`{}`), 0, &a)
}

func TestPathValues(t *testing.T) {
	mux := http.NewServeMux()
	var gotOrder int64
	var gotLine int
	var problems []diagnostics.Diagnostic

	mux.HandleFunc("PATCH /orders/{order}/lines/{line}", func(w http.ResponseWriter, r *http.Request) {
		id, d, ok := web.PathInt64(r, "order")
		if !ok {
			problems = append(problems, d)
		}
		gotOrder = id
		idx, d, ok := web.PathInt(r, "line")
		if !ok {
			problems = append(problems, d)
		}
		gotLine = idx
		w.WriteHeader(http.StatusOK)
	})

	t.Run("well formed", func(t *testing.T) {
		problems = nil
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPatch, "/orders/42/lines/7", http.NoBody))
		if len(problems) != 0 {
			t.Fatalf("problems on a valid path: %+v", problems)
		}
		if gotOrder != 42 || gotLine != 7 {
			t.Errorf("read order=%d line=%d", gotOrder, gotLine)
		}
	})

	t.Run("not a number", func(t *testing.T) {
		problems = nil
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPatch, "/orders/abc/lines/7", http.NoBody))
		if len(problems) != 1 {
			t.Fatalf("got %d problems, want 1", len(problems))
		}
		d := problems[0]
		if d.Code != web.CodeInvalidParam {
			t.Errorf("code = %q", d.Code)
		}
		// The client must be told *which* parameter, not that something failed.
		if d.Args["name"] != "order" || d.Args["got"] != "abc" {
			t.Errorf("the diagnostic does not name the parameter: %v", d.Args)
		}
		if d.Path.JSONPointer() != "/order" {
			t.Errorf("path = %q, want /order", d.Path.JSONPointer())
		}
	})
}

func TestQueryInt64SeparatesMissingFromInvalid(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/orders?line=abc", http.NoBody)

	if _, d, ok := web.QueryInt64(r, "order"); ok || d.Code != web.CodeMissingParam {
		t.Errorf("absent parameter gave ok=%v code=%q", ok, d.Code)
	}
	if _, d, ok := web.QueryInt64(r, "line"); ok || d.Code != web.CodeInvalidParam {
		t.Errorf("unparseable parameter gave ok=%v code=%q", ok, d.Code)
	}
}

// The status rule, stated in full and asserted in full. A warning must not
// change it: the element was processed, and reporting that as a failure makes
// a client retry work that succeeded.
func TestStatus(t *testing.T) {
	at := diagnostics.Path{}.Field("saleQty")
	for _, tc := range []struct {
		name  string
		diags []diagnostics.Diagnostic
		want  int
	}{
		{"none", nil, http.StatusOK},
		{"info only", []diagnostics.Diagnostic{
			diagnostics.NewDiagnostic(diagnostics.Info, "X.Normalised", at),
		}, http.StatusOK},
		{"warning only", []diagnostics.Diagnostic{
			diagnostics.NewDiagnostic(diagnostics.Warning, "X.Unusual", at),
		}, http.StatusOK},
		{"error", []diagnostics.Diagnostic{
			diagnostics.NewDiagnostic(diagnostics.Error, "X.Invalid", at),
		}, http.StatusUnprocessableEntity},
		{"critical", []diagnostics.Diagnostic{
			diagnostics.NewDiagnostic(diagnostics.Critical, "X.Gone", at),
		}, http.StatusUnprocessableEntity},
		{"warning beside an error", []diagnostics.Diagnostic{
			diagnostics.NewDiagnostic(diagnostics.Warning, "X.Unusual", at),
			diagnostics.NewDiagnostic(diagnostics.Error, "X.Invalid", at),
		}, http.StatusUnprocessableEntity},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := web.Status(tc.diags); got != tc.want {
				t.Errorf("Status = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestIsTransport(t *testing.T) {
	at := diagnostics.Path{}
	transport := diagnostics.NewDiagnostic(diagnostics.Error, web.CodeMalformedBody, at)
	business := diagnostics.NewDiagnostic(diagnostics.Error, "Sales.Order.InvalidQty", at)

	if !web.IsTransport(transport) {
		t.Error("a malformed-body diagnostic was not recognised as transport")
	}
	if web.IsTransport(business) {
		t.Error("a business diagnostic was taken for a transport one")
	}
}

// A remedy and its problem must carry the same path, because that identity is
// what lets a client pair them by comparison rather than by convention.
func TestProblemsAndRemediesSharePaths(t *testing.T) {
	at := diagnostics.Path{}.Field("lines").Index(2).Field("saleQty")

	problems := web.Problems([]diagnostics.Diagnostic{
		diagnostics.NewDiagnostic(diagnostics.Error, "Sales.Order.InvalidQty", at).
			WithMessage("QtyMustBeOverZero", map[string]any{"min": 1}),
	})
	remedies := web.Remedies([]diagnostics.Affordance{{
		Path: at, Action: "Sales.Order.CorrectQty", Method: "PATCH",
		Target: "/orders/{id}/lines/{i}",
		Inputs: []diagnostics.Input{{Name: "saleQty", Kind: "integer", Required: true}},
	}})

	if len(problems) != 1 || len(remedies) != 1 {
		t.Fatalf("rendered %d problems and %d remedies", len(problems), len(remedies))
	}
	if problems[0].Path != remedies[0].Path {
		t.Errorf("paths differ: %q vs %q", problems[0].Path, remedies[0].Path)
	}
	if problems[0].Path != "/lines/2/saleQty" {
		t.Errorf("path = %q, want a JSON Pointer", problems[0].Path)
	}
	// The message must stay an i18n key: rendering it here would pick a
	// language on behalf of a caller who knows better which to use.
	if problems[0].MessageID != "QtyMustBeOverZero" {
		t.Errorf("message = %q, want the MessageID", problems[0].MessageID)
	}
	if problems[0].Args["min"] != 1 {
		t.Errorf("the named arguments were dropped: %v", problems[0].Args)
	}
	if len(remedies[0].Inputs) != 1 || !remedies[0].Inputs[0].Required {
		t.Errorf("the remedy's inputs were dropped: %+v", remedies[0].Inputs)
	}
}

func TestProblemsAndRemediesOnEmptyInput(t *testing.T) {
	if got := web.Problems(nil); got != nil {
		t.Errorf("Problems(nil) = %v, want nil so it is omitted from JSON", got)
	}
	if got := web.Remedies(nil); got != nil {
		t.Errorf("Remedies(nil) = %v, want nil so it is omitted from JSON", got)
	}
}

// WriteHeader freezes the header map, so a Content-Type set after it is
// silently dropped and the client guesses. This is the one ordering a handler
// should not have to remember.
//
// It runs against a real server rather than a ResponseRecorder, and that is
// not incidental: a recorder keeps its header map writable after WriteHeader,
// so the same test through one passes whichever order the two calls are in.
// Verified by inverting them — the recorder version stayed green, this one
// does not.
func TestWriteJSONSetsContentTypeBeforeTheStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := web.WriteJSON(w, http.StatusUnprocessableEntity, web.Report{
			Problems: []web.Problem{{Code: "X.Invalid", Path: "/qty"}},
		}); err != nil {
			t.Errorf("WriteJSON: %v", err)
		}
	}))
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if got := resp.Header.Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q — set after WriteHeader, it never reaches the client", got)
	}
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Errorf("status = %d", resp.StatusCode)
	}

	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("the body is not JSON: %v", err)
	}
	if _, ok := body["problems"]; !ok {
		t.Error("the report carries no problems")
	}
	// An empty remedy list must be absent rather than null, so a client can
	// test for its presence.
	if _, ok := body["remedies"]; ok {
		t.Error("an empty remedy list was written out")
	}
}

// A body that will not marshal must not turn a deliberate status into a 200.
//
// WriteJSON encodes before it writes, so that a marshaler failing halfway
// cannot leave a truncated document under a status promising a whole one. The
// order has a trap of its own: returning before WriteHeader leaves net/http to
// send its default 200, and every caller in this package ignores WriteJSON's
// error — the recovery boundary among them. So the status still goes out, with
// an empty body, and the error is the caller's to log.
func TestWriteJSONKeepsTheStatusWhenTheBodyWillNotEncode(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// A channel is unmarshalable, and reaches Args because it is map[string]any.
		body := web.Report{Problems: []web.Problem{{
			Code: "X.Invalid",
			Args: map[string]any{"ch": make(chan int)},
		}}}
		if err := web.WriteJSON(w, http.StatusUnprocessableEntity, body); err == nil {
			t.Error("WriteJSON reported success for a body it could not encode")
		}
	}))
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Errorf("status = %d, want 422: an unencodable body downgraded a rejection to a success", resp.StatusCode)
	}
	// The body is empty, and an empty payload is not a JSON document: a
	// Content-Type claiming one sends a trusting client into a parse error.
	if got := resp.Header.Get("Content-Type"); got != "" {
		t.Errorf("Content-Type = %q on an empty body, want none", got)
	}
}

// The body is written whole, with a length: a client reading Content-Length
// rather than draining a chunked stream is the ordinary case, and it is what
// encoding straight to the wire could not offer.
func TestWriteJSONSendsAContentLength(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = web.WriteJSON(w, http.StatusOK, web.Report{
			Problems: []web.Problem{{Code: "X.Invalid", Path: "/qty"}},
		})
	}))
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	read, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.ContentLength != int64(len(read)) {
		t.Errorf("Content-Length = %d, body is %d bytes", resp.ContentLength, len(read))
	}
	// The trailing newline json.Encoder used to write is kept: a body compared
	// byte for byte by an existing client must not shift under it.
	if len(read) == 0 || read[len(read)-1] != '\n' {
		t.Error("the body does not end in a newline")
	}
}

// null parses, and decodes into a struct as no assignment at all: the handler
// would proceed on zero fields the client never sent.
func TestDecodeJSONRefusesNullIntoAStruct(t *testing.T) {
	for _, body := range []string{`null`, " \n\tnull\r\n"} {
		var a amendment
		d, ok := web.DecodeJSON(post(body), 1<<16, &a)
		if ok {
			t.Fatalf("%q was accepted into a struct", body)
		}
		if d.Code != web.CodeMalformedBody || d.MessageID != "RequestBodyNull" {
			t.Errorf("%q: code %q message %q, want %q RequestBodyNull", body, d.Code, d.MessageID, web.CodeMalformedBody)
		}
	}

	// Where null is a value the target can hold, it is decoded as one.
	m := map[string]int{"kept": 1}
	if d, ok := web.DecodeJSON(post(`null`), 1<<16, &m); !ok {
		t.Errorf("null into a map was refused: %+v", d)
	}
	var a amendment
	if _, ok := web.DecodeJSON(post(`{"orderId":7}`), 1<<16, &a); !ok || a.OrderID != 7 {
		t.Errorf("an object body was refused or mis-decoded: %+v", a)
	}
}

// ReportOf is the early-return refusal in one call: the diagnostics rendered
// as problems, in order, and no remedies.
func TestReportOf(t *testing.T) {
	a := diagnostics.NewDiagnostic(diagnostics.Error, web.CodeMissingParam, diagnostics.Path{}.Field("order"))
	b := diagnostics.NewDiagnostic(diagnostics.Warning, "Rule.Soft", diagnostics.Path{})
	r := web.ReportOf(a, b)
	if len(r.Problems) != 2 || r.Problems[0].Code != web.CodeMissingParam || r.Problems[1].Code != "Rule.Soft" {
		t.Errorf("problems = %+v", r.Problems)
	}
	if r.Remedies != nil {
		t.Errorf("remedies = %+v, want none", r.Remedies)
	}
	if r := web.ReportOf(); r.Problems != nil {
		t.Errorf("an empty report carried problems: %+v", r.Problems)
	}
}

// 204 and 304 carry no content: WriteJSON sends the status alone, with no
// Content-Type and no error, whatever body it was handed.
func TestWriteJSONSendsNoContentFor204And304(t *testing.T) {
	for _, status := range []int{http.StatusNoContent, http.StatusNotModified} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if err := web.WriteJSON(w, status, map[string]int{"a": 1}); err != nil {
				t.Errorf("status %d: WriteJSON = %v, want nil", status, err)
			}
		}))
		res, err := http.Get(srv.URL)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(res.Body)
		_ = res.Body.Close()
		srv.Close()
		if res.StatusCode != status || len(b) != 0 || res.Header.Get("Content-Type") != "" {
			t.Errorf("status %d: got %d, %d body bytes, Content-Type %q", status, res.StatusCode, len(b), res.Header.Get("Content-Type"))
		}
	}
}
