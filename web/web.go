// Package web is the reusable half of the request path: reading a request
// into a batch element, and rendering what comes back.
//
// # What is deliberately not here
//
// There is no handler and no adapter. ([Serve] runs and drains an
// [http.Server]; it never sees a request.) The architecture refuses the
// wrapper that decides for a handler: the **status code** and the
// **encoding** are the two decisions that matter in an HTTP handler, and
// anything that wraps them makes both invisible at the call site. The
// example's handler is short and it should stay short enough for a reader to
// see it whole.
//
// Three wrappers are here, and each acts only where the handler cannot
// answer for itself: [Recover] once it has panicked, [TryHandler] once a
// sentinel has unwound it, and [EarlyData] before it runs, refusing with 425
// a request an intermediary forwarded in early data. None of them touches
// the response of a handler that returns.
//
// Everything else in this package is a function a handler calls, never a
// thing that calls the handler:
//
//	func handle(w http.ResponseWriter, r *http.Request) {
//	    var a Amendment
//	    if d, ok := web.DecodeJSON(r, 1<<16, &a); !ok {
//	        web.WriteJSON(w, http.StatusBadRequest, web.ReportOf(d))
//	        return
//	    }
//	    res, err := svc.Apply(r.Context(), a)   // one gateway batch behind this
//	    ...
//	    web.WriteJSON(w, web.Status(res.Diagnostics), web.Report{
//	        Problems: web.Problems(res.Diagnostics),
//	        Remedies: web.Remedies(res.Affordances),
//	    })
//	}
//
// The status is still chosen on a visible line. That is the point.
//
// # Two return shapes, on purpose
//
// [DecodeJSON], [PathInt], [PathInt64], [QueryInt64] and [Bearer] end in
// (diagnostics.Diagnostic, bool); [Authenticate] and [Authorize] end in
// (diagnostics.Diagnostic, error). The bool says "refused, and the
// diagnostic is all there is": a malformed body or a missing wildcard has
// nothing more to tell than what goes back to the requester. The error says
// the same refusal and carries what must not go back: which half of a
// credential was wrong, which dimension of a grant fell short, for the
// caller to log. It also tells cases apart that a route may treat
// differently — errors.Is(err, [ErrCredentialsMissing]) is an anonymous
// caller, not a forged one. In both shapes the diagnostic is what the
// response carries.
//
// # Routing
//
// There is no router either. `http.ServeMux` has matched method, path and
// wildcards since Go 1.22, it is maintained by the people who maintain the
// parser underneath it, and writing another one would put a second
// hostile-input parser in a project whose whole position is that it does not
// write the first. [PathInt] and [PathInt64] read the wildcards it captures.
package web

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"

	"github.com/mlagarrigue/sluice/diagnostics"
	"github.com/mlagarrigue/sluice/web/internal/reqbody"
)

// Codes for the diagnostics this package raises itself. They are in the same
// hierarchical style as an application's own, so a client matches on them the
// same way.
const (
	CodeMalformedBody = "Transport.Request.Malformed"
	CodeBodyTooLarge  = "Transport.Request.TooLarge"
	CodeMissingParam  = "Transport.Request.MissingParameter"
	CodeInvalidParam  = "Transport.Request.InvalidParameter"
)

// DecodeJSON reads a request body into dst, bounded to limit bytes.
//
// The bound is a required argument with no usable zero, for the reason
// [diagnostics.NewCollector] takes one: a body limit that suits every endpoint
// does not exist, and an unbounded read from a socket is how a process is
// killed by a client that dialled it.
//
// A failure comes back as a [diagnostics.Diagnostic] rather than an error, because
// that is what the rest of the pipeline speaks — a rejected request is
// answered with its problem attached, never dropped. The distinction between
// "too large" and "malformed" is kept: they need different fixes and a caller
// that conflates them tells the client to check its JSON when the JSON was
// fine.
//
// A body of null is refused when dst points to a struct. It parses, but it
// decodes into a struct as no assignment at all, so the handler would go on
// with a value the client never sent.
//
// The request must say it carries JSON: one Content-Type, application/json or
// a +json subtype. Any other label, or none, is refused as malformed before a
// byte is read — a browser posts text/plain and form bodies across origins
// without a preflight, and a JSON document hidden in one would otherwise be
// decoded as if the client had asked to send it.
//
// Three readings are encoding/json's, kept as they are, and a hop in front
// that reads the document differently must know them: a name repeated in an
// object is accepted and the last one wins, and names match struct fields
// without regard to case, so "ID" and "id" are one field; invalid UTF-8 and
// a lone surrogate escape are accepted and replaced with U+FFFD, so the
// value decoded is not the bytes sent; a byte order mark ahead of the
// document is refused as malformed (RFC 8259 §8.1 lets a parser ignore it).
// Refusing the first two would take a second pass over the body, a second
// reading of hostile input this package does not add.
func DecodeJSON(r *http.Request, limit int64, dst any) (diagnostics.Diagnostic, bool) {
	if limit <= 0 {
		panic("web: DecodeJSON needs a positive byte limit; an unbounded read from a socket is not a default")
	}
	if ct := r.Header["Content-Type"]; len(ct) != 1 || !reqbody.JSONMediaType(ct[0]) {
		return reqbody.MediaType(CodeMalformedBody), false
	}

	// MaxBytesReader rather than io.LimitReader: it reports the overrun as a
	// distinguishable *http.MaxBytesError, where LimitReader hands the decoder
	// a truncated document that fails as a syntax error. With a nil
	// ResponseWriter it does not tell the server to stop reading or close the
	// connection — net/http drains or abandons what is left of the body on
	// its own terms once the handler returns.
	return reqbody.Decode(http.MaxBytesReader(nil, r.Body, limit), limit, dst, CodeMalformedBody, CodeBodyTooLarge)
}

// PathInt64 reads a [http.ServeMux] wildcard as an int64.
//
//	mux.HandleFunc("PATCH /orders/{order}/lines/{line}", handle)
//	id, d, ok := web.PathInt64(r, "order")
//
// The diagnostic names the parameter, so a client is told which one it got
// wrong rather than that something was wrong.
func PathInt64(r *http.Request, name string) (int64, diagnostics.Diagnostic, bool) {
	raw := r.PathValue(name)
	if raw == "" {
		return 0, missing(name), false
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, invalid(name, raw, "integer"), false
	}
	return v, diagnostics.Diagnostic{}, true
}

// PathInt reads a [http.ServeMux] wildcard as an int.
func PathInt(r *http.Request, name string) (int, diagnostics.Diagnostic, bool) {
	v, d, ok := PathInt64(r, name)
	if !ok {
		return 0, d, false
	}
	// Dead on 64-bit platforms, where int is int64; it is the 32-bit guard.
	if v < math.MinInt || v > math.MaxInt {
		return 0, invalid(name, r.PathValue(name), "integer"), false
	}
	return int(v), diagnostics.Diagnostic{}, true
}

// QueryInt64 reads a query parameter as an int64. Absent and unparseable are
// different diagnostics, because they have different fixes.
func QueryInt64(r *http.Request, name string) (int64, diagnostics.Diagnostic, bool) {
	// Parsed once per call: url.URL.Query re-parses RawQuery and allocates a
	// fresh map every time. Its size is bounded by net/url, not by the
	// client: since Go 1.26 (GODEBUG urlmaxqueryparams, default 10000) a
	// query with more parameters fails to parse, and Query discards that
	// error and returns no values — so such a request reads as missing the
	// parameter rather than costing an unbounded map.
	vals, ok := r.URL.Query()[name]
	if !ok || len(vals) == 0 {
		return 0, missing(name), false
	}
	raw := vals[0]
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, invalid(name, raw, "integer"), false
	}
	return v, diagnostics.Diagnostic{}, true
}

func missing(name string) diagnostics.Diagnostic {
	return diagnostics.NewDiagnostic(diagnostics.Error, CodeMissingParam, diagnostics.Path{}.Field(name)).
		WithMessage("ParameterMissing", map[string]any{"name": name}).
		WithOrigin(diagnostics.OriginProtocol)
}

func invalid(name, raw, kind string) diagnostics.Diagnostic {
	return diagnostics.NewDiagnostic(diagnostics.Error, CodeInvalidParam, diagnostics.Path{}.Field(name)).
		WithMessage("ParameterInvalid", map[string]any{"name": name, "got": raw, "want": kind}).
		WithOrigin(diagnostics.OriginProtocol)
}

// Status maps a set of diagnostics to an HTTP status code.
//
// It is a function the handler calls on a line the reader can see, not a
// behaviour something applies on its behalf. The mapping is small enough to
// state in full:
//
//	no diagnostics, or Info/Warning only → 200
//	any Error                            → 422 Unprocessable Entity
//	any Critical                         → 422 as well
//
// Warnings do **not** change the status. An element that carries three
// warnings and kept moving forward was processed, and reporting that as a
// failure would make a client retry work that succeeded — the diagnostics
// model's "an element can be valid, carry three warnings, and keep moving
// forward" (docs/design/architecture.md), read at the transport.
//
// 422 rather than 400 for both Error and Critical: the request parsed, its
// syntax was fine, and what failed is a rule about its content. A 400 tells a
// client to fix its encoding, which is the wrong instruction. Transport-level
// problems — a malformed body, a missing parameter — are the caller's to map
// to 400 if they wish; they carry [CodeMalformedBody] and friends, and
// [IsTransport] tells them apart.
func Status(diags []diagnostics.Diagnostic) int {
	for _, d := range diags {
		if d.Severity >= diagnostics.Error {
			return http.StatusUnprocessableEntity
		}
	}
	return http.StatusOK
}

// IsTransport reports whether a diagnostic came from this package rather than
// from a business rule — a body that would not parse, a parameter that was not
// there. Those usually deserve a 400 rather than a 422, and the decision is
// the handler's.
func IsTransport(d diagnostics.Diagnostic) bool {
	switch d.Code {
	case CodeMalformedBody, CodeBodyTooLarge, CodeMissingParam, CodeInvalidParam:
		return true
	}
	return false
}

// Problem is a diagnostic in the shape a client reads it.
//
// It carries the **MessageID**, never a rendered sentence: the library's
// diagnostics are i18n keys with named arguments, and rendering one here would
// pick a language on behalf of a caller who knows better which to use.
type Problem struct {
	Severity  string         `json:"severity"`
	Code      string         `json:"code"`
	MessageID string         `json:"messageId"`
	Args      map[string]any `json:"args,omitempty"`
	Path      string         `json:"path"`
	RuleID    string         `json:"ruleId,omitempty"`
}

// Remedy is an affordance in the shape a client reads it. Its Path is the same
// path the problem carries, which is what lets a client pair the two by
// comparison rather than by convention.
type Remedy struct {
	Action string  `json:"action"`
	Method string  `json:"method,omitempty"`
	Target string  `json:"target,omitempty"`
	Path   string  `json:"path"`
	Inputs []Input `json:"inputs,omitempty"`
}

// Input describes one field an action accepts. It carries every field of
// [diagnostics.Input]: a read-only field dropped here would be offered to the
// client as editable, and a suggested value dropped here would be a remedy
// the client cannot apply.
type Input struct {
	Name     string `json:"name"`
	Kind     string `json:"kind,omitempty"`
	Required bool   `json:"required"`
	ReadOnly bool   `json:"readOnly,omitempty"`
	Pattern  string `json:"pattern,omitempty"`
	Value    string `json:"value,omitempty"`
}

// Report is the envelope a rejection travels in: what went wrong and what can
// be done about it, side by side.
//
// The two live in one document on purpose. A 422 that says only what failed
// leaves the client to guess the fix, and guessing is what the affordance
// model exists to remove.
type Report struct {
	Problems []Problem `json:"problems,omitempty"`
	Remedies []Remedy  `json:"remedies,omitempty"`
}

// ReportOf is the report for a refusal that has its diagnostics in hand and
// no remedy to offer — the shape every early return of a handler takes:
//
//	if d, ok := web.DecodeJSON(r, 1<<16, &a); !ok {
//	    web.WriteJSON(w, http.StatusBadRequest, web.ReportOf(d))
//	    return
//	}
func ReportOf(ds ...diagnostics.Diagnostic) Report {
	return Report{Problems: Problems(ds)}
}

// Problems renders diagnostics for a client. Paths become JSON Pointers, which
// is the one form a JSON client can act on without knowing this library.
func Problems(ds []diagnostics.Diagnostic) []Problem {
	if len(ds) == 0 {
		return nil
	}
	out := make([]Problem, 0, len(ds))
	for _, d := range ds {
		out = append(out, Problem{
			Severity:  d.Severity.String(),
			Code:      d.Code,
			MessageID: d.MessageID,
			Args:      d.Args,
			Path:      d.Path.JSONPointer(),
			RuleID:    d.RuleID,
		})
	}
	return out
}

// Remedies renders affordances for a client.
func Remedies(as []diagnostics.Affordance) []Remedy {
	if len(as) == 0 {
		return nil
	}
	out := make([]Remedy, 0, len(as))
	for _, a := range as {
		var inputs []Input
		if len(a.Inputs) > 0 {
			inputs = make([]Input, 0, len(a.Inputs))
			for _, in := range a.Inputs {
				inputs = append(inputs, Input{
					Name: in.Name, Kind: in.Kind,
					Required: in.Required, ReadOnly: in.ReadOnly,
					Pattern: in.Pattern, Value: in.Value,
				})
			}
		}
		out = append(out, Remedy{
			Action: a.Action, Method: a.Method, Target: a.Target,
			Path: a.Path.JSONPointer(), Inputs: inputs,
		})
	}
	return out
}

// WriteJSON writes status and body, setting the content type first.
//
// The order is not a style choice: [http.ResponseWriter.WriteHeader] freezes
// the header map, so a Content-Type set after it is silently dropped and the
// client guesses. This is the one piece of sequencing a handler should not
// have to remember, which is why it is here and the status still is not.
//
// The body is encoded before anything is written. Encoding straight to the
// wire would commit the status first and then, on a marshaler that fails
// halfway, leave the client a truncated document under a status that promised
// a whole one. Encoding first means a failure is returned with nothing on the
// wire — the caller can still answer — and a success goes out in one write,
// with a Content-Length instead of chunked framing. A write failure after the
// status is committed is returned for the caller to log; there is no honest
// way to report it to the client.
func WriteJSON(w http.ResponseWriter, status int, body any) error {
	// 204 and 304 carry no content (RFC 9110 §15.3.5, §15.4.5): net/http
	// refuses the write with ErrBodyNotAllowed, so the body is not encoded
	// and only the status goes out — the same rule web/stream's Render keeps.
	if status == http.StatusNoContent || status == http.StatusNotModified {
		w.WriteHeader(status)
		return nil
	}
	buf, err := json.Marshal(body)
	if err != nil {
		// The status still goes out. Callers overwhelmingly ignore this
		// error — the recovery boundary among them — and returning with
		// nothing written would let net/http answer 200 for a request whose
		// handler meant anything but. A right status with an empty body beats
		// both a wrong status and a truncated document. No Content-Type: an
		// empty payload is not a JSON document, and a client trusting the
		// header would fail to parse it.
		w.WriteHeader(status)
		return fmt.Errorf("web: encoding the response body for status %d: %w", status, err)
	}
	h := w.Header()
	h.Set("Content-Type", "application/json")
	h.Set("Content-Length", strconv.Itoa(len(buf)+1))
	defend(h, status)
	w.WriteHeader(status)
	if _, err := w.Write(buf); err != nil {
		return fmt.Errorf("web: writing the response body after status %d: %w", status, err)
	}
	// The trailing newline json.Encoder writes, as its own write
	// rather than appended: an append onto a marshal result whose length
	// landed on a size class would copy the whole body to add one byte, and
	// net/http buffers the two writes into one flush anyway.
	if _, err := w.Write(jsonNewline); err != nil {
		return fmt.Errorf("web: writing the response body after status %d: %w", status, err)
	}
	return nil
}

// defend sets the two headers every JSON document goes out with. nosniff,
// because a browser that guesses the type of a document it was told is JSON
// may run it as a script or render it as a page, and the document may carry
// what the client sent. no-store on an error, because an error is the answer
// that quotes the request — the field that did not parse, the value out of
// range — and a shared cache must not hand it to the next client. The
// caller's own Cache-Control wins: a 404 it wants cached is its decision.
func defend(h http.Header, status int) {
	h.Set("X-Content-Type-Options", "nosniff")
	if status >= 400 && len(h.Values("Cache-Control")) == 0 {
		h.Set("Cache-Control", "no-store")
	}
}

// jsonNewline is the one-byte tail [WriteJSON] writes after the body, hoisted
// so the write does not allocate.
var jsonNewline = []byte{'\n'}
