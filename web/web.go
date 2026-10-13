// Package web is the reusable half of the request path: reading a request
// into a batch element, and rendering what comes back.
//
// # What is deliberately not here
//
// There is no handler and no adapter: nothing here wraps a handler.
// ([Serve] runs and drains an [http.Server]; it never sees a request.)
// The architecture refuses the wrapper, and the refusal holds: the **status
// code** and the **encoding**
// are the two decisions that matter in an HTTP handler, and anything that
// wraps them makes both invisible at the call site. The example's handler is
// eight lines and it should stay eight lines that a reader can see.
//
// So this package is functions a handler calls, never a thing that calls the
// handler:
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
	"errors"
	"fmt"
	"io"
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
func DecodeJSON(r *http.Request, limit int64, dst any) (diagnostics.Diagnostic, bool) {
	if limit <= 0 {
		panic("web: DecodeJSON needs a positive byte limit; an unbounded read from a socket is not a default")
	}

	// MaxBytesReader rather than io.LimitReader: it reports the overrun as a
	// distinguishable *http.MaxBytesError, where LimitReader hands the decoder
	// a truncated document that fails as a syntax error. With a nil
	// ResponseWriter it does not tell the server to stop reading or close the
	// connection — net/http drains or abandons what is left of the body on
	// its own terms once the handler returns.
	var body io.Reader = http.MaxBytesReader(nil, r.Body, limit)
	var first *firstByteReader
	if reqbody.IntoStruct(dst) {
		first = &firstByteReader{r: body}
		body = first
	}
	dec := json.NewDecoder(body)
	dec.DisallowUnknownFields()

	if err := dec.Decode(dst); err != nil {
		if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
			return diagnostics.NewDiagnostic(diagnostics.Error, CodeBodyTooLarge, diagnostics.Path{}).
				WithMessage("RequestBodyTooLarge", map[string]any{"limit": limit}).
				WithOrigin(diagnostics.OriginProtocol), false
		}
		return diagnostics.NewDiagnostic(diagnostics.Error, CodeMalformedBody, diagnostics.Path{}).
			WithMessage("RequestBodyMalformed", reqbody.MalformedArgs(err)).
			WithOrigin(diagnostics.OriginProtocol).
			WithCause(err), false
	}
	// A JSON value that starts with 'n' is null, and null decodes into a
	// struct as nothing at all: dst keeps its zero fields and the handler
	// proceeds on a request nobody wrote.
	if first != nil && first.b == 'n' {
		return reqbody.Null(CodeMalformedBody), false
	}

	// A second value in the body is not a stray byte, it is a different
	// request smuggled behind the first. Refused rather than ignored.
	if err := dec.Decode(&json.RawMessage{}); !errors.Is(err, io.EOF) {
		return diagnostics.NewDiagnostic(diagnostics.Error, CodeMalformedBody, diagnostics.Path{}).
			WithMessage("RequestBodyTrailingContent", nil).
			WithOrigin(diagnostics.OriginProtocol), false
	}
	return diagnostics.Diagnostic{}, true
}

// firstByteReader remembers the first non-whitespace byte read through it,
// which is enough to tell a null document from any other: no other JSON
// value begins with 'n'.
type firstByteReader struct {
	r io.Reader
	b byte
}

func (f *firstByteReader) Read(p []byte) (int, error) {
	n, err := f.r.Read(p)
	if f.b == 0 {
		for _, c := range p[:n] {
			if c != ' ' && c != '\t' && c != '\n' && c != '\r' {
				f.b = c
				break
			}
		}
	}
	return n, err
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
// failure would make a client retry work that succeeded — §1.1's "an element
// can be valid, carry three warnings, and keep moving forward", read at the
// transport.
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
// comparison rather than by convention (§4.4).
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
	w.WriteHeader(status)
	if _, err := w.Write(buf); err != nil {
		return fmt.Errorf("web: writing the response body after status %d: %w", status, err)
	}
	// The trailing newline json.Encoder used to write, as its own write
	// rather than appended: an append onto a marshal result whose length
	// landed on a size class would copy the whole body to add one byte, and
	// net/http buffers the two writes into one flush anyway.
	if _, err := w.Write(jsonNewline); err != nil {
		return fmt.Errorf("web: writing the response body after status %d: %w", status, err)
	}
	return nil
}

// jsonNewline is the one-byte tail [WriteJSON] writes after the body, hoisted
// so the write does not allocate.
var jsonNewline = []byte{'\n'}
