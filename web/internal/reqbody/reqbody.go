// Package reqbody holds what web and web/stream say in the same words about a
// request body: which targets refuse a JSON null, and what a client may be
// told about a decoding error. Two packages building the same diagnostic
// must build it identically, and this is where the one copy lives.
package reqbody

import (
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"reflect"
	"strings"

	"github.com/mlagarrigue/sluice/diagnostics"
)

// IntoStruct reports whether dst is a pointer to a struct — the target for
// which a JSON null body is refused rather than decoded as no change.
func IntoStruct(dst any) bool {
	t := reflect.TypeOf(dst)
	return t != nil && t.Kind() == reflect.Pointer && t.Elem().Kind() == reflect.Struct
}

// Null is the refusal of a JSON null where an object was expected. It is
// a malformed body as far as the client is concerned — the document is not
// the request the endpoint reads — with a message of its own so the client
// is told what was wrong with JSON that parsed.
func Null(code string) diagnostics.Diagnostic {
	return diagnostics.NewDiagnostic(diagnostics.Error, code, diagnostics.Path{}).
		WithMessage("RequestBodyNull", nil).
		WithOrigin(diagnostics.OriginProtocol)
}

// JSONMediaType reports whether a Content-Type value names a JSON document:
// application/json, or any subtype with the +json structured syntax suffix
// (RFC 6839 §3.1), such as application/merge-patch+json. Parameters are
// ignored — JSON defines none that change how it is read (RFC 8259 §11) —
// and the comparison is case-insensitive, as RFC 9110 §8.3.1 has it. It
// allocates nothing: it runs on every request that carries a body.
func JSONMediaType[T string | []byte](v T) bool {
	end := len(v)
	for i := range len(v) {
		if v[i] == ';' {
			end = i
			break
		}
	}
	start := 0
	for start < end && (v[start] == ' ' || v[start] == '\t') {
		start++
	}
	for end > start && (v[end-1] == ' ' || v[end-1] == '\t') {
		end--
	}
	// One media type, type "/" subtype, each a token (RFC 9110 §8.3.1): a
	// comma, a blank or a second slash inside makes it something else — a
	// list a hop reading its first member reads as another type.
	slash := -1
	for i := start; i < end; i++ {
		switch {
		case v[i] == '/' && slash < 0:
			slash = i
		case !isTChar(v[i]):
			return false
		}
	}
	if slash <= start || slash == end-1 {
		return false
	}
	if foldEqual(v, start, end, "application/json") {
		return true
	}
	// The suffix needs a subtype name in front of it.
	const suffix = "+json"
	return end-slash-1 > len(suffix) && foldEqual(v, end-len(suffix), end, suffix)
}

// isTChar reports whether c may appear in a token (RFC 9110 §5.6.2).
func isTChar(c byte) bool {
	switch {
	case 'a' <= c && c <= 'z', 'A' <= c && c <= 'Z', '0' <= c && c <= '9':
		return true
	}
	switch c {
	case '!', '#', '$', '%', '&', '\'', '*', '+', '-', '.', '^', '_', '`', '|', '~':
		return true
	}
	return false
}

// foldEqual reports whether v[start:end] equals want, ASCII case folded; want
// is lower case.
func foldEqual[T string | []byte](v T, start, end int, want string) bool {
	if end-start != len(want) {
		return false
	}
	for i := range len(want) {
		c := v[start+i]
		if 'A' <= c && c <= 'Z' {
			c += 'a' - 'A'
		}
		if c != want[i] {
			return false
		}
	}
	return true
}

// MediaType is the refusal of a body whose Content-Type is missing, repeated,
// or not JSON. A browser sends a text/plain, form or multipart body to another
// origin without asking first (no CORS preflight), so decoding JSON whatever
// the label says lets any page a user visits post to the endpoint with the
// user's cookies; a JSON label is what forces the preflight. The client is
// told what is accepted, not what it sent: the value is its own.
func MediaType(code string) diagnostics.Diagnostic {
	return diagnostics.NewDiagnostic(diagnostics.Error, code, diagnostics.Path{}).
		WithMessage("RequestBodyMediaType", map[string]any{"accepted": "application/json"}).
		WithOrigin(diagnostics.OriginProtocol)
}

// MalformedArgs builds the client-visible arguments of a
// RequestBodyMalformed message from a JSON decoding error.
//
// encoding/json's error text names Go types and struct fields — the server's
// internals, not the client's document — so the text itself never travels;
// copying it into the response would undo structurally what
// [diagnostics.Diagnostic] keeps unexported (its S7 guarantee). What does
// travel is what the client can act on: the byte offset of a syntax error,
// the JSON field and offset of a type mismatch, and the name an unknown-field
// refusal quotes, which is the client's own input. Anything else gets no
// detail at all. The full error belongs on the diagnostic's cause, where only
// the log can reach it.
func MalformedArgs(err error) map[string]any {
	if syn, ok := errors.AsType[*json.SyntaxError](err); ok {
		return map[string]any{"offset": syn.Offset}
	}
	if typ, ok := errors.AsType[*json.UnmarshalTypeError](err); ok {
		args := map[string]any{"offset": typ.Offset}
		if typ.Field != "" {
			args["field"] = typ.Field
		}
		return args
	}
	// DisallowUnknownFields reports a plain error; the quoted name is the
	// client's own field, safe to hand back.
	if name, ok := strings.CutPrefix(err.Error(), "json: unknown field "); ok {
		return map[string]any{"field": strings.Trim(name, `"`)}
	}
	return nil
}

// Decode reads one JSON document from r into dst under the rules both
// DecodeJSON functions state — unknown fields refused, null refused into a
// struct, a second value refused as smuggled — and returns the refusal under
// code, or under tooLarge past the limit, or ok. Past limit, r must fail with an *http.MaxBytesError, as
// http.MaxBytesReader does; a caller that already knows the size checks it
// before. The two paths differ only in how they learn a body is too large and
// in who picks the status.
func Decode(r io.Reader, limit int64, dst any, code, tooLarge string) (diagnostics.Diagnostic, bool) {
	var first *firstByteReader
	if IntoStruct(dst) {
		first = &firstByteReader{r: r}
		r = first
	}
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
			return TooLarge(tooLarge, limit), false
		}
		// The server's read deadline ran out under the body: the client
		// stopped sending, which is not a syntax error and must not read as
		// one.
		if ne, ok := errors.AsType[net.Error](err); ok && ne.Timeout() {
			return diagnostics.NewDiagnostic(diagnostics.Error, code, diagnostics.Path{}).
				WithMessage("RequestBodyTimeout", nil).
				WithOrigin(diagnostics.OriginProtocol).
				WithCause(err), false
		}
		return diagnostics.NewDiagnostic(diagnostics.Error, code, diagnostics.Path{}).
			WithMessage("RequestBodyMalformed", MalformedArgs(err)).
			WithOrigin(diagnostics.OriginProtocol).
			WithCause(err), false
	}
	// A JSON value that starts with 'n' is null, and null decodes into a
	// struct as nothing at all: dst keeps its zero fields and the handler
	// proceeds on a request nobody wrote.
	if first != nil && first.b == 'n' {
		return Null(code), false
	}
	// A second value in the body is not a stray byte, it is a different
	// request smuggled behind the first. Refused rather than ignored.
	if err := dec.Decode(&json.RawMessage{}); !errors.Is(err, io.EOF) {
		return diagnostics.NewDiagnostic(diagnostics.Error, code, diagnostics.Path{}).
			WithMessage("RequestBodyTrailingContent", nil).
			WithOrigin(diagnostics.OriginProtocol), false
	}
	return diagnostics.Diagnostic{}, true
}

// TooLarge is the refusal of a body past limit bytes, under code.
func TooLarge(code string, limit int64) diagnostics.Diagnostic {
	return diagnostics.NewDiagnostic(diagnostics.Error, code, diagnostics.Path{}).
		WithMessage("RequestBodyTooLarge", map[string]any{"limit": limit}).
		WithOrigin(diagnostics.OriginProtocol)
}

// Truncated is the refusal of a body that stopped arriving under code: the
// client left, its framing broke, or a read waited past the timeout. It is
// kept apart from a malformed document because the fix is not the client's
// JSON but its connection, and the server never saw enough bytes to say
// anything about the document itself.
func Truncated(code string) diagnostics.Diagnostic {
	return diagnostics.NewDiagnostic(diagnostics.Error, code, diagnostics.Path{}).
		WithMessage("RequestBodyTruncated", nil).
		WithOrigin(diagnostics.OriginProtocol)
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
