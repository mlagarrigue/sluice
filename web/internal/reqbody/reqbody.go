// Package reqbody holds what web and web/stream say in the same words about a
// request body: which targets refuse a JSON null, and what a client may be
// told about a decoding error. Two packages building the same diagnostic
// must build it identically, and this is where the one copy lives.
package reqbody

import (
	"encoding/json"
	"errors"
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
