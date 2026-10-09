package stream

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"

	"github.com/mlagarrigue/sluice/diagnostics"
	"github.com/mlagarrigue/sluice/web"
	"github.com/mlagarrigue/sluice/web/internal/reqbody"
)

// DecodeJSON reads an open exchange's body into dst, bounded to limit bytes,
// refusing the exchange when it cannot.
//
// It is [web.DecodeJSON] over a body that is already whole: the transport
// buffered and bounded it ([httpstream.Config.MaxBodyBytes]), so the limit
// here is the route's own, tighter, ceiling — a PATCH of one field has no
// business carrying a megabyte, whatever the connection allows. The bound is
// still a required argument with no usable zero, for the same reason web's
// is.
//
// The rules are web's exactly: unknown fields refused, a null body refused
// when dst is a struct, a second JSON value behind the first refused as the
// smuggling it is, and "too large" kept
// distinct from "malformed" because they need different fixes. Here the
// stage also picks the refusal's status — 413 past the limit, 400 otherwise
// — where web leaves that to its net/http caller.
func DecodeJSON(e *Exchange, limit int, dst any) bool {
	if limit <= 0 {
		panic("stream: DecodeJSON needs a positive byte limit; an unbounded body is not a default")
	}
	if e.Refused() {
		return false
	}
	body := e.Request.Body
	if len(body) > limit {
		e.Refuse(413, diagnostics.NewDiagnostic(diagnostics.Error, web.CodeBodyTooLarge, diagnostics.Path{}).
			WithMessage("RequestBodyTooLarge", map[string]any{"limit": limit}).
			WithOrigin(diagnostics.OriginProtocol))
		return false
	}

	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		e.Refuse(400, diagnostics.NewDiagnostic(diagnostics.Error, web.CodeMalformedBody, diagnostics.Path{}).
			WithMessage("RequestBodyMalformed", reqbody.MalformedArgs(err)).
			WithOrigin(diagnostics.OriginProtocol).
			WithCause(err))
		return false
	}
	// null decodes into a struct as nothing, leaving dst zero; refused as web
	// refuses it. A JSON value whose first byte is 'n' is null.
	if reqbody.IntoStruct(dst) {
		if t := bytes.TrimLeft(body, " \t\r\n"); len(t) > 0 && t[0] == 'n' {
			e.Refuse(400, reqbody.Null(web.CodeMalformedBody))
			return false
		}
	}
	if err := dec.Decode(&json.RawMessage{}); !errors.Is(err, io.EOF) {
		e.Refuse(400, diagnostics.NewDiagnostic(diagnostics.Error, web.CodeMalformedBody, diagnostics.Path{}).
			WithMessage("RequestBodyTrailingContent", nil).
			WithOrigin(diagnostics.OriginProtocol))
		return false
	}
	return true
}
