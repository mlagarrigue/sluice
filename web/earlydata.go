package web

import (
	"net/http"

	"github.com/mlagarrigue/sluice/diagnostics"
)

// CodeTooEarly is the diagnostic code of a request refused because it may be
// a replay of early data ([EarlyData]).
const CodeTooEarly = "Transport.Request.TooEarly"

// EarlyData answers 425 Too Early to a request carrying the Early-Data
// header field that allow does not accept, and hands every other request to
// next. Nil allow accepts none.
//
// An intermediary — a CDN, a TLS-terminating proxy — that receives a request
// in TLS early data, before its own handshake with the client has completed,
// forwards it with Early-Data: 1 (RFC 8470 §5.1). Such a request may be a
// replay: anyone who copied the client's first flight can send it again.
// Waiting does not make it safe, since the handshake in question is not this
// server's, so a request that cannot be safely processed MUST be refused with
// 425 (§5.2); the client retries it once its own handshake is through. The
// field's value is not read: §5.1 has an invalid one count as "1".
//
// allow decides which requests a duplicate does not hurt. The method alone
// does not tell (§2: a resource may have side effects on GET), so allow sees
// the whole request:
//
//	h = web.EarlyData(func(r *http.Request) bool {
//		return r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/catalog/")
//	}, h)
//
// A server that sits behind no such intermediary never sees the field, and
// the wrapper costs it one map lookup per request. The same rule, for the
// batch-shaped transport, is httpstream's Config.EarlyData.
func EarlyData(allow func(*http.Request) bool, next http.Handler) http.Handler {
	if next == nil {
		panic("web: EarlyData requires a handler")
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, marked := r.Header["Early-Data"]; marked && (allow == nil || !allow(r)) {
			_ = WriteJSON(w, http.StatusTooEarly, TooEarlyReport())
			return
		}
		next.ServeHTTP(w, r)
	})
}

// TooEarlyReport is the body [EarlyData] answers with: one diagnostic, code
// [CodeTooEarly], saying nothing about the request beyond that it came too
// early.
func TooEarlyReport() Report {
	return ReportOf(diagnostics.NewDiagnostic(diagnostics.Error, CodeTooEarly, diagnostics.Path{}).
		WithMessage("RequestTooEarly", nil).
		WithOrigin(diagnostics.OriginProtocol))
}
