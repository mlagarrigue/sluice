// Package stream is experimental: the middle of the architecture's request
// table for the native transport — routing, authentication, authorization,
// decoding and rendering as batch-shaped stages over [httpstream]'s requests.
//
// # Where it sits
//
// [httpstream] draws the outer arrows — byte to Frame to Request, Response
// back to byte — and [web] holds the supported path's middle over net/http.
// This package is the same middle, protocol-neutral: its stages see only the
// [httpstream.Request] contract every protocol normalises to, so one
// [httpstream.Handler] built from them serves HTTP/1.1, /2 and /3 without
// learning which carried the batch. The credential and grant vocabulary is
// [web]'s — [web.Verifier], [web.Authorizer], [web.Report] — because two
// spellings of the same boundary would drift apart.
//
// It lives outside the core by the library's own test: nothing in the core
// stops compiling if this package is removed. Nothing here is on the
// supported path, and no real consumer has built its request path on these
// stages yet: that is what keeps the label.
//
// # The element is the exchange
//
// Stages need somewhere to put what they learn — the matched route, the
// principal, the refusal — and the batch cannot carry it (ADR 0005: a
// field every operator must copy is a field that gets dropped). So context
// travels in the element type: an [Exchange] wraps one request and
// accumulates the pipeline's findings, which is exactly the wrapper that ADR
// says belongs outside the core.
//
// A handler over these stages reads top to bottom:
//
//	rt := stream.NewRouter(stream.Route{Method: "GET", Pattern: "/orders/{order}"})
//	handle := func(b sluice.Batch[httpstream.Request]) sluice.Batch[httpstream.Response] {
//	    ex := stream.Exchanges(nil, b)
//	    rt.Route(ex)
//	    stream.Authenticate(verifier, ex)
//	    stream.Authorize(authorizer, ex)
//	    for i := range ex {
//	        // decode, run the business stage, remember its answer
//	    }
//	    return stream.Render(ex, func(i int) (int, any) { /* the answer for ex[i] */ })
//	}
//
// # Refusals travel, nothing is filtered
//
// The rule is the gateway's, stated once here: an element removed from a
// batch is a caller left waiting, and over a pipelined connection it answers
// the *wrong* caller, because HTTP/1.1 correlates by position. So no stage
// shortens the batch. A stage that refuses an exchange records a status and
// a diagnostic on it ([Exchange.Refuse]) and every later stage steps over it;
// [Render] answers refused and served alike, positionally. A handler that
// sent only the exchanges still open to a later stage, and holds one answer
// per open exchange, uses [RenderOpen], which hands it each exchange's rank
// among the open ones instead of rebuilding that count itself.
//
// Each stage refuses with the status that names its boundary: no route is
// 404, a method the path exists under is 405, credentials are 401, a grant is
// 403, an unreadable body is 400 or 413. What a *served* exchange's business
// diagnostics map to is the caller's decision, made visibly in the render
// callback — [web.Status] is that mapping when the caller wants the default.
//
// # Bounds and borrowing
//
// An Exchange borrows the request, which borrows the connection's read
// buffer: everything here is valid for the handler call and must be copied if
// retained, the batch contract unchanged. The stages add at most one
// diagnostic each, so a batch's diagnostics are bounded by its stage count
// plus whatever the caller's own stages append — bounding those is the
// caller's, exactly as with [diagnostics.Collector].
package stream

import (
	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/diagnostics"
	"github.com/mlagarrigue/sluice/net/httpstream"
	"github.com/mlagarrigue/sluice/web"
)

// Codes for the refusals this package raises itself, in the same hierarchy as
// [web]'s so a client matches on them the same way.
const (
	// CodeNoRoute reports a target no route in the table matches.
	CodeNoRoute = "Transport.Request.NoRoute"

	// CodeMethodNotAllowed reports a target that exists under other methods.
	CodeMethodNotAllowed = "Transport.Request.MethodNotAllowed"
)

// RouteNone is [Exchange.Route] before routing has run or matched.
const RouteNone = -1

// Exchange is one request travelling the handler: the element the middle
// arrows of §1.1 operate on. Stages fill its fields as the batch moves; a
// refusal is recorded on it rather than removing it, so the exchange keeps
// the position its answer must be written at.
type Exchange struct {
	// Request is what arrived, under the contract [httpstream.Handler]
	// states: authority as "host", raw origin-form target, complete bounded
	// body. Every field borrows the connection's read buffer.
	Request httpstream.Request

	// Route is the index of the matched [Route] in the table given to
	// [NewRouter], or [RouteNone]. Params holds the wildcard values in
	// pattern order, undecoded, borrowing Request.Target.
	Route  int
	Params [][]byte

	// Principal and Grant are what [Authenticate] and [Authorize] establish.
	Principal web.Principal
	Grant     web.Grant

	// Status is zero while nothing has refused the exchange; the first
	// refusal sets it and later stages step over the element. Diagnostics
	// and Affordances accumulate what happened, refusals and warnings alike,
	// and travel with the element to [Render].
	Status      int
	Diagnostics []diagnostics.Diagnostic
	Affordances []diagnostics.Affordance

	// Headers go out on the answer whatever it is — a stage's Allow on a
	// 405, a caller's cache directive. Content-Type is [Render]'s.
	Headers []httpstream.Header
}

// Refuse records a refusal: the first status offered wins — the earliest
// stage saw the request in its most raw form and later stages never ran —
// and every diagnostic is kept.
func (e *Exchange) Refuse(status int, d diagnostics.Diagnostic) {
	if e.Status == 0 {
		e.Status = status
	}
	e.Diagnostics = append(e.Diagnostics, d)
}

// Refused reports whether a stage has refused the exchange. Stages skip a
// refused element; they never remove it.
func (e *Exchange) Refused() bool { return e.Status != 0 }

// Exchanges wraps a transport batch for the stages, appending to dst so a
// handler can reuse one slice across batches. The exchanges alias the
// requests and follow the same lifetime.
func Exchanges(dst []Exchange, b sluice.Batch[httpstream.Request]) []Exchange {
	for _, r := range b.Items {
		dst = append(dst, Exchange{Request: r, Route: RouteNone})
	}
	return dst
}

// foldEqual compares a borrowed header value against a constant,
// ASCII-case-insensitively — HTTP field values that name schemes are ASCII,
// and Unicode folding would be both wrong and slower.
func foldEqual(got []byte, want string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		c, w := got[i], want[i]
		if 'A' <= c && c <= 'Z' {
			c += 'a' - 'A'
		}
		if 'A' <= w && w <= 'Z' {
			w += 'a' - 'A'
		}
		if c != w {
			return false
		}
	}
	return true
}
