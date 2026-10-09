package stream

import (
	"encoding/json"
	"slices"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/net/httpstream"
	"github.com/mlagarrigue/sluice/web"
)

// Render answers every exchange, refused and served alike, as one positional
// batch of JSON responses — the batch-shaped sibling of writing a
// [web.Report] with [web.WriteJSON].
//
// A refused exchange is answered with the status its stage chose and the
// [web.Report] of its problems and remedies, which is what makes a refusal
// actionable rather than merely correct. For the rest, serve is called with
// the exchange's index and returns the status and body of the business
// answer; it is not called for a refused one. The two decisions that matter
// — the status and what the body says — stay in the caller's callback,
// where §1.1's example insists they remain visible; [web.Status] and
// [web.Problems] are the defaults to reach for there.
//
// Every response with a body carries Content-Type: application/json, then
// whatever [Exchange.Headers] accumulated. A 204 or 304 carries no body and
// no Content-Type, whatever serve returned for it. A body that will not
// marshal is answered as [web.WriteJSON] answers it — the intended status
// with an empty body and no Content-Type, since a right status with no
// document beats a wrong status with one. A
// response that streams is not this function's: build it directly and skip
// Render for that batch.
func Render(exchanges []Exchange, serve func(i int) (status int, body any)) sluice.Batch[httpstream.Response] {
	return RenderOpen(exchanges, func(i, _ int) (int, any) { return serve(i) })
}

// RenderOpen is [Render] for the handler that submitted one call per open
// exchange: serve also receives open, the exchange's rank among those not
// refused — 0 for the first served exchange, 1 for the next, in batch order.
//
// That rank is the index into a slice built by appending once per exchange
// still open after the last refusal, which is how a composed handler builds
// its gateway submission:
//
//	for i := range ex {
//	    // refuse, or append exactly one call for ex[i]
//	}
//	outcomes := gw.DoBatch(ctx, calls)
//	return stream.RenderOpen(ex, func(_, open int) (int, any) {
//	    o := outcomes[open]
//	    ...
//	})
//
// so the exchange-to-call mapping every such handler would otherwise keep by
// hand — an index of call positions and its inverse — is not written at all.
// The contract it rests on is that loop's: an exchange left open without a
// call shifts every later rank, so anything skipped is refused.
func RenderOpen(exchanges []Exchange, serve func(i, open int) (status int, body any)) sluice.Batch[httpstream.Response] {
	out := make([]httpstream.Response, len(exchanges))
	open := 0
	for i := range exchanges {
		e := &exchanges[i]
		var status int
		var body any
		if e.Refused() {
			status = e.Status
			body = web.Report{
				Problems: web.Problems(e.Diagnostics),
				Remedies: web.Remedies(e.Affordances),
			}
		} else {
			status, body = serve(i, open)
			open++
		}

		// A 204 or 304 has no content (RFC 9110 §15.3.5, §15.4.5), so there
		// is nothing for a Content-Type to describe and no body to marshal;
		// the transport would drop the bytes anyway.
		if status == 204 || status == 304 {
			out[i] = httpstream.Response{Status: status, Headers: slices.Clone(e.Headers)}
			continue
		}
		buf, err := json.Marshal(body)
		if err != nil {
			// Empty body, so no Content-Type: an empty application/json
			// payload is not a JSON document, and a client trusting the
			// header would fail to parse it.
			out[i] = httpstream.Response{Status: status, Headers: slices.Clone(e.Headers)}
			continue
		}
		headers := make([]httpstream.Header, 0, len(e.Headers)+1)
		headers = append(headers, httpstream.Header{Name: contentTypeName, Value: contentTypeJSON})
		headers = append(headers, e.Headers...)
		out[i] = httpstream.Response{Status: status, Headers: headers, Body: append(buf, '\n')}
	}
	return sluice.Batch[httpstream.Response]{Items: out}
}

// The one field every answer carries, shared rather than rebuilt per
// response.
var (
	contentTypeName = []byte("content-type")
	contentTypeJSON = []byte("application/json")
)
