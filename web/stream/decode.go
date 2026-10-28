package stream

import (
	"bytes"
	"errors"
	"math"

	"github.com/mlagarrigue/sluice/net/httpstream"
	"github.com/mlagarrigue/sluice/web"
	"github.com/mlagarrigue/sluice/web/internal/reqbody"
)

// DecodeJSON reads an open exchange's body into dst, bounded to limit bytes,
// refusing the exchange when it cannot.
//
// It is [web.DecodeJSON] over an exchange's body, whichever form it arrived
// in. The transport bounded it ([httpstream.Config.MaxBodyBytes]), so the
// limit here is the route's own, tighter, ceiling — a PATCH of one field has
// no business carrying a megabyte, whatever the connection allows. The bound
// is still a required argument with no usable zero, for the same reason web's
// is.
//
// A body that arrives through [httpstream.Request.Stream] — the default, the
// zero [httpstream.Config.BodyDelivery] being [httpstream.BodyStreamed] — is
// assembled here, up to limit, by [httpstream.Request.ReadAll]: a stage
// decoding one JSON value needs the value whole, and this is the step that
// turns the streamed form back into it. Nothing about the refusals changes
// with the form, which is the point: a body that rode along with the request's
// headers and one that arrived in a read of its own decode the same way.
//
// The rules are web's exactly: one Content-Type naming JSON, unknown fields
// refused, a null body refused
// when dst is a struct, a second JSON value behind the first refused as the
// smuggling it is, and "too large" kept
// distinct from "malformed" because they need different fixes. Here the
// stage also picks the refusal's status — 413 past the limit, 400 otherwise
// — where web leaves that to its net/http caller. So are the three readings
// web.DecodeJSON names as encoding/json's: a repeated name's last value
// wins and names match fields without regard to case, invalid UTF-8 and a
// lone surrogate become U+FFFD, a byte order mark is refused.
func DecodeJSON(e *Exchange, limit int64, dst any) bool {
	if limit <= 0 {
		panic("stream: DecodeJSON needs a positive byte limit; an unbounded body is not a default")
	}
	if e.Refused() {
		return false
	}
	if !jsonBody(e.Request.Headers) {
		e.Refuse(400, reqbody.MediaType(web.CodeMalformedBody))
		return false
	}
	body := e.Request.Body
	if body == nil {
		var err error
		if body, err = e.Request.ReadAll(nil, bound(limit)); err != nil {
			if errors.Is(err, httpstream.ErrTooLarge) {
				e.Refuse(413, reqbody.TooLarge(web.CodeBodyTooLarge, limit))
			} else {
				e.Refuse(400, reqbody.Truncated(web.CodeMalformedBody))
			}
			return false
		}
	}
	if int64(len(body)) > limit {
		e.Refuse(413, reqbody.TooLarge(web.CodeBodyTooLarge, limit))
		return false
	}
	if d, ok := reqbody.Decode(bytes.NewReader(body), limit, dst, web.CodeMalformedBody, web.CodeBodyTooLarge); !ok {
		e.Refuse(400, d)
		return false
	}
	return true
}

// bound turns the stage's limit into the int [httpstream.Request.ReadAll]
// takes. A limit past the largest int cannot bound anything on this platform,
// so it becomes that int: the transport's own MaxBodyBytes still holds.
func bound(limit int64) int {
	if limit > math.MaxInt {
		return math.MaxInt
	}
	return int(limit)
}

// jsonBody reports whether the request carries exactly one Content-Type and
// it names JSON, under the rule [web.DecodeJSON] states. Two lines are not a
// list to choose from: the field is not list-valued, and a request that
// labels its body twice has not said what it is.
func jsonBody(headers []httpstream.Header) bool {
	var ct []byte
	found := false
	for _, h := range headers {
		if !foldEqual(h.Name, "content-type") {
			continue
		}
		if found {
			return false
		}
		ct, found = h.Value, true
	}
	return found && reqbody.JSONMediaType(ct)
}
