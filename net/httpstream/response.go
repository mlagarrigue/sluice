package httpstream

import (
	"bytes"
	"fmt"
	"strconv"
	"time"

	"github.com/mlagarrigue/sluice"
)

// Response is what goes back for one request. Its fields may borrow what the
// pipeline had, on one condition: Body, Headers and the fields Trailers
// returns stay valid and unchanged until the response has been written.
// That moment is not the handler's return. HTTP/2 and HTTP/3 frame and send
// answers on goroutines of their own while later batches of the connection
// are being handled, and a write waiting on the client's flow-control window
// lasts as long as [Config.WriteTimeout] sees it progress; nothing tells the
// handler when it is over. A buffer the pipeline hands back to a pool or
// overwrites for its next batch is therefore not one to borrow from:
// allocate the body, or borrow from data that no longer changes. HTTP/1.1
// writes a batch before it reads the next, but a handler is not told the
// protocol, and the rule is the one that holds on all three.
type Response struct {
	// Status is the HTTP status code. Zero means 200. The writers refuse,
	// with [ErrMalformed], a code outside 200..599 (a 1xx is never a final
	// answer), a 426 (RFC 9110 §15.5.22 demands an Upgrade field, and this
	// server never switches protocols; advertise one with Alt-Svc instead)
	// and a 205 carrying a Body or a Stream (§15.3.6: no content on a
	// "reset the form" answer; a bare 205 leaves with Content-Length: 0).
	Status int

	// Headers are sent as given. Names are case-insensitive (RFC 9110
	// §5.1): HTTP/1.1 writes them as spelled, HTTP/2 and HTTP/3 put them
	// on the wire in lowercase, as those protocols require, without
	// touching the handler's bytes. Content-Length is written by
	// [WriteBatch] from the body, or from ContentLength for a Stream, and
	// must not be set here. Date is added by the writer when the handler
	// did not set one (RFC 9110 §6.6.1), on every final response, 5xx
	// included, ahead of these fields; a handler's own Date stays where
	// the handler put it, and must be an IMF-fixdate (§5.6.7), the one
	// form a sender may generate.
	//
	// The three writers refuse, with [ErrMalformed] or the protocol's
	// error, what no response may carry whatever the field: a name that is
	// not a token, a value holding CR, LF or another control octet
	// ([ErrHeaderInjection]), the framing and connection fields this
	// package states itself (Content-Length, Transfer-Encoding,
	// Connection, Keep-Alive, Proxy-Connection, Upgrade, a Trailer without
	// Trailers), the fields only a request may carry (Host, TE, Expect:
	// §2.2 forbids a sender the elements of another role), and a Date that
	// is not an IMF-fixdate. The rest of RFC 9110's sender rules is the
	// handler's, because it turns on each field's definition, which the
	// writer does not know: the grammar of a value (§2.2 — a Vary with an
	// empty member, a Content-Type without a subtype, go out as given) and
	// one line per field unless its definition makes it a list (§5.3 —
	// two Content-Type lines go out as two). A writer that checked these
	// would carry a catalogue of every field, and be wrong about the ones
	// defined after it.
	Headers []Header

	// Body is the payload, sent verbatim. Use it for an answer that is
	// already in hand, which is most of them.
	//
	// A HEAD request is answered like its GET: the writer states the
	// length of Body and sends none of its bytes, so a handler builds the
	// same response for both, as the [web/stream] router makes it do. A
	// HEAD answered with neither a Body nor a Stream states no
	// Content-Length at all, because RFC 9110 §8.6 allows one on a HEAD
	// response only when it is the GET twin's, which such a handler never
	// said; a GET answered the same way states Content-Length: 0.
	Body []byte

	// Stream produces the payload as it goes, for an answer that is not.
	//
	// It is what makes the response half of this package a stream stage
	// rather than a buffer with a stage in front of it: a report assembled
	// from a database cursor, an export, an event feed. The transport pulls,
	// so the producer never runs ahead of the socket and memory is bounded by
	// one batch rather than by the answer.
	//
	// Setting both Stream and Body is a programming error and is refused: the
	// two say different things about what the payload is.
	//
	// A streamed response has no Content-Length unless ContentLength
	// declares one. Each protocol ends the body its own way — HTTP/1.1 in
	// chunks, HTTP/2 and HTTP/3 in DATA frames ended by a flag — and none of
	// them needs the total in advance, nor do the proxies in front: a
	// chunked answer crosses nginx and Envoy as it is. A handler that
	// already holds the whole payload puts it in Body and gets its
	// Content-Length from the writer.
	//
	// A yield that returns false means the stream or the connection is gone;
	// the package documentation states what each protocol does then.
	//
	// Over HTTP/1.1, a client that half-closes its connection once its
	// request is sent — shutdown(SHUT_WR), as netcat and the simplest
	// HTTP/1.0 clients do — is still reading, and gets the whole answer
	// (RFC 9112 §9.6). Its EOF is not its departure: past it, the client
	// shows it left only by resetting the connection, which it does when a
	// write reaches it. A producer that yields nothing to write after the
	// half-close — empty batches, cadence — is therefore stopped by Serve's
	// shutdown or by its own end, not by the client going away, and
	// [Config.WriteTimeout] does not bound it either, since no write is under
	// way. A time bound was weighed and refused: it would cut, after
	// WriteTimeout of silence, the very client the RFC says is still there.
	Stream sluice.Stream[[]byte]

	// ContentLength, when positive, is the number of bytes Stream will
	// produce, sent as the response's Content-Length: HTTP/1.1 then frames
	// the body by it rather than in chunks, and the client can show how far
	// along it is. Zero means the length is not known, which is how a
	// streamed response goes out otherwise; an empty payload is a nil Body.
	// It is refused beside a Body, whose length is its own, without a
	// Stream, and when negative.
	//
	// The declared length is held to what the producer then yields, since a
	// length the producer misses by a byte is the framing desynchronisation
	// this package refuses everywhere else. A batch that would take the body
	// past it is not written, and the pull stops there; a producer that
	// ends short of it, or goes past it, ends the response unterminated —
	// the connection closed on HTTP/1.1, the stream reset on HTTP/2 and
	// HTTP/3 — so the client never reads a truncated answer as whole. On
	// HTTP/1.1 a producer that yields more after the declared length went
	// out whole only closes the connection: its client already holds the
	// bytes it was promised, and the surplus never reaches it.
	//
	// HTTP/1.1 carries trailers only after a chunked body, so a response
	// with Trailers is chunked there and its Content-Length not written —
	// the two framings cannot be sent together (RFC 9112 §6.2) — though the
	// length is still checked; HTTP/2 and HTTP/3 send both. A HEAD response
	// states the length without pulling the producer, and a 204 or 304
	// never states one, as for a Body.
	ContentLength int64

	// Trailers, when set, gives the fields sent after the payload: a
	// checksum computed as the body went out, or gRPC's grpc-status, which
	// a streamed answer only knows once its producer is done. It is a
	// function rather than a list for that reason: it is called once the
	// payload is complete — after Stream has returned, or straight away
	// for a Body — so a producer can leave in a variable what the trailers
	// then report.
	//
	// HTTP/1.1 sends them after the last chunk, which makes a Body response
	// with trailers chunked rather than Content-Length framed; HTTP/2 and
	// HTTP/3 in a HEADERS frame that ends the stream. They are held to the
	// header rules, and refused when they name a field RFC 9110 §6.5.1
	// says cannot be processed after the content — the ones of its own
	// that frame the message, describe the connection, route it,
	// authenticate it, control the response or describe the content's
	// format (see refusedTrailers): those were settled by the time the
	// trailers arrive. Any other field is the handler's to know it may be
	// sent there, a field from another specification (Set-Cookie, a
	// Cache-Control of RFC 9111) included. A Trailer header naming
	// them, which RFC 9110 §6.6.2 says a sender SHOULD send, is the
	// handler's to set; HTTP/1.1 refuses it on a response without
	// Trailers, as the promise of fields that never come.
	//
	// The function is not called for a response that carries no body —
	// a HEAD answer, a 204 or 304 — nor for an HTTP/1.0 client, whose
	// response has no chunked framing to put them in: those fields would
	// have nowhere to go. A function that panics, or returns a field the
	// rules refuse, fails the response as a refused header does when
	// nothing is written yet (a 500), and otherwise ends it unterminated —
	// the connection closed on HTTP/1.1, the stream reset on HTTP/2 and
	// HTTP/3 — so the client never reads a truncated answer as whole.
	Trailers func() []Header
}

// streamed reports whether the response produces its payload as it goes.
func (r Response) streamed() bool { return r.Stream != nil }

// trailers calls the response's Trailers function and checks what it
// returns, turning a panic in it into an error, as pull does for the
// producer: it runs on the writing goroutine too.
func (r Response) trailers() (fields []Header, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("httpstream: the response trailers panicked: %v", p)
		}
	}()
	fields = r.Trailers()
	for _, h := range fields {
		if err := checkTrailer(h); err != nil {
			return nil, err
		}
	}
	return fields, nil
}

// checkTrailer holds one response trailer to the octet rules every response
// header is held to, and refuses the fields RFC 9110 §6.5.1 says cannot be
// processed after the body (refusedTrailers), the request's own fields
// among them. The names are compared case-insensitively, so the one rule
// serves the HTTP/1.1 spelling and the lowercase HTTP/2 and HTTP/3 put on
// the wire.
func checkTrailer(h Header) error {
	if !isToken(h.Name) {
		return fmt.Errorf("%w: %q is not a trailer name", ErrMalformed, h.Name)
	}
	if hasCRLF(h.Value) {
		return ErrHeaderInjection
	}
	if hasFieldControl(h.Value) {
		return fmt.Errorf("%w: the value of trailer %q holds a control character", ErrMalformed, h.Name)
	}
	if err := checkResponseField(h.Name); err != nil {
		return err
	}
	for _, name := range refusedTrailers {
		if equalFold(h.Name, name) {
			return fmt.Errorf("%w: %s cannot be sent as a trailer", ErrMalformed, h.Name)
		}
	}
	return nil
}

// refusedTrailers are the fields checkTrailer refuses: RFC 9110 §6.5.1
// names the kinds of field whose "evaluation is necessary prior to
// receiving the content", and these are the fields RFC 9110 itself defines
// in each kind. Framing and the connection first, settled before the body
// and carried end to end by no protocol — Trailer among them, since a
// trailer cannot announce more trailers; then routing (§7.2), authentication
// (§11), the response controls (§10.2) and the content's format (§8.3 to
// §8.7). Validators (§8.8) stay allowed: an ETag computed as the body went
// out is what trailers are for. A field of another specification is not
// here: this package does not keep that catalogue, and the handler knows
// its own fields. The request's own fields are refused before this list,
// by checkResponseField.
var refusedTrailers = [...]string{
	"content-length", "transfer-encoding", "trailer", "te",
	"connection", "keep-alive", "proxy-connection", "upgrade",
	"host",
	"authorization", "proxy-authorization", "www-authenticate", "proxy-authenticate",
	"authentication-info", "proxy-authentication-info",
	"accept-ranges", "age", "location", "retry-after", "server", "vary",
	"content-type", "content-encoding", "content-language", "content-location", "content-range",
}

// checkResponseField refuses a field only a request may carry. RFC 9110
// §2.2 forbids a sender "protocol elements or syntax alternatives that are
// only allowed to be generated by participants in other roles", and a
// response is not the role of Host (§7.2, the request's routing), TE
// (§10.1.4, what the client accepts) or Expect (§10.1.1, what the client
// expects of the server). Each would tell a recipient something about a
// request it is not reading. One point for the three writers and the
// trailers; the other request fields (Accept, Range, If-Match…) are not
// listed: they mislead no one about the message, and a catalogue of every
// request field is a mirror this package would keep wrong.
func checkResponseField(name []byte) error {
	if equalFold(name, "host") || equalFold(name, "te") || equalFold(name, "expect") {
		return fmt.Errorf("%w: %s is a request field, not a response's", ErrMalformed, name)
	}
	return nil
}

// pull runs a streamed response's producer, turning a panic in it into an
// error the transport answers on that one response. The handler's own panics
// are caught by apply, but a producer runs later, on the writing goroutine,
// after apply has returned — without this, a producer's bug ends the process
// rather than its own stream, which is the opposite of the package's promise.
func pull(stream sluice.Stream[[]byte], yield func(sluice.Batch[[]byte]) bool) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("httpstream: the response producer panicked: %v", r)
		}
	}()
	stream(yield)
	return nil
}

// check refuses a response that says two things about its payload, a
// length that describes none, or content on a 205.
//
// A 205 with a Body or a Stream is refused rather than sent empty the way a
// 204 or 304 is: RFC 9110 §15.3.6 forbids generating content on it, and a
// handler that built content for a "reset the form" answer has mixed up two
// statuses — silently dropping it would hide that. A 205 without content
// leaves as every other 2xx does, with Content-Length: 0 — RFC 9112 §6.3
// frames a 205 like any response, so the length is what keeps the
// connection in step. This is the one point the three writers go through.
func (r Response) check() error {
	if r.Stream != nil && len(r.Body) > 0 {
		return fmt.Errorf("%w: a response with both a Body and a Stream", ErrMalformed)
	}
	if r.Status == 205 && (r.Stream != nil || len(r.Body) > 0) {
		return fmt.Errorf("%w: content on a 205 response", ErrMalformed)
	}
	if r.ContentLength < 0 {
		return fmt.Errorf("%w: a negative ContentLength", ErrMalformed)
	}
	if r.ContentLength > 0 && r.Stream == nil {
		return fmt.Errorf("%w: a ContentLength without a Stream", ErrMalformed)
	}
	return nil
}

// errLengthMismatch reports a streamed body that did not produce the
// ContentLength it declared.
var errLengthMismatch = fmt.Errorf("%w: the response producer did not yield its ContentLength", ErrMalformed)

// overLength reports whether a batch of chunks would take a body past its
// declared length want, zero for none, counting from sent, the bytes
// already out; a batch that would not is added to sent.
func overLength(sent *int64, want int64, chunks [][]byte) bool {
	if want == 0 {
		return false
	}
	n := *sent
	for _, c := range chunks {
		n += int64(len(c))
	}
	if n > want {
		return true
	}
	*sent = n
	return false
}

// ErrHeaderInjection reports a response header this package will not write.
//
// A value carrying CR or LF would end the field early and let whatever
// follows be read as another header or another response — response splitting,
// which is only closable at the point of writing. Data that reached a header
// from a request, a database or a template is exactly the data this catches.
var ErrHeaderInjection = fmt.Errorf("%w: a response header holds CR or LF", ErrMalformed)

// WriteBatch writes a batch of responses in order, as one write.
//
// The order is the contract HTTP/1.1 imposes on a pipelined connection: the
// nth response answers the nth request, and there is no field saying
// otherwise. A pipeline that reorders — [github.com/mlagarrigue/sluice/parallel.Unordered], a filter
// that drops an element — breaks that silently, which is why [Serve] takes a
// batch-to-batch function and checks the count rather than accepting any
// stream and hoping.
//
// One write per batch, not per response: a pipelined client that sent eight
// requests gets eight answers in one syscall, which is the other half of what
// batching the transport buys.
//
// requests is the batch these responses answer, positionally, the same
// correlation [Handler] promises. It is read, never retained: the nth
// request's method decides whether the nth response carries a body — a HEAD
// answer never does, whatever the handler put in it — and, when the nth
// request is the last one and said it would not keep the connection alive,
// the last response is told to say so too, since a client that gets no
// Connection: close on its final answer keeps pipelining into a socket about
// to close under it. requests may be shorter than responses or nil; the
// responses past its end are written as if a body were always allowed and
// the connection always continued.
//
// Every response that does not close says Connection: keep-alive and
// Keep-Alive: timeout=<seconds>, from cfg's IdleTimeout, as [Serve] writes
// them: a caller serving the connection itself answers the way Serve would
// (see appendKeepAlive). IdleTimeout is the one field of cfg it reads, and it
// is resolved as Serve resolves it: zero is the default, and a negative value
// is refused with the same error. The other fields are not checked here —
// resolving them all on every batch would cost more than writing one.
func WriteBatch(dst []byte, cfg Config, requests []Request, responses []Response) ([]byte, error) {
	idle := cfg.IdleTimeout
	if err := orDefault("Config.IdleTimeout", &idle, defaultIdleTimeout); err != nil {
		return nil, err
	}
	// Formatted once per call, on the stack, rather than once per response.
	var field [keepAliveFieldMax]byte
	keepAlive := appendKeepAlive(field[:0], idle)
	closeAfter := asksToClose(responses)
	date := dateFor(time.Now())
	for i, res := range responses {
		var method []byte
		http10 := false
		if i < len(requests) {
			method, http10 = requests[i].Method, requests[i].http10
		}
		closing := i == len(responses)-1 && (closeAfter || i < len(requests) && !requests[i].KeepAlive)
		var err error
		if dst, err = appendResponse(dst, method, closing, http10, keepAlive, res, date); err != nil {
			return nil, err
		}
	}
	return dst, nil
}

// keepAliveFieldMax holds appendKeepAlive's longest output: the fixed text
// and the 10 digits of the largest whole-second time.Duration.
const keepAliveFieldMax = len("Connection: keep-alive\r\nKeep-Alive: timeout=\r\n") + 10

// appendKeepAlive writes what an HTTP/1.1 response that does not close says
// about the connection: Connection: keep-alive, then Keep-Alive:
// timeout=<seconds> with idle, the connection's IdleTimeout, rounded down to
// whole seconds.
//
// The timeout is for the proxy in front: nginx keeps an upstream connection
// 60s by default and Envoy an hour, and one that outlives this server's idle
// timeout gets reused the moment it is being closed — a reset on a request
// that was never read, which the proxy reports as a 502. A proxy that reads
// the field closes first (RFC 2068 §19.7.1.1 defines it; RFC 9110 §7.6.1
// requires Connection to name it, since it is hop-by-hop). Neither of those
// two does: nginx ignores it (ngx_http_upstream.c) and Envoy strips it, so
// in front of them the proxy's own upstream idle timeout is what must be set
// below this one, as docs/guide/experimental.md shows. The option also
// answers an HTTP/1.0 client's Connection: keep-alive, which RFC 9112 §9.3
// honours only when the response says keep-alive back; such a client was
// served one request per connection before.
//
// Rounded down, never up: a timeout the server does not honour is the reset
// again. Zero seconds is written as such — the socket closes within the
// second, and the field says so.
func appendKeepAlive(dst []byte, idle time.Duration) []byte {
	dst = append(dst, "Connection: keep-alive\r\nKeep-Alive: timeout="...)
	dst = strconv.AppendInt(dst, int64(idle/time.Second), 10)
	return append(dst, '\r', '\n')
}

// appendConnection writes what the response says about the connection: the
// writer's own Connection: close on the batch's last response when nothing
// follows, else keepAlive, appendKeepAlive's announcement.
func appendConnection(dst []byte, closing bool, keepAlive []byte) []byte {
	if closing {
		return append(dst, "Connection: close\r\n"...)
	}
	return append(dst, keepAlive...)
}

// appendChunked writes one chunk of a streamed HTTP/1.1 body: its length in
// hexadecimal, the bytes, and a line ending. An empty chunk ends the body,
// which is why an empty batch must never be written as one.
func appendChunk(dst, chunk []byte) []byte {
	if len(chunk) == 0 {
		return dst // an empty batch is not the end; Filter emits these
	}
	dst = strconv.AppendUint(dst, uint64(len(chunk)), 16)
	dst = append(dst, '\r', '\n')
	dst = append(dst, chunk...)
	return append(dst, '\r', '\n')
}

// appendLastChunk closes a chunked body. Writing chunks is the easy half:
// one writer, no second framing for two hops to disagree about. The request
// side decodes the coding too now — see decodeChunked — but only in the one
// shape checkFraming leaves no disagreement in: chunked alone, with no
// Content-Length beside it.
func appendLastChunk(dst []byte) []byte {
	return append(dst, '0', '\r', '\n', '\r', '\n')
}

// appendLastChunkTrailers closes a chunked body with a trailer section
// (RFC 9112 §7.1.2): the last chunk, the fields as spelled, the empty line.
// fields have passed checkTrailer.
func appendLastChunkTrailers(dst []byte, fields []Header) []byte {
	dst = append(dst, '0', '\r', '\n')
	for _, h := range fields {
		dst = append(dst, h.Name...)
		dst = append(dst, ':', ' ')
		dst = append(dst, h.Value...)
		dst = append(dst, '\r', '\n')
	}
	return append(dst, '\r', '\n')
}

// appendStreamedHeader writes the status and headers of a produced response,
// framed as [streamedFraming] decided: chunked, by a length of size bytes, or
// neither. A HEAD or 204/304 response here must not announce a chunked body
// it will never send, or a pipelined client reads the next response's bytes
// as this one's body; a HEAD response still states the length its GET twin
// would.
//
// A response framed neither way is close-delimited — no framing header at
// all, the empty line to EOF — and the caller has already marked it closing,
// since EOF is the only end such a body has.
//
// keepAlive is the connection's announcement for a response that is not
// closing, see [appendConnection].
func appendStreamedHeader(dst []byte, closing, chunked bool, size int64, keepAlive []byte, res Response, date []byte) ([]byte, error) {
	dst, err := appendStatusAndHeaders(dst, res, date)
	if err != nil {
		return nil, err
	}
	dst = appendConnection(dst, closing, keepAlive)
	switch {
	case chunked:
		dst = append(dst, "Transfer-Encoding: chunked\r\n"...)
	case size > 0:
		dst = append(dst, "Content-Length: "...)
		dst = strconv.AppendInt(dst, size, 10)
		dst = append(dst, '\r', '\n')
	}
	dst = append(dst, '\r', '\n')
	return dst, nil
}

// streamedFraming decides how HTTP/1.1 frames a produced response. noLength
// and noBody are [noLengthAndBody]'s verdict on the request it answers.
//
// chunked is the default for a body that has one, and the only framing that
// carries trailers. size is the Content-Length to state, zero for none: what
// [statedLength] says unless the body is chunked (RFC 9112 §6.2 forbids
// both). closeDelimited is the body with
// neither, for an http10 request whose client never said it understands
// chunked: RFC 9112 §6.1 forbids Transfer-Encoding in the response then,
// because a 1.0 client reads the chunk sizes as body bytes — which is also
// why a declared length is worth most there, keeping the connection open.
func streamedFraming(res Response, http10, noLength, noBody bool) (chunked bool, size int64, closeDelimited bool) {
	chunked = !noBody && !http10 && (res.ContentLength == 0 || res.Trailers != nil)
	if !chunked {
		size, _ = statedLength(res, noLength, noBody)
	}
	closeDelimited = !noBody && !chunked && size == 0
	return chunked, size, closeDelimited
}

// appendStatusAndHeaders writes the status line and the caller's fields, and
// refuses the two things a response must never carry from a caller: a header
// that would split the response, and a framing header this package owns.
// date is [dateFor]'s value for the batch, written when the caller set none.
func appendStatusAndHeaders(dst []byte, res Response, date []byte) ([]byte, error) {
	status := res.Status
	if status == 0 {
		status = 200
	}
	if err := checkStatus(status); err != nil {
		return nil, err
	}
	dst = append(dst, "HTTP/1.1 "...)
	dst = strconv.AppendInt(dst, int64(status), 10)
	dst = append(dst, ' ')
	dst = append(dst, reason(status)...)
	dst = append(dst, '\r', '\n')
	dated, err := datedBy(res.Headers)
	if err != nil {
		return nil, err
	}
	if !dated {
		// RFC 9110 §6.6.1: an origin server with a clock sends Date on every
		// response. It leads the fields (§5.3: "Date on responses" first, so
		// a recipient can decide early not to handle the message); see
		// datedBy for one the handler set.
		dst = append(dst, "Date: "...)
		dst = append(dst, date...)
		dst = append(dst, '\r', '\n')
	}
	for _, h := range res.Headers {
		if !isToken(h.Name) {
			return nil, fmt.Errorf("%w: %q is not a header name", ErrMalformed, h.Name)
		}
		if hasCRLF(h.Value) {
			return nil, ErrHeaderInjection
		}
		if hasFieldControl(h.Value) {
			return nil, fmt.Errorf("%w: the value of %q holds a control character", ErrMalformed, h.Name)
		}
		if err := checkResponseField(h.Name); err != nil {
			return nil, err
		}
		if equalFold(h.Name, "content-length") || equalFold(h.Name, "transfer-encoding") {
			// Framing is this package's to state, and a second opinion on it
			// is the smuggling class arriving from the inside.
			return nil, fmt.Errorf("%w: %s is set by the writer, not the caller", ErrMalformed, h.Name)
		}
		if equalFold(h.Name, "connection") && onlyClose(h.Value) {
			// A handler asking for the connection to end is honoured, not
			// refused: the batch's last response carries the writer's own
			// Connection: close and the server stops reading (see
			// asksToClose). Written once, by the writer, never twice.
			continue
		}
		if equalFold(h.Name, "connection") || equalFold(h.Name, "keep-alive") ||
			equalFold(h.Name, "proxy-connection") || equalFold(h.Name, "upgrade") ||
			equalFold(h.Name, "trailer") && res.Trailers == nil {
			// So is the connection: a handler's Connection: keep-alive on a
			// response the writer is closing after, or an Upgrade promising
			// what this server never does, is a client misled about the
			// bytes that follow. HTTP/2 and HTTP/3 refuse the same fields. A
			// Trailer is the same promise on a response without Trailers.
			return nil, fmt.Errorf("%w: %s is connection-specific, not the caller's", ErrMalformed, h.Name)
		}
		dst = append(dst, h.Name...)
		dst = append(dst, ':', ' ')
		dst = append(dst, h.Value...)
		dst = append(dst, '\r', '\n')
	}
	return dst, nil
}

// datedBy says whether a handler set its own Date among headers. One that did
// is believed — a cache in front of it may be dating the entry — and is
// neither doubled nor moved: the three writers add theirs only when this is
// false, ahead of the handler's fields, which needs this pass before the one
// that writes them. Believed, but read: a Date a sender generates MUST be an
// IMF-fixdate (RFC 9110 §5.6.7), and one the writer could not parse is
// refused rather than sent for a cache to misdate the entry by. The parse
// costs only the handler that dates its own responses; the common path
// compares four bytes per field.
func datedBy(headers []Header) (dated bool, err error) {
	for _, h := range headers {
		// The length first, inline: most names are not four bytes long.
		if len(h.Name) == len("date") && equalFold(h.Name, "date") {
			if _, err := time.Parse(dateLayout, string(h.Value)); err != nil {
				return true, fmt.Errorf("%w: the Date %q is not an IMF-fixdate", ErrMalformed, h.Value)
			}
			return true, nil
		}
	}
	return false, nil
}

// noLengthAndBody applies RFC 9110 §8.6 / §9.3.2's constraint on a
// response's framing to the three protocols alike: a 204 or 304 never
// carries a length — the length of an absent payload is not information, it
// is a framing lie two hops can disagree about, the same class this package
// refuses everywhere else — and neither it nor a HEAD response ever carries
// body bytes on the wire, even if the handler set some. A HEAD response
// still states the Content-Length its GET twin would when the handler gave
// it one (the same handler answers both, naturally with a body; see
// [statedLength] for the HEAD answered without), which is why noLength and
// noBody are not the same bit: a client that read the bytes anyway would be
// reading the start of the next response, desynchronising the connection
// exactly as Transfer-Encoding smuggling does.
//
// The method is matched byte-for-byte: methods are case-sensitive tokens
// (RFC 9110 §9.1), so "head" is some other method whose response carries its
// body like any other. Folding the case here sent that response a
// Content-Length with no bytes behind it — the client read the next
// response's head as this one's body, the very desync this function exists
// to prevent.
func noLengthAndBody(status int, method []byte) (noLength, noBody bool) {
	noLength = status == 204 || status == 304
	noBody = noLength || bytes.Equal(method, headMethod)
	return noLength, noBody
}

// headMethod is the one method whose response framing differs from every
// other's, shared so the comparison above does not rebuild it per response.
var headMethod = []byte("HEAD")

// statedLength is the Content-Length a response states, and whether it
// states one, decided once for the three writers; noLength and noBody are
// [noLengthAndBody]'s verdict on the request it answers. A 204 or 304
// states none. A streamed response states the length it declared, or none:
// the end of its body is what the chunked framing, the DATA frame's
// END_STREAM and the stream's FIN say instead. Any other response states
// len(Body), zero included — except a HEAD answered with neither a Body nor
// a Stream. RFC 9110 §8.6 lets a HEAD response carry a Content-Length only
// when it equals the GET twin's, and a handler that answered HEAD with the
// metadata alone said nothing about that length: "0" would be a lie a
// cache then holds against the GET. A HEAD with a Body states len(Body),
// since the handler that serves both GET and HEAD built the body it would
// have sent; with a Stream, the length it declared.
func statedLength(res Response, noLength, noBody bool) (int64, bool) {
	switch {
	case noLength:
		return 0, false
	case res.streamed():
		return res.ContentLength, res.ContentLength > 0
	case noBody && len(res.Body) == 0:
		return 0, false
	}
	return int64(len(res.Body)), true
}

// appendResponse writes one response. method is the request it answers, used
// only to decide whether a body may follow — never to change the status or
// headers a handler chose. closing marks the last response of a batch whose
// last request will not be followed by another; keepAlive is what every
// other response says instead, see [appendConnection]. http10 marks a
// request that never said it reads chunked, whose response therefore
// carries no trailers.
func appendResponse(dst, method []byte, closing, http10 bool, keepAlive []byte, res Response, date []byte) ([]byte, error) {
	if err := res.check(); err != nil {
		return nil, err
	}
	status := res.Status
	if status == 0 {
		status = 200
	}
	noLength, noBody := noLengthAndBody(status, method)
	var trailers []Header
	chunked := res.Trailers != nil && !noBody && !http10
	if chunked {
		// Called before a byte is appended, so a refusal leaves dst as the
		// caller had it and the response can still be answered with a 500.
		var err error
		if trailers, err = res.trailers(); err != nil {
			return nil, err
		}
	}
	dst, err := appendStatusAndHeaders(dst, res, date)
	if err != nil {
		return nil, err
	}
	dst = appendConnection(dst, closing, keepAlive)

	if chunked {
		// Trailers only travel after a chunked body (RFC 9112 §7.1.2), so
		// the payload in hand goes out as one chunk rather than with a
		// Content-Length.
		dst = append(dst, "Transfer-Encoding: chunked\r\n\r\n"...)
		dst = appendChunk(dst, res.Body)
		return appendLastChunkTrailers(dst, trailers), nil
	}
	if n, ok := statedLength(res, noLength, noBody); ok {
		dst = append(dst, "Content-Length: "...)
		dst = strconv.AppendInt(dst, n, 10)
		dst = append(dst, '\r', '\n')
	}
	dst = append(dst, '\r', '\n')
	if !noBody {
		dst = append(dst, res.Body...)
	}
	return dst, nil
}

// checkStatus refuses a status code no version of HTTP can carry, a 1xx as
// a final one, and a 426.
//
// A 426 is refused because this transport cannot send one that is conformant:
// RFC 9110 §15.5.22 requires an Upgrade field on it, naming the protocols
// the server would switch to, and this server never switches protocols on a
// connection — the three writers refuse a handler's Upgrade for that reason
// (and HTTP/2 and HTTP/3 forbid the field outright). A 426 without Upgrade
// is a MUST violation; a 426 with it is a promise this package will not keep.
// A handler that wants a client on another protocol advertises it with
// Alt-Svc. Revisit if an upgrade is ever implemented.
//
// It is shared by the three protocols rather than written once: HTTP/1.1 puts
// the number in a status line, HTTP/2 and HTTP/3 in a :status field, and a
// handler that returns 0 or -1 or 900 must not become a malformed response on
// any of them. The HTTP/2 path in particular renders it with an unsigned
// helper, where a negative code becomes an empty field rather than an error.
//
// A 1xx is refused outright rather than framed like 204/304: RFC 9110 §8.6
// forbids a Content-Length on it too, but unlike 204/304 there is no sense in
// which a handler's 101 or 102 is a real final answer — the interim 100 this
// package sends itself for Expect: 100-continue does not go through here, so
// nothing that already works needs the code.
func checkStatus(status int) error {
	if status < 200 || status > 599 {
		return fmt.Errorf("%w: status %d is not a final status code", ErrMalformed, status)
	}
	if status == 426 {
		return fmt.Errorf("%w: a 426 needs an Upgrade this server never performs", ErrMalformed)
	}
	return nil
}

func hasCRLF(b []byte) bool {
	for _, c := range b {
		if c == '\r' || c == '\n' {
			return true
		}
	}
	return false
}

// hasFieldControl reports a value carrying any byte RFC 9110 §5.5 keeps out
// of field content beyond CR and LF: the other C0 controls (HTAB excepted)
// and DEL, or SP/HTAB at either edge. Outbound values are held to the same
// octet rules the request parser enforces inbound — a NUL or 0x0C a handler
// lets through is the same cross-hop disagreement as a CR, with a subtler
// trigger, and a value edged with whitespace is one an HTTP/1.1 reader trims
// and a strict HTTP/2 reader refuses as malformed (RFC 9113 §8.2.1).
func hasFieldControl(b []byte) bool {
	for _, c := range b {
		if (c < 0x20 && c != '\t') || c == 0x7f {
			return true
		}
	}
	return edgedWithWhitespace(b)
}

// transportError is the response this package writes for an error of its
// own — a request it refused (400, 408, 413, 414, 431, 501, 505) or a
// handler it could not answer (500: a panic, a response it will not put on
// the wire). RFC 9110 §15.5 and §15.6 have the server send a representation
// explaining the error; the transport knows no more than the status, so the
// body is one line of it, "<status> <reason>", as text/plain. That line is
// what a browser shows where an empty 4xx is a blank page, and what curl
// prints on HTTP/2 and HTTP/3, where no reason phrase travels with the
// status. Nothing from the request is quoted: an error body that echoed
// the input would be a reflection vector. The three writers frame it as
// any response — a HEAD still gets the length and type and no bytes
// (§9.3.2), once its method reached the refusal. 408 and 505 are the one
// place the line says less than it could (whether to retry; which versions
// the server speaks): what the server speaks depends on how its listeners
// were set up, which this package does not see.
func transportError(status int) Response {
	body := strconv.AppendInt(make([]byte, 0, 48), int64(status), 10)
	body = append(body, ' ')
	body = append(body, reason(status)...)
	body = append(body, '\n')
	return Response{
		Status:  status,
		Headers: []Header{{Name: []byte("Content-Type"), Value: textPlain}},
		Body:    body,
	}
}

// textPlain is the one media type the transport's own errors carry.
var textPlain = []byte("text/plain; charset=utf-8")

// reason renders the status text. The set is deliberately short: a code this
// package does not name is answered with its class, which is what a client
// acts on anyway.
func reason(status int) string {
	switch status {
	case 200:
		return "OK"
	case 201:
		return "Created"
	case 204:
		return "No Content"
	case 400:
		return "Bad Request"
	case 401:
		return "Unauthorized"
	case 403:
		return "Forbidden"
	case 404:
		return "Not Found"
	case 408:
		return "Request Timeout"
	case 411:
		return "Length Required"
	case 413:
		return "Content Too Large"
	case 422:
		return "Unprocessable Content"
	case 425:
		return "Too Early"
	case 431:
		return "Request Header Fields Too Large"
	case 500:
		return "Internal Server Error"
	case 501:
		return "Not Implemented"
	case 505:
		return "HTTP Version Not Supported"
	}
	// checkStatus has already refused anything outside 200..599.
	switch status / 100 {
	case 2:
		return "Success"
	case 3:
		return "Redirection"
	case 4:
		return "Client Error"
	default:
		return "Server Error"
	}
}

// onlyClose reports whether a Connection value says "close" and nothing
// else — the one connection option a handler may state. Anything else in the
// list (keep-alive, a hop-by-hop field name) promises what the writer
// decides, and stays refused.
func onlyClose(v []byte) bool {
	seen := false
	for len(v) > 0 {
		var member []byte
		member, v = cutMember(v)
		if len(member) == 0 {
			continue
		}
		if !equalFold(member, "close") {
			return false
		}
		seen = true
	}
	return seen
}

// asksToClose reports whether a handler asked, with Connection: close on any
// response of the batch, for the connection to end after it. The responses
// already produced still go out — their handlers ran, and HTTP/1.1 answers
// positionally — and the batch's last one carries the close.
func asksToClose(responses []Response) bool {
	for _, r := range responses {
		for _, h := range r.Headers {
			if equalFold(h.Name, "connection") && onlyClose(h.Value) {
				return true
			}
		}
	}
	return false
}
