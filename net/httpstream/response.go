package httpstream

import (
	"bytes"
	"fmt"
	"strconv"

	"github.com/mlagarrigue/sluice"
)

// Response is what goes back for one request. Its fields are read during the
// write and not retained, so they may borrow whatever the pipeline had.
type Response struct {
	// Status is the HTTP status code. Zero means 200.
	Status int

	// Headers are sent as given. Content-Length is written by [WriteBatch]
	// from the body and must not be set here.
	Headers []Header

	// Body is the payload, sent verbatim. Use it for an answer that is
	// already in hand, which is most of them.
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
	// A streamed response cannot carry a Content-Length, because its length
	// is not known when the headers go out. Each protocol says that its own
	// way — HTTP/1.1 in chunks, HTTP/2 and HTTP/3 in DATA frames ended by a
	// flag — and none of them needs the total in advance.
	//
	// A yield that returns false means the stream or the connection is gone;
	// the package documentation states what each protocol does then.
	Stream sluice.Stream[[]byte]
}

// streamed reports whether the response produces its payload as it goes.
func (r Response) streamed() bool { return r.Stream != nil }

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

// check refuses a response that says two things about its payload.
func (r Response) check() error {
	if r.Stream != nil && len(r.Body) > 0 {
		return fmt.Errorf("%w: a response with both a Body and a Stream", ErrMalformed)
	}
	return nil
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
// otherwise. A pipeline that reorders — [parallel.Unordered], a filter
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
func WriteBatch(dst []byte, requests []Request, responses []Response) ([]byte, error) {
	closeAfter := asksToClose(responses)
	for i, res := range responses {
		var method []byte
		if i < len(requests) {
			method = requests[i].Method
		}
		closing := i == len(responses)-1 && (closeAfter || i < len(requests) && !requests[i].KeepAlive)
		var err error
		if dst, err = appendResponse(dst, method, closing, res); err != nil {
			return nil, err
		}
	}
	return dst, nil
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

// appendStreamedHeader writes the status and headers of a response whose
// length is not yet known. noBody is [noLengthAndBody]'s verdict on the
// request it answers, which the caller already needs to skip the producer: a
// HEAD or 204/304 response here must not announce a chunked body it will
// never send, or a pipelined client reads the next response's bytes as this
// one's body.
//
// http10 marks a request whose client never said it understands chunked:
// RFC 9112 §6.1 forbids Transfer-Encoding in the response then, because a
// 1.0 client reads the chunk sizes as body bytes. The body is close-delimited
// instead — no framing header at all, the empty line to EOF — and the caller
// has already marked the response closing, since EOF is the only end such a
// body has.
func appendStreamedHeader(dst []byte, closing, http10, noBody bool, res Response) ([]byte, error) {
	dst, err := appendStatusAndHeaders(dst, res)
	if err != nil {
		return nil, err
	}
	if closing {
		dst = append(dst, "Connection: close\r\n"...)
	}
	if !noBody && !http10 {
		dst = append(dst, "Transfer-Encoding: chunked\r\n"...)
	}
	dst = append(dst, '\r', '\n')
	return dst, nil
}

// appendStatusAndHeaders writes the status line and the caller's fields, and
// refuses the two things a response must never carry from a caller: a header
// that would split the response, and a framing header this package owns.
func appendStatusAndHeaders(dst []byte, res Response) ([]byte, error) {
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
			equalFold(h.Name, "trailer") {
			// So is the connection: a handler's Connection: keep-alive on a
			// response the writer is closing after, or an Upgrade or Trailer
			// promising what this server never does, is a client misled about
			// the bytes that follow. HTTP/2 and HTTP/3 refuse the same fields.
			return nil, fmt.Errorf("%w: %s is connection-specific, not the caller's", ErrMalformed, h.Name)
		}
		dst = append(dst, h.Name...)
		dst = append(dst, ':', ' ')
		dst = append(dst, h.Value...)
		dst = append(dst, '\r', '\n')
	}
	return dst, nil
}

// noLengthAndBody applies RFC 9110 §8.6 / §9.3.2's constraint on a
// response's framing to the three protocols alike: a 204 or 304 never
// carries a length — the length of an absent payload is not information, it
// is a framing lie two hops can disagree about, the same class this package
// refuses everywhere else — and neither it nor a HEAD response ever carries
// body bytes on the wire, even if the handler set some. A HEAD response
// still states the Content-Length its GET twin would (the same handler
// answers both, naturally with a body), which is why noLength and noBody are
// not the same bit: a client that read the bytes anyway would be reading the
// start of the next response, desynchronising the connection exactly as
// Transfer-Encoding smuggling does.
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

// appendResponse writes one response. method is the request it answers, used
// only to decide whether a body may follow — never to change the status or
// headers a handler chose. closing marks the last response of a batch whose
// last request will not be followed by another.
func appendResponse(dst, method []byte, closing bool, res Response) ([]byte, error) {
	if err := res.check(); err != nil {
		return nil, err
	}
	dst, err := appendStatusAndHeaders(dst, res)
	if err != nil {
		return nil, err
	}
	if closing {
		dst = append(dst, "Connection: close\r\n"...)
	}

	status := res.Status
	if status == 0 {
		status = 200
	}
	noLength, noBody := noLengthAndBody(status, method)

	if !noLength {
		dst = append(dst, "Content-Length: "...)
		dst = strconv.AppendInt(dst, int64(len(res.Body)), 10)
		dst = append(dst, '\r', '\n')
	}
	dst = append(dst, '\r', '\n')
	if !noBody {
		dst = append(dst, res.Body...)
	}
	return dst, nil
}

// checkStatus refuses a status code no version of HTTP can carry, and a 1xx
// as a final one.
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
// and DEL. Outbound values are held to the same octet rules the request
// parser enforces inbound — a NUL or 0x0C a handler lets through is the same
// cross-hop disagreement as a CR, with a subtler trigger.
func hasFieldControl(b []byte) bool {
	for _, c := range b {
		if (c < 0x20 && c != '\t') || c == 0x7f {
			return true
		}
	}
	return false
}

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
