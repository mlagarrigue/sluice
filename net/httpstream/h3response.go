package httpstream

import (
	"fmt"
	"strconv"
	"time"
)

// appendH3StreamedBody writes one batch of a streamed response as DATA
// frames. The stream's FIN is what ends the body, so there is no terminator
// to write here.
func appendH3StreamedBody(dst []byte, chunks [][]byte) []byte {
	for _, c := range chunks {
		if len(c) == 0 {
			continue // an empty batch is cadence, not an end
		}
		dst = appendH3Frame(dst, h3Data, c)
	}
	return dst
}

// appendH3ResponseBasic writes one response's HEADERS frame, and its DATA frame if
// it has a body to send in full now — RFC 9110 §8.6's 204/304 never do,
// no matter who asks. It has no request to check against, so a HEAD
// response's body is this function's business only through [appendH3Response]
// — the internal responder path, which knows the method and calls that
// instead.
func appendH3ResponseBasic(dst []byte, r Response) ([]byte, error) {
	return appendH3Response(dst, r, nil, h3NoFieldSectionLimit, &h3Scratch{}, dateFor(time.Now()))
}

// h3Scratch is one writer's reusable per-response state: the field list
// and QPACK block appendH3Response builds, and the digits of the two numeric
// values it writes. Drawn from h3ScratchPool for one response, it is what makes the
// response path allocation-free — safe because [quic.Stream.Write] copies into its own frame before returning.
type h3Scratch struct {
	head   []byte
	fields []Header
	qpack  []byte
	names  []byte   // lowercased copies of the field names a handler spelled in HTTP/1.1 case
	status [3]byte  // checkStatus bounds a final status to 200..599
	clen   [20]byte // enough digits for any int
}

// appendH3Response is AppendH3Response's real body, plus two things the
// exported wrapper cannot know: the request's method, needed to withhold a
// HEAD response's bytes the same way the HTTP/1.1 and HTTP/2 paths do, and
// the peer's SETTINGS_MAX_FIELD_SECTION_SIZE — zero for unbounded — which
// RFC 9114 §4.2.2 says a sender SHOULD NOT exceed. A field section that
// would exceed it is refused here rather than sent to a client that told
// us it will not accept it. date is [dateFor]'s value for the batch.
func appendH3Response(dst []byte, r Response, method []byte, peerFieldSection uint64, sc *h3Scratch, date []byte) ([]byte, error) {
	if err := r.check(); err != nil {
		return nil, err
	}
	status := r.Status
	if status == 0 {
		status = 200
	}
	if err := checkStatus(status); err != nil {
		return nil, err
	}
	noLength, noBody := noLengthAndBody(status, method)
	fields := append(sc.fields[:0], Header{Name: h3StatusName, Value: appendDigits(sc.status[:0], status)})
	dated, err := datedBy(r.Headers)
	if err != nil {
		return nil, err
	}
	if !dated {
		// RFC 9110 §6.6.1's Date, as the other two writers add it and
		// where: on every final response the handler did not date itself,
		// first after :status. The value is the batch's, formatted once and
		// never written after.
		fields = append(fields, Header{Name: h3DateName, Value: date})
	}
	sc.names = sc.names[:0]
	for _, h := range r.Headers {
		// A name the handler spelled in HTTP/1.1 case goes on the wire
		// lowercase (RFC 9114 §4.2), from a copy in sc.names: the handler's
		// bytes are borrowed, never written. The copies outlive the loop,
		// since fields holds them until appendQPACK reads them.
		name := lowerName(h.Name, &sc.names)
		// The octet rule the HTTP/1.1 writer applies (response.go): QPACK
		// carries any byte, and a name holding a space or a colon becomes a
		// second field, or a malformed one, at the first hop that
		// re-serialises this response toward HTTP/1.1.
		if !isToken(name) {
			return nil, fmt.Errorf("%w: response field %q is not a token", ErrH3Protocol, h.Name)
		}
		if hasCRLF(h.Value) {
			return nil, ErrHeaderInjection
		}
		if hasFieldControl(h.Value) {
			return nil, fmt.Errorf("%w: the value of %q holds a control character", ErrH3Protocol, h.Name)
		}
		if err := checkResponseField(name); err != nil {
			return nil, err
		}
		// The HTTP/2 writer's refusals, for the same reasons: a
		// connection-specific field makes the response malformed (RFC 9114
		// §4.2), and framing is this package's to state.
		// TE, whose §4.2 exception holds in a request alone, was refused
		// above by checkResponseField as a request's field.
		if connectionSpecific(name) {
			return nil, fmt.Errorf("%w: the connection-specific response field %q", ErrH3Protocol, h.Name)
		}
		if string(name) == "content-length" {
			return nil, fmt.Errorf("%w: content-length is set by the writer, not the caller", ErrH3Protocol)
		}
		fields = append(fields, Header{Name: name, Value: h.Value})
	}
	if n, ok := statedLength(r, noLength, noBody); ok {
		v := strconv.AppendInt(sc.clen[:0], n, 10)
		fields = append(fields, Header{Name: h3ContentLengthName, Value: v})
	}
	if err := checkH3FieldSection(fields, peerFieldSection); err != nil {
		return nil, err
	}

	sc.fields = fields
	sc.qpack = appendQPACK(sc.qpack[:0], fields)
	dst = appendH3Frame(dst, h3Headers, sc.qpack)
	if !noBody && len(r.Body) > 0 {
		dst = appendH3Frame(dst, h3Data, r.Body)
	}
	if !noBody && !r.streamed() && r.Trailers != nil {
		// A body in hand has its trailers in the same write, so a refusal
		// is still answered with a 500.
		return appendH3Trailers(dst, r, peerFieldSection, sc)
	}
	return dst, nil
}

// checkH3FieldSection refuses a field section over the peer's
// SETTINGS_MAX_FIELD_SECTION_SIZE — h3NoFieldSectionLimit when none was
// announced, zero when zero was — measured as §4.2.2 does: the uncompressed
// name and value, plus 32 per field.
func checkH3FieldSection(fields []Header, peerFieldSection uint64) error {
	var size uint64
	for _, h := range fields {
		size += uint64(len(h.Name)+len(h.Value)) + 32
	}
	if size > peerFieldSection {
		return fmt.Errorf("%w: a field section of %d bytes, over the peer's %d limit",
			ErrH3Protocol, size, peerFieldSection)
	}
	return nil
}

// appendH3Trailers writes a response's trailer section as the HEADERS frame
// that follows its DATA (RFC 9114 §4.1), held to [checkTrailer] and to the
// peer's field section limit as the head is. Its names go on the wire
// lowercase, from copies, as the head's do.
func appendH3Trailers(dst []byte, r Response, peerFieldSection uint64, sc *h3Scratch) ([]byte, error) {
	trailers, err := r.trailers()
	if err != nil {
		return nil, err
	}
	fields := sc.fields[:0]
	sc.names = sc.names[:0]
	for _, h := range trailers {
		fields = append(fields, Header{Name: lowerName(h.Name, &sc.names), Value: h.Value})
	}
	sc.fields = fields
	if err := checkH3FieldSection(fields, peerFieldSection); err != nil {
		return nil, err
	}
	sc.qpack = appendQPACK(sc.qpack[:0], fields)
	return appendH3Frame(dst, h3Headers, sc.qpack), nil
}

// h3StatusName, h3ContentLengthName and h3DateName are the three field names
// the responder writes itself, kept as package bytes so no response
// re-allocates them.
var (
	h3StatusName        = []byte(":status")
	h3ContentLengthName = []byte("content-length")
	h3DateName          = []byte("date")
)

// appendDigits writes v in decimal, the no-string itoa the response scratch
// wants: itoa's string return would allocate once per use here.
func appendDigits(dst []byte, v int) []byte {
	if v == 0 {
		return append(dst, '0')
	}
	var b [20]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	return append(dst, b[i:]...)
}
