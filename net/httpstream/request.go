package httpstream

import (
	"bytes"
	"errors"
	"fmt"
)

// Header is one field of a request or a response.
//
// Name and Value borrow the connection's read buffer on the way in, so they
// follow the batch contract: valid for the duration of the call that received
// the batch, copied if retained.
type Header struct {
	Name  []byte
	Value []byte
}

// Request is one parsed request. Every field borrows the read buffer — see
// [Header] — which is what makes parsing a request allocation-free.
type Request struct {
	Method []byte

	// Target is raw and origin-form: it begins with "/", the query is still
	// attached, and nothing has been percent-decoded — decoding before the
	// handler decides what a segment means is how two layers route one
	// request differently.
	Target []byte

	Headers []Header
	Body    []byte

	// KeepAlive reports whether the connection may serve another request
	// after this one, per the version and the Connection header.
	KeepAlive bool

	// http10 marks a request parsed from an HTTP/1.0 request line. It is
	// unexported on purpose: the Handler contract promises a request never
	// says which protocol carried it, but the response writer has to know —
	// RFC 9112 §6.1 forbids Transfer-Encoding in a response unless the
	// request was HTTP/1.1, so a streamed answer to a 1.0 client is
	// close-delimited rather than chunked. KeepAlive cannot stand in for it:
	// a 1.0 request may ask for keep-alive.
	http10 bool
}

// Get returns the value of the first header with this name, matched
// case-insensitively as HTTP requires, or nil.
//
// It scans rather than indexes: [Config.MaxHeaders] bounds the list at a few
// dozen, where a map would cost an allocation per request to save comparisons
// nobody measured.
func (r Request) Get(name string) []byte {
	for _, h := range r.Headers {
		if equalFold(h.Name, name) {
			return h.Value
		}
	}
	return nil
}

// hostFieldName is shared rather than built per request: the name is a
// constant, and it is the value that borrows the read buffer.
var hostFieldName = []byte("host")

// withAuthority delivers a request's authority under the one name a [Handler]
// can ask for whichever protocol carried it.
//
// The three transports name it differently — HTTP/1.1 sends a Host field,
// HTTP/2 and HTTP/3 a :authority pseudo-header — and a handler that has to
// know which one it is talking to has lost what taking a batch of requests
// was for. So an authority is delivered as "host", and [Request.Get] finds it
// the same way everywhere.
//
// A request carrying both must have them agree (RFC 9113 §8.3.1, RFC 9114
// §4.3.1): two hops reading a different authority out of one request is the
// routing half of the smuggling family, and the disagreement is refused here
// rather than resolved in favour of either.
func withAuthority(regular []Header, authority []byte, protocolErr error) ([]Header, error) {
	if authority == nil {
		return regular, nil
	}
	for _, h := range regular {
		if !equalFold(h.Name, "host") {
			continue
		}
		if !bytes.Equal(h.Value, authority) {
			return nil, fmt.Errorf("%w: host %q and :authority %q name different hosts",
				protocolErr, h.Value, authority)
		}
		return regular, nil
	}
	return append(regular, Header{Name: hostFieldName, Value: authority}), nil
}

// tchar is RFC 9110's token character set, which both a method and a header
// name must be made of. Anything outside it is refused rather than
// normalised: normalising is how two hops come to disagree.
var tchar = func() (t [256]bool) {
	const special = "!#$%&'*+-.^_`|~"
	for i := range 26 {
		t['a'+i], t['A'+i] = true, true
	}
	for i := range 10 {
		t['0'+i] = true
	}
	for i := range len(special) {
		t[special[i]] = true
	}
	return t
}()

func isToken(b []byte) bool {
	if len(b) == 0 {
		return false
	}
	for _, c := range b {
		if !tchar[c] {
			return false
		}
	}
	return true
}

// equalFold compares a header name against a constant, ASCII-case-insensitive.
// HTTP field names are ASCII by definition, so Unicode folding would be both
// wrong and slower.
func equalFold(got []byte, want string) bool {
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

// parseRequest reads one request out of the front of buf.
//
// It returns the number of bytes consumed, or errIncomplete when buf holds a
// prefix of a request and nothing is wrong with it yet. Every other error is
// terminal for the connection: the framing is in doubt, and there is no
// resynchronising a stream whose boundaries are unknown.
//
// A chunked body is decoded in place, so on success buf's consumed region may
// have been rewritten — see decodeChunked. Nothing past consumed is touched,
// which is what lets the next pipelined request parse from the same buffer.
//
// progress, when not nil, carries a chunked body's validated prefix from one
// call to the next, so a body that arrives over many reads is walked once
// rather than once per read — see chunkProgress. The caller owns it and
// guarantees it belongs to the request at the front of buf.
func parseRequest(buf []byte, cfg Config, arena []Header, progress *chunkProgress) (req Request, consumed int, out []Header, err error) {
	out = arena
	start := len(arena)

	line, rest, ok := cutCRLF(buf, cfg.MaxRequestLineBytes)
	if !ok {
		if len(buf) > cfg.MaxRequestLineBytes+1 {
			return req, 0, out, fmt.Errorf("%w: the request line is over %d bytes", ErrTooLarge, cfg.MaxRequestLineBytes)
		}
		return req, 0, out, errIncomplete
	}
	consumed = len(buf) - len(rest)

	method, target, version, err := splitRequestLine(line)
	if err != nil {
		return req, 0, out, err
	}
	req.Method, req.Target = method, target

	// HTTP/1.1 keeps the connection open unless told otherwise; HTTP/1.0
	// closes it unless told otherwise. Getting this backwards is a hang, not
	// a wrong answer, so the two versions are separated here rather than
	// folded into a default.
	http11 := string(version) == "HTTP/1.1"
	req.KeepAlive = http11
	req.http10 = !http11

	out, n, err := parseHeaders(out, rest, cfg)
	if err != nil {
		return req, 0, out, err
	}
	// A view into the arena, taken after every append: appending may move the
	// backing array, so a sub-slice handed out mid-parse would dangle the way
	// a retained batch does. The caller re-slices once the batch is complete.
	req.Headers = out[start:]
	consumed += n
	rest = rest[n:]

	chunked, err := checkFraming(req, http11)
	if err != nil {
		return req, 0, out, err
	}
	if hasClose, hasKeepAlive := connectionOptions(req.Headers); hasClose {
		// "close" wins over "keep-alive" when both appear: honouring the
		// close is a MUST (RFC 9112 §9.6); honouring keep-alive never is.
		req.KeepAlive = false
	} else if hasKeepAlive {
		req.KeepAlive = true
	}

	if chunked {
		body, n, cerr := decodeChunked(rest, cfg, progress, consumed)
		if cerr != nil {
			if errors.Is(cerr, errIncomplete) && fieldHasToken(req.Headers, "expect", "100-continue") {
				return req, 0, out, errExpectContinue
			}
			return req, 0, out, cerr
		}
		req.Body = body
		return req, consumed + n, out, nil
	}
	length, err := contentLength(req, cfg)
	if err != nil {
		return req, 0, out, err
	}
	if len(rest) < length {
		if fieldHasToken(req.Headers, "expect", "100-continue") {
			return req, 0, out, errExpectContinue
		}
		return req, 0, out, errIncomplete
	}
	req.Body = rest[:length]
	return req, consumed + length, out, nil
}

// connectionOptions scans every Connection field line as the comma-separated
// list RFC 9110 §7.6.1 says it is. The whole-value comparison this replaces
// missed "close" inside a list or on a second Connection line — a MUST NOT
// (RFC 9112 §9.6): the connection persisted, and requests pipelined behind
// the close would have been processed for a client that stopped reading.
func connectionOptions(headers []Header) (hasClose, hasKeepAlive bool) {
	for _, h := range headers {
		if !equalFold(h.Name, "connection") {
			continue
		}
		for v := h.Value; len(v) > 0; {
			var member []byte
			member, v = cutMember(v)
			switch {
			case equalFold(member, "close"):
				hasClose = true
			case equalFold(member, "keep-alive"):
				hasKeepAlive = true
			}
		}
	}
	return hasClose, hasKeepAlive
}

// fieldHasToken reports whether token appears as a member of the
// comma-separated list formed by every field line named name. Expect is read
// this way (RFC 9110 §10.1.1): "Expect: 100-continue, x" compared as one
// value matched nothing, so the client waited for a 100 that never came and
// the request died at ReadTimeout instead of being served.
func fieldHasToken(headers []Header, name, token string) bool {
	for _, h := range headers {
		if !equalFold(h.Name, name) {
			continue
		}
		for v := h.Value; len(v) > 0; {
			var member []byte
			member, v = cutMember(v)
			if equalFold(member, token) {
				return true
			}
		}
	}
	return false
}

// cutMember takes the next member off a comma-separated field value, OWS
// trimmed on both sides, which is RFC 9110 §5.6.1's list rule.
func cutMember(v []byte) (member, rest []byte) {
	if i := indexByte(v, ','); i >= 0 {
		return trimOWS(v[:i]), v[i+1:]
	}
	return trimOWS(v), nil
}

// splitRequestLine takes the line apart on exactly two spaces, and refuses
// everything a server that is not a proxy has no business accepting.
func splitRequestLine(line []byte) (method, target, version []byte, err error) {
	first := indexByte(line, ' ')
	if first < 0 {
		return nil, nil, nil, fmt.Errorf("%w: the request line has no method", ErrMalformed)
	}
	second := indexByte(line[first+1:], ' ')
	if second < 0 {
		return nil, nil, nil, fmt.Errorf("%w: the request line has no version", ErrMalformed)
	}
	second += first + 1

	method, target, version = line[:first], line[first+1:second], line[second+1:]
	if indexByte(version, ' ') >= 0 {
		return nil, nil, nil, fmt.Errorf("%w: the request line has more than three fields", ErrMalformed)
	}
	if !isToken(method) {
		return nil, nil, nil, fmt.Errorf("%w: the method is not a token", ErrMalformed)
	}
	// Origin-form only. Absolute-form and authority-form exist for proxies,
	// and a server that accepts them while routing on the path is how a
	// request reaches somewhere its Host said it would not.
	if len(target) == 0 || target[0] != '/' {
		return nil, nil, nil, fmt.Errorf("%w: the target is not origin-form", ErrMalformed)
	}
	for _, c := range target {
		if c <= 0x20 || c == 0x7f {
			return nil, nil, nil, fmt.Errorf("%w: the target holds a control character", ErrMalformed)
		}
	}
	if v := string(version); v != "HTTP/1.1" && v != "HTTP/1.0" {
		return nil, nil, nil, fmt.Errorf("%w: unsupported version %q", ErrMalformed, version)
	}
	return method, target, version, nil
}

// parseHeaders reads fields up to the blank line, returning what it consumed
// including that line.
func parseHeaders(arena []Header, buf []byte, cfg Config) ([]Header, int, error) {
	start, consumed := len(arena), 0
	for {
		if consumed > cfg.MaxHeaderBytes {
			return arena, 0, fmt.Errorf("%w: headers over %d bytes", ErrTooLarge, cfg.MaxHeaderBytes)
		}
		// The budget counts line endings, so the line itself gets two
		// bytes less than what is left of it.
		limit := cfg.MaxHeaderBytes - consumed - 2
		line, rest, ok := cutCRLF(buf[consumed:], limit)
		if !ok {
			if len(buf)-consumed > limit+1 {
				return arena, 0, fmt.Errorf("%w: a header line is over the bound", ErrTooLarge)
			}
			return arena, 0, errIncomplete
		}
		consumed = len(buf) - len(rest)
		if len(line) == 0 {
			return arena, consumed, nil // the blank line ends the fields
		}
		// Obsolete line folding: a continuation line begins with whitespace.
		// RFC 9112 §5.2 says a server must reject it, and the reason is the
		// usual one — two hops that unfold differently see two requests.
		if line[0] == ' ' || line[0] == '\t' {
			return arena, 0, fmt.Errorf("%w: obsolete line folding", ErrMalformed)
		}
		if len(arena)-start >= cfg.MaxHeaders {
			return arena, 0, fmt.Errorf("%w: over %d header fields", ErrTooLarge, cfg.MaxHeaders)
		}
		h, err := parseHeaderLine(line)
		if err != nil {
			return arena, 0, err
		}
		arena = append(arena, h)
	}
}

func parseHeaderLine(line []byte) (Header, error) {
	colon := indexByte(line, ':')
	if colon < 0 {
		return Header{}, fmt.Errorf("%w: a header field has no colon", ErrMalformed)
	}
	name := line[:colon]
	// No space before the colon. RFC 9112 §5.1 requires the rejection, and
	// tolerating it is exactly how one hop reads a header the next does not.
	if !isToken(name) {
		return Header{}, fmt.Errorf("%w: %q is not a header name", ErrMalformed, name)
	}
	value := trimOWS(line[colon+1:])
	for _, c := range value {
		// RFC 9110 §5.5: field content is field-vchar — every C0 control but
		// HTAB is out, and so is DEL. CR, LF and NUL are the splitting bytes,
		// but the rest of the range is refused too: a 0x0C or 0x7F that one
		// hop strips and the next delivers is the same disagreement with a
		// subtler trigger.
		if (c < 0x20 && c != '\t') || c == 0x7f {
			return Header{}, fmt.Errorf("%w: a header value holds a control character", ErrMalformed)
		}
	}
	return Header{Name: name, Value: value}, nil
}

// checkFraming settles how the body is framed before a byte of it is read.
// Everything refused here is refused because leaving it to a later hop is the
// request-smuggling family.
//
// Transfer-Encoding is accepted in exactly one form: "chunked" alone, on an
// HTTP/1.1 request, with no Content-Length beside it. That shape leaves one
// framing and nothing for two hops to disagree about. Every other shape is
// refused: beside Content-Length it is the smuggling pair itself; on HTTP/1.0
// the sender cannot know the coding is understood, so RFC 9112 §6.1 calls the
// framing faulty; and a coding this server does not implement is a feature
// gap, not a malformed request — answered 501, as the same section asks.
func checkFraming(req Request, http11 bool) (chunked bool, err error) {
	hosts, codings, lengths := 0, 0, 0
	var coding []byte
	for _, h := range req.Headers {
		switch {
		case equalFold(h.Name, "transfer-encoding"):
			codings++
			coding = h.Value
		case equalFold(h.Name, "content-length"):
			lengths++
		case equalFold(h.Name, "host"):
			hosts++
		}
	}
	if http11 && hosts != 1 {
		return false, fmt.Errorf("%w: HTTP/1.1 requires exactly one Host, got %d", ErrMalformed, hosts)
	}
	if hosts > 1 {
		return false, fmt.Errorf("%w: %d Host headers", ErrMalformed, hosts)
	}
	if codings == 0 {
		return false, nil
	}
	switch {
	case lengths > 0:
		return false, fmt.Errorf("%w: Transfer-Encoding beside Content-Length", ErrMalformed)
	case !http11:
		return false, fmt.Errorf("%w: Transfer-Encoding on an HTTP/1.0 request", ErrMalformed)
	case codings > 1 || !equalFold(coding, "chunked"):
		// "chunked, chunked" and "gzip, chunked" land here too: the first is
		// nonsense this server will not guess at, the second needs a decoder
		// it does not have. 501 fits both — the request may be well-formed.
		return false, fmt.Errorf("%w: %q", errUnsupportedCoding, coding)
	}
	return true, nil
}

// maxChunkLineBytes bounds one chunk-size line, extensions included. The
// extensions are ignored (RFC 9112 §7.1.1 requires ignoring unrecognized
// ones), so a long one buys the sender nothing but this buffer — and a
// kilobyte is already far past the dozen hex digits any accepted size needs.
const maxChunkLineBytes = 1 << 10

// chunkProgress is how far into a chunked body the walk has validated, kept
// across the reads a body arrives over. Without it every read re-walked the
// body from its first chunk, and a client sending one-byte chunks one packet
// at a time cost O(n²) CPU per request — some 12 s for 40 000 chunks, with
// ReadTimeout bounding the wall time but not the work per byte.
//
// The walk is resumable because validation only ever reads: nothing in the
// body is moved until the whole framing has been accepted (see
// decodeChunked), so the bytes behind r are exactly what they were when
// they were validated. The reader keeps the offsets honest across its
// buffer compactions — see reader.consume.
type chunkProgress struct {
	// at is the request's first byte in the reader's buffer; a progress
	// whose at is not the request being parsed belongs to another one and
	// is discarded. headersEnd is where the body starts, relative to at,
	// and is the check that the headers reparsed on this call framed the
	// same body the saved offsets index.
	at, headersEnd int
	// w and r are the decoded and encoded byte counts of the chunks fully
	// validated so far: size line, data and the CRLF after it.
	w, r int
	// trailer is set once the last chunk has been read; r then points into
	// the trailer section and trailerBytes is what of it has been accepted
	// against MaxHeaderBytes; trailerFields is how many field lines that
	// was, against MaxHeaders.
	trailer       bool
	trailerBytes  int
	trailerFields int
}

// decodeChunked decodes a chunked request body (RFC 9112 §7.1) in place at
// the front of buf, returning the decoded bytes and how much of buf the
// encoded form consumed.
//
// In place, because the body must stay a view into the connection's read
// buffer like every other request field. Decoded bytes are strictly shorter
// than their encoding, so each chunk's data is copied down over the framing
// that preceded it and the write position never catches the read position.
// The copy is deferred to a second pass over framing the first has fully
// validated: an incomplete body is resumed from the same bytes after the
// next read, and a chunk already moved would be re-decoded from its own
// debris.
//
// progress resumes the validating pass where the previous call left it when
// it describes this body (same headersEnd), and is left describing the
// validated prefix when the body is still incomplete. On any other outcome
// it is cleared: the request is done, or the connection is.
//
// Trailer fields are parsed and discarded — parsed, so a trailer section
// that is not made of field lines is refused like any other framing fault,
// and discarded because this package delivers complete requests, where a
// field arriving after the body has nobody left to inform.
func decodeChunked(buf []byte, cfg Config, progress *chunkProgress, headersEnd int) (body []byte, consumed int, err error) {
	var local chunkProgress
	if progress == nil {
		progress = &local
	} else if progress.headersEnd != headersEnd {
		*progress = chunkProgress{}
	}
	progress.headersEnd = headersEnd
	if _, _, err := walkChunks(buf, cfg, progress, false); err != nil {
		if !errors.Is(err, errIncomplete) {
			*progress = chunkProgress{}
		}
		return nil, 0, err
	}
	*progress = chunkProgress{}
	w, r, _ := walkChunks(buf, cfg, &local, true)
	return buf[:w], r, nil
}

// walkChunks walks the chunked framing at the front of buf from the state in
// p, copying the data down over it when move is set. w is where the decoded
// body ends, r where the encoded form does. p is advanced past each chunk
// and trailer line as it is validated, so an errIncomplete leaves it at the
// last whole one and the next call starts there.
func walkChunks(buf []byte, cfg Config, p *chunkProgress, move bool) (w, r int, err error) {
	w, r = p.w, p.r
	if move {
		// The moving pass starts from zero: chunk data is copied down over
		// framing already validated, so it cannot share a cursor with the
		// validating pass that walked it.
		w, r = 0, 0
	}
	for !p.trailer || move {
		line, rest, ok := cutCRLF(buf[r:], maxChunkLineBytes)
		if !ok {
			if len(buf)-r > maxChunkLineBytes+1 {
				return 0, 0, fmt.Errorf("%w: a chunk-size line over %d bytes", ErrMalformed, maxChunkLineBytes)
			}
			return 0, 0, errIncomplete
		}
		size, serr := parseChunkSize(line, cfg.MaxBodyBytes-w)
		if serr != nil {
			return 0, 0, serr
		}
		r = len(buf) - len(rest)
		if size == 0 {
			if !move {
				p.r, p.trailer = r, true
			}
			break
		}
		if len(rest) < size+2 {
			return 0, 0, errIncomplete
		}
		if rest[size] != '\r' || rest[size+1] != '\n' {
			return 0, 0, fmt.Errorf("%w: chunk data is not ended by CRLF", ErrMalformed)
		}
		if move {
			copy(buf[w:], rest[:size])
		}
		w += size
		r += size + 2
		if !move {
			p.w, p.r = w, r
		}
	}
	// The trailer section: field lines up to a blank one, bounded the way the
	// header section is — in bytes and in count, since a thousand one-byte
	// trailers cost parsing rather than bytes just as headers do.
	trailerBytes, trailerFields := 0, 0
	if !move {
		trailerBytes, trailerFields = p.trailerBytes, p.trailerFields
	}
	for {
		limit := cfg.MaxHeaderBytes - trailerBytes - 2
		line, rest, ok := cutCRLF(buf[r:], limit)
		if !ok {
			if len(buf)-r > limit+1 {
				return 0, 0, fmt.Errorf("%w: trailers over %d bytes", ErrTooLarge, cfg.MaxHeaderBytes)
			}
			return 0, 0, errIncomplete
		}
		trailerBytes += len(buf) - len(rest) - r
		r = len(buf) - len(rest)
		if len(line) == 0 {
			return w, r, nil
		}
		if trailerFields >= cfg.MaxHeaders {
			return 0, 0, fmt.Errorf("%w: over %d trailer fields", ErrTooLarge, cfg.MaxHeaders)
		}
		trailerFields++
		if _, err := parseHeaderLine(line); err != nil {
			return 0, 0, err
		}
		if !move {
			p.r, p.trailerBytes, p.trailerFields = r, trailerBytes, trailerFields
		}
	}
}

// parseChunkSize reads the hexadecimal size off a chunk-size line, bounded by
// budget — the body bytes this request may still accept — so the sum can
// neither overflow nor outgrow MaxBodyBytes. What follows the size is a
// chunk extension: its meaning is ignored, as RFC 9112 §7.1.1 requires, but
// its grammar is not. An extension holding a NUL, a bare LF or an unterminated
// quote is a line two parsers split differently, which is the request
// smuggling shape this package refuses everywhere else.
func parseChunkSize(line []byte, budget int) (int, error) {
	n, i := 0, 0
digits:
	for ; i < len(line); i++ {
		var d int
		switch c := line[i]; {
		case '0' <= c && c <= '9':
			d = int(c - '0')
		case 'a' <= c && c <= 'f':
			d = int(c-'a') + 10
		case 'A' <= c && c <= 'F':
			d = int(c-'A') + 10
		default:
			break digits
		}
		n = n*16 + d
		if n > budget {
			return 0, fmt.Errorf("%w: a chunked body over the bound", ErrTooLarge)
		}
	}
	if i == 0 {
		return 0, fmt.Errorf("%w: chunk size %q is not hexadecimal", ErrMalformed, line)
	}
	if !validChunkExt(line[i:]) {
		return 0, fmt.Errorf("%w: chunk size line %q is not a size and extensions", ErrMalformed, line)
	}
	return n, nil
}

// validChunkExt checks RFC 9112 §7.1.1's grammar, with BWS being optional
// whitespace (RFC 9110 §5.6.3):
//
//	chunk-ext     = *( BWS ";" BWS chunk-ext-name [ BWS "=" BWS chunk-ext-val ] )
//	chunk-ext-val = token / quoted-string
//
// So "5 ;a=b" is accepted: the space is BWS ahead of the separator, not part
// of the size. Whitespace with no extension after it ("5 ") is not.
func validChunkExt(b []byte) bool {
	for len(b) > 0 {
		b = skipWS(b)
		if len(b) == 0 || b[0] != ';' {
			return false
		}
		b = skipWS(b[1:])
		n := tokenLen(b)
		if n == 0 {
			return false
		}
		b = b[n:]
		if rest := skipWS(b); len(rest) > 0 && rest[0] == '=' {
			b = skipWS(rest[1:])
			if len(b) > 0 && b[0] == '"' {
				n = quotedStringLen(b)
			} else {
				n = tokenLen(b)
			}
			if n <= 0 {
				return false
			}
			b = b[n:]
		}
	}
	return true
}

// skipWS drops the leading spaces and tabs of b, and only the leading ones:
// the chunk-ext grammar allows whitespace before a separator, not at the end.
func skipWS(b []byte) []byte {
	for len(b) > 0 && (b[0] == ' ' || b[0] == '\t') {
		b = b[1:]
	}
	return b
}

// tokenLen is the length of the token at the front of b, zero if none.
func tokenLen(b []byte) int {
	for i, c := range b {
		if !tchar[c] {
			return i
		}
	}
	return len(b)
}

// quotedStringLen is the length of the RFC 9110 §5.6.4 quoted-string at the
// front of b, quotes included, or -1 if it is malformed or unterminated.
func quotedStringLen(b []byte) int {
	for i := 1; i < len(b); i++ {
		switch c := b[i]; {
		case c == '"':
			return i + 1
		case c == '\\':
			// quoted-pair = "\" ( HTAB / SP / VCHAR / obs-text )
			if i+1 == len(b) || !qdChar(b[i+1]) && b[i+1] != '"' && b[i+1] != '\\' {
				return -1
			}
			i++
		case !qdChar(c):
			return -1
		}
	}
	return -1
}

// qdChar is qdtext: HTAB, SP, and visible or obs-text octets other than the
// quote and the backslash, which have their own roles.
func qdChar(c byte) bool {
	return c == '\t' || c == ' ' || (c >= 0x21 && c != '"' && c != '\\' && c != 0x7f)
}

func contentLength(req Request, cfg Config) (int, error) {
	var raw []byte
	seen := 0
	for _, h := range req.Headers {
		if equalFold(h.Name, "content-length") {
			seen++
			raw = h.Value
		}
	}
	if seen == 0 {
		return 0, nil
	}
	if seen > 1 {
		return 0, fmt.Errorf("%w: %d Content-Length headers", ErrMalformed, seen)
	}
	if len(raw) == 0 {
		return 0, fmt.Errorf("%w: an empty Content-Length", ErrMalformed)
	}
	n := 0
	for _, c := range raw {
		// Digits only: no sign, no whitespace, no hex. strconv would accept a
		// leading '+' that a peer may not.
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("%w: Content-Length %q is not a number", ErrMalformed, raw)
		}
		n = n*10 + int(c-'0')
		if n > cfg.MaxBodyBytes {
			return 0, fmt.Errorf("%w: a body over %d bytes", ErrTooLarge, cfg.MaxBodyBytes)
		}
	}
	return n, nil
}

// cutCRLF splits the line ending at the first CRLF, the line holding at most
// limit bytes before it. A caller finding no line refuses once buf holds more
// than limit+1 bytes: a line that long cannot end in time any more. A bare
// LF is not a line ending here: accepting one is how a request that one hop
// reads as a body becomes a request the next reads as a header.
func cutCRLF(buf []byte, limit int) (line, rest []byte, ok bool) {
	end := min(len(buf), limit+2)
	for i := 0; i+1 < end; i++ {
		if buf[i] == '\r' && buf[i+1] == '\n' {
			return buf[:i], buf[i+2:], true
		}
	}
	return nil, buf, false
}

// indexByte is a plain loop on purpose. bytes.IndexByte was measured here
// and rejected: +8.5% to +13% on BenchmarkReadPipelined at every depth
// (paired A/B, count=8, p=0.000). Request lines and header lines are short,
// and the non-inlinable call into the vectorised assembly costs more than
// the vectorisation returns. Re-measure before "fixing" this back.
func indexByte(b []byte, c byte) int {
	for i := range b {
		if b[i] == c {
			return i
		}
	}
	return -1
}

func trimOWS(b []byte) []byte {
	for len(b) > 0 && (b[0] == ' ' || b[0] == '\t') {
		b = b[1:]
	}
	for len(b) > 0 && (b[len(b)-1] == ' ' || b[len(b)-1] == '\t') {
		b = b[:len(b)-1]
	}
	return b
}
