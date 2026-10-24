package httpstream

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"net/netip"
)

// Header is one field of a request or a response.
//
// Name and Value borrow the connection's read buffer on the way in, so they
// follow the batch contract: valid for the duration of the call that received
// the batch, copied if retained.
//
// Nothing else of the wire is carried — in particular not the never-indexed
// bit an HTTP/2 or HTTP/3 client may set on a field (RFC 7541 §6.2.3,
// RFC 9204 §4.5.4). The response encoders set that bit by name alone, on
// Set-Cookie, Authorization and Proxy-Authorization, and never index
// anything: a response is safe to send, but a program that relays a field
// it received under another name cannot preserve the bit, which RFC 9204
// §7.1.3 asks of an intermediary. This server is an origin, not a relay;
// the field that would carry the bit is the thing to add when a relay is
// built on it.
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
	// request differently. The one other form is "*", on an OPTIONS request
	// only (RFC 9110 §7.1: the server as a whole, not a resource on it).
	Target []byte

	// Headers is the request's field section, in the order the client sent
	// it.
	Headers []Header

	// Trailers is the request's trailer section, in the order the client
	// sent it, or nil when it sent none: the fields after the body of a
	// chunked HTTP/1.1 request, or of a trailing HEADERS on HTTP/2 and
	// HTTP/3. Under [BodyWhole] the whole request is read before the
	// handler is called, so they are here as soon as the body is; a
	// streamed body's trailers arrive after the handler was given the
	// request and are checked against the header bounds without being
	// kept, so this is nil for it ([Request.Stream]). They are held to the bounds the
	// headers are held to, separately: a trailer section of its own up to
	// [Config.MaxHeaders] fields and [Config.MaxHeaderBytes] bytes.
	//
	// They are kept apart from Headers on purpose and nothing in them is
	// interpreted. RFC 9110 §6.5 has a trailer stored separately and never
	// merged into the header section unless the field's own definition
	// allows it: a Content-Length or Authorization arriving after the body
	// came too late to frame, route or authorise this request, and a
	// handler that reads one from here takes that on knowingly.
	Trailers []Header

	// Body is the request's content, exactly the bytes the client sent once
	// the transfer coding is undone: a chunked body arrives joined, and
	// nothing else is decoded. A Content-Encoding (gzip, br) is the
	// handler's to read and to undo — the field stays in Headers and the
	// body stays compressed. The package neither decodes a coding nor
	// refuses one with a 415: it has no view on what a handler accepts, and
	// a second parser between the socket and the handler is what this
	// package exists not to be.
	//
	// Body is nil when the body arrives through [Request.Stream] instead.
	Body []byte

	// body is a streamed body's state, nil when the body is whole in Body:
	// one pointer, so that streaming costs the requests that do not stream
	// nothing but its copy.
	body *bodyStream

	// KeepAlive reports whether the connection may serve another request
	// after this one, per the version and the Connection header.
	KeepAlive bool

	// RemoteAddr is the address and port the request came from: the TCP
	// peer on HTTP/1.1 and HTTP/2, the UDP peer on HTTP/3 as it stood when
	// the batch was handed over (a QUIC connection may move). It is the
	// transport's peer, so behind a proxy it is the proxy; a forwarded
	// address is in the headers, and trusting it is the handler's decision.
	// The zero value means the listener's addresses are not IP addresses (a
	// Unix socket, a test pipe).
	RemoteAddr netip.AddrPort

	// TLS is the state of the TLS session that carried the request — version,
	// cipher suite, the protocol ALPN chose, the certificates a client
	// presented — or nil in clear text. It is shared by every request of the
	// connection and must not be modified. It is what mutual TLS authorises
	// on (PeerCertificates, VerifiedChains); that NegotiatedProtocol names
	// the HTTP version is a fact of the handshake, not something a handler
	// should branch on, since the request is the same whichever carried it.
	TLS *tls.ConnectionState

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

// fieldCount is how many fields a decoded block yields once its cookie
// crumbs are joined (RFC 9113 §8.2.3, RFC 9114 §4.2.1): a crumb after the
// first is part of one field, not a field. It is what [Config.MaxHeaders]
// bounds, so a client that splits its cookie per pair — net/http's HTTP/2
// transport does — stands where an HTTP/1.1 client sending the same cookie
// in one field stands, instead of 70 cookies being refused on HTTP/2 and
// HTTP/3 alone.
func fieldCount(fields []Header) int {
	crumbs := 0
	for _, h := range fields {
		if string(h.Name) == "cookie" {
			crumbs++
		}
	}
	if crumbs > 1 {
		return len(fields) - (crumbs - 1)
	}
	return len(fields)
}

// appendCrumb joins one more cookie crumb to the cookie's value with the
// delimiter RFC 9113 §8.2.3 and RFC 9114 §4.2.1 prescribe, "; ". The first
// crumb's bytes sit in the decoder's buffer next to the fields that follow,
// so the join takes a buffer of its own the first time (owned false) and
// grows that one afterwards.
func appendCrumb(cookie []byte, owned bool, crumb []byte) []byte {
	if !owned {
		cookie = append(make([]byte, 0, 2*(len(cookie)+2+len(crumb))), cookie...)
	}
	cookie = append(cookie, ';', ' ')
	return append(cookie, crumb...)
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

// buildRequest turns the decoded field section of an HTTP/2 or HTTP/3 request
// into a [Request], applying the rules RFC 9113 §8.3 and RFC 9114 §4.3 state
// — one function for both, because the two sections are the same rules and
// the two protocols once held their own copies, which had drifted: a request
// one refused and the other delivered to the handler. The rules exist because
// a request that reaches an HTTP/1.1 hop unchecked is how a smuggled request
// is written.
//
// Errors wrap protocolErr; the caller maps them to its own stream error. The
// regular fields are returned in fields' own storage, the pseudo-headers
// dropped; declared is the content-length the head stated, or -1.
// connectionSpecific reports whether name — lowercase, as HTTP/2 and HTTP/3
// put it on the wire — is one of the fields RFC 9113 §8.2.2 and RFC 9114 §4.2
// forbid in any message of theirs: Connection, and the fields RFC 9110
// §7.6.1 gives connection-specific semantics. Their one exception, TE, is
// not in the list: it is allowed in a request alone, with the value
// "trailers" alone, which is each reader's check to make.
func connectionSpecific(name []byte) bool {
	switch string(name) {
	case "connection", "keep-alive", "proxy-connection", "transfer-encoding", "upgrade":
		return true
	}
	return false
}

// checkRequestTrailer holds one received trailer field to the rules a
// request's header fields pass in buildRequest: no pseudo-header (RFC 9113
// §8.1, RFC 9114 §4.1), a lowercase token for a name and no control
// character in the value (RFC 9113 §8.2.1, RFC 9114 §4.2), no
// connection-specific field and no TE but "trailers" (RFC 9113 §8.2.2,
// RFC 9114 §4.2). A trailer reaches whatever reads the request last, so it
// is just as good a smuggling carrier as a header, and the same hop that
// would split a request on a Transfer-Encoding header would split it on the
// trailer. The error wraps protocolErr; the caller makes it the stream's,
// HTTP/2's PROTOCOL_ERROR or HTTP/3's H3_MESSAGE_ERROR.
func checkRequestTrailer(h Header, protocolErr error) error {
	if len(h.Name) == 0 {
		return fmt.Errorf("%w: an empty trailer name", protocolErr)
	}
	if h.Name[0] == ':' {
		return fmt.Errorf("%w: the pseudo-header %s in a trailer section", protocolErr, h.Name)
	}
	if !isToken(h.Name) || !lowerASCII(h.Name) {
		return fmt.Errorf("%w: %q is not a trailer field name", protocolErr, h.Name)
	}
	if !validFieldValue(h.Value) {
		return fmt.Errorf("%w: the trailer %q holds a control character", protocolErr, h.Name)
	}
	if connectionSpecific(h.Name) {
		return fmt.Errorf("%w: the connection-specific field %q in a trailer section", protocolErr, h.Name)
	}
	if string(h.Name) == "te" && !equalFold(h.Value, "trailers") {
		return fmt.Errorf("%w: TE may only be \"trailers\"", protocolErr)
	}
	return nil
}

func buildRequest(fields []Header, protocolErr error) (req Request, declared int, err error) {
	var method, path, scheme, authority []byte
	regular := fields[:0]
	declared = -1
	seenRegular, hostSeen := false, false
	cookie, cookieOwned := -1, false // the cookie field's index in regular, once seen; whether its value is a buffer of our own
	for _, h := range fields {
		// h.Name stays []byte throughout: binding string(h.Name) to a
		// variable allocates per field (verified with -gcflags=-m), while a
		// conversion feeding only a switch or comparison does not.
		if len(h.Name) == 0 {
			return req, 0, fmt.Errorf("%w: an empty field name", protocolErr)
		}
		if h.Name[0] == ':' {
			if seenRegular {
				return req, 0, fmt.Errorf("%w: pseudo-header %s after a regular field", protocolErr, h.Name)
			}
			var dst *[]byte
			switch string(h.Name) {
			case ":method":
				dst = &method
			case ":path":
				dst = &path
			case ":scheme":
				dst = &scheme
			case ":authority":
				dst = &authority
			default:
				return req, 0, fmt.Errorf("%w: unknown pseudo-header %s", protocolErr, h.Name)
			}
			if *dst != nil {
				return req, 0, fmt.Errorf("%w: %s appears twice", protocolErr, h.Name)
			}
			*dst = h.Value
			continue
		}
		seenRegular = true
		if !lowerASCII(h.Name) {
			return req, 0, fmt.Errorf("%w: %q is not lowercase", protocolErr, h.Name)
		}
		// The h1 parser's octet rules, applied to what HPACK or QPACK decoded
		// (RFC 9113 §8.2.1, RFC 9114 §4.1.2). Either can carry any byte, so
		// without these a name with an embedded colon or a value with CR LF
		// reaches the handler — and the first hop that re-serialises the
		// request toward HTTP/1.1 turns that value into a second request.
		if !isToken(h.Name) {
			return req, 0, fmt.Errorf("%w: %q is not a header name", protocolErr, h.Name)
		}
		if !validFieldValue(h.Value) {
			return req, 0, fmt.Errorf("%w: the value of %q holds a control character", protocolErr, h.Name)
		}
		if connectionSpecific(h.Name) {
			// Connection-specific fields have no meaning here and are how
			// one request becomes two HTTP/1.1 requests at the next hop.
			return req, 0, fmt.Errorf("%w: the connection-specific field %q", protocolErr, h.Name)
		}
		switch string(h.Name) {
		case "host":
			// The same rule as :authority below: a host that is not an
			// authority is malformed, not a request with an odd host.
			hostSeen = true
			if !validAuthority(h.Value) {
				return req, 0, fmt.Errorf("%w: host %q is not an authority", protocolErr, h.Value)
			}
		case "te":
			// RFC 9113 §8.2.2: no value other than "trailers" — compared
			// without case, as RFC 9110 §10.1.4's ABNF string is.
			if !equalFold(h.Value, "trailers") {
				return req, 0, fmt.Errorf("%w: TE may only be \"trailers\"", protocolErr)
			}
		case "content-length":
			// Remembered for the end of the stream, where the DATA sum is
			// known. A repeat is refused as the h1 parser refuses it: two
			// lengths on one request is the smuggling family's opening move.
			if declared >= 0 {
				return req, 0, fmt.Errorf("%w: content-length appears twice", protocolErr)
			}
			n, err := parseContentLength(h.Value)
			if err != nil {
				return req, 0, fmt.Errorf("%w: content-length %q is not a length", protocolErr, h.Value)
			}
			declared = n
		case "cookie":
			// RFC 9113 §8.2.3, RFC 9114 §4.2.1: a client may split its
			// cookie into crumbs, one field per pair, and they MUST be
			// joined by "; " before a generic server application sees them
			// — the handler here. Left apart, [Request.Get] returned the
			// first pair alone and every session cookie behind it went
			// missing on HTTP/2 and HTTP/3 only.
			if cookie >= 0 {
				regular[cookie].Value = appendCrumb(regular[cookie].Value, cookieOwned, h.Value)
				cookieOwned = true
				continue
			}
			cookie = len(regular)
		}
		regular = append(regular, h)
	}
	if string(method) == "CONNECT" {
		// RFC 9113 §8.5, RFC 9114 §4.4: a CONNECT carries :method and
		// :authority and omits :scheme and :path; one that does not is
		// malformed. Well formed, it is a request for a tunnel, which is
		// answered 501 like the router answers it (see errConnect) — until
		// 2026-10-20 the rule below reset it as a request without :path,
		// and a CONNECT with :path and :scheme reached the handler.
		if scheme != nil || path != nil {
			return req, 0, fmt.Errorf("%w: a CONNECT with :scheme or :path", protocolErr)
		}
		if authority == nil {
			return req, 0, fmt.Errorf("%w: a CONNECT without :authority", protocolErr)
		}
		return req, 0, fmt.Errorf("%w: a tunnel to %q", errConnect, authority)
	}
	if method == nil || path == nil || scheme == nil {
		return req, 0, fmt.Errorf("%w: a request without :method, :path and :scheme", protocolErr)
	}
	// Pseudo-header values compose the request line of any downstream
	// HTTP/1.1 hop, which is why RFC 9113 §10.3 and RFC 9114 §10.3 ask for
	// octet-level checks on them specifically: these are the same refusals
	// the h1 request line makes.
	if !isToken(method) {
		return req, 0, fmt.Errorf("%w: :method is not a token", protocolErr)
	}
	if !isToken(scheme) {
		return req, 0, fmt.Errorf("%w: :scheme is not a token", protocolErr)
	}
	if err := checkTarget(method, path, protocolErr); err != nil {
		return req, 0, err
	}
	if authority != nil && !validAuthority(authority) {
		return req, 0, fmt.Errorf("%w: :authority %q is not an authority", protocolErr, authority)
	}
	// RFC 9110 §4.2.1, RFC 9114 §4.3.1: a scheme with a mandatory authority
	// component needs :authority or host, as HTTP/1.1 needs Host (400).
	// Without this a handler routing on host saw it missing on two of the
	// three protocols only. Other schemes, which may have no authority at
	// all, are delivered as they came.
	if authority == nil && !hostSeen && (equalFold(scheme, "http") || equalFold(scheme, "https")) {
		return req, 0, fmt.Errorf("%w: no :authority or host for scheme %q", protocolErr, scheme)
	}
	regular, err = withAuthority(regular, authority, protocolErr)
	if err != nil {
		return req, 0, err
	}
	return Request{Method: method, Target: path, Headers: regular, KeepAlive: true}, declared, nil
}

// checkTarget applies the request-target rule every protocol shares: the
// origin-form (RFC 9112 §3.2.1, RFC 9113 §8.3.1, RFC 9114 §4.3.1), or the
// asterisk-form on an OPTIONS request alone (RFC 9112 §3.2.4) — "*" names the
// server itself, which only OPTIONS may ask about, and any other method with
// it is a request for no resource. HTTP/1.1's absolute-form has been taken
// apart by absoluteForm before this, its authority bound for the Host; what
// is left is origin-form. The authority-form is CONNECT's alone, and a
// CONNECT is answered 501 before any target is looked at (errConnect): a
// server that routed a CONNECT on a path would tunnel to somewhere its
// authority said it would not.
//
// The octets are checked too: the target composes the request line of any
// downstream HTTP/1.1 hop (RFC 9113 §10.3, RFC 9114 §10.3), and HPACK or
// QPACK can carry any byte.
func checkTarget(method, target []byte, protocolErr error) error {
	if len(target) == 1 && target[0] == '*' {
		if string(method) != "OPTIONS" {
			return fmt.Errorf("%w: the asterisk-form target on a %s request", protocolErr, method)
		}
		return nil
	}
	if len(target) == 0 || target[0] != '/' {
		return fmt.Errorf("%w: the target is neither origin-form nor absolute-form", protocolErr)
	}
	for _, c := range target {
		if c <= 0x20 || c == 0x7f {
			return fmt.Errorf("%w: the target holds a control character", protocolErr)
		}
	}
	return nil
}

// absoluteForm takes an HTTP/1.1 absolute-form target apart (RFC 9112
// §3.2.2: a server MUST accept it, though most clients send it to a proxy
// only): "http://h:8080/x?q=1" yields the authority "h:8080" and the
// origin-form "/x?q=1", which is what [Request.Target] promises. A target
// that is already origin-form or asterisk-form is returned as it is, with no
// authority. The scheme is "http" or "https" without regard to case (RFC
// 9110 §4.2.3); any other scheme, or a target that is neither form, is
// refused — the authority-form among them, on a method other than CONNECT,
// which was answered before this. The authority is not judged here: it
// replaces the Host, and
// checkFraming holds it to the one rule a Host is held to, which refuses it
// empty (RFC 9110 §4.2.1) and with a userinfo (§4.2.4).
//
// An empty path is "/" (RFC 9110 §4.2.1); "http://h" points at the "/" of
// its own "://", so no byte is written or allocated for it. "http://h?q=1"
// is the one shape with nothing contiguous to point at — the "/" has to be
// put in front of the query — and it allocates. Nothing in the buffer is
// rewritten: the parser is run again over the same bytes when a request
// arrives in pieces, and must read them the same way each time.
func absoluteForm(target []byte) (authority, path []byte, err error) {
	if len(target) == 0 || target[0] == '/' || target[0] == '*' {
		return nil, target, nil
	}
	sep := indexByte(target, ':')
	if sep < 0 || len(target) < sep+3 || target[sep+1] != '/' || target[sep+2] != '/' {
		return nil, nil, fmt.Errorf("%w: the target is neither origin-form nor absolute-form", ErrMalformed)
	}
	if scheme := target[:sep]; !equalFold(scheme, "http") && !equalFold(scheme, "https") {
		return nil, nil, fmt.Errorf("%w: the target's scheme %q is not http or https", ErrMalformed, scheme)
	}
	rest := target[sep+3:]
	end := len(rest)
	for i, c := range rest {
		if c == '/' || c == '?' {
			end = i
			break
		}
	}
	authority, path = rest[:end], rest[end:]
	switch {
	case len(path) == 0:
		path = target[sep+2 : sep+3]
	case path[0] == '?':
		path = append(append(make([]byte, 0, 1+len(path)), '/'), path...)
	}
	return authority, path, nil
}

// authorityChar is RFC 3986 §3.2.2's reg-name alphabet: unreserved and
// sub-delims. The '%' of a percent-encoding is checked apart, with the two hex
// digits that must follow it. A userinfo's '@', a path's '/', a space and
// every control character are outside it, which is what makes the rule below
// a grammar check and not a list of things to refuse.
var authorityChar = func() (t [256]bool) {
	const allowed = "-._~!$&'()*+,;="
	for i := range 26 {
		t['a'+i], t['A'+i] = true, true
	}
	for i := range 10 {
		t['0'+i] = true
	}
	for i := range len(allowed) {
		t[allowed[i]] = true
	}
	return t
}()

// validAuthority says whether b is an authority a request may name: RFC 9110
// §7.2's uri-host [ ":" port ] — a registered name, an IPv4 address, or an IP
// literal in brackets, with a port of digits, possibly none. It is the one
// rule for HTTP/1.1's Host and HTTP/2's and HTTP/3's :authority, and it has
// the same shape on each because the three deliver the value under one name
// and a handler routes on it.
//
// The empty authority is refused: RFC 9114 §4.3.1 says so outright, RFC 9112
// §3.3 lets a server reject it, and a handler routing on a host that names
// nothing is a request that should have been refused. The userinfo RFC 3986
// allows is refused too (RFC 9113 §8.3.1 and RFC 9114 §4.3.1 forbid it, and
// RFC 9110's Host never had it): "u@h" delivered as a host is how a routing
// layer reads the host that follows the '@' while the one before it was
// looked at.
//
// The grammar is RFC 3986 §3.2.2's, whole: a reg-name's '%' starts two hex
// digits, and an IP literal is an IPv6 address or an IPvFuture. An IPv6
// address with a zone ("[fe80::1%25eth0]", the form RFC 6874 once added) is
// refused: RFC 9844 §3 withdrew that syntax from RFC 3986 altogether, and a
// zone means something only on the host that wrote it (RFC 4007), so one
// that arrives names a link this server cannot know.
func validAuthority(b []byte) bool {
	if len(b) == 0 {
		return false
	}
	host := b
	if b[0] == '[' {
		end := indexByte(b, ']')
		if end < 0 || !validIPLiteral(b[1:end]) {
			return false
		}
		host = b[end+1:]
		if len(host) == 0 {
			return true
		}
		if host[0] != ':' {
			return false
		}
	}
	for i := 0; i < len(host); i++ {
		switch c := host[i]; {
		case c == ':':
			for _, d := range host[i+1:] {
				if d < '0' || d > '9' {
					return false
				}
			}
			return true
		case c == '%':
			if i+2 >= len(host) || !isHex(host[i+1]) || !isHex(host[i+2]) {
				return false
			}
			i += 2
		case !authorityChar[c]:
			return false
		}
	}
	return true
}

// validIPLiteral says whether b, the inside of an authority's brackets, is
// RFC 3986 §3.2.2's IPv6address or IPvFuture: "v", hex digits, ".", then
// unreserved, sub-delims and ':'. The IPv6 address is parsed rather than
// matched character by character; a zone is refused (see validAuthority) and
// so is a bare IPv4 address, which needs no brackets.
func validIPLiteral(b []byte) bool {
	if len(b) > 0 && (b[0] == 'v' || b[0] == 'V') {
		dot := indexByte(b, '.')
		if dot < 2 || dot == len(b)-1 {
			return false
		}
		for _, c := range b[1:dot] {
			if !isHex(c) {
				return false
			}
		}
		for _, c := range b[dot+1:] {
			if !authorityChar[c] && c != ':' {
				return false
			}
		}
		return true
	}
	addr, err := netip.ParseAddr(string(b))
	return err == nil && addr.Is6() && addr.Zone() == ""
}

// isHex says whether c is one of RFC 3986's HEXDIG, in either case.
func isHex(c byte) bool {
	return '0' <= c && c <= '9' || 'a' <= c && c <= 'f' || 'A' <= c && c <= 'F'
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
// rather than once per read, and for a Content-Length body the length the
// accepted head asked for, so the head is parsed once rather than once per
// read — see chunkProgress. The caller owns it and guarantees it belongs to
// the request at the front of buf.
func parseRequest(buf []byte, cfg Config, arena []Header, progress *chunkProgress) (req Request, consumed int, out []Header, err error) {
	out = arena
	start := len(arena)

	// A Content-Length body still short of its end, on a request whose head
	// an earlier call already parsed and accepted: nothing before the body's
	// last byte can change the answer, so the head is not parsed again.
	// Re-parsing it on every read cost up to MaxHeaderBytes per byte of a
	// body dripped one byte at a time, and the client nothing.
	if progress != nil && progress.need > 0 && len(buf) < progress.need {
		if progress.expect {
			return req, 0, out, errExpectContinue
		}
		return req, 0, out, errIncomplete
	}

	// RFC 9112 §2.2: at least one empty line before the request-line is
	// ignored — the CRLF some clients send after a body. One, exactly: a
	// second is a request line with no method, as it always was.
	skipped := 0
	if len(buf) >= 2 && buf[0] == '\r' && buf[1] == '\n' {
		buf, skipped = buf[2:], 2
	}
	line, rest, ok := cutCRLF(buf, cfg.MaxRequestLineBytes)
	if !ok {
		if len(buf) > cfg.MaxRequestLineBytes+1 {
			return req, 0, out, fmt.Errorf("%w: over %d bytes", errRequestLineTooLong, cfg.MaxRequestLineBytes)
		}
		return req, 0, out, errIncomplete
	}
	consumed = skipped + len(buf) - len(rest)

	method, target, version, authority, err := splitRequestLine(line)
	if err != nil {
		return req, 0, out, err
	}
	req.Method, req.Target = method, target

	// HTTP/1.1 keeps the connection open unless told otherwise; HTTP/1.0
	// closes it unless told otherwise. Getting this backwards is a hang, not
	// a wrong answer, so the two versions are separated here rather than
	// folded into a default. A minor version above 1 is processed as 1.1
	// (RFC 9110 §6.2) and persists like it (RFC 9112 §9.3, "HTTP/1.1 (or
	// later)"); splitRequestLine has already refused every other major.
	http11 := version[7] != '0'
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
	headersEnd := len(out)
	consumed += n
	rest = rest[n:]

	// RFC 9112 §3.2.2: an origin server that receives an absolute-form
	// target MUST ignore the received Host and use the target's authority.
	// It is substituted here, before checkFraming judges the Host, so the
	// handler reads one host under one name whatever form the client chose,
	// and a missing Host on HTTP/1.1 is still the 400 §3.2 asks for. An
	// HTTP/1.0 request without a Host keeps none, as it did in origin-form.
	if authority != nil {
		for i := range req.Headers {
			if equalFold(req.Headers[i].Name, "host") {
				req.Headers[i].Value = authority
			}
		}
	}

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
		body, n, trailers, cerr := decodeChunked(rest, cfg, progress, consumed, out)
		if cfg.BodyDelivery == BodyStreamed && (errors.Is(cerr, errIncomplete) || errors.Is(cerr, ErrTooLarge)) {
			// Not whole in this read, or past what Body may hold: the body
			// streams, decoded as it arrives (bodyStream), and the head is
			// handed over now. The interim 100 a client may be waiting for is
			// sent when the handler first reads.
			if progress != nil {
				*progress = chunkProgress{}
			}
			return req, consumed, out, streamBody{chunked: true}
		}
		if cerr != nil {
			if errors.Is(cerr, errIncomplete) && expectsContinue(req) {
				return req, 0, out, errExpectContinue
			}
			return req, 0, out, cerr
		}
		out = trailers
		req.Body = body
		// The same views as Headers, after the same appends; capped, so a
		// handler appending to Headers cannot write over the trailers.
		req.Headers = out[start:headersEnd:headersEnd]
		if len(out) > headersEnd {
			req.Trailers = out[headersEnd:]
		}
		return req, consumed + n, out, nil
	}
	length, err := contentLength(req, cfg)
	if err != nil {
		return req, 0, out, err
	}
	if cfg.BodyDelivery == BodyStreamed && (len(rest) < length || length > cfg.MaxBodyBytes) {
		return req, consumed, out, streamBody{length: length}
	}
	if len(rest) < length {
		expect := expectsContinue(req)
		if progress != nil {
			// The head is accepted: the next calls only wait for the body.
			*progress = chunkProgress{at: progress.at, need: consumed + length, expect: expect}
		}
		if expect {
			return req, 0, out, errExpectContinue
		}
		return req, 0, out, errIncomplete
	}
	if progress != nil && progress.need != 0 {
		*progress = chunkProgress{} // the body that was waited for is whole
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

// expectsContinue reports whether the request is owed an interim 100 before
// its body. Never on HTTP/1.0: RFC 9110 §15.2 forbids a 1xx to such a client
// (it would read the interim as the response), and §10.1.1 says Expect is
// ignored there — the body is waited for as if the field were absent.
func expectsContinue(req Request) bool {
	return !req.http10 && fieldHasToken(req.Headers, "expect", "100-continue")
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
func splitRequestLine(line []byte) (method, target, version, authority []byte, err error) {
	first := indexByte(line, ' ')
	if first < 0 {
		return nil, nil, nil, nil, fmt.Errorf("%w: the request line has no method", ErrMalformed)
	}
	second := indexByte(line[first+1:], ' ')
	if second < 0 {
		return nil, nil, nil, nil, fmt.Errorf("%w: the request line has no version", ErrMalformed)
	}
	second += first + 1

	method, target, version = line[:first], line[first+1:second], line[second+1:]
	if indexByte(version, ' ') >= 0 {
		return nil, nil, nil, nil, fmt.Errorf("%w: the request line has more than three fields", ErrMalformed)
	}
	if !isToken(method) {
		return nil, nil, nil, nil, fmt.Errorf("%w: the method is not a token", ErrMalformed)
	}
	if string(method) == "CONNECT" {
		// The authority-form target is CONNECT's alone (RFC 9112 §3.2.3) and
		// a tunnel is what it asks for: 501 whatever the target, as on
		// HTTP/2 and HTTP/3 (errConnect), where this was 400 "neither
		// origin-form nor absolute-form" until 2026-10-20. The target is not
		// parsed: an empty or invalid port is rejected with the rest
		// (RFC 9110 §9.3.6 asks for the rejection, not the status).
		return nil, nil, nil, nil, fmt.Errorf("%w: a tunnel to %q", errConnect, target)
	}
	if authority, target, err = absoluteForm(target); err != nil {
		return nil, nil, nil, nil, err
	}
	if err := checkTarget(method, target, ErrMalformed); err != nil {
		return nil, nil, nil, nil, err
	}
	// HTTP-version = "HTTP/" DIGIT "." DIGIT (RFC 9112 §2.3): anything of
	// another shape is not a request line. Of that shape, 505 refuses a major
	// version (RFC 9110 §15.6.6), so only a major other than 1 is declined;
	// any minor of 1 is served.
	v := string(version)
	if len(v) != 8 || v[:5] != "HTTP/" || v[5] < '0' || v[5] > '9' || v[6] != '.' || v[7] < '0' || v[7] > '9' {
		return nil, nil, nil, nil, fmt.Errorf("%w: %q is not an HTTP version", ErrMalformed, version)
	}
	if v[5] != '1' {
		return nil, nil, nil, nil, fmt.Errorf("%w: %q", errVersionUnsupported, version)
	}
	return method, target, version, authority, nil
}

// parseHeaders reads fields up to the blank line, returning what it consumed
// including that line.
func parseHeaders(arena []Header, buf []byte, cfg Config) ([]Header, int, error) {
	start, consumed := len(arena), 0
	for {
		if consumed > cfg.MaxHeaderBytes {
			return arena, 0, fmt.Errorf("%w: over %d bytes", errHeadersTooLarge, cfg.MaxHeaderBytes)
		}
		// The budget counts line endings, so the line itself gets two
		// bytes less than what is left of it.
		limit := cfg.MaxHeaderBytes - consumed - 2
		line, rest, ok := cutCRLF(buf[consumed:], limit)
		if !ok {
			if len(buf)-consumed > limit+1 {
				return arena, 0, fmt.Errorf("%w: a header line is over the bound", errHeadersTooLarge)
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
			return arena, 0, fmt.Errorf("%w: over %d fields", errHeadersTooLarge, cfg.MaxHeaders)
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
// Transfer-Encoding is read as the list RFC 9110 §5.6.1.2 describes: every
// line of the field, members cut by cutMember, empty members ignored (a
// recipient MUST), names compared without case. The body is accepted in
// exactly one form: "chunked" as the only coding, on an HTTP/1.1 request, with
// no Content-Length beside it. That shape leaves one framing and nothing for
// two hops to disagree about. Every other shape is refused, and the status
// says why:
//
//   - beside Content-Length it is the smuggling pair itself, 400;
//   - on HTTP/1.0 the sender cannot know the coding is understood, so RFC
//     9112 §6.1 calls the framing faulty, 400;
//   - chunked anywhere but last ("chunked, gzip", "chunked, chunked") is a
//     body whose length cannot be determined, 400 and the connection closed
//     (RFC 9112 §6.3), as is a field that names no coding at all;
//   - a final coding this server does not implement ("gzip", "gzip, chunked",
//     "chunked;q=1") is a feature gap, not a malformed request — 501, as §6.1
//     asks. The connection closes there too: no framing was established.
func checkFraming(req Request, http11 bool) (chunked bool, err error) {
	hosts, lengths := 0, 0
	// unknown is the first coding this server has no decoder for; chunkedThen
	// is true once a chunked member is followed by any other member.
	var unknown []byte
	declared, lastChunked, chunkedThen := false, false, false
	for _, h := range req.Headers {
		switch {
		case equalFold(h.Name, "transfer-encoding"):
			declared = true
			for v := h.Value; len(v) > 0; {
				var member []byte
				member, v = cutMember(v)
				if len(member) == 0 {
					continue
				}
				if lastChunked {
					chunkedThen = true
				}
				lastChunked = equalFold(member, "chunked")
				if !lastChunked && unknown == nil {
					unknown = member
				}
			}
		case equalFold(h.Name, "content-length"):
			lengths++
		case equalFold(h.Name, "host"):
			// RFC 9112 §3.2: a Host with an invalid value is answered 400,
			// and the value is valid by the rule :authority is held to.
			if !validAuthority(h.Value) {
				return false, fmt.Errorf("%w: Host %q is not an authority", ErrMalformed, h.Value)
			}
			hosts++
		}
	}
	if http11 && hosts != 1 {
		return false, fmt.Errorf("%w: HTTP/1.1 requires exactly one Host, got %d", ErrMalformed, hosts)
	}
	if hosts > 1 {
		return false, fmt.Errorf("%w: %d Host headers", ErrMalformed, hosts)
	}
	if !declared {
		return false, nil
	}
	switch {
	case lengths > 0:
		return false, fmt.Errorf("%w: Transfer-Encoding beside Content-Length", ErrMalformed)
	case !http11:
		return false, fmt.Errorf("%w: Transfer-Encoding on an HTTP/1.0 request", ErrMalformed)
	case chunkedThen:
		return false, fmt.Errorf("%w: chunked is not the final transfer coding", ErrMalformed)
	case !lastChunked && unknown == nil:
		return false, fmt.Errorf("%w: Transfer-Encoding names no coding", ErrMalformed)
	case unknown != nil:
		return false, fmt.Errorf("%w: %q", errUnsupportedCoding, unknown)
	}
	return true, nil
}

// maxChunkLineBytes bounds one chunk-size line, extensions included. The
// extensions are ignored (RFC 9112 §7.1.1 requires ignoring unrecognized
// ones), so a long one buys the sender nothing but this buffer — and a
// kilobyte is already far past the dozen hex digits any accepted size needs.
const maxChunkLineBytes = 1 << 10

// chunkProgress is how far into its body a request's parse has got, kept
// across the reads the body arrives over: for a chunked body, how far the
// walk has validated; for a Content-Length body, how long the request is
// (need). Without it every read re-walked the
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

	// need, for a Content-Length body, is how many bytes from at the
	// request takes, once its head has been parsed and accepted; zero until
	// then. A call on fewer bytes returns at once, without parsing the head
	// again (see parseRequest); expect is whether that call owes the
	// interim 100 the head asked for.
	need   int
	expect bool
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
// Trailer fields are parsed — a trailer section that is not made of field
// lines is refused like any other framing fault — and appended to arena by
// the moving pass, which returns it as out. They are views into buf past
// the body, where the in-place copy never writes: decoded data only moves
// toward the front, and the trailer section is behind every chunk.
func decodeChunked(buf []byte, cfg Config, progress *chunkProgress, headersEnd int, arena []Header) (body []byte, consumed int, out []Header, err error) {
	var local chunkProgress
	if progress == nil {
		progress = &local
	} else if progress.headersEnd != headersEnd {
		*progress = chunkProgress{}
	}
	progress.headersEnd = headersEnd
	if _, _, _, err := walkChunks(buf, cfg, progress, false, nil); err != nil {
		if !errors.Is(err, errIncomplete) {
			*progress = chunkProgress{}
		}
		return nil, 0, arena, err
	}
	*progress = chunkProgress{}
	w, r, out, _ := walkChunks(buf, cfg, &local, true, arena)
	return buf[:w], r, out, nil
}

// walkChunks walks the chunked framing at the front of buf from the state in
// p, copying the data down over it when move is set. w is where the decoded
// body ends, r where the encoded form does. p is advanced past each chunk
// and trailer line as it is validated, so an errIncomplete leaves it at the
// last whole one and the next call starts there. The moving pass appends
// the trailer fields to out; the validating pass only counts them.
func walkChunks(buf []byte, cfg Config, p *chunkProgress, move bool, out []Header) (w, r int, fields []Header, err error) {
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
				return 0, 0, out, fmt.Errorf("%w: a chunk-size line over %d bytes", ErrMalformed, maxChunkLineBytes)
			}
			return 0, 0, out, errIncomplete
		}
		size, serr := parseChunkSize(line, cfg.MaxBodyBytes-w)
		if serr != nil {
			return 0, 0, out, serr
		}
		r = len(buf) - len(rest)
		if size == 0 {
			if !move {
				p.r, p.trailer = r, true
			}
			break
		}
		if len(rest) < size+2 {
			return 0, 0, out, errIncomplete
		}
		if rest[size] != '\r' || rest[size+1] != '\n' {
			return 0, 0, out, fmt.Errorf("%w: chunk data is not ended by CRLF", ErrMalformed)
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
				return 0, 0, out, fmt.Errorf("%w: trailers over %d bytes", errHeadersTooLarge, cfg.MaxHeaderBytes)
			}
			return 0, 0, out, errIncomplete
		}
		trailerBytes += len(buf) - len(rest) - r
		r = len(buf) - len(rest)
		if len(line) == 0 {
			return w, r, out, nil
		}
		if trailerFields >= cfg.MaxHeaders {
			return 0, 0, out, fmt.Errorf("%w: over %d trailer fields", errHeadersTooLarge, cfg.MaxHeaders)
		}
		trailerFields++
		h, err := parseHeaderLine(line)
		if err != nil {
			return 0, 0, out, err
		}
		if move {
			out = append(out, h)
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
	// A streamed body has a bound of its own; the whole-body bound still
	// decides, in parseRequest, which bodies are handed over whole.
	bound := max(cfg.MaxBodyBytes, cfg.MaxStreamedBodyBytes)
	n := 0
	for _, c := range raw {
		// Digits only: no sign, no whitespace, no hex. strconv would accept a
		// leading '+' that a peer may not.
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("%w: Content-Length %q is not a number", ErrMalformed, raw)
		}
		n = n*10 + int(c-'0')
		if n > bound {
			return 0, fmt.Errorf("%w: a body over %d bytes", ErrTooLarge, bound)
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
