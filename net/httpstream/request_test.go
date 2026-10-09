package httpstream

import (
	"errors"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
)

// req assembles a request with CRLF endings, so a test reads as the bytes on
// the wire rather than as an escape sequence.
func req(lines ...string) []byte {
	return []byte(strings.Join(lines, "\r\n") + "\r\n\r\n")
}

func parse(t *testing.T, raw []byte) (Request, int, error) {
	t.Helper()
	req, n, _, err := parseRequest(raw, Config{}.withDefaults(), nil, nil)
	return req, n, err
}

// chunkedReq assembles a chunked POST whose encoded body is the raw bytes
// given, written as one string so a test reads as the wire.
func chunkedReq(encoded string) []byte {
	return append(req("POST /x HTTP/1.1", "Host: h", "Transfer-Encoding: chunked"), encoded...)
}

// The refusals are the product. Each of these is a request some server
// somewhere accepts, and each is a way for two hops to disagree about where a
// request ends — which is what request smuggling is made of.
func TestParseRefusals(t *testing.T) {
	big := strings.Repeat("a", 9<<10)
	tests := []struct {
		name string
		raw  []byte
		want error
	}{
		{
			"a transfer coding this server does not implement",
			req("POST /x HTTP/1.1", "Host: h", "Transfer-Encoding: gzip"),
			errUnsupportedCoding,
		},
		{
			"chunked stacked on itself",
			req("POST /x HTTP/1.1", "Host: h", "Transfer-Encoding: chunked, chunked"),
			errUnsupportedCoding,
		},
		{
			"Transfer-Encoding on HTTP/1.0",
			req("POST /x HTTP/1.0", "Transfer-Encoding: chunked"),
			ErrMalformed,
		},
		{
			"Transfer-Encoding beside Content-Length — the smuggling pair",
			req("POST /x HTTP/1.1", "Host: h", "Content-Length: 0", "Transfer-Encoding: chunked"),
			ErrMalformed,
		},
		{
			"a chunk size that is not hexadecimal",
			chunkedReq("zz\r\nhello\r\n0\r\n\r\n"),
			ErrMalformed,
		},
		{
			"chunk data not ended by CRLF",
			chunkedReq("5\r\nhelloXY0\r\n\r\n"),
			ErrMalformed,
		},
		{
			"a chunked body over the bound",
			chunkedReq("200000\r\n"),
			ErrTooLarge,
		},
		{
			"a trailer that is not a field line",
			chunkedReq("0\r\nnot a field line\r\n\r\n"),
			ErrMalformed,
		},
		{"two Content-Lengths", req("POST /x HTTP/1.1", "Host: h", "Content-Length: 0", "Content-Length: 5"), ErrMalformed},
		{"a signed Content-Length", req("POST /x HTTP/1.1", "Host: h", "Content-Length: +5"), ErrMalformed},
		{"a hex Content-Length", req("POST /x HTTP/1.1", "Host: h", "Content-Length: 0x5"), ErrMalformed},
		{"an empty Content-Length", req("POST /x HTTP/1.1", "Host: h", "Content-Length:"), ErrMalformed},
		{"no Host on HTTP/1.1", req("GET /x HTTP/1.1"), ErrMalformed},
		{"two Hosts", req("GET /x HTTP/1.1", "Host: a", "Host: b"), ErrMalformed},
		{"a space before the colon", req("GET /x HTTP/1.1", "Host: h", "X-Y : v"), ErrMalformed},
		{"obsolete line folding", req("GET /x HTTP/1.1", "Host: h", "X: a", "  continued"), ErrMalformed},
		{"absolute-form target", req("GET http://elsewhere/x HTTP/1.1", "Host: h"), ErrMalformed},
		{"authority-form target", req("CONNECT h:443 HTTP/1.1", "Host: h"), ErrMalformed},
		{"a version nobody speaks", req("GET /x HTTP/3.0", "Host: h"), ErrMalformed},
		{"four fields on the request line", req("GET /x HTTP/1.1 extra", "Host: h"), ErrMalformed},
		{"a method that is not a token", req("G(ET /x HTTP/1.1", "Host: h"), ErrMalformed},
		{"a control character in the target", []byte("GET /x\x00y HTTP/1.1\r\nHost: h\r\n\r\n"), ErrMalformed},
		{"a bare LF ending the request line", []byte("GET /x HTTP/1.1\nHost: h\r\n\r\n"), ErrMalformed},
		{"a bare LF inside a header value", []byte("GET /x HTTP/1.1\r\nHost: h\r\nX: a\nY: b\r\n\r\n"), ErrMalformed},
		{"a NUL inside a header value", []byte("GET /x HTTP/1.1\r\nHost: h\r\nX: a\x00b\r\n\r\n"), ErrMalformed},
		{"a C0 control inside a header value", []byte("GET /x HTTP/1.1\r\nHost: h\r\nX: a\x0cb\r\n\r\n"), ErrMalformed},
		{"a DEL inside a header value", []byte("GET /x HTTP/1.1\r\nHost: h\r\nX: a\x7fb\r\n\r\n"), ErrMalformed},
		{"a request line past the bound", req("GET /"+big+" HTTP/1.1", "Host: h"), ErrTooLarge},
		{"a body past the bound", req("POST /x HTTP/1.1", "Host: h", "Content-Length: 2000000"), ErrTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := parse(t, tt.raw)
			if !errors.Is(err, tt.want) {
				t.Fatalf("parse returned %v, want %v", err, tt.want)
			}
		})
	}
}

// Too many headers is bounded separately from their size: a thousand one-byte
// fields cost lookups rather than bytes.
func TestParseRefusesTooManyHeaders(t *testing.T) {
	lines := []string{"GET /x HTTP/1.1", "Host: h"}
	for i := range 200 {
		lines = append(lines, "X-"+string(rune('a'+i%26))+": v")
	}
	if _, _, err := parse(t, req(lines...)); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("parse returned %v, want ErrTooLarge", err)
	}
}

// A prefix is not a failure: it is a read that has not finished, and telling
// the two apart is what lets the reader wait rather than answer 400.
func TestParseReportsAnIncompleteRequest(t *testing.T) {
	full := req("POST /x HTTP/1.1", "Host: h", "Content-Length: 4")
	full = append(full, "abcd"...)
	for n := 1; n < len(full); n++ {
		if _, _, err := parse(t, full[:n]); !errors.Is(err, errIncomplete) {
			t.Fatalf("a %d-byte prefix returned %v, want errIncomplete", n, err)
		}
	}
	r, used, err := parse(t, full)
	if err != nil {
		t.Fatalf("the whole request returned %v", err)
	}
	if used != len(full) {
		t.Errorf("consumed %d of %d bytes", used, len(full))
	}
	if string(r.Body) != "abcd" {
		t.Errorf("body = %q", r.Body)
	}
}

func TestParseAcceptsWhatItShould(t *testing.T) {
	raw := req("POST /orders/7 HTTP/1.1", "Host: h", "Content-Type: application/json", "Content-Length: 2")
	raw = append(raw, "{}"...)

	r, _, err := parse(t, raw)
	if err != nil {
		t.Fatalf("parse returned %v", err)
	}
	if string(r.Method) != "POST" || string(r.Target) != "/orders/7" {
		t.Errorf("method/target = %q %q", r.Method, r.Target)
	}
	if got := string(r.Get("content-type")); got != "application/json" {
		t.Errorf("Get is not case-insensitive: %q", got)
	}
	if !r.KeepAlive {
		t.Error("HTTP/1.1 defaults to keep-alive")
	}
}

// The two versions default the connection opposite ways, and getting it
// backwards is a hang rather than a wrong answer.
func TestKeepAliveDefaultsPerVersion(t *testing.T) {
	tests := []struct {
		name string
		raw  []byte
		want bool
	}{
		{"1.1 keeps open", req("GET /x HTTP/1.1", "Host: h"), true},
		{"1.1 with close", req("GET /x HTTP/1.1", "Host: h", "Connection: close"), false},
		{"1.0 closes", req("GET /x HTTP/1.0"), false},
		{"1.0 with keep-alive", req("GET /x HTTP/1.0", "Connection: keep-alive"), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, _, err := parse(t, tt.raw)
			if err != nil {
				t.Fatalf("parse returned %v", err)
			}
			if r.KeepAlive != tt.want {
				t.Errorf("KeepAlive = %v, want %v", r.KeepAlive, tt.want)
			}
		})
	}
}

// Parsing hands out views into the read buffer, which is what makes it free.
func TestParseAllocatesNothing(t *testing.T) {
	raw := req("POST /x HTTP/1.1", "Host: h", "Content-Length: 2")
	raw = append(raw, "{}"...)
	cfg := Config{}.withDefaults()

	var sink Request
	arena := make([]Header, 0, 16) // the reader's, reused across batches
	allocs := testing.AllocsPerRun(200, func() {
		sink, _, arena, _ = parseRequest(raw, cfg, arena[:0], nil)
	})
	if allocs != 0 {
		t.Errorf("parsing allocated %.1f times per request; every field is a view into the buffer", allocs)
	}
	if string(sink.Body) != "{}" {
		t.Errorf("body = %q", sink.Body)
	}
}

// A chunked body — the sole Transfer-Encoding, no Content-Length — is
// decoded: sizes and extensions dropped, data concatenated, trailers parsed
// and discarded. Every strict prefix must read as incomplete, which is also
// what catches an in-place decode that corrupts bytes it will re-walk after
// the next read.
func TestChunkedBodyIsDecoded(t *testing.T) {
	raw := chunkedReq("5\r\nhello\r\n6;ext=1\r\n world\r\n0\r\nX-Trailer: v\r\n\r\n")
	for n := 1; n < len(raw); n++ {
		if _, _, err := parse(t, raw[:n]); !errors.Is(err, errIncomplete) {
			t.Fatalf("a %d-byte prefix returned %v, want errIncomplete", n, err)
		}
	}
	r, used, err := parse(t, raw)
	if err != nil {
		t.Fatalf("the whole request returned %v", err)
	}
	if used != len(raw) {
		t.Errorf("consumed %d of %d bytes", used, len(raw))
	}
	if string(r.Body) != "hello world" {
		t.Errorf("body = %q, want %q", r.Body, "hello world")
	}
}

// A chunk extension's meaning is ignored but its grammar (RFC 9112 §7.1.1) is
// not: a NUL, a bare LF or an unterminated quote inside one is a chunk-size
// line two parsers split differently.
func TestChunkExtensionGrammar(t *testing.T) {
	for _, ext := range []string{
		";a", ";a=b", ";a=\"q\"", ";a=\"q\\\"x\"", ";a;b=c", " ; a = b", ";a=\"\x80\"",
	} {
		r, _, err := parse(t, chunkedReq("5"+ext+"\r\nhello\r\n0\r\n\r\n"))
		if err != nil {
			t.Errorf("extension %q refused: %v", ext, err)
		} else if string(r.Body) != "hello" {
			t.Errorf("extension %q: body = %q", ext, r.Body)
		}
	}
	for _, ext := range []string{
		";a\x00b", ";a\nb", ";\"unterminated", ";a=\"unterminated", ";", ";=b", ";a=", "x", " ", ";a ",
		";a=\"\x01\"", ";a=b c",
	} {
		if _, _, err := parse(t, chunkedReq("5"+ext+"\r\nhello\r\n0\r\n\r\n")); !errors.Is(err, ErrMalformed) {
			t.Errorf("extension %q: got %v, want ErrMalformed", ext, err)
		}
	}
}

// The bytes behind a chunked body are the next pipelined request, and the
// in-place decode must leave them exactly where the consumed count says they
// start.
func TestChunkedBodyLeavesThePipelinedRequestBehindItIntact(t *testing.T) {
	raw := chunkedReq("5\r\nhello\r\n0\r\n\r\n")
	raw = append(raw, req("GET /y HTTP/1.1", "Host: h")...)

	first, used, err := parse(t, raw)
	if err != nil {
		t.Fatalf("the chunked request returned %v", err)
	}
	if string(first.Body) != "hello" {
		t.Errorf("body = %q", first.Body)
	}
	second, _, err := parse(t, raw[used:])
	if err != nil {
		t.Fatalf("the request behind the chunked body returned %v — the decode wrote past its own framing", err)
	}
	if string(second.Target) != "/y" {
		t.Errorf("second target = %q, want /y", second.Target)
	}
}

// Connection is a comma-separated list across any number of field lines
// (RFC 9110 §7.6.1), and a "close" inside one is a MUST (RFC 9112 §9.6): a
// whole-value comparison kept the connection open for a client that stopped
// reading it.
func TestConnectionOptionsAreReadAsAList(t *testing.T) {
	tests := []struct {
		name string
		raw  []byte
		want bool
	}{
		{"close inside a list", req("GET /x HTTP/1.1", "Host: h", "Connection: foo, close"), false},
		{"close on a second Connection line", req("GET /x HTTP/1.1", "Host: h", "Connection: foo", "Connection: close"), false},
		{"close with odd case and OWS", req("GET /x HTTP/1.1", "Host: h", "Connection: foo ,\tCLOSE"), false},
		{"close wins over keep-alive", req("GET /x HTTP/1.1", "Host: h", "Connection: keep-alive, close"), false},
		{"keep-alive inside a list on 1.0", req("GET /x HTTP/1.0", "Connection: x, Keep-Alive"), true},
		{"an unrelated token changes nothing", req("GET /x HTTP/1.1", "Host: h", "Connection: closely"), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, _, err := parse(t, tt.raw)
			if err != nil {
				t.Fatalf("parse returned %v", err)
			}
			if r.KeepAlive != tt.want {
				t.Errorf("KeepAlive = %v, want %v", r.KeepAlive, tt.want)
			}
		})
	}
}

// Expect is a list too (RFC 9110 §10.1.1): a 100-continue member must be
// seen wherever it sits, or the client waits for a 100 that never comes and
// the request dies at ReadTimeout.
func TestExpectContinueIsFoundInsideAList(t *testing.T) {
	tests := []struct {
		name string
		raw  []byte
	}{
		{"inside a list", req("POST /x HTTP/1.1", "Host: h", "Content-Length: 5", "Expect: x, 100-Continue")},
		{"on a second Expect line", req("POST /x HTTP/1.1", "Host: h", "Content-Length: 5", "Expect: x", "Expect: 100-continue")},
		{"with a chunked body still to come", req("POST /x HTTP/1.1", "Host: h", "Transfer-Encoding: chunked", "Expect: x, 100-continue")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, _, err := parse(t, tt.raw); !errors.Is(err, errExpectContinue) {
				t.Fatalf("a header-complete request with a pending body returned %v, want errExpectContinue", err)
			}
		})
	}
}

// HTAB is the one control byte field content keeps (RFC 9110 §5.5), so the
// refusal of the rest of the C0 range must not sweep it up.
func TestParseKeepsAnHTABInsideAHeaderValue(t *testing.T) {
	r, _, err := parse(t, []byte("GET /x HTTP/1.1\r\nHost: h\r\nX: a\tb\r\n\r\n"))
	if err != nil {
		t.Fatalf("parse refused a tab inside a value: %v", err)
	}
	if got := r.Get("x"); string(got) != "a\tb" {
		t.Errorf("value = %q, want %q", got, "a\tb")
	}
}

// The bounds are what their names say, to the byte: MaxRequestLineBytes is
// the line without its CRLF, MaxHeaderBytes the header section with its line
// endings and the blank line closing it.
func TestRequestLineAndHeaderBoundsAreExact(t *testing.T) {
	line := "GET /" + strings.Repeat("a", 20) + " HTTP/1.1"
	cfg := Config{MaxRequestLineBytes: len(line)}.withDefaults()
	raw := req(line, "Host: h")
	if _, _, _, err := parseRequest(raw, cfg, nil, nil); err != nil {
		t.Errorf("a request line of exactly MaxRequestLineBytes: %v", err)
	}
	cfg.MaxRequestLineBytes--
	if _, _, _, err := parseRequest(raw, cfg, nil, nil); !errors.Is(err, ErrTooLarge) {
		t.Errorf("a request line one byte over: %v, want ErrTooLarge", err)
	}
	// Incomplete, not refused, while the line can still end within the bound.
	cfg.MaxRequestLineBytes = len(line)
	if _, _, _, err := parseRequest([]byte(line+"\r"), cfg, nil, nil); !errors.Is(err, errIncomplete) {
		t.Errorf("a whole line awaiting its LF: %v, want errIncomplete", err)
	}

	headers := "Host: h\r\nX-A: " + strings.Repeat("b", 30) + "\r\n\r\n"
	raw = append([]byte("GET / HTTP/1.1\r\n"), headers...)
	cfg = Config{MaxHeaderBytes: len(headers)}.withDefaults()
	if _, _, _, err := parseRequest(raw, cfg, nil, nil); err != nil {
		t.Errorf("headers of exactly MaxHeaderBytes: %v", err)
	}
	cfg.MaxHeaderBytes--
	if _, _, _, err := parseRequest(raw, cfg, nil, nil); !errors.Is(err, ErrTooLarge) {
		t.Errorf("headers one byte over: %v, want ErrTooLarge", err)
	}
}

// Trailers are bounded in count as headers are: a thousand one-byte trailer
// fields fit any byte bound and still cost a parse each.
func TestTrailerFieldsAreBoundedInCount(t *testing.T) {
	cfg := Config{MaxHeaders: 4}.withDefaults()
	trailers := func(n int) []byte {
		return chunkedReq("0\r\n" + strings.Repeat("T: v\r\n", n) + "\r\n")
	}
	if _, _, _, err := parseRequest(trailers(4), cfg, nil, nil); err != nil {
		t.Errorf("MaxHeaders trailer fields: %v", err)
	}
	raw := trailers(5)
	if _, _, _, err := parseRequest(raw, cfg, nil, nil); !errors.Is(err, ErrTooLarge) {
		t.Errorf("MaxHeaders+1 trailer fields: %v, want ErrTooLarge", err)
	}
	// The count survives a resume: fed a byte at a time, the same request is
	// refused rather than accepted piecewise.
	var progress chunkProgress
	var err error
	for n := 1; n <= len(raw); n++ {
		_, _, _, err = parseRequest(raw[:n], cfg, nil, &progress)
		if !errors.Is(err, errIncomplete) {
			break
		}
	}
	if !errors.Is(err, ErrTooLarge) {
		t.Errorf("MaxHeaders+1 trailer fields fed bytewise: %v, want ErrTooLarge", err)
	}
}

// timeoutConn is a client that never sends: every read times out. It counts
// the reads, so a test can tell whether a silent connection was waited on
// once or twice.
type timeoutConn struct {
	net.Conn
	reads int
}

func (c *timeoutConn) Read([]byte) (int, error) {
	c.reads++
	return 0, os.ErrDeadlineExceeded
}
func (c *timeoutConn) SetReadDeadline(time.Time) error  { return nil }
func (c *timeoutConn) SetWriteDeadline(time.Time) error { return nil }
func (c *timeoutConn) SetDeadline(time.Time) error      { return nil }
func (c *timeoutConn) Write(b []byte) (int, error)      { return len(b), nil }
func (c *timeoutConn) Close() error                     { return nil }

// A connection silent through the protocol sniff is done: handing its empty
// prefix to the HTTP/1.1 reader waited a second IdleTimeout on it.
func TestSilentConnectionIsWaitedOnOnce(t *testing.T) {
	c := &timeoutConn{}
	cfg := Config{}.withDefaults()
	serveAny(t.Context(), c, cfg, h2ConfigOf(cfg, 0), func(b sluice.Batch[Request]) sluice.Batch[Response] {
		t.Error("the handler ran for a connection that sent nothing")
		return sluice.Batch[Response]{}
	})
	if c.reads != 1 {
		t.Errorf("%d reads of a silent connection, want 1", c.reads)
	}
}
