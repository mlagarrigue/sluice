package httpstream

import (
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// A cookie split into crumbs counts as one field against MaxHeaders,
// whatever else the block holds; a lone cookie is one field already.
func TestFieldCountJoinsTheCookieCrumbs(t *testing.T) {
	h := func(names ...string) []Header {
		fields := make([]Header, len(names))
		for i, n := range names {
			fields[i] = Header{Name: []byte(n)}
		}
		return fields
	}
	tests := []struct {
		fields []Header
		want   int
	}{
		{h(), 0},
		{h("cookie"), 1},
		{h("cookie", "x"), 2},
		{h("cookie", "x", "cookie"), 2},
		{h("cookie", "cookie", "cookie", "x"), 2},
	}
	for _, tt := range tests {
		if got := fieldCount(tt.fields); got != tt.want {
			t.Errorf("fieldCount(%d fields) = %d, want %d", len(tt.fields), got, tt.want)
		}
	}
}

// Empty crumbs join like any other: the delimiter alone.
func TestAppendCrumbOfEmptyCrumbs(t *testing.T) {
	if got := appendCrumb([]byte{}, false, nil); string(got) != "; " {
		t.Fatalf("appendCrumb(empty, empty) = %q, want %q", got, "; ")
	}
	if got := appendCrumb([]byte("a"), false, nil); string(got) != "a; " {
		t.Fatalf("appendCrumb(a, empty) = %q, want %q", got, "a; ")
	}
}

func TestIsHexIsHEXDIG(t *testing.T) {
	const hexdig = "0123456789abcdefABCDEF"
	for c := range 256 {
		if got, want := isHex(byte(c)), strings.IndexByte(hexdig, byte(c)) >= 0; got != want {
			t.Errorf("isHex(%q) = %v, want %v", byte(c), got, want)
		}
	}
}

func TestEqualFoldFoldsBothEnds(t *testing.T) {
	tests := []struct {
		got, want string
		eq        bool
	}{
		{"Z", "z", true},
		{"A", "a", true},
		{"z", "Z", true},
		{"a", "A", true},
		{"[", "{", false},
		{"@", "`", false},
	}
	for _, tt := range tests {
		if eq := equalFold([]byte(tt.got), tt.want); eq != tt.eq {
			t.Errorf("equalFold(%q, %q) = %v, want %v", tt.got, tt.want, eq, tt.eq)
		}
	}
}

// A percent-encoding is two hex digits after the "%", the last one too.
func TestPercentEncodingAtTheEnd(t *testing.T) {
	for _, target := range []string{"/a%4", "/a%g1", "/a%1g"} {
		if err := checkTarget([]byte("GET"), []byte(target), ErrMalformed); !errors.Is(err, ErrMalformed) {
			t.Errorf("checkTarget(%q) = %v, want ErrMalformed", target, err)
		}
		if validAuthority([]byte("h" + target[2:])) {
			t.Errorf("validAuthority(%q) = true, want false", "h"+target[2:])
		}
	}
	if err := checkTarget([]byte("GET"), []byte("/a%4f"), ErrMalformed); err != nil {
		t.Errorf("checkTarget(/a%%4f) = %v", err)
	}
	if !validAuthority([]byte("h%4f")) {
		t.Error("validAuthority(h%4f) = false")
	}
}

func TestAbsoluteFormAtItsEdges(t *testing.T) {
	authority, path, err := absoluteForm([]byte("http://"))
	if err != nil || len(authority) != 0 || string(path) != "/" {
		t.Errorf(`absoluteForm("http://") = %q, %q, %v; want "", "/", nil`, authority, path, err)
	}
	for _, target := range []string{"http:", "http:/", "h:x"} {
		if _, _, err := absoluteForm([]byte(target)); err == nil || !strings.Contains(err.Error(), "neither origin-form nor absolute-form") {
			t.Errorf("absoluteForm(%q) = %v, want neither form", target, err)
		}
	}
	if _, _, err := absoluteForm([]byte("://h/")); err == nil || !strings.Contains(err.Error(), `scheme "" is not http or https`) {
		t.Errorf(`absoluteForm("://h/") = %v, want the empty scheme named`, err)
	}
}

// Each refusal of the request line says what it found, at the edge of the
// rule that found it.
func TestSplitRequestLineAtItsEdges(t *testing.T) {
	tests := []struct {
		line string
		want string // in the error; "" for a line accepted
		is   error
	}{
		{" / HTTP/1.1", "the method is not a token", ErrMalformed},
		{"GET  HTTP/1.1", "neither origin-form nor absolute-form", ErrMalformed},
		{"GET /  HTTP/1.1", "more than three fields", ErrMalformed},
		{"GET / HTTP/0.9", `"HTTP/0.9"`, errVersionUnsupported},
		{"GET / HTTP/9.1", `"HTTP/9.1"`, errVersionUnsupported},
		{"GET / HTTP/1.0", "", nil},
		{"GET / HTTP/1.9", "", nil},
		{"GET / HTTP/:.1", "is not an HTTP version", ErrMalformed},
		{"GET / HTTP/1.:", "is not an HTTP version", ErrMalformed},
		{"GET / HTTP//.1", "is not an HTTP version", ErrMalformed},
		{"GET / HTTP/1./", "is not an HTTP version", ErrMalformed},
	}
	for _, tt := range tests {
		_, _, _, _, err := splitRequestLine([]byte(tt.line))
		if tt.want == "" {
			if err != nil {
				t.Errorf("%q: %v, want accepted", tt.line, err)
			}
			continue
		}
		if !errors.Is(err, tt.is) || !strings.Contains(err.Error(), tt.want) {
			t.Errorf("%q: %v, want %v saying %q", tt.line, err, tt.is, tt.want)
		}
	}
}

func TestHeaderLineWithAnEmptyName(t *testing.T) {
	if _, err := parseHeaderLine([]byte(":x")); err == nil || !strings.Contains(err.Error(), "is not a header name") {
		t.Errorf("parseHeaderLine(%q) = %v, want the empty name refused as a name", ":x", err)
	}
}

// The header section's budget counts every line ending, the blank line's
// too: a section of exactly MaxHeaderBytes is read, one byte more is
// refused, and no prefix of a section that fits is refused for its length.
func TestHeaderSectionAtItsBound(t *testing.T) {
	const bound = 32
	cfg := Config{MaxHeaderBytes: bound, MaxHeaders: 3}
	for _, lines := range []int{1, 2} {
		for total := bound - 2; total <= bound+2; total++ {
			raw := headerSection(lines, total)
			_, n, err := parseHeaders(nil, raw, cfg)
			if total <= bound {
				if err != nil || n != len(raw) {
					t.Errorf("%d lines, %d bytes: %d, %v; want the section read", lines, total, n, err)
				}
				for p := range raw {
					if _, _, err := parseHeaders(nil, raw[:p], cfg); !errors.Is(err, errIncomplete) {
						t.Errorf("%d lines, %d bytes, prefix %d: %v, want errIncomplete", lines, total, p, err)
					}
				}
				continue
			}
			if !errors.Is(err, errHeadersTooLarge) {
				t.Errorf("%d lines, %d bytes: %v, want errHeadersTooLarge", lines, total, err)
			}
		}
	}

	// Lines that take the whole budget leave none for the blank line, which
	// is refused as a line over the bound before it arrives; one byte short
	// of the budget, the section waits for its next byte.
	full := headerSection(1, bound+2)[:bound]
	if _, _, err := parseHeaders(nil, full, cfg); err == nil || !strings.Contains(err.Error(), "a header line is over the bound") {
		t.Errorf("a section at the budget: %v, want a line over the bound", err)
	}
	short := headerSection(1, bound+1)[:bound-1]
	if _, _, err := parseHeaders(nil, short, cfg); !errors.Is(err, errIncomplete) {
		t.Errorf("a section one byte short of the budget: %v, want errIncomplete", err)
	}
}

// headerSection is a header section of lines field lines, blank line
// included, total bytes long (total at least 6 per line).
func headerSection(lines, total int) []byte {
	var b strings.Builder
	value := total - 2 - lines*5 // "N: " and CRLF per line
	for i := range lines {
		n := value / lines
		if i == 0 {
			n += value % lines
		}
		b.WriteString(string(rune('a'+i)) + ": " + strings.Repeat("v", n) + "\r\n")
	}
	b.WriteString("\r\n")
	return []byte(b.String())
}

func TestHeaderCountAtItsBound(t *testing.T) {
	cfg := Config{MaxHeaderBytes: 1 << 10, MaxHeaders: 3}
	if _, _, err := parseHeaders(nil, []byte("a: 1\r\nb: 2\r\nc: 3\r\n\r\n"), cfg); err != nil {
		t.Errorf("three fields under MaxHeaders 3: %v", err)
	}
	if _, _, err := parseHeaders(nil, []byte("a: 1\r\nb: 2\r\nc: 3\r\nd: 4\r\n\r\n"), cfg); !errors.Is(err, errHeadersTooLarge) {
		t.Errorf("four fields under MaxHeaders 3: %v, want errHeadersTooLarge", err)
	}
}

// A chunked body at each of its bounds: the chunk-size line's length, the
// body's size summed over its chunks, the trailer section's bytes. What
// fits is decoded and none of its prefixes is refused, whether each read
// resumes the walk or starts it again; one byte more is refused.
func TestChunkedBodyAtItsBounds(t *testing.T) {
	cfg := Config{MaxBodyBytes: 8, MaxHeaderBytes: 32, MaxHeaders: 4}
	ext := func(n int) string { return "1;" + strings.Repeat("e", n-2) + "\r\na\r\n0\r\n\r\n" }
	trailer := func(total int) string { return "1\r\na\r\n0\r\n" + string(headerSection(1, total)) }
	trailers := func(total int) string { return "1\r\na\r\n0\r\n" + string(headerSection(2, total)) }
	tests := []struct {
		name    string
		encoded string
		body    string // "" when refused
		refused error
	}{
		{"one chunk of the bound", "8\r\n12345678\r\n0\r\n\r\n", "12345678", nil},
		{"two chunks of the bound", "4\r\n1234\r\n4\r\n5678\r\n0\r\n\r\n", "12345678", nil},
		{"one chunk over", "9\r\n123456789\r\n0\r\n\r\n", "", ErrTooLarge},
		{"two chunks over", "5\r\n12345\r\n4\r\n6789\r\n0\r\n\r\n", "", ErrTooLarge},
		{"a size line of the bound", ext(maxChunkLineBytes), "a", nil},
		{"a size line over", ext(maxChunkLineBytes + 1), "", ErrMalformed},
		{"a trailer section of the bound", trailer(32), "a", nil},
		{"two trailers of the bound", trailers(32), "a", nil},
		{"a trailer section over", trailer(33), "", errHeadersTooLarge},
		{"two trailers over", trailers(33), "", errHeadersTooLarge},
	}
	for _, tt := range tests {
		raw := []byte(tt.encoded)
		body, n, _, err := decodeChunked(append([]byte(nil), raw...), cfg, nil, 0, nil)
		if tt.refused != nil {
			if !errors.Is(err, tt.refused) {
				t.Errorf("%s: %v, want %v", tt.name, err, tt.refused)
			}
			continue
		}
		if err != nil || string(body) != tt.body || n != len(raw) {
			t.Errorf("%s: %q, %d, %v; want %q, %d", tt.name, body, n, err, tt.body, len(raw))
		}
		var progress chunkProgress
		for p := range raw {
			if _, _, _, err := decodeChunked(raw[:p], cfg, nil, 0, nil); !errors.Is(err, errIncomplete) {
				t.Errorf("%s, prefix %d: %v, want errIncomplete", tt.name, p, err)
			}
			if _, _, _, err := decodeChunked(raw[:p], cfg, &progress, 0, nil); !errors.Is(err, errIncomplete) {
				t.Errorf("%s, prefix %d resumed: %v, want errIncomplete", tt.name, p, err)
			}
		}
		body, _, _, err = decodeChunked(append([]byte(nil), raw...), cfg, &progress, 0, nil)
		if err != nil || string(body) != tt.body {
			t.Errorf("%s resumed: %q, %v; want %q", tt.name, body, err, tt.body)
		}
	}
}

// A chunked body whole in the read keeps its trailers apart from the
// headers, and a body without any leaves none.
func TestChunkedTrailersOfAWholeBody(t *testing.T) {
	r, _, err := parse(t, chunkedReq("1\r\na\r\n0\r\nX-T: 1\r\n\r\n"))
	if err != nil || len(r.trailers) != 1 || string(r.trailers[0].Name) != "X-T" {
		t.Fatalf("trailers = %q, %v; want X-T alone", r.trailers, err)
	}
	r, _, err = parse(t, chunkedReq("1\r\na\r\n0\r\n\r\n"))
	if err != nil || r.trailers != nil {
		t.Fatalf("trailers = %q, %v; want none", r.trailers, err)
	}
}

// Under BodyStreamed, a chunked body short of its end streams, and the
// walk the parse had saved is dropped: the body's reader starts it over.
func TestAStreamedChunkedBodyDropsTheSavedWalk(t *testing.T) {
	cfg := Config{BodyDelivery: BodyStreamed}.mustResolve()
	raw := chunkedReq("5\r\nab")
	var sb streamBody
	if _, _, _, err := parseRequest(raw, cfg, nil, nil); !errors.As(err, &sb) || !sb.chunked {
		t.Fatalf("no progress: %v, want a chunked body streamed", err)
	}
	progress := chunkProgress{w: 3, r: 7}
	if _, _, _, err := parseRequest(raw, cfg, nil, &progress); !errors.As(err, &sb) || !sb.chunked {
		t.Fatalf("with progress: %v, want a chunked body streamed", err)
	}
	if progress != (chunkProgress{}) {
		t.Errorf("progress = %+v after the body streamed, want it cleared", progress)
	}
}

// Under BodyStreamed, a Content-Length body whole in the read and of
// exactly MaxBodyBytes is handed over whole.
func TestAStreamedConfigHandsOverABodyOfTheBound(t *testing.T) {
	cfg := Config{BodyDelivery: BodyStreamed, MaxBodyBytes: 8, MaxStreamedBodyBytes: 64}.mustResolve()
	raw := append(req("POST /x HTTP/1.1", "Host: h", "Content-Length: 8"), "12345678"...)
	r, n, _, err := parseRequest(raw, cfg, nil, nil)
	if err != nil || string(r.Body) != "12345678" || n != len(raw) {
		t.Fatalf("parse = %q, %d, %v; want the body whole", r.Body, n, err)
	}
	raw = append(req("POST /x HTTP/1.1", "Host: h", "Content-Length: 9"), "123456789"...)
	if _, _, _, err := parseRequest(raw, cfg, nil, nil); !errors.As(err, new(streamBody)) {
		t.Fatalf("a body one byte over: %v, want it streamed", err)
	}
}

func TestContentLengthOfTheBound(t *testing.T) {
	cfg := Config{MaxBodyBytes: 8, MaxStreamedBodyBytes: 4}
	r := Request{Headers: []Header{{Name: []byte("Content-Length"), Value: []byte("8")}}}
	if n, err := contentLength(r, cfg); n != 8 || err != nil {
		t.Errorf("Content-Length 8 under a bound of 8: %d, %v", n, err)
	}
	r.Headers[0].Value = []byte("9")
	if _, err := contentLength(r, cfg); !errors.Is(err, ErrTooLarge) {
		t.Errorf("Content-Length 9 under a bound of 8: %v, want ErrTooLarge", err)
	}
}

func TestQuotedStringLen(t *testing.T) {
	tests := []struct {
		in   string
		want int
	}{
		{`"a"`, 3},
		{`"!"`, 3},
		{`"\\"`, 4},
		{`"\""`, 4},
		{`"a\"b"`, 6},
		{`"a\` + "\t" + `"`, 5},
		{`"a\`, -1},
		{`"a\` + "\x01" + `"`, -1},
		{`"a\` + "\x7f" + `"`, -1},
		{`"a` + "\x01" + `"`, -1},
		{`"ab`, -1},
		{`"`, -1},
	}
	for _, tt := range tests {
		if got := quotedStringLen([]byte(tt.in)); got != tt.want {
			t.Errorf("quotedStringLen(%q) = %d, want %d", tt.in, got, tt.want)
		}
	}
}

// A head dripped a byte per read is parsed at the byte one parse would
// take it, at each edge of the request line's and the header section's
// bounds, with or without the empty line RFC 9112 §2.2 lets come first.
func TestADrippedHeadAtTheBoundsIsParsedWhereOneParseIs(t *testing.T) {
	cfg := Config{BodyDelivery: BodyWhole, MaxHeaders: 3, MaxHeaderBytes: 48, MaxRequestLineBytes: 24}.mustResolve()
	const host = "Host: h\r\n"
	for _, lead := range []string{"", "\r\n"} {
		for line := cfg.MaxRequestLineBytes - 2; line <= cfg.MaxRequestLineBytes+2; line++ {
			target := "/" + strings.Repeat("a", line-len("GET  HTTP/1.1")-1)
			head := lead + "GET " + target + " HTTP/1.1\r\n" + host + "\r\n"
			compareDrip(t, cfg, "line "+itoa(line)+" lead "+itoa(len(lead)), []byte(head))
		}
		for _, lines := range []int{1, 2} {
			for total := cfg.MaxHeaderBytes - 2; total <= cfg.MaxHeaderBytes+2; total++ {
				head := lead + "GET / HTTP/1.1\r\n" + host + string(headerSection(lines, total-len(host)))
				compareDrip(t, cfg, "section "+itoa(total)+" lines "+itoa(lines)+" lead "+itoa(len(lead)), []byte(head))
			}
		}
	}
}

// compareDrip checks that raw dripped a byte per read ends at the byte, and
// with the error, that one parse of each prefix ends at.
func compareDrip(t *testing.T, cfg Config, name string, raw []byte) {
	t.Helper()
	want, wantErr := -1, error(nil)
	for n := 1; n <= len(raw) && want < 0; n++ {
		if _, _, _, err := parseRequest(raw[:n], cfg, nil, nil); !errors.Is(err, errIncomplete) {
			want, wantErr = n, err
		}
	}
	var progress chunkProgress
	var scan headScan
	got, gotErr := -1, error(nil)
	for n := 1; n <= len(raw) && got < 0; n++ {
		if headStillShort(raw[:n], &cfg, &scan) {
			continue
		}
		_, _, _, err := parseRequest(raw[:n], cfg, nil, &progress)
		if errors.Is(err, errIncomplete) {
			noteShortHead(raw[:n], &cfg, &progress, &scan)
			continue
		}
		got, gotErr = n, err
	}
	if got != want || (gotErr == nil) != (wantErr == nil) || gotErr != nil && gotErr.Error() != wantErr.Error() {
		t.Errorf("%s dripped: ended at byte %d with %v, one parse at byte %d with %v", name, got, gotErr, want, wantErr)
	}
}

func TestStatusAtItsEdges(t *testing.T) {
	if err := checkStatus(599); err != nil {
		t.Errorf("checkStatus(599) = %v", err)
	}
	if err := checkStatus(600); !errors.Is(err, ErrMalformed) {
		t.Errorf("checkStatus(600) = %v, want ErrMalformed", err)
	}
	for status, want := range map[int]string{299: "Success", 399: "Redirection", 499: "Client Error", 599: "Server Error"} {
		if got := reason(status); got != want {
			t.Errorf("reason(%d) = %q, want %q", status, got, want)
		}
	}
}

// Close reports a cut only when a request was in flight: a connection idle
// between two requests is closed without a word — on HTTP/2 too, whose
// phase was left busy by the read that brought the request in until the
// next frame arrived.
func TestServerCloseOfIdleConnectionsSaysNothing(t *testing.T) {
	for _, p := range stopProtocols[:2] {
		t.Run(p.name, func(t *testing.T) {
			h := newStopHandler()
			close(h.release)
			log := &lockedWriter{}
			srv := &Server{Config: Config{Logger: slog.New(slog.NewTextHandler(log, nil))}, Handler: h.handle}
			c, _ := p.start(t, srv)
			c.ask(t)
			if body := c.answer(t); body != "/slow" {
				t.Fatalf("the answer was %q", body)
			}
			// The answer is read before the server's side has finished
			// with it: the phase turns idle a moment later.
			for deadline := time.Now().Add(5 * time.Second); srv.live.busy() > 0 && time.Now().Before(deadline); {
				time.Sleep(time.Millisecond)
			}
			if err := srv.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if strings.Contains(log.String(), "in flight") {
				t.Errorf("Close of an idle connection logged %q", log.String())
			}
		})
	}
}
