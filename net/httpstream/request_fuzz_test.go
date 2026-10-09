package httpstream

import (
	"errors"
	"testing"
)

// FuzzParseRequest is the test this package needs most.
//
// Everything else here checks a refusal someone thought of. This checks the
// ones nobody did: for any bytes at all, parsing must either refuse or return
// a consistent result, and it must never panic, never read past the buffer,
// and never claim to have consumed more than it was given. A parser exposed to
// hostile input that has not been fuzzed is a parser whose refusals are a
// wish list.
func FuzzParseRequest(f *testing.F) {
	f.Add([]byte("GET /x HTTP/1.1\r\nHost: h\r\n\r\n"))
	f.Add([]byte("POST /x HTTP/1.1\r\nHost: h\r\nContent-Length: 2\r\n\r\n{}"))
	f.Add([]byte("GET /x HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n"))
	f.Add([]byte("GET /x HTTP/1.1\r\nHost: a\r\nHost: b\r\n\r\n"))
	f.Add([]byte("GET /x HTTP/1.1\nHost: h\r\n\r\n"))
	f.Add([]byte("GET /x HTTP/1.1\r\nX : v\r\nHost: h\r\n\r\n"))
	f.Add([]byte("\r\n\r\n"))
	f.Add([]byte{})

	cfg := Config{}.withDefaults()
	f.Fuzz(func(t *testing.T, data []byte) {
		// A chunked body is decoded in place, so a successful parse may
		// rewrite data's consumed region; the determinism check at the end
		// needs the bytes as they arrived.
		orig := append([]byte(nil), data...)
		req, n, arena, err := parseRequest(data, cfg, nil, nil)
		if err != nil {
			if n != 0 {
				t.Errorf("a refused request reported %d bytes consumed", n)
			}
			return
		}
		if n < 0 || n > len(data) {
			t.Fatalf("consumed %d of %d bytes", n, len(data))
		}
		// Everything handed out must be a view into what was given, or the
		// caller is holding memory this parser invented.
		if !within(data, req.Method) || !within(data, req.Target) || !within(data, req.Body) {
			t.Fatal("a field does not point into the input buffer")
		}
		for _, h := range arena {
			if !within(data, h.Name) || !within(data, h.Value) {
				t.Fatal("a header does not point into the input buffer")
			}
			if !isToken(h.Name) {
				t.Errorf("an accepted header name is not a token: %q", h.Name)
			}
			for _, c := range h.Value {
				// RFC 9110 §5.5's field-vchar: every C0 control but HTAB is
				// out, and so is DEL — not only the CR/LF/NUL splitting bytes.
				if (c < 0x20 && c != '\t') || c == 0x7f {
					t.Errorf("an accepted header value holds a control character: %q", h.Value)
				}
			}
		}
		// An accepted request must satisfy the framing rules: exactly the
		// invariants the refusals above are meant to guarantee. The one
		// Transfer-Encoding shape accepted is chunked alone, with no
		// Content-Length beside it.
		if te := req.Get("transfer-encoding"); te != nil {
			if !equalFold(te, "chunked") || req.Get("content-length") != nil {
				t.Errorf("a request with Transfer-Encoding %q was accepted", te)
			}
		}
		if len(req.Body) > cfg.MaxBodyBytes {
			t.Errorf("a body of %d bytes was accepted", len(req.Body))
		}
		// Parsing the same bytes twice must agree — on the pristine copy,
		// since the first parse may have decoded a chunked body in place.
		if _, n2, _, err2 := parseRequest(orig, cfg, nil, nil); n2 != n || !errors.Is(err2, err) {
			t.Errorf("parsing is not deterministic: (%d,%v) then (%d,%v)", n, err, n2, err2)
		}
		// The same bytes arriving one at a time, resumed through a saved
		// chunk walk, must land on the same body: a walk resumed from a
		// stale or misplaced offset would accept or decode something else.
		if len(orig) <= 4<<10 {
			fed := append([]byte(nil), orig...)
			var progress chunkProgress
			var r3 Request
			var n3 int
			err3 := errIncomplete
			for k := 1; k <= len(fed) && errors.Is(err3, errIncomplete); k++ {
				r3, n3, _, err3 = parseRequest(fed[:k], cfg, nil, &progress)
			}
			if n3 != n || !errors.Is(err3, err) || string(r3.Body) != string(req.Body) {
				t.Errorf("incremental parse disagrees: (%d,%v,%q) vs (%d,%v,%q)", n3, err3, r3.Body, n, err, req.Body)
			}
		}
	})
}

// within reports whether view is a sub-slice of buf, or empty.
//
// It compares contents rather than addresses: this package holds no unsafe,
// and a view whose bytes appear in the input at the length it claims is what
// the property is actually about — that nothing was invented or copied.
func within(buf, view []byte) bool {
	if len(view) == 0 {
		return true
	}
	if len(view) > len(buf) {
		return false
	}
	for i := 0; i+len(view) <= len(buf); i++ {
		if string(buf[i:i+len(view)]) == string(view) {
			return true
		}
	}
	return false
}
