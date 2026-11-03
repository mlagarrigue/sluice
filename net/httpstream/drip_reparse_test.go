package httpstream

import (
	"errors"
	"net"
	"strings"
	"testing"
	"time"
)

// What a body dripped one byte per read costs the reader, through the reader
// itself (h1Conn.next and bodyStream.next), not parseRequest alone: a
// connection that delivers one byte per Read, and a hook that runs inside
// Read — on the reader's own goroutine, so with no race — to plant a byte in
// what the reader already holds. A planted byte that changes the outcome at
// the very next read proves that region is looked at again on every read:
// up to MaxRequestLineBytes + MaxHeaderBytes per byte of a chunked body under
// BodyWhole, and up to MaxHeaderBytes per byte of a trailer line streamed.

var errDripStop = errors.New("test: the drip stops here")

type dripConn struct {
	net.Conn
	data []byte
	off  int
	stop int
	hook func(off int)
}

func (c *dripConn) Read(p []byte) (int, error) {
	if c.off >= c.stop || c.off >= len(c.data) {
		return 0, errDripStop
	}
	if c.hook != nil {
		c.hook(c.off)
	}
	p[0] = c.data[c.off]
	c.off++
	return 1, nil
}

func (c *dripConn) SetReadDeadline(time.Time) error { return nil }
func (c *dripConn) Write(p []byte) (int, error)     { return len(p), nil }

// Under BodyWhole, once the head of a request whose body is still short has
// been accepted, the method byte is overwritten in the reader's buffer and
// two more body bytes are delivered. A reader that does not parse the
// accepted head again ends on the drip's own error (the Content-Length
// control does); one that does fails on the request line.
func TestAWholeChunkedBodyDoesNotReparseItsHead(t *testing.T) {
	cfg := Config{BodyDelivery: BodyWhole, ReadTimeout: 30 * time.Second}.mustResolve()
	head := "POST /x HTTP/1.1\r\nHost: h\r\n"
	cases := []struct{ name, prefix string }{
		{"content-length", head + "Content-Length: 100\r\n\r\nab"},
		{"chunked", head + "Transfer-Encoding: chunked\r\n\r\n64\r\nab"},
	}
	errs := map[string]error{}
	for _, tc := range cases {
		data := []byte(tc.prefix + strings.Repeat("c", 98) + "\r\n0\r\n\r\n")
		r := &h1Conn{cfg: cfg}
		conn := &dripConn{data: data, stop: len(tc.prefix) + 2}
		conn.hook = func(off int) {
			if off == len(tc.prefix) {
				r.buf[0] = '(' // POST becomes (OST: not a token
			}
		}
		r.conn = conn
		if batch, ok := r.next(); ok {
			t.Fatalf("%s: a batch of %d out of a body short of its end", tc.name, len(batch))
		}
		errs[tc.name] = r.err
	}
	if !errors.Is(errs["content-length"], errDripStop) {
		t.Fatalf("control: the Content-Length request ended on %v, not the drip's end; the test cannot tell", errs["content-length"])
	}
	if !errors.Is(errs["chunked"], errDripStop) {
		t.Errorf("the chunked request ended on %v: its accepted head was parsed again by a read of its body", errs["chunked"])
	}
}

// Under BodyStreamed (the default), a trailer field line still short of its
// CRLF: once eight bytes of it are held, a CRLF is planted at its second
// byte, already looked at, and two more bytes are delivered. A scan that
// resumes where it stopped does not see it and ends on the drip's error; a
// scan from the line's start cuts the line "X" and refuses it.
func TestAStreamedTrailerLineIsScannedOnce(t *testing.T) {
	cfg := Config{ReadTimeout: 30 * time.Second}.mustResolve()
	head := "POST /x HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n"
	data := []byte(head + "0\r\nX-Trailer: " + strings.Repeat("a", 64) + "\r\n\r\n")
	r := &h1Conn{cfg: cfg}
	conn := &dripConn{data: data, stop: len(data)}
	r.conn = conn
	batch, ok := r.next()
	if !ok || len(batch) != 1 || r.body == nil {
		t.Fatalf("the head: %d requests, body %v, %v", len(batch), r.body != nil, r.err)
	}
	b := r.body
	planted := false
	conn.hook = func(off int) {
		if !planted && b.state == chunkTrailer && r.n-b.pos >= 8 {
			r.buf[b.pos+1], r.buf[b.pos+2] = '\r', '\n'
			planted = true
			conn.stop = off + 2
		}
	}
	var err error
	for {
		var data []byte
		data, err = b.next()
		if err != nil || data == nil {
			break
		}
	}
	if !planted {
		t.Fatalf("the CRLF was never planted; the body ended on %v", err)
	}
	if !errors.Is(err, errDripStop) {
		t.Errorf("the body ended on %v: the partial trailer line was scanned again from its start by a later read", err)
	}
}
