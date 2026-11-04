package httpstream

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"
)

// scriptConn delivers data in the reads its sizes say — a size of zero
// takes what the reader has room for — then a byte per read, and records
// what is written. Past the data it returns errDripStop. hook runs inside
// each Read, on the reader's goroutine, before the bytes are copied.
type scriptConn struct {
	net.Conn
	data  []byte
	off   int
	sizes []int
	hook  func(off int)
	wrote bytes.Buffer
}

func (c *scriptConn) Read(p []byte) (int, error) {
	if c.off >= len(c.data) {
		return 0, errDripStop
	}
	if c.hook != nil {
		c.hook(c.off)
	}
	n := 1
	if len(c.sizes) > 0 {
		n = c.sizes[0]
		c.sizes = c.sizes[1:]
		if n == 0 {
			n = len(p)
		}
	}
	n = copy(p[:min(n, len(p))], c.data[c.off:])
	c.off += n
	return n, nil
}

func (c *scriptConn) SetReadDeadline(time.Time) error  { return nil }
func (c *scriptConn) SetWriteDeadline(time.Time) error { return nil }
func (c *scriptConn) Write(p []byte) (int, error)      { return c.wrote.Write(p) }

// A request at every bound at once — the empty line RFC 9112 §2.2 lets
// come first, a request line of MaxRequestLineBytes, a header section of
// MaxHeaderBytes and a body of MaxBodyBytes — fits the reader's buffer: the
// buffer's ceiling counts the line endings the bounds leave out.
func TestARequestAtEveryBoundFitsTheBuffer(t *testing.T) {
	cfg := Config{BodyDelivery: BodyWhole, MaxRequestLineBytes: 24, MaxHeaderBytes: 32, MaxBodyBytes: 8}.mustResolve()
	line := "POST /" + strings.Repeat("a", 24-len("POST / HTTP/1.1")) + " HTTP/1.1"
	head := "Host: hhh\r\nContent-Length: 8\r\n\r\n"
	if len(line) != cfg.MaxRequestLineBytes || len(head) != cfg.MaxHeaderBytes {
		t.Fatalf("the request is %d and %d bytes, not at the bounds", len(line), len(head))
	}
	raw := "\r\n" + line + "\r\n" + head + "12345678"
	for _, sizes := range [][]int{{0, 0}, nil} {
		r := &h1Conn{cfg: cfg, conn: &scriptConn{data: []byte(raw), sizes: sizes}}
		batch, ok := r.next()
		if !ok || len(batch) != 1 || string(batch[0].Body) != "12345678" {
			t.Fatalf("reads %v: %d requests, %v", sizes, len(batch), r.err)
		}
	}
}

// What one connection's reader takes into a batch: MaxRequestsPerBatch
// requests, and no more than MaxRequestsPerConn leaves — counted over the
// batches already served, not only this one — the request that spends it
// saying close.
func TestBatchesAtTheirBounds(t *testing.T) {
	get := string(req("GET / HTTP/1.1", "Host: h"))
	tests := []struct {
		name         string
		batch, conn  int
		requests     int
		want         []int
		lastKeepsAll bool
	}{
		{"the batch bound", 2, 100, 3, []int{2, 1}, true},
		{"the connection bound over two batches", 2, 3, 4, []int{2, 1}, false},
	}
	for _, tt := range tests {
		cfg := Config{MaxRequestsPerBatch: tt.batch, MaxRequestsPerConn: tt.conn}.mustResolve()
		r := &h1Conn{cfg: cfg, conn: &scriptConn{data: []byte(strings.Repeat(get, tt.requests)), sizes: []int{0}}}
		var got []int
		var last Request
		for {
			batch, ok := r.next()
			if !ok {
				break
			}
			got = append(got, len(batch))
			last = batch[len(batch)-1]
			if !r.keepAlive(batch) {
				break
			}
		}
		if len(got) != len(tt.want) || got[0] != tt.want[0] || got[1] != tt.want[1] {
			t.Errorf("%s: batches %v, want %v", tt.name, got, tt.want)
		}
		if last.KeepAlive != tt.lastKeepsAll {
			t.Errorf("%s: the last request's KeepAlive is %v", tt.name, last.KeepAlive)
		}
	}
}

// A body that waits for its invitation gets one 100 Continue, however many
// reads its body takes to arrive.
func TestAnExpectedBodyIsInvitedOnce(t *testing.T) {
	cfg := Config{BodyDelivery: BodyWhole}.mustResolve()
	head := string(req("POST /x HTTP/1.1", "Host: h", "Expect: 100-continue", "Content-Length: 4"))
	conn := &scriptConn{data: []byte(head + "abcd"), sizes: []int{len(head)}}
	r := &h1Conn{cfg: cfg, conn: conn}
	batch, ok := r.next()
	if !ok || len(batch) != 1 || string(batch[0].Body) != "abcd" {
		t.Fatalf("%d requests, %v", len(batch), r.err)
	}
	if n := strings.Count(conn.wrote.String(), "100 Continue"); n != 1 {
		t.Errorf("%d interim responses written, want one: %q", n, conn.wrote.String())
	}
}

// A request whose body streams and which spends the connection's budget
// says close, like any other request that spends it.
func TestAStreamedBodySpendingTheBudgetSaysClose(t *testing.T) {
	head := string(req("POST /x HTTP/1.1", "Host: h", "Content-Length: 4"))
	for _, budget := range []int{1, 2} {
		cfg := Config{MaxRequestsPerConn: budget}.mustResolve()
		r := &h1Conn{cfg: cfg, conn: &scriptConn{data: []byte(head + "abcd"), sizes: []int{len(head)}}}
		batch, ok := r.next()
		if !ok || len(batch) != 1 || r.body == nil {
			t.Fatalf("budget %d: %d requests, body %v, %v", budget, len(batch), r.body != nil, r.err)
		}
		if want := budget > 1; batch[0].KeepAlive != want {
			t.Errorf("budget %d: KeepAlive %v, want %v", budget, batch[0].KeepAlive, want)
		}
	}
}

// readConn reads through its function.
type readConn struct {
	net.Conn
	read func([]byte) (int, error)
}

func (c *readConn) Read(p []byte) (int, error)      { return c.read(p) }
func (c *readConn) SetReadDeadline(time.Time) error { return nil }

// The connection's phase follows its reads: busy from the first byte of a
// request, idle still after a read that brought nothing, and a read the
// shutdown closed between two requests is a clean end.
func TestTheReaderMarksItsPhase(t *testing.T) {
	cfg := Config{}.mustResolve()
	st := &connState{drain: t.Context()}
	r := &h1Conn{cfg: cfg, state: st, conn: &readConn{read: func(p []byte) (int, error) { p[0] = 'G'; return 1, nil }}}
	if err := r.fill(); err != nil || st.phase.Load() != connBusy {
		t.Errorf("a byte read: %v, phase %d, want busy", err, st.phase.Load())
	}

	st = &connState{drain: t.Context()}
	r = &h1Conn{cfg: cfg, state: st, conn: &readConn{read: func([]byte) (int, error) { return 0, io.EOF }}}
	if err := r.fill(); !errors.Is(err, io.EOF) || st.phase.Load() != connIdle {
		t.Errorf("nothing read: %v, phase %d, want io.EOF and idle", err, st.phase.Load())
	}

	ctx, cancel := context.WithCancel(t.Context())
	st = &connState{drain: ctx}
	r = &h1Conn{cfg: cfg, state: st, conn: &readConn{read: func([]byte) (int, error) { cancel(); return 0, net.ErrClosed }}}
	if err := r.fill(); err != io.EOF { //nolint:errorlint // the clean end is io.EOF itself
		t.Errorf("closed under the read by the shutdown: %v, want io.EOF", err)
	}
}

// The request behind a batch, its head accepted and its body short, is not
// parsed again once the batch ahead of it is consumed: its method byte is
// overwritten in between, and the reader ends on the drip's end rather than
// on a refusal.
func TestAPipelinedBodyIsNotParsedAgainAfterTheBatchAhead(t *testing.T) {
	cfg := Config{BodyDelivery: BodyWhole}.mustResolve()
	get := string(req("GET / HTTP/1.1", "Host: h"))
	post := string(req("POST /x HTTP/1.1", "Host: h", "Content-Length: 100")) + "ab"
	conn := &scriptConn{data: []byte(get + post + strings.Repeat("c", 98)), sizes: []int{len(get) + len(post)}}
	r := &h1Conn{cfg: cfg, conn: conn}
	batch, ok := r.next()
	if !ok || len(batch) != 1 {
		t.Fatalf("%d requests, %v", len(batch), r.err)
	}
	r.buf[len(get)] = '(' // POST becomes (OST: not a token
	conn.data = conn.data[:len(get)+len(post)+2]
	if batch, ok := r.next(); ok {
		t.Fatalf("a batch of %d out of a body short of its end", len(batch))
	}
	if !errors.Is(r.err, errDripStop) {
		t.Errorf("the reader ended on %v: the accepted head was parsed again", r.err)
	}
}

// streamed reads a request whose head comes in one read and whose body
// streams, dripped a byte per read or in reads as large as the buffer, and
// returns the body, its trailers and the error that ended it.
func streamed(t *testing.T, cfg Config, head, encoded string, drip bool) (string, []Header, error) {
	t.Helper()
	sizes := []int{len(head)}
	if !drip {
		sizes = append(sizes, make([]int, 64)...)
	}
	r := &h1Conn{cfg: cfg, conn: &scriptConn{data: []byte(head + encoded), sizes: sizes}}
	batch, ok := r.next()
	if !ok || len(batch) != 1 || r.body == nil {
		t.Fatalf("the head: %d requests, body %v, %v", len(batch), r.body != nil, r.err)
	}
	var body []byte
	for {
		data, err := r.body.next()
		if err != nil {
			return string(body), nil, err
		}
		if data == nil {
			return string(body), batch[0].Trailers(), nil
		}
		body = append(body, data...)
	}
}

// A streamed chunked body at each of its bounds, dripped and read in large
// reads: the chunk-size line's length, the body's size over its chunks, the
// trailer section's bytes and fields. What fits is read whole, trailers
// included; one more is refused.
func TestAStreamedChunkedBodyAtItsBounds(t *testing.T) {
	cfg := Config{MaxBodyBytes: 8, MaxHeaderBytes: 64, MaxHeaders: 4}.mustResolve()
	head := string(req("POST /x HTTP/1.1", "Host: h", "Transfer-Encoding: chunked"))
	ext := func(n int) string { return "1;" + strings.Repeat("e", n-2) + "\r\na\r\n0\r\n\r\n" }
	trailer := func(lines, total int) string { return "1\r\na\r\n0\r\n" + string(headerSection(lines, total)) }
	count := func(n int) string { return "1\r\na\r\n0\r\n" + strings.Repeat("t: vv\r\n", n) + "\r\n" }
	tests := []struct {
		name     string
		encoded  string
		body     string
		trailers int // the trailer value bytes
		refused  error
	}{
		{"two chunks of the bound", "4\r\n1234\r\n4\r\n5678\r\n0\r\n\r\n", "12345678", 0, nil},
		{"two chunks over", "5\r\n12345\r\n4\r\n6789\r\n0\r\n\r\n", "", 0, ErrTooLarge},
		{"a size line of the bound", ext(maxChunkLineBytes), "a", 0, nil},
		{"a size line over", ext(maxChunkLineBytes + 1), "", 0, ErrMalformed},
		{"a trailer section of the bound", trailer(1, 64), "a", 64 - 7, nil},
		{"two trailers of the bound", trailer(2, 64), "a", 64 - 12, nil},
		{"a trailer section over", trailer(1, 65), "", 0, errHeadersTooLarge},
		{"two trailers over", trailer(2, 65), "", 0, errHeadersTooLarge},
		{"trailer fields of the bound", count(4), "a", 4 * 2, nil},
		{"a trailer field over", count(5), "", 0, errHeadersTooLarge},
	}
	for _, tt := range tests {
		for _, drip := range []bool{true, false} {
			body, trailers, err := streamed(t, cfg, head, tt.encoded, drip)
			if tt.refused != nil {
				if !errors.Is(err, tt.refused) {
					t.Errorf("%s, drip %v: %v, want %v", tt.name, drip, err, tt.refused)
				}
				continue
			}
			values := 0
			for _, h := range trailers {
				if len(h.Name) != 1 || strings.Trim(string(h.Value), "v") != "" {
					t.Errorf("%s, drip %v: trailer %q: %q", tt.name, drip, h.Name, h.Value)
				}
				values += len(h.Value)
			}
			if err != nil || body != tt.body || values != tt.trailers {
				t.Errorf("%s, drip %v: %q with %d trailer bytes, %v; want %q with %d", tt.name, drip, body, values, err, tt.body, tt.trailers)
			}
		}
	}
}

// A trailer section near MaxHeaderBytes is larger than the buffer the head
// was read into: the body's reads grow it.
func TestAStreamedTrailerLargerThanTheFirstBuffer(t *testing.T) {
	cfg := Config{MaxBodyBytes: 8}.mustResolve()
	head := string(req("POST /x HTTP/1.1", "Host: h", "Transfer-Encoding: chunked"))
	value := strings.Repeat("v", cfg.MaxHeaderBytes-64)
	_, trailers, err := streamed(t, cfg, head, "1\r\na\r\n0\r\nt: "+value+"\r\n\r\n", false)
	if err != nil || len(trailers) != 1 || len(trailers[0].Value) != len(value) {
		t.Fatalf("%d trailers, %v; want the one of %d bytes", len(trailers), err, len(value))
	}
}

// The end of a streamed body is told to whoever watches for it, once.
func TestAStreamedBodyEndIsTold(t *testing.T) {
	head := string(req("POST /x HTTP/1.1", "Host: h", "Content-Length: 4"))
	r := &h1Conn{cfg: Config{}.mustResolve(), conn: &scriptConn{data: []byte(head + "abcd"), sizes: []int{len(head)}}}
	if _, ok := r.next(); !ok || r.body == nil {
		t.Fatalf("the head: body %v, %v", r.body != nil, r.err)
	}
	told := 0
	r.body.onDone = func() { told++ }
	for {
		data, err := r.body.next()
		if err != nil || data == nil {
			break
		}
	}
	if told != 1 {
		t.Errorf("the end was told %d times, want once", told)
	}
}

// With no time left on its clock, a streamed body's read is refused before
// it waits.
func TestAStreamedBodyWithNoTimeLeft(t *testing.T) {
	cfg := Config{}.mustResolve()
	cfg.ReadTimeout = 0
	head := string(req("POST /x HTTP/1.1", "Host: h", "Content-Length: 4"))
	r := &h1Conn{cfg: cfg, conn: &scriptConn{data: []byte(head + "abcd"), sizes: []int{len(head)}}}
	if _, ok := r.next(); !ok || r.body == nil {
		t.Fatalf("the head: body %v, %v", r.body != nil, r.err)
	}
	if _, err := r.body.next(); !errors.Is(err, os.ErrDeadlineExceeded) || !strings.Contains(err.Error(), "did not move") {
		t.Errorf("the body read: %v, want the read timeout", err)
	}
}

// ReadAll takes a body of exactly its limit, whole or streamed.
func TestReadAllOfTheLimit(t *testing.T) {
	whole := Request{Body: []byte("12345678")}
	if got, err := whole.ReadAll(nil, 8); err != nil || string(got) != "12345678" {
		t.Errorf("whole, limit 8: %q, %v", got, err)
	}
	if _, err := whole.ReadAll(nil, 7); !errors.Is(err, ErrTooLarge) {
		t.Errorf("whole, limit 7: %v, want ErrTooLarge", err)
	}
	head := string(req("POST /x HTTP/1.1", "Host: h", "Content-Length: 8"))
	for limit, ok := range map[int]bool{8: true, 7: false} {
		r := &h1Conn{cfg: Config{}.mustResolve(), conn: &scriptConn{data: []byte(head + "12345678"), sizes: []int{len(head)}}}
		batch, _ := r.next()
		if len(batch) != 1 || r.body == nil {
			t.Fatalf("the head: %d requests, %v", len(batch), r.err)
		}
		got, err := batch[0].ReadAll(nil, limit)
		if ok && (err != nil || string(got) != "12345678") || !ok && !errors.Is(err, ErrTooLarge) {
			t.Errorf("streamed, limit %d: %q, %v", limit, got, err)
		}
	}
}
