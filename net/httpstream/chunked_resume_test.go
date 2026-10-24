package httpstream

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
)

// A chunked body fed one byte at a time through a saved walk is validated
// once: the progress only ever advances, and the result matches the one-shot
// parse. This is the resumption decodeChunked relies on, checked where it
// is cheapest to see.
func TestChunkedWalkResumesWhereItStopped(t *testing.T) {
	raw := chunkedReq("5\r\nhello\r\n6;ext=1\r\n world\r\n0\r\nX-Trailer: v\r\n\r\n")
	cfg := Config{BodyDelivery: BodyWhole}.mustResolve()
	var progress chunkProgress
	last := progress
	for n := 1; n < len(raw); n++ {
		if _, _, _, err := parseRequest(raw[:n], cfg, nil, &progress); !errors.Is(err, errIncomplete) {
			t.Fatalf("a %d-byte prefix returned %v, want errIncomplete", n, err)
		}
		if progress.r < last.r || progress.w < last.w {
			t.Fatalf("after %d bytes the walk went backwards: %+v then %+v", n, last, progress)
		}
		last = progress
	}
	if last.r == 0 {
		t.Fatal("no chunk was ever recorded as validated")
	}
	r, used, _, err := parseRequest(raw, cfg, nil, &progress)
	if err != nil || used != len(raw) || string(r.Body) != "hello world" {
		t.Fatalf("resumed parse: body=%q used=%d err=%v", r.Body, used, err)
	}
	if progress != (chunkProgress{}) {
		t.Errorf("a completed request left its progress behind: %+v", progress)
	}
}

// oneWritePerRead is what a socket looks like when the client sends each
// chunk in its own packet: every Write on the client end is one Read on the
// server end, which is what net.Pipe guarantees.
func oneWritePerRead(t *testing.T, writes ...[]byte) net.Conn {
	t.Helper()
	client, server := net.Pipe()
	go func() {
		defer client.Close()
		for _, w := range writes {
			if _, err := client.Write(w); err != nil {
				return
			}
		}
	}()
	t.Cleanup(func() { server.Close() })
	return server
}

// readAll drains the connection's requests into copies, since every field
// borrows the read buffer.
func readAll(t *testing.T, c net.Conn, cfg Config) (bodies [][]byte, err error) {
	t.Helper()
	src := Requests(c, cfg)
	src.Stream()(func(b sluice.Batch[Request]) bool {
		for i := range b.Items {
			r := &b.Items[i]
			bodies = append(bodies, append([]byte(nil), r.Body...))
		}
		return true
	})
	return bodies, src.Err()
}

// rewalkProbe is a connection that delivers one write per Read and, before
// every Read once the first chunk is in, overwrites that chunk's size digit
// in the reader's own buffer with a byte no size line may hold. A walk that
// resumes from the saved progress never reads it again; one that restarts
// from the body's first chunk fails on it. The digit is put back before the
// terminating chunk, which the moving pass then walks from the start.
type rewalkProbe struct {
	net.Conn // the deadlines; never read
	r        *reader
	writes   [][]byte
	at       int // the first chunk's size digit in r.buf
}

func (p *rewalkProbe) Read(b []byte) (int, error) {
	if len(p.writes) == 0 {
		return 0, io.EOF
	}
	if p.r.n > p.at {
		if len(p.writes) == 1 {
			p.r.buf[p.at] = '1'
		} else {
			p.r.buf[p.at] = 'z'
		}
	}
	n := copy(b, p.writes[0])
	if p.writes[0] = p.writes[0][n:]; len(p.writes[0]) == 0 {
		p.writes = p.writes[1:]
	}
	return n, nil
}

// 40 000 one-byte chunks, each in its own read, used to cost twelve seconds
// of CPU: every read re-walked the body from its first chunk. The bound on
// the wall clock that guarded it failed under a loaded runner at 6.5 s
// (CI 2026-10-16) without the walk restarting; the probe asks the question
// itself — is a validated byte ever read twice? — and the clock has no say.
func TestChunkedBodyArrivingOneChunkPerReadIsLinear(t *testing.T) {
	const chunks = 1000
	head := req("POST /x HTTP/1.1", "Host: h", "Transfer-Encoding: chunked", "Connection: close")
	writes := make([][]byte, 0, chunks+2)
	writes = append(writes, head)
	var want bytes.Buffer
	for i := range chunks {
		c := byte('a' + i%26)
		want.WriteByte(c)
		writes = append(writes, fmt.Appendf(nil, "1\r\n%c\r\n", c))
	}
	writes = append(writes, []byte("0\r\n\r\n"))

	client, server := net.Pipe()
	t.Cleanup(func() { client.Close(); server.Close() })
	r := &reader{cfg: Config{BodyDelivery: BodyWhole, ReadTimeout: 30 * time.Second}.mustResolve()}
	r.conn = &rewalkProbe{Conn: server, r: r, writes: writes, at: len(head)}
	var bodies [][]byte
	r.stream(func(b sluice.Batch[Request]) bool {
		for i := range b.Items {
			bodies = append(bodies, append([]byte(nil), b.Items[i].Body...))
		}
		return true
	})
	if r.err != nil {
		t.Fatalf("stream ended with %v: the walk read a chunk it had already validated", r.err)
	}
	if len(bodies) != 1 || !bytes.Equal(bodies[0], want.Bytes()) {
		t.Fatalf("got %d bodies; want one of %d bytes", len(bodies), want.Len())
	}
}

// The saved walk indexes the read buffer, which shifts when a batch is
// consumed: a chunked request pipelined behind a complete one starts its
// walk at one offset and resumes at another.
func TestChunkedWalkSurvivesBufferCompaction(t *testing.T) {
	first := req("GET /a HTTP/1.1", "Host: h")
	second := chunkedReq("5\r\nhello\r\n")
	rest := []byte("6\r\n world\r\n0\r\n\r\n" + "GET /c HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n")

	bodies, err := readAll(t, oneWritePerRead(t, append(first, second...), rest), Config{BodyDelivery: BodyWhole, ReadTimeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("stream ended with %v", err)
	}
	got := make([]string, len(bodies))
	for i, b := range bodies {
		got[i] = string(b)
	}
	if want := []string{"", "hello world", ""}; strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("bodies = %q, want %q", got, want)
	}
}

// A Content-Length body fed one byte at a time used to re-parse the request
// line and every header on each read — up to MaxHeaderBytes per byte of
// body, free for the client that drips it. The headers are parsed once now:
// a call on a prefix still short of the body returns before touching them,
// which shows as an arena left as it was given. Expect: 100-continue, whose
// incomplete calls return errExpectContinue instead, must not reopen the
// path.
func TestBodyDripDoesNotReparseHeaders(t *testing.T) {
	cfg := Config{BodyDelivery: BodyWhole}.mustResolve()
	for _, expect := range []bool{false, true} {
		var head strings.Builder
		head.WriteString("POST /up HTTP/1.1\r\nHost: h\r\nContent-Length: 64\r\n")
		if expect {
			head.WriteString("Expect: 100-continue\r\n")
		}
		for i := 0; head.Len() < cfg.MaxHeaderBytes/2; i++ {
			fmt.Fprintf(&head, "X-Pad-%d: %s\r\n", i, strings.Repeat("v", cfg.MaxHeaderBytes/80))
		}
		head.WriteString("\r\n")
		body := strings.Repeat("b", 64)
		raw := []byte(head.String() + body)
		want := errIncomplete
		if expect {
			want = errExpectContinue
		}

		var progress chunkProgress
		parsed := 0
		for n := head.Len(); n < len(raw); n++ {
			_, _, arena, err := parseRequest(raw[:n], cfg, nil, &progress)
			if !errors.Is(err, want) {
				t.Fatalf("expect=%v: a %d-byte prefix returned %v, want %v", expect, n, err, want)
			}
			if len(arena) > 0 {
				parsed++
			}
		}
		if parsed > 1 {
			t.Errorf("expect=%v: the headers were parsed on %d of %d reads of the body, want once", expect, parsed, len(body))
		}
		r, used, _, err := parseRequest(raw, cfg, nil, &progress)
		if err != nil || used != len(raw) || string(r.Body) != body {
			t.Fatalf("expect=%v: completed parse: %d body bytes, used %d of %d, err %v", expect, len(r.Body), used, len(raw), err)
		}
		if progress != (chunkProgress{}) {
			t.Errorf("expect=%v: a completed request left its progress behind: %+v", expect, progress)
		}
	}
}
