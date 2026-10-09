package httpstream

import (
	"bytes"
	"errors"
	"fmt"
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
	cfg := Config{}.withDefaults()
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
		for _, r := range b.Items {
			bodies = append(bodies, append([]byte(nil), r.Body...))
		}
		return true
	})
	return bodies, src.Err()
}

// 40 000 one-byte chunks, each in its own read, used to cost twelve seconds
// of CPU: every read re-walked the body from its first chunk. Resumed from
// the saved walk it is linear, and the bound here is loose enough for -race
// while being an order of magnitude under what the quadratic walk took.
func TestChunkedBodyArrivingOneChunkPerReadIsLinear(t *testing.T) {
	const chunks = 40_000
	writes := make([][]byte, 0, chunks+2)
	writes = append(writes, req("POST /x HTTP/1.1", "Host: h", "Transfer-Encoding: chunked", "Connection: close"))
	var want bytes.Buffer
	for i := range chunks {
		c := byte('a' + i%26)
		want.WriteByte(c)
		writes = append(writes, fmt.Appendf(nil, "1\r\n%c\r\n", c))
	}
	writes = append(writes, []byte("0\r\n\r\n"))

	start := time.Now()
	bodies, err := readAll(t, oneWritePerRead(t, writes...), Config{ReadTimeout: 30 * time.Second})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("stream ended with %v", err)
	}
	if len(bodies) != 1 || !bytes.Equal(bodies[0], want.Bytes()) {
		t.Fatalf("got %d bodies, first %d bytes long; want one of %d bytes", len(bodies), len(bodies[0]), want.Len())
	}
	if elapsed > 2*time.Second {
		t.Fatalf("decoding %d chunks took %v; the walk is being restarted on every read", chunks, elapsed)
	}
}

// The saved walk indexes the read buffer, which shifts when a batch is
// consumed: a chunked request pipelined behind a complete one starts its
// walk at one offset and resumes at another.
func TestChunkedWalkSurvivesBufferCompaction(t *testing.T) {
	first := req("GET /a HTTP/1.1", "Host: h")
	second := chunkedReq("5\r\nhello\r\n")
	rest := []byte("6\r\n world\r\n0\r\n\r\n" + "GET /c HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n")

	bodies, err := readAll(t, oneWritePerRead(t, append(first, second...), rest), Config{ReadTimeout: 5 * time.Second})
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
