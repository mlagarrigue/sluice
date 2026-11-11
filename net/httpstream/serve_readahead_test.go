package httpstream

import (
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/mlagarrigue/sluice"
)

// What a connection holds read from its socket and not yet parsed stays
// within two request buffers however many batches it serves: the request
// reader's own buffer, and the read-ahead the peer watch puts back in front
// of it. Each watch used to read up to a whole buffer of its own and append
// it to what the previous ones had left, so a client pipelining without a
// pause made the connection hold one buffer more per batch served.
//
// Both ways a watch starts are covered — a streamed response
// (writeStreamedBatch) and a handler that asks for its context's Done
// (clientCtx.armLocked) — and a control with no watch at all, so the test
// cannot fail for a reason other than the watch. net.Pipe in a synctest
// bubble keeps the client's flood and the handler's pauses deterministic.
func TestReadAheadStaysWithinTheConnectionBound(t *testing.T) {
	for _, tc := range []struct {
		name  string
		watch string // "", "streamed" or "done"
	}{
		{"no-watch control", ""},
		{"streamed response", "streamed"},
		{"handler asks for Done", "done"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				held, bound := readAheadUnder(t, tc.watch)
				if held > 2*int64(bound) {
					t.Fatalf("%d bytes read and not parsed after %d batches, over two request buffers (2 x %d)", held, readAheadBatches, bound)
				}
			})
		})
	}
}

const readAheadBatches = 200

// readAheadUnder serves a client that pipelines GETs without a pause and
// returns the most the connection held read and unparsed, measured as each
// batch reached the handler.
func readAheadUnder(t *testing.T, watch string) (most int64, bound int) {
	t.Helper()
	cfg := Config{
		Logger:              discardLogger(),
		MaxRequestLineBytes: 1 << 10,
		MaxHeaderBytes:      4 << 10,
		MaxBodyBytes:        16 << 10,
		MaxRequestsPerConn:  1 << 20,
	}.mustResolve()
	bound = cfg.maxBuffer()

	server, client := net.Pipe()
	counted := &readCounter{Conn: server}
	const request = "GET / HTTP/1.1\r\nHost: a\r\n\r\n"
	var batches, parsed int64
	enough := make(chan struct{})
	pause := func() { time.Sleep(2 * time.Millisecond) }

	served := make(chan struct{})
	go func() {
		defer close(served)
		defer func() { _ = server.Close() }()
		serveConn(t.Context(), &prefixConn{Conn: counted}, nil, cfg, func(ctx context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
			parsed += int64(b.Len()) * int64(len(request))
			most = max(most, counted.read.Load()-parsed)
			batches++
			if batches == readAheadBatches {
				close(enough)
			}
			res := make([]Response, b.Len())
			for i := range res {
				res[i].Status = 200
			}
			switch watch {
			case "streamed":
				res[0].Stream = func(yield func(sluice.Batch[[]byte]) bool) {
					pause()
					yield(sluice.Batch[[]byte]{Items: [][]byte{[]byte("x")}})
				}
			case "done":
				_ = ctx.Done()
				pause()
			default:
				pause()
			}
			return sluice.Batch[Response]{Items: res}
		})
	}()

	go func() { _, _ = io.Copy(io.Discard, client) }()
	go func() {
		block := make([]byte, 0, 1000*len(request))
		for range 1000 {
			block = append(block, request...)
		}
		for {
			if _, err := client.Write(block); err != nil {
				return
			}
		}
	}()

	<-enough
	_ = client.Close()
	<-served
	return most, bound
}

// readCounter counts what is read off the socket, by the request reader and
// the peer watch alike.
type readCounter struct {
	net.Conn
	read atomic.Int64
}

func (c *readCounter) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.read.Add(int64(n))
	return n, err
}
