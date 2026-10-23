package httpstream

import (
	"bufio"
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
)

// BenchmarkServeH1RoundTrip is one HTTP/1.1 request answered per round trip
// on a loopback connection: a batch of one, the case where whatever the
// server does per batch weighs most. "watched" has the handler ask its
// context for Done, which is what a handler handing it to a query does, and
// which arms the watch on the client (see [Handler]).
//
// "serial" is one connection, one request at a time, on an otherwise idle
// machine: every wake-up of an idle thread is paid in full. "parallel" is
// GOMAXPROCS times four connections at once, the shape of a server under
// load, where the threads are already awake.
func BenchmarkServeH1RoundTrip(b *testing.B) {
	for _, mode := range []string{"serial", "parallel"} {
		for _, watched := range []bool{false, true} {
			name := mode + "/plain"
			if watched {
				name = mode + "/watched"
			}
			b.Run(name, func(b *testing.B) {
				addr := serveRoundTrips(b, watched)
				b.ReportAllocs()
				if mode == "serial" {
					rt := dialRoundTrip(b, addr)
					b.ResetTimer()
					for b.Loop() {
						rt.do(b)
					}
					return
				}
				b.SetParallelism(4)
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					rt := dialRoundTrip(b, addr)
					for pb.Next() {
						rt.do(b)
					}
				})
			})
		}
	}
}

// serveRoundTrips serves a fixed small answer until the benchmark ends.
func serveRoundTrips(b *testing.B, watched bool) string {
	b.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	body := []byte("ok")
	go func() {
		defer close(done)
		_ = Serve(ctx, ln, Config{ShutdownGrace: time.Second, MaxRequestsPerConn: 1 << 30}, func(ctx context.Context, in sluice.Batch[Request]) sluice.Batch[Response] {
			if watched {
				select {
				case <-ctx.Done():
				default:
				}
			}
			out := make([]Response, len(in.Items))
			for i := range out {
				out[i] = Response{Status: 200, Body: body}
			}
			return sluice.Batch[Response]{Items: out}
		})
	}()
	b.Cleanup(func() { cancel(); <-done })
	return ln.Addr().String()
}

// roundTrip is one client connection that knows the length of the answer.
type roundTrip struct {
	c   net.Conn
	r   *bufio.Reader
	buf []byte
}

var roundTripRequest = []byte("GET / HTTP/1.1\r\nHost: h\r\n\r\n")

// dialRoundTrip connects and reads one answer, whose length is fixed.
func dialRoundTrip(b *testing.B, addr string) *roundTrip {
	b.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = c.Close() })
	r := bufio.NewReader(c)
	if _, err := c.Write(roundTripRequest); err != nil {
		b.Fatal(err)
	}
	n := 0
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			b.Fatal(err)
		}
		n += len(line)
		if line == "\r\n" {
			break
		}
	}
	if _, err := r.Discard(len("ok")); err != nil {
		b.Fatal(err)
	}
	return &roundTrip{c: c, r: r, buf: make([]byte, n+len("ok"))}
}

func (rt *roundTrip) do(b *testing.B) {
	b.Helper()
	if _, err := rt.c.Write(roundTripRequest); err != nil {
		b.Error(err)
		return
	}
	if _, err := io.ReadFull(rt.r, rt.buf); err != nil {
		b.Error(err)
	}
}
