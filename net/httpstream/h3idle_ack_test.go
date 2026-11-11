package httpstream

import (
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/mlagarrigue/sluice/internal/dgram"
	"github.com/mlagarrigue/sluice/net/quic"
)

// deafConn is a client socket that stops hearing: once deaf is set, every
// datagram the server sends is read and dropped, so the client acknowledges
// nothing more while what it sends still leaves. Seen from the server, that
// is a peer which keeps sending packets and never acknowledges one.
type deafConn struct {
	net.PacketConn
	deaf atomic.Bool
}

func (c *deafConn) ReadFrom(p []byte) (int, net.Addr, error) {
	for {
		n, addr, err := c.PacketConn.ReadFrom(p)
		if err != nil || !c.deaf.Load() {
			return n, addr, err
		}
	}
}

// The idle close writes its GOAWAY and waits for it to be acknowledged; that
// wait is a write waiting on the peer, bounded by WriteTimeout like every
// write on the control stream. It was bounded by the forced-stop context
// alone, which only a cancelled server ends: a peer that kept pinging and
// acknowledged nothing restarted QUIC's idle timer with each packet and held
// the connection — and its place under MaxConns — for as long as the server
// ran. A server stop that comes during the wait ends the connection as an
// idle close, with nil. QUIC's own idle timeout is set far past the
// observation, so that only the HTTP/3 clock can end the connection.
func TestH3AnIdleCloseWaitsForItsAcknowledgementWithinWriteTimeout(t *testing.T) {
	const idle = 300 * time.Millisecond
	for _, tc := range []struct {
		name  string
		deaf  bool
		write time.Duration // WriteTimeout
		stop  bool          // the server is stopped during the wait
	}{
		{"a peer that acknowledges (control)", false, idle, false},
		{"a peer that pings and acknowledges nothing", true, idle, false},
		{"a server stop during the wait", true, 20 * idle, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				serverPC, inner := dgram.Pair()
				clientPC := &deafConn{PacketConn: inner}
				defer func() { _ = serverPC.Close() }()
				defer func() { _ = clientPC.Close() }()
				// As in h3idle_conformance_test.go: the closing period runs
				// out before the bubble's clock stops.
				defer time.Sleep(10 * time.Second)
				params := quic.DefaultConfig()
				params.MaxIdleTimeout = time.Hour
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				served := make(chan error, 1)
				go func() {
					conn, err := acceptOne(t, serverPC, h3ServerTLS(t), params)
					if err != nil {
						served <- err
						return
					}
					served <- ServeH3Conn(ctx, conn, Config{
						IdleTimeout: idle, WriteTimeout: tc.write, ReadTimeout: idle,
					}, echoTarget)
				}()
				conn, err := quic.Dial(t.Context(), clientPC, serverPC.LocalAddr(), h3ClientTLS(), params)
				if err != nil {
					t.Fatalf("Dial: %v", err)
				}
				defer func() { _ = conn.Close() }()
				// One request, answered in full and acknowledged: the
				// connection owes nothing after it.
				s, err := conn.OpenStream(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				if _, err := s.WriteFinal(h3Request("GET", "/once")); err != nil {
					t.Fatal(err)
				}
				if err := s.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
					t.Fatal(err)
				}
				if _, err := io.ReadAll(s); err != nil {
					t.Fatalf("reading the answer: %v", err)
				}
				time.Sleep(idle / 10) // the answer's acknowledgement leaves
				clientPC.deaf.Store(tc.deaf)
				// Then a PING every third of an IdleTimeout, whether or not it
				// is acknowledged; the stop, if any, once the idle close has
				// begun (one to two IdleTimeouts) and well inside its wait.
				const watch = 40 * idle
				start := time.Now()
				var returned bool
				var result error
			pings:
				for time.Since(start) < watch {
					if tc.stop && ctx.Err() == nil && time.Since(start) > 4*idle {
						cancel()
					}
					pctx, stop := context.WithTimeout(t.Context(), idle/3)
					_ = conn.Ping(pctx)
					<-pctx.Done()
					stop()
					select {
					case result = <-served:
						returned = true
						break pings
					default:
					}
				}
				if !returned {
					t.Fatalf("ServeH3Conn still holds an idle connection %v after its last request, with IdleTimeout %v and WriteTimeout %v",
						time.Since(start), idle, tc.write)
				}
				if result != nil {
					t.Errorf("ServeH3Conn returned %v for an idle close", result)
				}
			})
		})
	}
}
