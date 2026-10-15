package httpstream

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/internal/dgram"
	"github.com/mlagarrigue/sluice/net/quic"
)

// countingPacketConn counts the datagrams written through it.
type countingPacketConn struct {
	net.PacketConn
	writes atomic.Int64
}

func (p *countingPacketConn) WriteTo(b []byte, to net.Addr) (int, error) {
	p.writes.Add(1)
	return p.PacketConn.WriteTo(b, to)
}

// A small buffered response leaves in one datagram: HEADERS, DATA and the
// FIN together, with the ACK of the request. Written then closed in two
// calls, the FIN went in a datagram of its own — one more per request.
func TestH3SmallResponseIsOneDatagram(t *testing.T) {
	rawServer, clientPC := dgram.Pair()
	serverPC := &countingPacketConn{PacketConn: rawServer}
	t.Cleanup(func() { _ = serverPC.Close(); _ = clientPC.Close() })
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	served := make(chan struct{})
	t.Cleanup(func() { <-served })
	go func() {
		defer close(served)
		buf := make([]byte, 2048)
		_ = serverPC.SetReadDeadline(time.Now().Add(10 * time.Second))
		n, peer, err := serverPC.ReadFrom(buf)
		if err != nil {
			t.Error(err)
			return
		}
		conn, err := quic.Accept(serverPC, peer, buf[:n], h3ServerTLS(t), quic.DefaultParameters())
		if err != nil {
			t.Error(err)
			return
		}
		_ = ServeH3(ctx, conn, Config{}, func(b sluice.Batch[Request]) sluice.Batch[Response] {
			res := make([]Response, b.Len())
			for i := range res {
				res[i] = Response{Status: 200, Body: []byte("small")}
			}
			return sluice.Batch[Response]{Items: res}
		})
	}()
	conn, err := quic.Dial(clientPC, serverPC.LocalAddr(), h3ClientTLS(), quic.DefaultParameters())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	// The handshake and the server's SETTINGS leave a tail: wait until the
	// server has been silent for a while before counting.
	const quiet = 100 * time.Millisecond
	deadline := time.Now().Add(5 * time.Second)
	for last := serverPC.writes.Load(); ; last = serverPC.writes.Load() {
		time.Sleep(quiet)
		if serverPC.writes.Load() == last {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the server never went quiet after the handshake")
		}
	}

	before := serverPC.writes.Load()
	s, err := conn.OpenStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.WriteClose(h3Request("GET", "/small")); err != nil {
		t.Fatal(err)
	}
	if body, _ := readH3Body(t, s); body != "small" {
		t.Fatalf("body %q, want %q", body, "small")
	}
	// Counted once the FIN is read, so every datagram of the response has
	// been written, and not later: the client acknowledges the response
	// max_ack_delay (25 ms) after it, the server's PTO fires barely past
	// that, and a client timer a millisecond late — common under -race —
	// draws two probes (RFC 9002 §6.2.4) that are not this test's subject.
	// The request's ACK is still counted: it either rode the response or
	// went before it.
	if n := serverPC.writes.Load() - before; n != 1 {
		t.Errorf("the server wrote %d datagrams for one small response, want 1", n)
	}
}
