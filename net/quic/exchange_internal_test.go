package quic

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// countingPC counts the datagrams written through it.
type countingPC struct {
	net.PacketConn
	writes atomic.Int64
}

func (p *countingPC) WriteTo(b []byte, to net.Addr) (int, error) {
	p.writes.Add(1)
	return p.PacketConn.WriteTo(b, to)
}

// countedPair is a client and a server over loopback, handshake done and
// the path settled, with every datagram the server writes counted.
func countedPair(t *testing.T) (client, server *Conn, sent *atomic.Int64) {
	t.Helper()
	serverPC := &countingPC{PacketConn: loopbackSocket(t)}
	l, err := NewListener(serverPC, ServerTLSForTest(t), DefaultParameters(), ListenerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	accepted := make(chan *Conn, 1)
	go func() {
		c, err := l.Accept()
		if err != nil {
			t.Error(err)
		}
		accepted <- c
	}()
	client = dialInternal(t, loopbackSocket(t), serverPC.LocalAddr())
	server = <-accepted
	if server == nil {
		t.Fatal("no server connection")
	}
	t.Cleanup(func() { _ = client.Close(); _ = server.Close() })

	// The handshake leaves a tail behind it — HANDSHAKE_DONE, connection IDs,
	// the path MTU probe and their acknowledgements. Wait until the server
	// has been silent for several delayed-ACK periods.
	deadline := time.Now().Add(5 * time.Second)
	for last := serverPC.writes.Load(); ; {
		time.Sleep(4 * localMaxAckDelay)
		now := serverPC.writes.Load()
		if now == last {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the server never went quiet after the handshake")
		}
		last = now
	}
	return client, server, &serverPC.writes
}

// A small request answered with a small response costs the server one
// datagram: the response, its FIN, the ACK of the request and nothing else.
// It cost three — the delayed ACK of the request left alone 25 ms after the
// response had gone without it (RFC 9000 §13.2.1 says an ACK SHOULD ride
// other frames), and the stream's slot came back as a MAX_STREAMS of its
// own the moment the stream retired.
func TestASmallExchangeCostsTheServerOneDatagram(t *testing.T) {
	client, server, sent := countedPair(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	before := sent.Load()
	served := make(chan error, 1)
	go func() {
		s, err := server.AcceptStream(ctx)
		if err != nil {
			served <- err
			return
		}
		if _, err := io.ReadAll(s); err != nil {
			served <- err
			return
		}
		_, err = s.WriteClose([]byte("response"))
		served <- err
	}()
	s, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.WriteClose([]byte("request")); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(s)
	if err != nil || !bytes.Equal(got, []byte("response")) {
		t.Fatalf("read %q, %v; want %q", got, err, "response")
	}
	if err := <-served; err != nil {
		t.Fatal(err)
	}
	time.Sleep(4 * localMaxAckDelay) // whatever was delayed has left by now
	if n := sent.Load() - before; n != 1 {
		t.Errorf("the server wrote %d datagrams for one small exchange, want 1", n)
	}
}

// A retired peer stream's slot comes back an eighth of the initial limit at
// a time, never later: the peer is not left waiting on STREAMS_BLOCKED
// (RFC 9000 §4.6), and a limit under eight returns every slot at once.
func TestStreamSlotsComeBackAnEighthAtATime(t *testing.T) {
	for _, tc := range []struct {
		name          string
		initial, done uint64
		wantLimit     uint64
		wantGrants    int
	}{
		{"default limit", 100, 25, 124, 2},
		{"limit one", 1, 3, 4, 3},
		{"limit seven", 7, 3, 10, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &Conn{}
			c.params.InitialMaxStreamsBidi = tc.initial
			c.localMaxStreamsBidi = tc.initial
			grants := 0
			for range tc.done {
				before := len(c.grants)
				c.returnStreamSlotLocked(false)
				if len(c.grants) > before {
					grants++
				}
			}
			if c.localMaxStreamsBidi != tc.wantLimit || grants != tc.wantGrants {
				t.Errorf("after %d retirements: limit %d in %d MAX_STREAMS, want %d in %d",
					tc.done, c.localMaxStreamsBidi, grants, tc.wantLimit, tc.wantGrants)
			}
		})
	}
}
