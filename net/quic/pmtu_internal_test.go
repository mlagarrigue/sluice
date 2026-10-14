package quic

import (
	"bytes"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// mtuPC is a socket on a path of a given MTU: a datagram larger than it is
// silently dropped, the way a router without ICMP drops it — reported sent,
// never delivered. It also records the largest datagram that passed, which
// is what the tests read.
type mtuPC struct {
	net.PacketConn
	mtu int

	mu      sync.Mutex
	largest int
	dropped int
}

func (p *mtuPC) WriteTo(b []byte, to net.Addr) (int, error) {
	p.mu.Lock()
	if len(b) > p.mtu {
		p.dropped++
		p.mu.Unlock()
		return len(b), nil
	}
	p.largest = max(p.largest, len(b))
	p.mu.Unlock()
	return p.PacketConn.WriteTo(b, to)
}

func (p *mtuPC) stats() (largest, dropped int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.largest, p.dropped
}

func (p *mtuPC) resetLargest() {
	p.mu.Lock()
	p.largest = 0
	p.mu.Unlock()
}

// pmtuPair dials a client at a server through two sockets bounded to mtu,
// both ends with the default parameters (max_udp_payload_size 1452), and
// returns both connections once the handshake is confirmed on each.
func pmtuPair(t *testing.T, mtu int) (client, server *Conn, clientPC *mtuPC) {
	t.Helper()
	serverPC := &mtuPC{PacketConn: loopbackSocket(t), mtu: mtu}
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
			close(accepted)
			return
		}
		accepted <- c
	}()
	clientPC = &mtuPC{PacketConn: loopbackSocket(t), mtu: mtu}
	client = dialInternal(t, clientPC, serverPC.LocalAddr())
	server = <-accepted
	if server == nil {
		t.Fatal("no server connection")
	}
	t.Cleanup(func() { _ = server.Close() })
	waitFor(t, "both handshakes confirmed", func() bool {
		return client.datagramSize() > 0 && server.datagramSize() > 0 && confirmed(client) && confirmed(server)
	})
	return client, server, clientPC
}

func confirmed(c *Conn) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.handshakeConfirmed
}

func (c *Conn) datagramSize() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.maxDatagram
}

func (c *Conn) pmtuSnapshot() pmtuState {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pmtu
}

// transfer moves 64 KiB client to server on one stream and returns once the
// server read it all.
func transfer(t *testing.T, client, server *Conn) {
	t.Helper()
	const size = 64 << 10
	done := make(chan error, 1)
	go func() {
		s, err := server.AcceptStream(t.Context())
		if err != nil {
			done <- err
			return
		}
		got, err := io.ReadAll(s)
		if err == nil && (len(got) != size || !bytes.Equal(got, bytes.Repeat([]byte{0x5a}, size))) {
			err = io.ErrUnexpectedEOF
		}
		done <- err
	}()
	s, err := client.OpenStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(bytes.Repeat([]byte{0x5a}, size)); err != nil {
		t.Fatal(err)
	}
	if err := s.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("the server did not read the transfer: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the transfer stalled")
	}
}

// RFC 9000 §14.3, RFC 8899 §4.1-4.2: on a path that carries 1500-byte
// frames, a PING+PADDING probe of 1452 bytes leaves once the handshake is
// confirmed, and its acknowledgement lifts the datagram size to 1452 — on
// both ends, each probing its own direction. A bulk write then fills
// datagrams past 1200.
func TestPMTUProbeLiftsDatagramsTo1452(t *testing.T) {
	client, server, clientPC := pmtuPair(t, 1500)
	waitFor(t, "the client's probe acknowledged", func() bool { return client.datagramSize() == 1452 })
	waitFor(t, "the server's probe acknowledged", func() bool { return server.datagramSize() == 1452 })
	if largest, _ := clientPC.stats(); largest != 1452 {
		t.Errorf("largest datagram sent during probing = %d, want the 1452-byte probe", largest)
	}
	if st := client.pmtuSnapshot(); !st.done || st.inFlight || st.tries != 1 {
		t.Errorf("client pmtu state after the proof = %+v, want done after one try", st)
	}
	clientPC.resetLargest()
	transfer(t, client, server)
	if largest, _ := clientPC.stats(); largest < 1400 || largest > 1452 {
		t.Errorf("largest datagram of a 64 KiB write = %d, want a full 1452-byte datagram less the frame overhead", largest)
	}
}

// RFC 8899 §5.1.2-5.3.1: on a path that carries only 1280 bytes (the IPv6
// minimum), three probes are lost and the search completes at 1200 — no
// stall, no close, and no congestion event for the probes' losses (RFC 9000
// §14.4).
func TestPMTUProbeFallsBackTo1200AfterThreeLosses(t *testing.T) {
	client, server, clientPC := pmtuPair(t, 1280)
	transfer(t, client, server)
	waitFor(t, "the client's search complete", func() bool { return client.pmtuSnapshot().done })
	if got := client.datagramSize(); got != 1200 {
		t.Errorf("maxDatagram = %d on a 1280-byte path, want 1200", got)
	}
	if st := client.pmtuSnapshot(); st.tries != pmtuMaxProbes || st.inFlight {
		t.Errorf("client pmtu state = %+v, want %d tries, none in flight", st, pmtuMaxProbes)
	}
	if _, dropped := clientPC.stats(); dropped != pmtuMaxProbes {
		t.Errorf("the path dropped %d datagrams, want exactly the %d probes", dropped, pmtuMaxProbes)
	}
	client.mu.Lock()
	recovered, inFlight, cwnd := !client.cc.recoveryStart.IsZero(), client.cc.inFlight, client.cc.cwnd
	client.mu.Unlock()
	if recovered {
		t.Errorf("the probes' losses were a congestion event (cwnd %d); §14.4 says they are not", cwnd)
	}
	if inFlight < 0 {
		t.Errorf("bytes in flight = %d after the probes were released", inFlight)
	}
}

// RFC 9000 §14.3: the size is per path. A rebind forgets the proof — the
// datagram size is 1200 again at once — and probes the new path once it is
// validated.
func TestPMTUProbeRestartsOnRebind(t *testing.T) {
	client, _, _ := pmtuPair(t, 1500)
	waitFor(t, "the client's probe acknowledged", func() bool { return client.datagramSize() == 1452 })
	fresh := &mtuPC{PacketConn: loopbackSocket(t), mtu: 1500}
	if err := client.Rebind(fresh); err != nil {
		t.Fatal(err)
	}
	if got := client.datagramSize(); got != 1200 {
		t.Errorf("maxDatagram = %d right after Rebind, want 1200: the new path is unproven", got)
	}
	waitFor(t, "the new path probed", func() bool { return client.datagramSize() == 1452 })
	if largest, _ := fresh.stats(); largest != 1452 {
		t.Errorf("largest datagram on the new socket = %d, want the 1452-byte probe", largest)
	}
}
