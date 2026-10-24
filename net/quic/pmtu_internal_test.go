package quic

import (
	"bytes"
	"context"
	"io"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/internal/varint"
)

// mtuPC is a socket on a path of a given IP MTU: a datagram whose UDP
// payload, with the 48 bytes of IPv6 and UDP headers, exceeds it is
// silently dropped, the way a router without ICMP drops it — reported
// sent, never delivered. The MTU can drop while the connection runs (a
// black hole, RFC 8899 §4.3). It also records the largest datagram that
// passed, which is what the tests read.
type mtuPC struct {
	net.PacketConn

	mu      sync.Mutex
	mtu     int
	largest int
	dropped int
}

func newMTUPC(t *testing.T, mtu int) *mtuPC {
	t.Helper()
	return &mtuPC{PacketConn: loopbackSocket(t), mtu: mtu}
}

func (p *mtuPC) WriteTo(b []byte, to net.Addr) (int, error) {
	p.mu.Lock()
	if len(b)+48 > p.mtu {
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

func (p *mtuPC) setMTU(mtu int) {
	p.mu.Lock()
	p.mtu = mtu
	p.mu.Unlock()
}

// pmtuPair dials a client at a server through two sockets bounded to mtu,
// both ends with the default parameters (max_udp_payload_size 1452), and
// returns both connections once the handshake is confirmed on each.
func pmtuPair(t *testing.T, mtu int) (client, server *Conn, clientPC *mtuPC) {
	t.Helper()
	return pmtuPairWith(t, mtu, DefaultParameters())
}

// pmtuPairWith is pmtuPair with both ends built from params.
func pmtuPairWith(t *testing.T, mtu int, params TransportParameters) (client, server *Conn, clientPC *mtuPC) {
	t.Helper()
	serverPC := newMTUPC(t, mtu)
	l, err := NewListener(serverPC, ServerTLSForTest(t), params, ListenerConfig{})
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
	clientPC = newMTUPC(t, mtu)
	client, err = Dial(context.Background(), clientPC, serverPC.LocalAddr(), ClientTLSForTest(), params)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
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
// both ends, each probing its own direction. 1452 is the default ceiling,
// so the search is complete after that one probe. A bulk write then fills
// datagrams past 1200.
func TestPMTUProbeLiftsDatagramsTo1452(t *testing.T) {
	client, server, clientPC := pmtuPair(t, 1500)
	waitFor(t, "the client's probe acknowledged", func() bool { return client.datagramSize() == 1452 })
	waitFor(t, "the server's probe acknowledged", func() bool { return server.datagramSize() == 1452 })
	if largest, _ := clientPC.stats(); largest != 1452 {
		t.Errorf("largest datagram sent during probing = %d, want the 1452-byte probe", largest)
	}
	if st := client.pmtuSnapshot(); !st.done || st.inFlight || st.probes != 1 {
		t.Errorf("client pmtu state after the proof = %+v, want done after one probe", st)
	}
	if got := client.Stats().DatagramSize; got != 1452 {
		t.Errorf("Stats().DatagramSize = %d, want 1452", got)
	}
	clientPC.resetLargest()
	transfer(t, client, server)
	if largest, _ := clientPC.stats(); largest < 1400 || largest > 1452 {
		t.Errorf("largest datagram of a 64 KiB write = %d, want a full 1452-byte datagram less the frame overhead", largest)
	}
}

// RFC 8899 §5.3.1-5.3.2: on a path narrower than 1452, the lost sizes
// narrow the search until the range is 20 bytes or less: a tunnel's 1420
// settles within 20 bytes of its 1372-byte payload, the IPv6 minimum of
// 1280 at 1212 or more instead of falling back to 1200 — no stall, no
// close, and no congestion event for the probes' losses (RFC 9000 §14.4).
func TestPMTUSearchSettlesWithin20Bytes(t *testing.T) {
	for _, mtu := range []int{1420, 1280} {
		t.Run(strconv.Itoa(mtu), func(t *testing.T) {
			client, server, clientPC := pmtuPair(t, mtu)
			transfer(t, client, server)
			waitFor(t, "the client's search complete", func() bool { return client.pmtuSnapshot().done })
			path := mtu - 48
			if got := client.datagramSize(); got > path || got < path-pmtuPrecision {
				t.Errorf("maxDatagram = %d on a path carrying %d, want within %d bytes below it", got, path, pmtuPrecision)
			}
			st := client.pmtuSnapshot()
			if st.inFlight || st.lost <= path || st.lost-client.datagramSize() > pmtuPrecision {
				t.Errorf("client pmtu state = %+v, want a range of at most %d bytes above the size, none in flight", st, pmtuPrecision)
			}
			if _, dropped := clientPC.stats(); dropped%pmtuMaxProbes != 0 || dropped > st.probes {
				t.Errorf("the path dropped %d datagrams of %d probes, want only probes, %d of each size lost", dropped, st.probes, pmtuMaxProbes)
			}
			client.mu.Lock()
			recovered, inFlight, cwnd := !client.reno().recoveryStart.IsZero(), client.reno().inFlight, client.reno().cwnd
			client.mu.Unlock()
			if recovered {
				t.Errorf("the probes' losses were a congestion event (cwnd %d); §14.4 says they are not", cwnd)
			}
			if inFlight < 0 {
				t.Errorf("bytes in flight = %d after the probes were released", inFlight)
			}
		})
	}
}

// RFC 8899 §5.3.1: MaxDatagramSize raises the ceiling, and while nothing is
// lost the probe after 1452 is the ceiling itself — a 9000-byte jumbo path
// both ends opened up reaches 8952 in two probes.
func TestPMTUSearchReachesAJumboCeiling(t *testing.T) {
	params := DefaultParameters()
	params.MaxDatagramSize = 8952
	params.MaxUDPPayloadSize = 8952
	client, server, clientPC := pmtuPairWith(t, 9000, params)
	waitFor(t, "the client at the ceiling", func() bool { return client.datagramSize() == 8952 })
	if st := client.pmtuSnapshot(); !st.done || st.probes != 2 {
		t.Errorf("client pmtu state = %+v, want done after two probes, 1452 then 8952", st)
	}
	clientPC.resetLargest()
	transfer(t, client, server)
	if largest, _ := clientPC.stats(); largest < 8900 || largest > 8952 {
		t.Errorf("largest datagram of a 64 KiB write = %d, want a full 8952-byte datagram less the frame overhead", largest)
	}
}

// The ceiling is bounded by what the peer reads: a client opened up to 8952
// facing a server that announces the default 1452 stops at 1452.
func TestPMTUCeilingBoundedByThePeer(t *testing.T) {
	params := DefaultParameters()
	params.MaxDatagramSize = 8952
	client, _, _ := pmtuPairWith(t, 9000, params)
	waitFor(t, "the client's search complete", func() bool { return client.pmtuSnapshot().done })
	if got := client.datagramSize(); got != 1452 {
		t.Errorf("maxDatagram = %d against a peer reading 1452, want 1452", got)
	}
}

// MaxDatagramSize below the 1200 bytes every path carries or above the
// largest UDP payload is refused before anything is sent; the bounds
// themselves and zero are accepted.
func TestMaxDatagramSizeOutOfRangeIsRefused(t *testing.T) {
	for _, n := range []int{1199, 65528, -1} {
		p := DefaultParameters()
		p.MaxDatagramSize = n
		if err := p.check(); err == nil {
			t.Errorf("MaxDatagramSize %d accepted", n)
		}
		if _, err := Dial(context.Background(), loopbackSocket(t), loopbackSocket(t).LocalAddr(), ClientTLSForTest(), p); err == nil {
			t.Errorf("Dial with MaxDatagramSize %d succeeded", n)
		}
	}
	for _, n := range []int{0, 1200, 65527} {
		p := DefaultParameters()
		p.MaxDatagramSize = n
		if err := p.check(); err != nil {
			t.Errorf("MaxDatagramSize %d refused: %v", n, err)
		}
	}
}

// RFC 8899 §4.3: a path that shrinks under a running connection — 1500 to
// 1280 with nothing said — swallows every full-size datagram. Three probe
// timeouts later the size falls back to 1200, what was lost is resent cut
// to it, the transfer completes, and the search runs again down to the
// new path.
func TestPMTUBlackHoleFallsBackAndSearchesAgain(t *testing.T) {
	client, server, clientPC := pmtuPair(t, 1500)
	waitFor(t, "the client's probe acknowledged", func() bool { return client.datagramSize() == 1452 })
	clientPC.setMTU(1280)
	transfer(t, client, server)
	if got := client.Stats().MTUBlackHoles; got == 0 {
		t.Error("Stats().MTUBlackHoles = 0 after the path shrank, want the fall back counted")
	}
	waitFor(t, "the search run again", func() bool { return client.pmtuSnapshot().done })
	if got := client.datagramSize(); got > 1232 || got < 1232-pmtuPrecision {
		t.Errorf("maxDatagram = %d after the path shrank to carry 1232, want within %d bytes below it", got, pmtuPrecision)
	}
	if err := client.Err(); err != nil {
		t.Errorf("the connection ended: %v", err)
	}
}

// RFC 8899 §4.3's third indicator without a probe timeout: the full-size
// packets are lost while smaller ones are acknowledged. Losses beside an
// acknowledged full-size packet are congestion, and do not count.
func TestPMTUBlackHoleCountsOnlyUnansweredFullSizeLosses(t *testing.T) {
	c := &Conn{maxDatagram: 1452, cc: newNewReno(1452)}
	c.watchBlackHoleLocked(&ackOutcome{largeLost: 5, largeAcked: true})
	c.watchBlackHoleLocked(&ackOutcome{largeLost: 2})
	if c.maxDatagram != 1452 {
		t.Fatalf("maxDatagram = %d after two unanswered losses, want 1452 kept", c.maxDatagram)
	}
	c.watchBlackHoleLocked(&ackOutcome{largeLost: 1})
	if c.maxDatagram != 1200 || c.pmtuBlackHoles != 1 || c.pmtu.done {
		t.Errorf("after a third: maxDatagram %d, black holes %d, done %v; want 1200, 1, a new search", c.maxDatagram, c.pmtuBlackHoles, c.pmtu.done)
	}
}

// A payload recorded under a larger datagram size is cut, on a frame
// boundary or inside a STREAM or CRYPTO frame, so that its first part fits
// the room: the same bytes at the same offsets, the FIN on the last part.
func TestFitFramesCutsInsideDataFrames(t *testing.T) {
	data := bytes.Repeat([]byte("0123456789"), 140)
	stream := varint.Append(nil, FrameStream|streamOFF|streamLEN|streamFIN)
	stream = varint.Append(stream, 4)
	stream = varint.Append(stream, 1<<20)
	stream = varint.Append(stream, uint64(len(data)))
	stream = append(stream, data...)
	crypto := varint.Append(nil, frameCrypto)
	crypto = varint.Append(crypto, 300)
	crypto = varint.Append(crypto, uint64(len(data)))
	crypto = append(crypto, data...)
	maxData := appendFrame(nil, frameMaxData, 1<<30)

	for _, tc := range []struct {
		name    string
		payload []byte
	}{
		{"stream", stream},
		{"control then stream", append(append([]byte(nil), maxData...), stream...)},
		{"crypto", crypto},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const room = 1150
			var got []byte
			var off uint64
			var fins int
			rest := tc.payload
			for parts := 0; rest != nil; parts++ {
				if parts > 3 {
					t.Fatal("more than three parts")
				}
				var now []byte
				now, rest = fitFrames(rest, room)
				if len(now) > room {
					t.Fatalf("part %d is %d bytes, over the room of %d", parts, len(now), room)
				}
				frames, err := parseFrames(nil, now)
				if err != nil {
					t.Fatalf("part %d does not parse: %v", parts, err)
				}
				for _, f := range frames {
					if !f.IsStream() && f.Type != frameCrypto {
						continue
					}
					if len(got) == 0 {
						off = f.offset
					} else if f.offset != off+uint64(len(got)) {
						t.Fatalf("part %d resumes at offset %d, want %d", parts, f.offset, off+uint64(len(got)))
					}
					got = append(got, f.Data...)
					if f.Fin {
						fins++
					}
				}
			}
			if !bytes.Equal(got, data) {
				t.Errorf("the parts carry %d bytes, not the %d sent", len(got), len(data))
			}
			if want := btoi(tc.name != "crypto"); fins != want {
				t.Errorf("%d FIN, want %d", fins, want)
			}
		})
	}
	if now, rest := fitFrames(maxData, 2); !bytes.Equal(now, maxData) || rest != nil {
		t.Error("a frame that cannot be cut was not left whole")
	}
}

func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}

// RFC 8899 §3: a probe leaves with Don't Fragment set, or it proves
// nothing — a router may carry it in fragments the full-size datagrams
// after it would then depend on. On a socket DF could not be set on
// (dfSocket false: a platform without the option), the search does not
// run and datagrams stay at the 1200 bytes every path carries (RFC 9000
// §14.2). The pair's sockets do take DF, so the state is installed by hand
// on a connection whose search had already completed: the restart that
// would probe again must not.
func TestPMTUSearchNeedsDontFragment(t *testing.T) {
	client, server, clientPC := pmtuPair(t, 1500)
	waitFor(t, "the client's probe acknowledged", func() bool { return client.datagramSize() == 1452 })
	client.mu.Lock()
	client.fragmentable = true
	client.resetPMTULocked()
	client.maybeProbePMTULocked()
	client.mu.Unlock()
	clientPC.resetLargest()
	transfer(t, client, server)
	if largest, _ := clientPC.stats(); largest > 1200 {
		t.Errorf("largest datagram on a fragmentable socket = %d, want 1200 at most", largest)
	}
	if st := client.pmtuSnapshot(); st.probes != 0 || st.inFlight {
		t.Errorf("pmtu state on a fragmentable socket = %+v, want no probe", st)
	}
}

// RFC 9000 §14.3: the size is per path. A rebind forgets the proof — the
// datagram size is 1200 again at once — and probes the new path once it is
// validated.
func TestPMTUProbeRestartsOnRebind(t *testing.T) {
	client, _, _ := pmtuPair(t, 1500)
	waitFor(t, "the client's probe acknowledged", func() bool { return client.datagramSize() == 1452 })
	fresh := newMTUPC(t, 1500)
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
