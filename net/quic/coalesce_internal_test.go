package quic

import (
	"context"
	"crypto/tls"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/internal/dgram"
)

// tapPC records every datagram written through a packet connection, with
// the moment it left, and passes it on untouched.
type tapPC struct {
	net.PacketConn
	mu     sync.Mutex
	writes []tapWrite
	start  time.Time
}

type tapWrite struct {
	data []byte
	at   time.Duration
}

func newTap(pc net.PacketConn) *tapPC { return &tapPC{PacketConn: pc, start: time.Now()} }

func (t *tapPC) WriteTo(p []byte, addr net.Addr) (int, error) {
	t.mu.Lock()
	t.writes = append(t.writes, tapWrite{data: append([]byte(nil), p...), at: time.Since(t.start)})
	t.mu.Unlock()
	return t.PacketConn.WriteTo(p, addr)
}

func (t *tapPC) recorded() []tapWrite {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]tapWrite(nil), t.writes...)
}

// packetTypes lists the long-header packet types a datagram carries, in
// order; a short header ends the list with 0xff.
func packetTypes(t *testing.T, datagram []byte) []byte {
	t.Helper()
	var types []byte
	for len(datagram) > 0 {
		if datagram[0]&0x80 == 0 {
			return append(types, 0xff)
		}
		h, rest, err := parseLongHeader(datagram)
		if err != nil {
			t.Fatalf("a datagram this end wrote does not parse: %v", err)
		}
		types = append(types, h.Type)
		datagram = rest
	}
	return types
}

func hasTypes(types []byte, want ...byte) bool {
	seen := map[byte]bool{}
	for _, ty := range types {
		seen[ty] = true
	}
	for _, w := range want {
		if !seen[w] {
			return false
		}
	}
	return true
}

// RFC 9000 §12.2 / RFC 9001 §4: a handshake's flights coalesce. The server's
// first datagram carries its Initial (ServerHello) and the start of its
// Handshake flight together, in one 1200-byte-bounded datagram, instead of
// a 1200-byte Initial that is mostly padding followed by the Handshake
// packets on their own. The client's second datagram likewise carries its
// Initial (the ACK) and its Handshake (Finished) in one, padded to 1200 as
// §14.1 asks of every client datagram carrying an Initial — with the padding
// in the packet that closes the datagram, not in the Initial.
func TestHandshakeFlightsCoalesce(t *testing.T) {
	serverPC, clientPC := dgram.Pair()
	defer serverPC.Close()
	defer clientPC.Close()
	serverTap, clientTap := newTap(serverPC), newTap(clientPC)

	done := make(chan error, 1)
	go func() {
		buf := make([]byte, 2048)
		_ = serverPC.SetReadDeadline(time.Now().Add(10 * time.Second))
		n, peer, err := serverPC.ReadFrom(buf)
		if err != nil {
			done <- err
			return
		}
		conn, err := Accept(serverTap, datagram{Peer: peer, Payload: buf[:n]}, ServerTLSForTest(t), DefaultParameters())
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		done <- nil
	}()
	client, err := Dial(context.Background(), clientTap, serverPC.LocalAddr(), ClientTLSForTest(), DefaultParameters())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the client to confirm", func() bool {
		client.mu.Lock()
		defer client.mu.Unlock()
		return client.handshakeConfirmed
	})

	server := serverTap.recorded()
	for i, w := range server {
		t.Logf("server datagram %d: %4d bytes at %v, packets %x", i, len(w.data), w.at, packetTypes(t, w.data))
	}
	cl := clientTap.recorded()
	for i, w := range cl {
		t.Logf("client datagram %d: %4d bytes at %v, packets %x", i, len(w.data), w.at, packetTypes(t, w.data))
	}
	// The Hellos are each about 1.2 KB with the hybrid ML-KEM key share, so
	// each spans two Initial packets and the first of them fills a datagram
	// alone; coalescing shows on the *tail* of each Hello: the server's
	// second Initial shares its datagram with the start of the Handshake
	// flight, and the client's Initial ACK of it shares one with Finished.
	coalesced := func(ws []tapWrite) (int, bool) {
		for i, w := range ws {
			if hasTypes(packetTypes(t, w.data), packetInitial, packetHandshake) {
				return i, true
			}
		}
		return 0, false
	}
	if _, ok := coalesced(server); !ok {
		t.Error("no server datagram carries Initial and Handshake together")
	}
	// The bound is on the handshake's datagrams: once confirmed, each end
	// probes the path with a 1452-byte 1-RTT datagram (pmtu.go), which a
	// loopback carries.
	for i, w := range server {
		if len(w.data) > 1200 && carriesLongHeader(packetTypes(t, w.data)) {
			t.Errorf("server datagram %d is %d bytes, over the 1200 this end allows itself", i, len(w.data))
		}
	}
	if _, ok := coalesced(cl); !ok {
		t.Error("no client datagram carries Initial and Handshake together")
	}
	for i, w := range cl {
		types := packetTypes(t, w.data)
		if hasTypes(types, packetInitial) && len(w.data) != 1200 {
			t.Errorf("client datagram %d carries an Initial in %d bytes, want exactly 1200 (§14.1, padded in the closing packet)", i, len(w.data))
		}
		if len(w.data) > 1200 && carriesLongHeader(types) {
			t.Errorf("client datagram %d is %d bytes, over 1200", i, len(w.data))
		}
	}
	t.Logf("handshake datagrams: %d from the server, %d from the client", len(server), len(cl))
}

// carriesLongHeader says whether any packet in the datagram is Initial or
// Handshake — what packetTypes reports as anything but 0xff.
func carriesLongHeader(types []byte) bool {
	for _, typ := range types {
		if typ != 0xff {
			return true
		}
	}
	return false
}

// RFC 9000 §12.2: "Receivers SHOULD ignore any subsequent packets with a
// different Destination Connection ID than the first packet in the
// datagram." A sender never mixes identifiers in one datagram (a MUST NOT
// on its side), so a packet that does is not the peer's — Initial keys are
// public, and anyone on the path can coalesce one behind a genuine packet.
// Here the second Initial names another identifier this end issued, one
// every other check accepts; the datagram's first packet is the rule that
// refuses it, and refuses it before any key is tried. The first packet is
// processed as before.
func TestCoalescedPacketUnderAnotherDCIDIsIgnored(t *testing.T) {
	dcid, scid := []byte("dcid0000"), []byte("scid0000")
	c := newConn(&fuzzSink{addr: "coal:self"}, fakeFuzzAddr("coal:peer"), dcid, scid, true, DefaultParameters())
	defer c.Close()
	if err := c.installInitial(dcid); err != nil {
		t.Fatal(err)
	}
	other := []byte("scid0001")
	c.mu.Lock()
	c.localCIDs = append(c.localCIDs, localCID{seq: 1, cid: other})
	c.mu.Unlock()
	if !c.expectedDCID(other, spaceInitial) {
		t.Fatal("the second identifier is not accepted on its own; the test is checking the wrong thing")
	}

	server := []byte("srv-cid0")
	datagram := sealedInitialFrom(t, true, dcid, scid, server, 0, false)
	datagram = append(datagram, sealedInitialFrom(t, true, dcid, other, server, 5, false)...)
	if err := c.receive(datagram, c.peer); err != nil {
		t.Fatal(err)
	}
	if n := initialRanges(c); n != 1 {
		t.Errorf("Initial ranges = %d, want 1: the first packet processed, the second — under another DCID — ignored", n)
	}
	if n := discardedCount(c); n != 1 {
		t.Errorf("discarded = %d, want 1: the second packet counted as a rejection", n)
	}
}

// A burst of 0-RTT writes coalesces while another writer is counted in
// (writersIn above one): the writes gather their packets into full
// datagrams, and the writer that brings the count back to zero sends what
// gathered. The overlap is set by hand — a writer counted in, as
// Stream.write counts itself before it takes mu — so the property holds
// whatever the scheduler does; the interop test that measures the burst on
// a simulated path depends on goroutines overlapping and cannot.
func TestZeroRTTWritesCoalesceWhileAnotherWriterIsCounted(t *testing.T) {
	tap := newTap(&fuzzSink{addr: "zrtt:self"})
	dcid, scid := []byte{1, 2, 3, 4}, []byte{5, 6, 7, 8}
	c := assembleConn(tap, fakeFuzzAddr("zrtt:peer"), dcid, scid, true, DefaultParameters())
	t.Cleanup(func() { _ = c.Close() })
	c.initialDCID = dcid
	c.remembered = TransportParameters{
		InitialMaxData:          1 << 20,
		InitialMaxStreamDataUni: 1 << 16,
		InitialMaxStreamsUni:    64,
		ActiveConnIDLimit:       4,
	}
	if err := c.installEarlySealer(tls.TLS_AES_128_GCM_SHA256, make([]byte, 32)); err != nil {
		t.Fatalf("early sealer: %v", err)
	}
	srv := DefaultParameters()
	srv.initialSourceCID = dcid
	srv.originalDCID = dcid
	if err := c.handlePeerParameters(appendParameters(nil, srv)); err != nil {
		t.Fatalf("server parameters: %v", err)
	}

	// The interop runner's shape, smaller: a request of 250 bytes per
	// stream and the end of the stream in a second call, as an HTTP/3
	// client writes them.
	const streams, request = 10, 250
	c.writersIn.Add(1)
	for i := range streams {
		s, err := c.OpenUniStream(context.Background())
		if err != nil {
			t.Fatalf("OpenUniStream: %v", err)
		}
		if _, err := s.Write(make([]byte, request)); err != nil {
			t.Fatalf("writing in 0-RTT: %v", err)
		}
		if i == streams-1 {
			// The other writer leaves: this last call is the one that
			// brings the count to zero.
			c.writersIn.Add(-1)
		}
		if _, err := s.WriteFinal(nil); err != nil {
			t.Fatalf("closing in 0-RTT: %v", err)
		}
	}

	sent := tap.recorded()
	total := 0
	for _, w := range sent {
		total += len(w.data)
		for _, ty := range packetTypes(t, w.data) {
			if ty != packetZeroRTT {
				t.Fatalf("a datagram carried packet type %#x, want 0-RTT only", ty)
			}
		}
	}
	if total < streams*request {
		t.Fatalf("%d bytes left in %d datagrams, under the %d-byte request: the gathered datagram was not sent", total, len(sent), streams*request)
	}
	// Twenty writes, coalesced: every datagram is filled to within one
	// packet of the datagram size, except the last two — the one the last
	// counted writer's packets gathered, which the final call flushes, and
	// that call's own packet, sent alone because no other writer is
	// counted any more. One datagram per write is twenty.
	c.mu.Lock()
	room := c.maxDatagram - (request + 64)
	c.mu.Unlock()
	for i, w := range sent[:max(len(sent)-2, 0)] {
		if len(w.data) < room {
			t.Errorf("datagram %d of %d carries %d bytes, under the %d a coalesced one reaches: 0-RTT packets no longer coalesce while another writer is counted",
				i+1, len(sent), len(w.data), room)
		}
	}
	if len(sent) >= 2*streams {
		t.Errorf("%d writes left in %d datagrams: one each, nothing coalesced", 2*streams, len(sent))
	}
}
