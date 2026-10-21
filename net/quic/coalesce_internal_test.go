package quic

import (
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
		conn, err := Accept(serverTap, Datagram{Peer: peer, Payload: buf[:n]}, ServerTLSForTest(t), DefaultParameters())
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		done <- nil
	}()
	client, err := Dial(clientTap, serverPC.LocalAddr(), ClientTLSForTest(), DefaultParameters())
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
