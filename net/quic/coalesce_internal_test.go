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
		conn, err := Accept(serverTap, peer, buf[:n], ServerTLSForTest(t), DefaultParameters())
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
	for i, w := range server {
		if len(w.data) > 1200 {
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
		if len(w.data) > 1200 {
			t.Errorf("client datagram %d is %d bytes, over 1200", i, len(w.data))
		}
	}
	t.Logf("handshake datagrams: %d from the server, %d from the client", len(server), len(cl))
}
