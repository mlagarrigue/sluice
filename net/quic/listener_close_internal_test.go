package quic

import (
	"crypto/tls"
	"net"
	"net/netip"
	"runtime"
	"sync"
	"testing"
	"time"
)

func newInternalUDPListener(t *testing.T, network, addr string, lcfg ListenerConfig) (*Listener, *net.UDPConn) {
	t.Helper()
	ua, err := net.ResolveUDPAddr(network, addr)
	if err != nil {
		t.Fatal(err)
	}
	pc, err := net.ListenUDP(network, ua)
	if err != nil {
		t.Fatal(err)
	}
	l, err := NewListener(pc, ServerTLSForTest(t), DefaultParameters(), lcfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l, pc
}

func dialInternal(t *testing.T, pc net.PacketConn, to net.Addr) *Conn {
	t.Helper()
	c, err := Dial(pc, to, ClientTLSForTest(), DefaultParameters())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func loopbackSocket(t *testing.T) *net.UDPConn {
	t.Helper()
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pc.Close() })
	return pc
}

func queued(l *Listener) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.accepted)
}

func pendingCount(l *Listener) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.pendingConns)
}

func isClosed(c *Conn) bool {
	select {
	case <-c.closed:
		return true
	default:
		return false
	}
}

// A connection whose handshake completed but which nobody Accepted yet is
// ended by Listener.Close. Nothing can Accept it any more; before, it kept
// its goroutines and its peer kept waiting until the idle timeout. The
// peer learning at once is the proof the CONNECTION_CLOSE left before the
// socket closed.
func TestListenerCloseEndsQueuedConnections(t *testing.T) {
	l, pc := newInternalUDPListener(t, "udp", "127.0.0.1:0", ListenerConfig{})
	client := dialInternal(t, loopbackSocket(t), pc.LocalAddr())
	waitFor(t, "the connection to be queued for Accept", func() bool {
		return queued(l) == 1 && pendingCount(l) == 0
	})
	var server *Conn
	l.mu.Lock()
	for _, c := range l.byDCID {
		server = c
	}
	l.mu.Unlock()

	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !isClosed(server) {
		t.Error("the queued server connection survived Listener.Close")
	}
	select {
	case <-client.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the client never learned its unaccepted connection was closed")
	}
	if _, err := l.Accept(); err == nil {
		t.Error("Accept handed out a connection after Close")
	}
}

// A handshake that completes while the accept queue is full parks on it;
// Close must end that connection too, along with the one in the queue.
func TestListenerCloseEndsConnectionsBlockedOnAFullQueue(t *testing.T) {
	// MaxPendingConns sizes the accept queue: one slot.
	l, pc := newInternalUDPListener(t, "udp", "127.0.0.1:0", ListenerConfig{MaxPendingConns: 1})
	first := dialInternal(t, loopbackSocket(t), pc.LocalAddr())
	waitFor(t, "the first connection to be queued", func() bool {
		return queued(l) == 1 && pendingCount(l) == 0
	})
	second := dialInternal(t, loopbackSocket(t), pc.LocalAddr())
	var parked *Conn
	waitFor(t, "the second handshake to finish and park", func() bool {
		l.mu.Lock()
		for c := range l.pendingConns {
			parked = c
		}
		l.mu.Unlock()
		if parked == nil {
			return false
		}
		parked.mu.Lock()
		defer parked.mu.Unlock()
		return parked.handshakeConfirmed
	})

	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for i, c := range []*Conn{first, second} {
		select {
		case <-c.Done():
		case <-time.After(5 * time.Second):
			t.Fatalf("client %d never learned its connection was closed", i)
		}
	}
	if !isClosed(parked) {
		t.Error("the parked server connection survived Listener.Close")
	}
}

// RFC 9000 §5.2.2: with the pending table full, a new decryptable Initial
// is answered with a stateless CONNECTION_REFUSED close under the keys the
// client's own identifier derives, and creates nothing.
func TestListenerRefusesWhenThePendingTableIsFull(t *testing.T) {
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	l, err := NewListener(pc, &tls.Config{MinVersion: tls.VersionTLS13, NextProtos: []string{"h3"}}, DefaultParameters(), ListenerConfig{MaxPendingConns: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	// Two slots: the first attempt goes direct (below the half-way mark),
	// the second has to come back with a Retry token.
	nowhere := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 5555}
	l.dispatch(sealedClientInitial(t, []byte("odcid-aa"), []byte("cscid-bb")), nowhere, l.pc)
	newSCID := []byte("rscid-01")
	token := appendRetryToken(nil, l.key, nowhere, []byte("odcid-cc"), newSCID)
	l.dispatch(sealedClientInitialWithToken(t, newSCID, []byte("cscid-dd"), token), nowhere, l.pc)
	if n := pendingCount(l); n != 2 {
		t.Fatalf("priming left %d pending connections, want 2", n)
	}
	before := l.DiscardedPackets()

	clientPC := loopbackSocket(t)
	dcid := []byte("odcid-ee")
	l.dispatch(sealedClientInitial(t, dcid, []byte("cscid-ff")), clientPC.LocalAddr(), l.pc)

	if n := pendingCount(l); n != 2 {
		t.Errorf("a full table still took a connection: %d pending", n)
	}
	if got := l.DiscardedPackets(); got != before+1 {
		t.Errorf("DiscardedPackets = %d, want %d: the refused Initial is still counted", got, before+1)
	}
	_ = clientPC.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 2048)
	n, _, err := clientPC.ReadFrom(buf)
	if err != nil {
		t.Fatalf("no answer to the refused Initial: %v", err)
	}
	h, _, err := parseLongHeader(buf[:n])
	if err != nil || h.Type != packetInitial {
		t.Fatalf("answer is not an Initial (%v)", err)
	}
	if string(h.DCID) != "cscid-ff" || string(h.SCID) != string(dcid) {
		t.Errorf("identifiers not echoed swapped: dcid %q scid %q", h.DCID, h.SCID)
	}
	secrets, err := initialSecrets(dcid)
	if err != nil {
		t.Fatal(err)
	}
	opener, err := newPacketOpener(secrets.Server)
	if err != nil {
		t.Fatal(err)
	}
	payload, _, err := opener.Open(h.Raw, h.PNOffset, 0)
	if err != nil {
		t.Fatalf("the close does not open under the client's Initial keys: %v", err)
	}
	frames, err := parseFrames(nil, payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 1 || frames[0].Type != frameConnectionClose || frames[0].value != transportConnectionRefused {
		t.Fatalf("frames %+v, want one transport CONNECTION_CLOSE with CONNECTION_REFUSED", frames)
	}
}

// RETIRE_CONNECTION_ID from the peer drops the retired identifier from the
// listener's demux table and registers its replacement: without the
// removal every rotation would leave a dead entry behind for the
// connection's whole life.
func TestListenerRetiredIdentifierLeavesTheTable(t *testing.T) {
	l, pc := newInternalUDPListener(t, "udp", "127.0.0.1:0", ListenerConfig{})
	dialInternal(t, loopbackSocket(t), pc.LocalAddr())
	server, err := l.Accept()
	if err != nil {
		t.Fatal(err)
	}
	var victim localCID
	waitFor(t, "the server to issue identifiers", func() bool {
		server.mu.Lock()
		defer server.mu.Unlock()
		for _, e := range server.localCIDs {
			if e.seq != 0 && e.seq != server.rxLocalCIDSeq {
				victim = e
				return true
			}
		}
		return false
	})
	routes := func(cid []byte) bool {
		l.mu.Lock()
		defer l.mu.Unlock()
		return l.byDCID[string(cid)] == server
	}
	if !routes(victim.cid) {
		t.Fatal("an issued identifier is not in the demux table")
	}
	server.mu.Lock()
	issued := len(server.localCIDs)
	server.mu.Unlock()

	if err := server.handleRetireConnectionID(victim.seq); err != nil {
		t.Fatalf("retire: %v", err)
	}
	if routes(victim.cid) {
		t.Error("the retired identifier still routes to the connection")
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	if len(server.localCIDs) != issued {
		t.Errorf("%d identifiers after retiring one, want %d (a replacement)", len(server.localCIDs), issued)
	}
	for _, e := range server.localCIDs {
		if !routes(e.cid) {
			t.Errorf("identifier %d does not route", e.seq)
		}
	}
}

// srcRecorder notes the source address of every datagram a client reads.
type srcRecorder struct {
	net.PacketConn
	mu   sync.Mutex
	from []netip.Addr
}

func (r *srcRecorder) ReadFrom(p []byte) (int, net.Addr, error) {
	n, a, err := r.PacketConn.ReadFrom(p)
	if ua, ok := a.(*net.UDPAddr); ok && err == nil {
		r.mu.Lock()
		r.from = append(r.from, ua.AddrPort().Addr().Unmap())
		r.mu.Unlock()
	}
	return n, a, err
}

// A listener bound to the unspecified address answers from the address a
// datagram reached, not whichever the kernel's route prefers: a client
// that dialled 127.0.0.2 from 127.0.0.1 hears only from 127.0.0.2. Without
// packet information every answer came from 127.0.0.1 — a different path
// to QUIC, and no answer at all through a NAT that tracks the pair. Linux
// puts all of 127/8 on lo, which is what makes this testable on one host.
func TestListenerAnswersFromTheAddressDialled(t *testing.T) {
	l, pc := newInternalUDPListener(t, "udp4", "0.0.0.0:0", ListenerConfig{})
	if runtime.GOOS != "linux" {
		t.Skip("packet information is wired on Linux only")
	}
	want := netip.MustParseAddr("127.0.0.2")
	to := net.UDPAddrFromAddrPort(netip.AddrPortFrom(want, uint16(pc.LocalAddr().(*net.UDPAddr).Port)))
	rec := &srcRecorder{PacketConn: loopbackSocket(t)}
	client := dialInternal(t, rec, to)
	server, err := l.Accept()
	if err != nil {
		t.Fatal(err)
	}
	if got := server.pc.LocalAddr().(*net.UDPAddr).AddrPort().Addr(); got != want {
		t.Errorf("server connection sends from %v, want %v", got, want)
	}

	go func() {
		s, err := server.AcceptStream()
		if err != nil {
			return
		}
		buf := make([]byte, 64)
		n, _ := s.Read(buf)
		_, _ = s.Write(buf[:n])
		_ = s.CloseWrite()
	}()
	s, err := client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 64)
	if n, err := s.Read(buf); err != nil || string(buf[:n]) != "ping" {
		t.Fatalf("echo: %q, %v", buf[:n], err)
	}

	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.from) == 0 {
		t.Fatal("the client recorded no datagrams")
	}
	for i, a := range rec.from {
		if a != want {
			t.Fatalf("datagram %d came from %v, want %v", i, a, want)
		}
	}
	if server.Migrations() != 0 {
		t.Errorf("the server saw %d path changes for a client that never moved", server.Migrations())
	}
}

// A completed connection parked on a full accept queue that ends there —
// its peer closed, or its idle timer fired — leaves at once: its pending
// slot and demux entries are freed, and it never reaches Accept. It used
// to stay parked until Listener.Close.
func TestParkedConnectionThatEndsLeavesThePendingTable(t *testing.T) {
	l, pc := newInternalUDPListener(t, "udp", "127.0.0.1:0", ListenerConfig{MaxPendingConns: 1})
	dialInternal(t, loopbackSocket(t), pc.LocalAddr())
	waitFor(t, "the first connection to be queued", func() bool {
		return queued(l) == 1 && pendingCount(l) == 0
	})
	second := dialInternal(t, loopbackSocket(t), pc.LocalAddr())
	var parked *Conn
	waitFor(t, "the second handshake to finish and park", func() bool {
		l.mu.Lock()
		for c := range l.pendingConns {
			parked = c
		}
		l.mu.Unlock()
		if parked == nil {
			return false
		}
		parked.mu.Lock()
		defer parked.mu.Unlock()
		return parked.handshakeConfirmed
	})

	_ = second.Close() // its CONNECTION_CLOSE ends the parked server side
	waitFor(t, "the parked connection to leave the pending table", func() bool {
		return isClosed(parked) && pendingCount(l) == 0
	})
	l.mu.Lock()
	for _, c := range l.byDCID {
		if c == parked {
			t.Error("the ended connection still routes")
			break
		}
	}
	l.mu.Unlock()
	if queued(l) != 1 {
		t.Errorf("%d connections queued, want only the first", queued(l))
	}
}
