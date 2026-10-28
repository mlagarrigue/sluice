package quic

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/netip"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/internal/varint"
)

// Tests for preferred_address (RFC 9000 §9.6, §18.2): the parameter's
// encoding, the client's probe-then-move, the server's follow onto its
// preferred socket, and the whole exchange over loopback UDP.

func preferredParams(pa *preferredAddress, dcid, odcid []byte) []byte {
	p := DefaultParameters()
	p.initialSourceCID = dcid
	p.originalDCID = odcid
	p.preferredAddress = pa
	return appendParameters(nil, p)
}

// §18.2: the parameter round-trips with both families, with one, and is
// refused with a zero-length or oversized identifier (TRANSPORT_PARAMETER_ERROR)
// or a length that does not add up.
func TestPreferredAddressCodec(t *testing.T) {
	v4 := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 7).To4(), Port: 4433}
	v6 := &net.UDPAddr{IP: net.ParseIP("2001:db8::7"), Port: 4434}
	for name, pa := range map[string]*preferredAddress{
		"both": {ipv4: v4, ipv6: v6, cid: []byte("pref-cid"), statelessResetToken: [16]byte{1, 2, 3}},
		"v4":   {ipv4: v4, cid: []byte("p"), statelessResetToken: [16]byte{9}},
		"v6":   {ipv6: v6, cid: make([]byte, 20)},
	} {
		t.Run(name, func(t *testing.T) {
			p, err := parseParameters(preferredParams(pa, []byte("abcd"), nil))
			if err != nil {
				t.Fatal(err)
			}
			got := p.preferredAddress
			if got == nil {
				t.Fatal("preferred_address did not parse")
			}
			if (got.ipv4 == nil) != (pa.ipv4 == nil) || (got.ipv4 != nil && !sameAddr(got.ipv4, pa.ipv4)) {
				t.Errorf("IPv4 = %v, want %v", got.ipv4, pa.ipv4)
			}
			if (got.ipv6 == nil) != (pa.ipv6 == nil) || (got.ipv6 != nil && !sameAddr(got.ipv6, pa.ipv6)) {
				t.Errorf("IPv6 = %v, want %v", got.ipv6, pa.ipv6)
			}
			if !bytes.Equal(got.cid, pa.cid) || got.statelessResetToken != pa.statelessResetToken {
				t.Errorf("CID/token = %x/%x, want %x/%x", got.cid, got.statelessResetToken, pa.cid, pa.statelessResetToken)
			}
		})
	}

	raw := func(val []byte) []byte {
		b := varint.Append(nil, paramPreferredAddress)
		b = varint.Append(b, uint64(len(val)))
		return append(b, val...)
	}
	zeroCID := make([]byte, preferredAddressLen)
	_, err := parseParameters(raw(zeroCID))
	te := &transportError{}
	if !errors.As(err, &te) || te.code != transportParameterError {
		t.Errorf("a zero-length identifier returned %v, want TRANSPORT_PARAMETER_ERROR", err)
	}
	tooLong := make([]byte, preferredAddressLen+21)
	tooLong[24] = 21
	if _, err := parseParameters(raw(tooLong)); !errors.As(err, &te) || te.code != transportParameterError {
		t.Errorf("a 21-byte identifier returned %v, want TRANSPORT_PARAMETER_ERROR", err)
	}
	// §18.2 and §20.1: a malformed length is the same TRANSPORT_PARAMETER_ERROR,
	// not the INTERNAL_ERROR an unwrapped error would close with.
	short := make([]byte, preferredAddressLen+3)
	short[24] = 8 // claims 8, carries 3
	if _, err := parseParameters(raw(short)); !errors.As(err, &te) || te.code != transportParameterError {
		t.Errorf("an identifier longer than what follows it returned %v, want TRANSPORT_PARAMETER_ERROR", err)
	}
	if _, err := parseParameters(raw(make([]byte, 10))); !errors.As(err, &te) || te.code != transportParameterError {
		t.Errorf("a 10-byte preferred_address returned %v, want TRANSPORT_PARAMETER_ERROR", err)
	}
}

// newPreferredClient is a client Conn past its handshake's parameter
// exchange, whose server offered pref as its preferred address: the state
// right before HANDSHAKE_DONE arrives.
func newPreferredClient(t *testing.T, name string, pref *net.UDPAddr) (*Conn, *recordingPC) {
	t.Helper()
	pc := &recordingPC{addr: fakeFuzzAddr(name + ":self")}
	peer := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1).To4(), Port: 443}
	c := newConn(pc, peer, []byte("srv-cid0"), []byte("cli-cid0"), true, DefaultParameters())
	t.Cleanup(func() { _ = c.Close() })
	c.initialDCID = []byte("srv-cid0")
	c.pinPeerSCID([]byte("srv-cid0"))
	installAppSealer(t, c)
	pa := &preferredAddress{ipv4: pref, cid: []byte("pref-cid"), statelessResetToken: [16]byte{7}}
	if err := c.handlePeerParameters(preferredParams(pa, []byte("srv-cid0"), []byte("srv-cid0"))); err != nil {
		t.Fatal(err)
	}
	return c, pc
}

// §9.6.2 on the client: the offered identifier is pooled as sequence 1 at
// once; confirmation sends a 1200-byte PATH_CHALLENGE to the preferred
// address under that identifier while the path in use stays the peer; the
// matching PATH_RESPONSE — arriving from the preferred address — moves the
// connection there, under that identifier, with a migration counted.
func TestClientMovesToTheServersPreferredAddress(t *testing.T) {
	pref := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2).To4(), Port: 4433}
	c, pc := newPreferredClient(t, "pref", pref)

	c.mu.Lock()
	pooled := false
	for _, e := range c.peerCIDs {
		if e.seq == 1 && bytes.Equal(e.cid, []byte("pref-cid")) && e.token == [16]byte{7} {
			pooled = true
		}
	}
	c.mu.Unlock()
	if !pooled {
		t.Fatal("the preferred address's identifier was not pooled as sequence 1 (§5.1.1)")
	}
	if got := c.preferredAddress(); !sameAddr(got, pref) {
		t.Fatalf("PreferredAddress() = %v, want %v", got, pref)
	}

	sentBefore := len(pc.recorded())
	c.mu.Lock()
	c.confirmLocked()
	before := c.peer
	c.mu.Unlock()
	if !sameAddr(before, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1).To4(), Port: 443}) {
		t.Fatalf("the path in use moved to %v before any validation", before)
	}
	var probe *recordedWrite
	for i, w := range pc.recorded() {
		if sameAddr(w.to, pref) {
			probe = &w
		}
		// §8.2.2: an answer validates the path its challenge went on, and
		// says nothing of which that was. The same challenge on the path in
		// use, answered there, moved the client to an address the server
		// had not answered at.
		if i >= sentBefore && sameAddr(w.to, before) && len(w.data) >= 1200 {
			t.Errorf("a %d-byte datagram, the challenge's size, went to the path in use on confirmation", len(w.data))
		}
	}
	if probe == nil {
		t.Fatal("no datagram went to the preferred address on confirmation")
	}
	if len(probe.data) < 1200 {
		t.Errorf("the probe left in a %d-byte datagram, want 1200 (§8.2.1)", len(probe.data))
	}
	if !bytes.Equal(probe.data[1:9], []byte("pref-cid")) {
		t.Errorf("the probe named identifier %q, want the preferred address's", probe.data[1:9])
	}

	// The answer comes back from the preferred address: the receive gate
	// must let it through for a client, and the move completes.
	c.mu.Lock()
	resp := varint.Append(nil, framePathResponse)
	resp = append(resp, c.pathProbe.data[:]...)
	c.rxFrom = pref
	c.mu.Unlock()
	if err := c.frames(spaceApplication, resp, time.Now()); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	peer, dcid, migrations, pending := c.peer, c.dcid, c.migrations, c.pathProbe.pending
	c.mu.Unlock()
	if !sameAddr(peer, pref) {
		t.Errorf("after validation the peer is %v, want %v", peer, pref)
	}
	if !bytes.Equal(dcid, []byte("pref-cid")) {
		t.Errorf("after validation sends name %q, want the preferred identifier", dcid)
	}
	if migrations != 1 || pending {
		t.Errorf("migrations=%d pending=%v, want 1/false", migrations, pending)
	}
}

// The receive gate itself: a client otherwise drops datagrams from any
// address but the peer's; while probing the preferred address, that one
// address is admitted too.
func TestClientAdmitsDatagramsFromThePreferredAddressWhileProbing(t *testing.T) {
	pref := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2).To4(), Port: 4433}
	c, _ := newPreferredClient(t, "gate", pref)
	stray := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 9).To4(), Port: 1}
	short := []byte{0x40, 'c', 'l', 'i', '-', 'c', 'i', 'd', '0', 1, 2, 3, 4, 5}
	_ = c.receive(short, pref)
	if n := c.Stats().DiscardedPackets; n != 1 {
		t.Fatalf("before probing, a datagram from the preferred address counted %d discards, want 1", n)
	}
	c.mu.Lock()
	c.confirmLocked()
	c.mu.Unlock()
	_ = c.receive(short, stray)
	_ = c.receive(short, pref)
	// The stray is refused at the gate (one more discard); the preferred
	// address's datagram passes it and fails only at authentication, which
	// is also a discard — so the distinguishing observable is the gate's
	// own check, exercised through rxFrom.
	c.mu.Lock()
	from := c.rxFrom
	c.mu.Unlock()
	if !sameAddr(from, pref) {
		t.Fatalf("the gate did not admit the preferred address while probing (rxFrom %v)", from)
	}
}

// A preferred address that never answers: the probe retransmits and gives
// up, the connection stays on the handshake's path with no error, and the
// offer is simply not taken (§9.6.2 permits a client to ignore it).
func TestUnansweredPreferredAddressLeavesTheConnectionWhereItIs(t *testing.T) {
	pref := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2).To4(), Port: 4433}
	c, _ := newPreferredClient(t, "silent", pref)
	c.mu.Lock()
	c.confirmLocked()
	now := c.pathProbe.sentAt
	for i := 0; i <= pathProbeMaxRetransmits; i++ {
		now = now.Add(time.Hour)
		c.pathProbeDeadlineLocked(now)
	}
	peer, dcid, migrations, pending, target := c.peer, c.dcid, c.migrations, c.pathProbe.pending, c.pathProbe.target
	c.mu.Unlock()
	if !sameAddr(peer, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1).To4(), Port: 443}) || !bytes.Equal(dcid, []byte("srv-cid0")) {
		t.Errorf("the connection moved to %v / %q without validation", peer, dcid)
	}
	if migrations != 0 || pending || target != nil {
		t.Errorf("migrations=%d pending=%v target=%v after giving up, want 0/false/nil", migrations, pending, target)
	}
	select {
	case <-c.Done():
		t.Fatalf("the connection ended: %v", c.Err())
	default:
	}
}

// A preferred address of the other family than the path in use is not
// probed: there is nothing to move to.
func TestPreferredAddressOfAnotherFamilyIsIgnored(t *testing.T) {
	pc := &recordingPC{addr: fakeFuzzAddr("fam:self")}
	peer := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1).To4(), Port: 443}
	c := newConn(pc, peer, []byte("srv-cid0"), []byte("cli-cid0"), true, DefaultParameters())
	defer c.Close()
	c.initialDCID = []byte("srv-cid0")
	c.pinPeerSCID([]byte("srv-cid0"))
	installAppSealer(t, c)
	pa := &preferredAddress{ipv6: &net.UDPAddr{IP: net.ParseIP("2001:db8::2"), Port: 4433}, cid: []byte("pref-cid")}
	if err := c.handlePeerParameters(preferredParams(pa, []byte("srv-cid0"), []byte("srv-cid0"))); err != nil {
		t.Fatal(err)
	}
	if got := c.preferredAddress(); got != nil {
		t.Fatalf("PreferredAddress() = %v for an IPv6-only offer on an IPv4 path, want nil", got)
	}
	c.mu.Lock()
	c.confirmLocked()
	pending := c.pathProbe.pending
	c.mu.Unlock()
	if pending {
		t.Fatal("a path probe is pending for an address of another family")
	}
	// What confirmation sends is the DPLPMTUD probe of the path in use
	// (pmtu.go), never a PATH_CHALLENGE.
	for i, frames := range openAppPackets(t, c, pc) {
		for _, f := range frames {
			if f.Type == framePathChallenge {
				t.Fatalf("datagram %d carries a PATH_CHALLENGE for an address of another family", i)
			}
		}
	}
}

// NewListener refuses a preferred socket bound to the unspecified address:
// a client cannot be told to send to 0.0.0.0.
func TestListenerRefusesAnUnspecifiedPreferredAddress(t *testing.T) {
	main, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer main.Close()
	unspecified, err := net.ListenUDP("udp4", &net.UDPAddr{})
	if err != nil {
		t.Fatal(err)
	}
	defer unspecified.Close()
	if l, err := NewListener(main, ServerTLSForTest(t), DefaultParameters(), ListenerConfig{PreferredAddress: unspecified}); err == nil {
		l.Close()
		t.Fatal("a preferred socket on 0.0.0.0 was accepted")
	}
}

// The whole §9.6 exchange over loopback: a listener on 127.0.0.1 whose
// preferred socket is bound to 127.0.0.2. The client dials the first, is
// told about the second in the handshake, probes it, and moves; the
// server's connection follows onto the preferred socket once the client's
// data arrives there, and the data keeps flowing both ways. Skipped where
// 127.0.0.2 is not a local address (macOS configures only 127.0.0.1).
func TestPreferredAddressEndToEnd(t *testing.T) {
	main, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	pref, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 2)})
	if err != nil {
		main.Close()
		t.Skipf("no 127.0.0.2 on this host: %v", err)
	}
	l, err := NewListener(main, ServerTLSForTest(t), DefaultParameters(), ListenerConfig{PreferredAddress: pref})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	accepted := make(chan *Conn, 1)
	go func() {
		c, err := l.Accept()
		if err != nil {
			close(accepted)
			return
		}
		accepted <- c
	}()

	cpc, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer cpc.Close()
	client, err := Dial(context.Background(), cpc, main.LocalAddr(), ClientTLSForTest(), DefaultParameters())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	var server *Conn
	select {
	case c, ok := <-accepted:
		if !ok {
			t.Fatal("Accept failed")
		}
		server = c
	case <-time.After(10 * time.Second):
		t.Fatal("the handshake never completed")
	}
	if got := client.preferredAddress(); !sameAddr(got, pref.LocalAddr()) {
		t.Fatalf("the client learned preferred address %v, want %v", got, pref.LocalAddr())
	}

	waitFor(t, "the client to validate and adopt the preferred path", func() bool {
		client.mu.Lock()
		defer client.mu.Unlock()
		return sameAddr(client.peer, pref.LocalAddr()) && !client.pathProbe.pending
	})
	if n := client.Migrations(); n != 1 {
		t.Errorf("client migrations = %d, want 1", n)
	}

	got := make(chan []byte, 8)
	server.OnStreamFrames(CreditOnDelivery, func(fs []Frame) error {
		for _, f := range fs {
			if f.IsStream() {
				got <- append([]byte(nil), f.Data...)
			}
		}
		return nil
	})
	s, err := client.OpenStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write([]byte("to the preferred address")); err != nil {
		t.Fatal(err)
	}
	select {
	case data := <-got:
		if string(data) != "to the preferred address" {
			t.Errorf("the server read %q", data)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("data sent to the preferred address never arrived")
	}
	waitFor(t, "the server to send from its preferred socket", func() bool {
		server.mu.Lock()
		defer server.mu.Unlock()
		return server.pc == net.PacketConn(pref)
	})
	if n := server.Migrations(); n != 1 {
		t.Errorf("server migrations = %d, want 1", n)
	}

	// And the reverse direction, now that the server sends from 127.0.0.2:
	// the client accepts what comes from its (new) peer address.
	echo := make(chan []byte, 8)
	client.OnStreamFrames(CreditOnDelivery, func(fs []Frame) error {
		for _, f := range fs {
			if f.IsStream() {
				echo <- append([]byte(nil), f.Data...)
			}
		}
		return nil
	})
	ss, err := server.OpenStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ss.Write([]byte("from the preferred address")); err != nil {
		t.Fatal(err)
	}
	select {
	case data := <-echo:
		if string(data) != "from the preferred address" {
			t.Errorf("the client read %q", data)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("data sent from the preferred socket never reached the client")
	}
	if err := client.Err(); err != nil {
		t.Fatal(err)
	}
}

// §5.1.1 on the server: the identifier announced in preferred_address is
// sequence 1 of this end's pool, and a client may name it from the moment
// it reads the transport parameters — which is before its Handshake flight
// completes. quic-go does exactly that: its Handshake packets carrying the
// client Finished already use the preferred identifier, while staying on
// the original path. A long-header gate that knew only scid dropped them,
// and the server resent its own Handshake until its deadline closed the
// connection with INTERNAL_ERROR.
func TestServerAcceptsItsPreferredIdentifierInLongHeaders(t *testing.T) {
	pc := &recordingPC{addr: fakeFuzzAddr("pref-srv:self")}
	peer := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1).To4(), Port: 50000}
	c := newConn(pc, peer, []byte("cli-cid0"), []byte("srv-cid0"), false, DefaultParameters())
	t.Cleanup(func() { _ = c.Close() })
	c.localCIDs = append(c.localCIDs, localCID{seq: 1, cid: []byte("pref-cid")})

	for _, space := range []int{spaceInitial, spaceHandshake} {
		if !c.expectedDCID([]byte("srv-cid0"), space) {
			t.Errorf("space %d: scid refused", space)
		}
		if !c.expectedDCID([]byte("pref-cid"), space) {
			t.Errorf("space %d: the preferred_address identifier (sequence 1) refused", space)
		}
		if c.expectedDCID([]byte("else-cid"), space) {
			t.Errorf("space %d: an identifier this end never issued accepted", space)
		}
	}
}

// §9.6.2 on the server: a PATH_CHALLENGE that arrives on the preferred
// socket from the client's address is the client validating the offered
// path, and "the server MUST probe on the path toward the client from its
// preferred address". The first datagram the server sends from that socket
// carries the answer and its own PATH_CHALLENGE together, under a pooled
// identifier never used on the main path (§9.5); the main socket sends
// nothing for it, and nothing moves until the client's first non-probing
// packet arrives there and the server's own probe is answered, whichever
// comes last — the move then adopts that identifier. The public
// interop runner checks exactly this: the first server packet on the new
// path must carry a PATH_CHALLENGE (ngtcp2 client, 2026-10-10).
func TestServerProbesTheClientFromItsPreferredSocket(t *testing.T) {
	c, main := newMigratableServer(t, "pref")
	pref := &recordingPC{addr: fakeFuzzAddr("pref:preferred")}
	tok := make([]byte, 16)
	if err := c.frames(spaceApplication, ncidFrame(1, 0, []byte("cli-cid1"), tok), time.Now()); err != nil {
		t.Fatal(err)
	}
	peer := fakeFuzzAddr("pref:peerA")

	rxVia := func(via net.PacketConn) {
		c.mu.Lock()
		c.rxFrom, c.rxVia, c.rxDatagramLen, c.rxHighest = peer, via, 1200, true
		c.mu.Unlock()
	}
	challenge := append(varint.Append(nil, framePathChallenge), 1, 2, 3, 4, 5, 6, 7, 8)
	rxVia(pref)
	if err := c.frames(spaceApplication, challenge, time.Now()); err != nil {
		t.Fatal(err)
	}

	if n := len(main.recorded()); n != 0 {
		t.Fatalf("%d datagrams left by the main socket, want none", n)
	}
	writes := pref.recorded()
	if len(writes) != 1 {
		t.Fatalf("%d datagrams left by the preferred socket, want 1", len(writes))
	}
	if !sameAddr(writes[0].to, peer) || len(writes[0].data) < 1200 {
		t.Errorf("the datagram went to %v in %d bytes, want %v in 1200", writes[0].to, len(writes[0].data), peer)
	}
	if !bytes.HasPrefix(writes[0].data[1:], []byte("cli-cid1")) {
		t.Errorf("the datagram names identifier %x, want the fresh cli-cid1 (§9.5)", writes[0].data[1:9])
	}
	frames := openShort(t, writes[0].data, len("cli-cid1"))
	var response, probe bool
	for _, f := range frames {
		switch f.Type {
		case framePathResponse:
			response = bytes.Equal(f.Data, []byte{1, 2, 3, 4, 5, 6, 7, 8})
		case framePathChallenge:
			probe = true
		}
	}
	if !response || !probe {
		t.Fatalf("the first datagram from the preferred socket carries response=%v challenge=%v, want both", response, probe)
	}
	c.mu.Lock()
	pending, via, pc, migrations := c.pathProbe.pending, c.pathProbe.via, c.pc, c.migrations
	c.mu.Unlock()
	if !pending || via != net.PacketConn(pref) {
		t.Errorf("probe pending=%v via=%v, want pending from the preferred socket", pending, via)
	}
	if pc != net.PacketConn(main) || migrations != 0 {
		t.Errorf("a probing-only packet moved the connection (pc=%v, migrations=%d)", pc, migrations)
	}

	// The client's first non-probing packet on the preferred socket, its
	// answer to this end's probe still on the way: §9.6.2's two conditions
	// are not both met, and non-probing packets keep leaving from the
	// original address.
	rxVia(pref)
	c.maybeMigrate([]Frame{{Type: framePing}}, time.Now())
	c.mu.Lock()
	pc, migrations = c.pc, c.migrations
	data := c.pathProbe.data
	c.mu.Unlock()
	if pc != net.PacketConn(main) || migrations != 0 {
		t.Errorf("after the client's data alone: on the preferred socket=%v migrations=%d, want the main socket and 0 (§9.6.2: the path is not validated yet)", pc == net.PacketConn(pref), migrations)
	}
	// The answer arrives: both conditions hold, the connection moves and
	// adopts the identifier it probed under.
	resp := append(varint.Append(nil, framePathResponse), data[:]...)
	rxVia(pref)
	if err := c.frames(spaceApplication, resp, time.Now()); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	var dcid []byte
	pc, dcid, migrations = c.pc, c.dcid, c.migrations
	c.mu.Unlock()
	if pc != net.PacketConn(pref) || migrations != 1 {
		t.Errorf("after the client's data and its answer: on the preferred socket=%v migrations=%d, want true and 1", pc == net.PacketConn(pref), migrations)
	}
	if string(dcid) != "cli-cid1" {
		t.Errorf("the connection sends under %q, want the identifier it probed under", dcid)
	}
}

// The usual order: the client answers the server's probe before its first
// non-probing packet reaches the preferred socket. The answer alone moves
// nothing (§9.6.2 wants the non-probing packet too); that packet then does.
func TestServerMovesToItsPreferredSocketOnceProbedAndWrittenTo(t *testing.T) {
	c, main := newMigratableServer(t, "order")
	pref := &recordingPC{addr: fakeFuzzAddr("order:preferred")}
	tok := make([]byte, 16)
	if err := c.frames(spaceApplication, ncidFrame(1, 0, []byte("cli-cid1"), tok), time.Now()); err != nil {
		t.Fatal(err)
	}
	peer := fakeFuzzAddr("order:peerA")
	rxVia := func(via net.PacketConn) {
		c.mu.Lock()
		c.rxFrom, c.rxVia, c.rxDatagramLen, c.rxHighest = peer, via, 1200, true
		c.mu.Unlock()
	}
	challenge := append(varint.Append(nil, framePathChallenge), 1, 2, 3, 4, 5, 6, 7, 8)
	rxVia(pref)
	if err := c.frames(spaceApplication, challenge, time.Now()); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	data := c.pathProbe.data
	c.mu.Unlock()
	resp := append(varint.Append(nil, framePathResponse), data[:]...)
	rxVia(pref)
	if err := c.frames(spaceApplication, resp, time.Now()); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	pc, migrations, pending := c.pc, c.migrations, c.pathProbe.pending
	c.mu.Unlock()
	if pc != net.PacketConn(main) || migrations != 0 || pending {
		t.Fatalf("after the answer alone: pc=%v migrations=%d probe pending=%v, want the main socket, 0 and false", pc, migrations, pending)
	}
	rxVia(pref)
	c.maybeMigrate([]Frame{{Type: framePing}}, time.Now())
	c.mu.Lock()
	pc, dcid, migrations := c.pc, c.dcid, c.migrations
	c.mu.Unlock()
	if pc != net.PacketConn(pref) || migrations != 1 || string(dcid) != "cli-cid1" {
		t.Fatalf("after the client's data: pc=%v migrations=%d dcid=%q, want the preferred socket, 1 and cli-cid1", pc, migrations, dcid)
	}
}

// §9.5 on the server's preferred socket: "An endpoint MUST NOT reuse a
// connection ID when sending from more than one local address". A client
// that has issued no identifier beyond the one in use leaves the server
// nothing to send under from that socket, so nothing leaves it — neither
// the answer to the client's challenge nor a probe — and a non-probing
// packet arriving there does not move the connection either. The main
// socket stays silent too: the response belongs to the path it answers.
// The client's validation then fails and §9.6.1 keeps it on the original
// address.
func TestServerWithoutASpareIdentifierSendsNothingFromItsPreferredSocket(t *testing.T) {
	c, main := newMigratableServer(t, "spare")
	pref := &recordingPC{addr: fakeFuzzAddr("spare:preferred")}
	peer := fakeFuzzAddr("spare:peerA")
	rxVia := func() {
		c.mu.Lock()
		c.rxFrom, c.rxVia, c.rxDatagramLen, c.rxHighest = peer, pref, 1200, true
		c.mu.Unlock()
	}

	rxVia()
	challenge := append(varint.Append(nil, framePathChallenge), 1, 2, 3, 4, 5, 6, 7, 8)
	if err := c.frames(spaceApplication, challenge, time.Now()); err != nil {
		t.Fatal(err)
	}
	if n := len(pref.recorded()); n != 0 {
		t.Fatalf("%d datagrams left the preferred socket under the identifier in use, want none (§9.5)", n)
	}
	if n := len(main.recorded()); n != 0 {
		t.Fatalf("%d datagrams left the main socket, want none", n)
	}
	c.mu.Lock()
	pending := c.pathProbe.pending
	c.mu.Unlock()
	if pending {
		t.Error("a probe is armed with no identifier to send it under")
	}

	rxVia()
	c.maybeMigrate([]Frame{{Type: framePing}}, time.Now())
	c.mu.Lock()
	pc, dcid, migrations := c.pc, c.dcid, c.migrations
	c.mu.Unlock()
	if pc != net.PacketConn(main) || migrations != 0 || string(dcid) != "peercid0" {
		t.Errorf("after a non-probing packet: pc=%v migrations=%d dcid=%q, want the main socket, 0, peercid0", pc, migrations, dcid)
	}
}

// openShort decrypts a 1-RTT packet a test Conn sealed with installAppSealer
// and returns its frames.
func openShort(t *testing.T, packet []byte, dcidLen int) []Frame {
	t.Helper()
	sec, err := initialSecrets([]byte("appkeys0"))
	if err != nil {
		t.Fatal(err)
	}
	opener, err := newPacketOpener(sec.Client)
	if err != nil {
		t.Fatal(err)
	}
	payload, _, err := opener.Open(packet, 1+dcidLen, 0)
	if err != nil {
		t.Fatalf("the datagram does not open: %v", err)
	}
	frames, err := parseFrames(nil, payload)
	if err != nil {
		t.Fatalf("the payload does not parse: %v", err)
	}
	return frames
}

// A dual-stack preferred socket announces one address per family, named in
// PreferredIPv4 and PreferredIPv6; the parameter then offers each client the address of its
// own family (§9.6.1 — a client reached over IPv6 never moves to an IPv4
// address, which is what left ngtcp2 on the handshake path, 2026-10-10).
// The refusals: an address of the other family, an address a concretely
// bound socket does not answer at, and an unspecified address. Two of one
// family no longer has a spelling.
func TestListenerAnnouncesOneAddressPerFamily(t *testing.T) {
	main, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer main.Close()
	dual, err := net.ListenUDP("udp", &net.UDPAddr{})
	if err != nil {
		t.Skipf("no dual-stack socket on this host: %v", err)
	}
	defer dual.Close()
	port := dual.LocalAddr().(*net.UDPAddr).Port

	l, err := NewListener(main, ServerTLSForTest(t), DefaultParameters(), ListenerConfig{
		PreferredAddress: dual, PreferredIPv4: netip.MustParseAddr("127.0.0.1"), PreferredIPv6: netip.IPv6Loopback(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	c := newConn(main, fakeFuzzAddr("fam:peer"), []byte("peercid0"), []byte("selfcid0"), false, DefaultParameters())
	defer c.Close()
	if err := l.prepareConn(c); err != nil {
		t.Fatal(err)
	}
	pa := c.params.preferredAddress
	if pa == nil || pa.ipv4 == nil || pa.ipv6 == nil {
		t.Fatalf("announced %+v, want both families", pa)
	}
	if !pa.ipv4.IP.Equal(net.IPv4(127, 0, 0, 1)) || pa.ipv4.Port != port || !pa.ipv6.IP.Equal(net.IPv6loopback) || pa.ipv6.Port != port {
		t.Errorf("announced %v and %v, want 127.0.0.1 and ::1 on port %d", pa.ipv4, pa.ipv6, port)
	}
	v4 := pa.addrFor(&net.UDPAddr{IP: net.IPv4(10, 0, 0, 1), Port: 1})
	v6 := pa.addrFor(&net.UDPAddr{IP: net.ParseIP("fd00::1"), Port: 1})
	if !sameAddr(v4, pa.ipv4) || !sameAddr(v6, pa.ipv6) {
		t.Errorf("a v4 client is offered %v and a v6 one %v", v4, v6)
	}

	refused := []struct {
		name, field string
		lcfg        ListenerConfig
	}{
		{"IPv6 as PreferredIPv4", "PreferredIPv4", ListenerConfig{PreferredIPv4: netip.IPv6Loopback()}},
		{"IPv4 as PreferredIPv6", "PreferredIPv6", ListenerConfig{PreferredIPv6: netip.MustParseAddr("127.0.0.1")}},
		{"mapped IPv4 as PreferredIPv6", "PreferredIPv6", ListenerConfig{PreferredIPv6: netip.MustParseAddr("::ffff:127.0.0.1")}},
		{"unspecified", "PreferredIPv4", ListenerConfig{PreferredIPv4: netip.IPv4Unspecified()}},
	}
	for _, r := range refused {
		r.lcfg.PreferredAddress = dual
		l, err := NewListener(main, ServerTLSForTest(t), DefaultParameters(), r.lcfg)
		if err == nil {
			l.Close()
			t.Errorf("%s was accepted", r.name)
		} else if !strings.Contains(err.Error(), "ListenerConfig."+r.field) {
			t.Errorf("%s: the error %q does not name %s", r.name, err, r.field)
		}
	}
	if l, err := NewListener(main, ServerTLSForTest(t), DefaultParameters(), ListenerConfig{PreferredIPv4: netip.MustParseAddr("127.0.0.1")}); err == nil {
		l.Close()
		t.Error("PreferredIPv4 without a PreferredAddress socket was accepted")
	}
	mapped, err := ListenerConfig{PreferredAddress: dual, PreferredIPv4: netip.MustParseAddr("::ffff:127.0.0.1")}.resolve()
	if err != nil || mapped.PreferredIPv4 != netip.MustParseAddr("127.0.0.1") {
		t.Errorf("a mapped IPv4 PreferredIPv4 resolved to %v, %v; want 127.0.0.1", mapped.PreferredIPv4, err)
	}
	concrete, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer concrete.Close()
	if l, err := NewListener(main, ServerTLSForTest(t), DefaultParameters(), ListenerConfig{
		PreferredAddress: concrete, PreferredIPv4: netip.MustParseAddr("127.0.0.2"),
	}); err == nil {
		l.Close()
		t.Error("an address the concretely bound socket does not answer at was accepted")
	}
}

// RFC 9000 §9.1, §9.6.2: the datagram a server sends from its preferred
// socket before that path is validated is a probing packet — PATH_RESPONSE,
// PATH_CHALLENGE, PADDING — and an ACK frame would make it non-probing. An
// acknowledgement owed to the client waits for the next packet on the path
// in use.
func TestPathResponseOffThePathCarriesNoAck(t *testing.T) {
	c, main := newMigratableServer(t, "noack")
	pref := &recordingPC{addr: fakeFuzzAddr("noack:preferred")}
	tok := make([]byte, 16)
	now := time.Now()
	if err := c.frames(spaceApplication, ncidFrame(1, 0, []byte("cli-cid1"), tok), now); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	c.spaces[spaceApplication].ack.record(7, now) // an acknowledgement is owed
	c.rxFrom, c.rxVia, c.rxDatagramLen, c.rxHighest = fakeFuzzAddr("noack:peerA"), pref, 1200, true
	c.mu.Unlock()

	challenge := append(varint.Append(nil, framePathChallenge), 1, 2, 3, 4, 5, 6, 7, 8)
	if err := c.frames(spaceApplication, challenge, now); err != nil {
		t.Fatal(err)
	}
	writes := pref.recorded()
	if len(writes) != 1 {
		t.Fatalf("%d datagrams left by the preferred socket, want 1", len(writes))
	}
	for _, f := range openShort(t, writes[0].data, len("cli-cid1")) {
		switch f.Type {
		case framePathResponse, framePathChallenge, framePadding:
		default:
			t.Errorf("the probing datagram from the preferred socket carries frame type %#x, want only PATH_RESPONSE, PATH_CHALLENGE and PADDING", f.Type)
		}
	}
	// The acknowledgement is not lost with the probe: it is still pending
	// for the next packet on the path in use — or the delayed-ACK timer,
	// which runs on its own goroutine, has already sent it by the main
	// socket. Either is right; the probe carrying it was not.
	c.mu.Lock()
	dcidLen := len(c.dcid)
	stillPending := c.spaces[spaceApplication].ack.pending()
	c.mu.Unlock()
	sentOnPath := false
	for _, w := range main.recorded() {
		for _, f := range openShort(t, w.data, dcidLen) {
			if f.Type == frameACK || f.Type == frameACK|1 {
				sentOnPath = true
			}
		}
	}
	if !stillPending && !sentOnPath {
		t.Error("the acknowledgement owed is neither pending nor sent on the main socket: lost with the probe")
	}
}

// §18.2: "A server that chooses a zero-length connection ID MUST NOT
// provide a preferred address", and "A client MUST treat a violation of
// these requirements as a connection error of type
// TRANSPORT_PARAMETER_ERROR". Only a non-conformant server gets here (this
// package's Listener always takes 8 bytes); the client refuses the offer
// before pooling its identifier or remembering the address.
func TestClientRefusesAPreferredAddressUnderAnEmptyServerCID(t *testing.T) {
	pc := &recordingPC{addr: fakeFuzzAddr("pref-empty:self")}
	peer := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1).To4(), Port: 443}
	c := newConn(pc, peer, []byte("srv-cid0"), []byte("cli-cid0"), true, DefaultParameters())
	t.Cleanup(func() { _ = c.Close() })
	c.initialDCID = []byte("srv-cid0")
	if !c.pinPeerSCID(nil) {
		t.Fatal("an empty server SCID was not pinned; the test is checking the wrong thing")
	}
	pa := &preferredAddress{ipv4: &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2).To4(), Port: 4433}, cid: []byte("pref-cid"), statelessResetToken: [16]byte{7}}
	err := c.handlePeerParameters(preferredParams(pa, nil, []byte("srv-cid0")))
	te := &transportError{}
	if !errors.As(err, &te) || te.code != transportParameterError {
		t.Fatalf("handlePeerParameters = %v, want TRANSPORT_PARAMETER_ERROR for a preferred_address under a zero-length server identifier", err)
	}
	if got := c.preferredAddress(); got != nil {
		t.Errorf("PreferredAddress() = %v after the refused offer, want nil", got)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, e := range c.peerCIDs {
		if e.seq == 1 {
			t.Errorf("the refused offer's identifier %q was pooled as sequence 1", e.cid)
		}
	}
}

// RFC 9000 §9.5: "an endpoint MUST NOT reuse a connection ID when sending
// to more than one destination address". The probe of the server's preferred address leaves under the
// identifier the server offered with it (sequence 1): from that packet on,
// the identifier is the preferred path's. A Rebind while the probe is in
// flight takes a fresh identifier for the address in use, and with only
// sequence 1 beside the one in use there is none — it is refused, rather
// than sending from the new socket to the original address under the
// identifier an observer just saw toward the preferred one.
func TestRebindDuringThePreferredProbeRefusesTheOfferedIdentifier(t *testing.T) {
	first := &recordingPC{addr: fakeFuzzAddr("pref13:first")}
	c := newConn(first, fakeFuzzAddr("pref13:peer"), []byte("peercid0"), []byte("selfcid0"), true, DefaultParameters())
	defer c.Close()
	installAppSealer(t, c)
	c.mu.Lock()
	c.handshakeConfirmed = true
	c.peerCIDs = append(c.peerCIDs, peerCID{seq: 1, cid: []byte("peercid1")})
	c.preferred = fakeFuzzAddr("pref13:preferred")
	c.probePreferredAddressLocked(time.Now())
	probing := c.pathProbe.target != nil
	c.mu.Unlock()
	if !probing {
		t.Fatal("no probe of the preferred address in flight; the case under test is not reached")
	}
	if err := c.Rebind(&recordingPC{addr: fakeFuzzAddr("pref13:second")}); !errors.Is(err, ErrNoFreshConnectionID) {
		t.Fatalf("Rebind during the probe returned %v, want ErrNoFreshConnectionID: sequence 1 already left toward the preferred address", err)
	}
}

// RFC 9000 §9.6.3: "a client that migrates before it has moved to the
// preferred address SHOULD perform path validation to both the original
// and preferred server address from the client's new address
// concurrently", and "if path validation of the server's preferred address
// succeeds, the client MUST abandon validation of the original address and
// migrate to using the server's preferred address". A Rebind while the
// preferred address is probed challenges both from the new socket, each
// under an identifier never used (§9.5: sequence 1 already left from the
// old socket), and the preferred address's answer moves the client there.
// A Rebind used to drop the preferred probe and challenge the original
// address alone.
func TestRebindDuringThePreferredProbeValidatesBothAddresses(t *testing.T) {
	first := &recordingPC{addr: fakeFuzzAddr("pref14:first")}
	original, preferred := fakeFuzzAddr("pref14:peer"), fakeFuzzAddr("pref14:preferred")
	c := newConn(first, original, []byte("peercid0"), []byte("selfcid0"), true, DefaultParameters())
	defer c.Close()
	installAppSealer(t, c)
	c.mu.Lock()
	c.handshakeConfirmed = true
	for i := 1; i <= 3; i++ {
		c.peerCIDs = append(c.peerCIDs, peerCID{seq: uint64(i), cid: []byte("peercid" + string(rune('0'+i)))})
	}
	c.preferred = preferred
	c.probePreferredAddressLocked(time.Now())
	c.mu.Unlock()

	second := &recordingPC{addr: fakeFuzzAddr("pref14:second")}
	if err := c.Rebind(second); err != nil {
		t.Fatal(err)
	}
	// What left the new socket, by destination: the identifier and the
	// challenge each datagram carries.
	sec, err := initialSecrets([]byte("appkeys0"))
	if err != nil {
		t.Fatal(err)
	}
	opener, err := newPacketOpener(sec.Client)
	if err != nil {
		t.Fatal(err)
	}
	cids, challenges := map[string]string{}, map[string][]byte{}
	for _, w := range second.recorded() {
		h, err := parseShortHeader(append([]byte(nil), w.data...), 8)
		if err != nil {
			t.Fatal(err)
		}
		payload, _, err := opener.Open(h.Raw, h.PNOffset, 0)
		if err != nil {
			t.Fatal(err)
		}
		frames, err := parseFrames(nil, payload)
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range frames {
			if f.Type == framePathChallenge {
				cids[w.to.String()], challenges[w.to.String()] = string(h.DCID), f.Data
			}
		}
	}
	toOriginal, toPreferred := cids[original.String()], cids[preferred.String()]
	if toOriginal == "" || toPreferred == "" {
		t.Fatalf("challenges from the new socket went to %v; want both %v and %v", cids, original, preferred)
	}
	if toOriginal == toPreferred || toPreferred == "peercid1" || toOriginal == "peercid0" {
		t.Fatalf("challenges under %q (original) and %q (preferred); want two identifiers never used before", toOriginal, toPreferred)
	}

	// The preferred address answers.
	response := append(varint.Append(nil, framePathResponse), challenges[preferred.String()]...)
	if err := c.frames(spaceApplication, response, time.Now()); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !sameAddr(c.peer, preferred) || string(c.dcid) != toPreferred || c.pathProbe.pending {
		t.Fatalf("after the preferred address answered: peer %v under %q, original still probed %v; want %v under %q, the original's validation abandoned",
			c.peer, c.dcid, c.pathProbe.pending, preferred, toPreferred)
	}
}

// RFC 9000 §21.5.6: "Endpoints SHOULD NOT allow connections or migration to
// a loopback address if the same service was provided by a service at a
// non-loopback address." A server's preferred_address is probed by the
// client with a 1200-byte PATH_CHALLENGE, so an offer of loopback reached
// from elsewhere, of the unspecified address, of a multicast group or of
// port 0 — nothing a server can be reached at — is not taken: the client
// stays where it is. The offered identifier stays in the pool (§5.1.1).
func TestAnUnusablePreferredAddressIsNotTaken(t *testing.T) {
	for _, tc := range []struct {
		name  string
		offer *net.UDPAddr
		taken bool
	}{
		{"loopback from a non-loopback server", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 4433}, false},
		{"the unspecified address", &net.UDPAddr{IP: net.IPv4zero, Port: 4433}, false},
		{"a multicast group", &net.UDPAddr{IP: net.IPv4(224, 0, 0, 1), Port: 4433}, false},
		{"port 0", &net.UDPAddr{IP: net.IPv4(198, 51, 100, 7), Port: 0}, false},
		{"an ordinary address", &net.UDPAddr{IP: net.IPv4(198, 51, 100, 7), Port: 4433}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dcid, scid := []byte{1, 2, 3, 4}, []byte{5, 6, 7, 8}
			peer := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 443}
			c := assembleConn(&fuzzSink{addr: "pa:self"}, peer, dcid, scid, true, DefaultParameters())
			t.Cleanup(func() { _ = c.Close() })
			c.initialDCID = dcid
			c.peerSCID = dcid // the server's own identifier, not zero-length
			srv := DefaultParameters()
			srv.initialSourceCID = dcid
			srv.originalDCID = dcid
			srv.preferredAddress = &preferredAddress{ipv4: tc.offer, cid: []byte{9, 9, 9, 9}}
			if err := c.handlePeerParameters(appendParameters(nil, srv)); err != nil {
				t.Fatalf("server parameters: %v", err)
			}
			c.mu.Lock()
			taken := c.preferred != nil
			pooled := slices.ContainsFunc(c.peerCIDs, func(e peerCID) bool { return e.seq == 1 })
			c.mu.Unlock()
			if taken != tc.taken {
				t.Errorf("the offer of %v was taken = %v, want %v", tc.offer, taken, tc.taken)
			}
			if !pooled {
				t.Error("the identifier offered with the address is not pooled as sequence 1")
			}
		})
	}
}

// RFC 9000 §9.3.2: a migrated path that does not validate falls back to the
// last validated one. A server that had just followed its client to B, and
// then received the client's challenge on its preferred socket from B,
// replaced the migration's probe with one from that socket (§9.6.2); when
// that one went unanswered, its abandonment cleared the probe and nothing
// else — the server stayed on B, held to three times what B sent, with no
// probe left to validate it. The abandonment now reverts as the
// migration's own would have.
func TestAbandonedPreferredProbeDoesNotStrandAnUnvalidatedMigration(t *testing.T) {
	c, _ := newMigratableServer(t, "strand22")
	pref := &recordingPC{addr: fakeFuzzAddr("strand22:preferred")}
	tok := make([]byte, 16)
	for seq, cid := range []string{"cli-cid1", "cli-cid2"} {
		if err := c.frames(spaceApplication, ncidFrame(uint64(seq+1), 0, []byte(cid), tok), time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	a, b := fakeFuzzAddr("strand22:peerA"), fakeFuzzAddr("strand22:peerB")

	now := time.Now()
	rxFromNewAddress(c, b, 1200, true)
	c.maybeMigrate([]Frame{{Type: framePing}}, now)
	c.mu.Lock()
	moved, unvalidated := sameAddr(c.peer, b), c.amplActive
	c.rxFrom, c.rxVia, c.rxDatagramLen, c.rxHighest = b, pref, 1200, true
	c.mu.Unlock()
	if !moved || !unvalidated {
		t.Fatalf("after B's packet: on B %v, unvalidated %v; want both", moved, unvalidated)
	}
	challenge := append(varint.Append(nil, framePathChallenge), 1, 2, 3, 4, 5, 6, 7, 8)
	if err := c.frames(spaceApplication, challenge, now); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	via := c.pathProbe.via
	c.mu.Unlock()
	if via != net.PacketConn(pref) {
		t.Fatal("the challenge on the preferred socket did not start the server's probe from it")
	}

	for range pathProbeMaxRetransmits + 2 {
		c.mu.Lock()
		now = now.Add(time.Hour)
		c.pathProbeDeadlineLocked(now)
		c.mu.Unlock()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.amplActive && !sameAddr(c.peer, a) {
		t.Errorf("after the preferred probe was given up: on %v, still under the amplification limit, no probe pending %v; want the validated path %v", c.peer, !c.pathProbe.pending, a)
	}
}
