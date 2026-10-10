package quic

import (
	"bytes"
	"errors"
	"net"
	"testing"
	"time"
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
		b := AppendVarint(nil, paramPreferredAddress)
		b = AppendVarint(b, uint64(len(val)))
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
	short := make([]byte, preferredAddressLen+3)
	short[24] = 8 // claims 8, carries 3
	if _, err := parseParameters(raw(short)); err == nil {
		t.Error("an identifier longer than what follows it parsed")
	}
	if _, err := parseParameters(raw(make([]byte, 10))); err == nil {
		t.Error("a 10-byte preferred_address parsed")
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
	c.adoptPeerCID([]byte("srv-cid0"))
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

	c.mu.Lock()
	c.confirmLocked()
	before := c.peer
	c.mu.Unlock()
	if !sameAddr(before, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1).To4(), Port: 443}) {
		t.Fatalf("the path in use moved to %v before any validation", before)
	}
	var probe *recordedWrite
	for i := range pc.recorded() {
		w := pc.recorded()[i]
		if sameAddr(w.to, pref) {
			probe = &w
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
	resp := AppendVarint(nil, framePathResponse)
	resp = append(resp, c.pathProbe.data[:]...)
	c.rxFrom = pref
	c.mu.Unlock()
	if _, err := c.frames(spaceApplication, resp, time.Now()); err != nil {
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
	c.adoptPeerCID([]byte("srv-cid0"))
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
	if pending || len(pc.recorded()) != 0 {
		t.Fatalf("a probe was sent (pending=%v, %d datagrams) for an address of another family", pending, len(pc.recorded()))
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
	client, err := Dial(cpc, main.LocalAddr(), ClientTLSForTest(), DefaultParameters())
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
	server.OnStreamFrames(func(fs []Frame) error {
		for _, f := range fs {
			if f.IsStream() {
				got <- append([]byte(nil), f.Data...)
			}
		}
		return nil
	})
	s, err := client.OpenStream()
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
	client.OnStreamFrames(func(fs []Frame) error {
		for _, f := range fs {
			if f.IsStream() {
				echo <- append([]byte(nil), f.Data...)
			}
		}
		return nil
	})
	ss, err := server.OpenStream()
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
