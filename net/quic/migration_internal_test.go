package quic

import (
	"errors"
	"net"
	"os"
	"sync"
	"testing"
	"time"
)

// Tests for connection-identifier rotation (RFC 9000 §5.1) and server-side
// passive migration (§9): the synthetic ones drive a Conn's frame handlers
// directly, the way quic_scenarios_test.go does; the end-to-end ones run a
// real handshake over loopback UDP, the way the listener tests do.

// ncidFrame encodes one NEW_CONNECTION_ID frame as a peer would send it.
func ncidFrame(seq, retire uint64, cid, token []byte) []byte {
	b := AppendVarint(nil, frameNewConnectionID)
	b = AppendVarint(b, seq)
	b = AppendVarint(b, retire)
	b = append(b, byte(len(cid)))
	b = append(b, cid...)
	b = append(b, token...)
	return b
}

// controlFrames parses what a connection has queued on its control buffer.
func controlFrames(t *testing.T, c *Conn) []Frame {
	t.Helper()
	c.mu.Lock()
	raw := append([]byte(nil), c.control...)
	c.mu.Unlock()
	frames, err := parseFrames(nil, raw)
	if err != nil {
		t.Fatalf("the control queue does not parse: %v", err)
	}
	return frames
}

// testRegistrar is a cidRegistrar with nothing but the table — what a
// Listener provides, minus the Listener.
type testRegistrar struct {
	mu    sync.Mutex
	table map[string]*Conn
}

func newTestRegistrar() *testRegistrar {
	return &testRegistrar{table: make(map[string]*Conn)}
}

func (r *testRegistrar) resetTokenFor(cid []byte) [16]byte {
	var t [16]byte
	copy(t[:], cid) // deterministic and distinct per identifier; enough for tests
	t[15] = 0xeb
	return t
}

func (r *testRegistrar) addLocalCID(c *Conn) ([]byte, error) {
	cid, err := randomID(listenerCIDLen)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.table[string(cid)] = c
	r.mu.Unlock()
	return cid, nil
}

func (r *testRegistrar) removeLocalCID(cid []byte) {
	r.mu.Lock()
	delete(r.table, string(cid))
	r.mu.Unlock()
}

func (r *testRegistrar) has(cid []byte) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.table[string(cid)]
	return ok
}

// recordingPC captures every outgoing datagram and its destination — the
// observable a path-selection test needs — and swallows nothing else.
type recordingPC struct {
	mu     sync.Mutex
	writes []recordedWrite
	addr   fakeFuzzAddr
}

type recordedWrite struct {
	data []byte
	to   net.Addr
}

func (r *recordingPC) WriteTo(p []byte, addr net.Addr) (int, error) {
	r.mu.Lock()
	r.writes = append(r.writes, recordedWrite{data: append([]byte(nil), p...), to: addr})
	r.mu.Unlock()
	return len(p), nil
}

func (r *recordingPC) recorded() []recordedWrite {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]recordedWrite(nil), r.writes...)
}

func (r *recordingPC) ReadFrom(p []byte) (int, net.Addr, error) { return 0, nil, net.ErrClosed }
func (r *recordingPC) Close() error                             { return nil }
func (r *recordingPC) LocalAddr() net.Addr                      { return r.addr }
func (r *recordingPC) SetDeadline(t time.Time) error            { return nil }
func (r *recordingPC) SetReadDeadline(t time.Time) error        { return nil }
func (r *recordingPC) SetWriteDeadline(t time.Time) error       { return nil }

// installAppSealer gives a bare test Conn something to seal 1-RTT packets
// with, the same trick conformance_internal_test.go uses.
func installAppSealer(t *testing.T, c *Conn) {
	t.Helper()
	sec, err := initialSecrets([]byte("appkeys0"))
	if err != nil {
		t.Fatal(err)
	}
	sealer, err := newPacketSealer(sec.Client)
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	c.spaces[spaceApplication].sealer = sealer
	c.mu.Unlock()
}

// wantViolation asserts an error is a *transportError with the given code.
func wantViolation(t *testing.T, err error, code uint64, what string) {
	t.Helper()
	te := &transportError{}
	if !errors.As(err, &te) || te.code != code {
		t.Fatalf("%s returned %v, want transport error %#x", what, err, code)
	}
}

// --- A. Local identifier issuance -----------------------------------------

// A connection with a registrar tops its identifiers up to
// min(active_connection_id_limit, 4) once confirmed, numbering from 1 with
// Retire Prior To 0, and queues each as a retransmittable NEW_CONNECTION_ID.
func TestIssuanceTopsUpToTheLimit(t *testing.T) {
	reg := newTestRegistrar()
	c := newConn(&fuzzSink{addr: "issue:self"}, fakeFuzzAddr("issue:peer"),
		[]byte("peercid0"), []byte("selfcid0"), false, DefaultParameters())
	defer c.Close()

	c.mu.Lock()
	c.registrar = reg
	c.handshakeConfirmed = true
	c.hasPeerParams = true
	c.peerParams.ActiveConnIDLimit = 8 // the peer invites more than this end keeps
	c.issueLocalCIDsLocked()
	locals := len(c.localCIDs)
	nextSeq := c.nextLocalSeq
	c.mu.Unlock()

	if locals != maxLocalActiveCIDs {
		t.Fatalf("%d active local identifiers, want %d", locals, maxLocalActiveCIDs)
	}
	if nextSeq != maxLocalActiveCIDs {
		t.Errorf("nextLocalSeq = %d, want %d (sequences 1..%d issued)", nextSeq, maxLocalActiveCIDs, maxLocalActiveCIDs-1)
	}
	var issued int
	for _, f := range controlFrames(t, c) {
		if f.Type != frameNewConnectionID {
			continue
		}
		issued++
		if f.offset != 0 {
			t.Errorf("sequence %d was issued with Retire Prior To %d, want 0", f.value, f.offset)
		}
		if cid := f.Data[:len(f.Data)-16]; !reg.has(cid) {
			t.Errorf("sequence %d's identifier is not registered for routing", f.value)
		}
	}
	if issued != maxLocalActiveCIDs-1 {
		t.Errorf("%d NEW_CONNECTION_ID frames queued, want %d", issued, maxLocalActiveCIDs-1)
	}
}

// §19.16's two violations, and the legal retirement between them: a retired
// identifier leaves the routing table and a replacement is issued so the
// active count holds — sequence 0 included, once alternatives exist.
func TestRetireConnectionIDHandling(t *testing.T) {
	reg := newTestRegistrar()
	c := newConn(&fuzzSink{addr: "retire:self"}, fakeFuzzAddr("retire:peer"),
		[]byte("peercid0"), []byte("selfcid0"), false, DefaultParameters())
	defer c.Close()
	c.mu.Lock()
	c.registrar = reg
	c.handshakeConfirmed = true
	c.hasPeerParams = true
	c.peerParams.ActiveConnIDLimit = 4
	c.issueLocalCIDsLocked()
	var seq1CID []byte
	for _, e := range c.localCIDs {
		if e.seq == 1 {
			seq1CID = e.cid
		}
	}
	c.mu.Unlock()

	// A sequence this end never issued.
	_, err := c.frames(spaceApplication, appendFrame(nil, frameRetireConnectionID, 9), time.Now())
	wantViolation(t, err, transportProtocolViolation, "retiring a never-issued sequence")

	// The identifier the frame's own packet arrived under.
	c.mu.Lock()
	c.rxLocalCIDSeq = 2
	c.mu.Unlock()
	_, err = c.frames(spaceApplication, appendFrame(nil, frameRetireConnectionID, 2), time.Now())
	wantViolation(t, err, transportProtocolViolation, "retiring the arrival identifier")

	// Legal: retire sequence 1 (the packet arrived under 2) — replaced,
	// count kept, routing updated.
	if _, err := c.frames(spaceApplication, appendFrame(nil, frameRetireConnectionID, 1), time.Now()); err != nil {
		t.Fatalf("a legal retirement failed: %v", err)
	}
	c.mu.Lock()
	locals := len(c.localCIDs)
	var has1, has4 bool
	for _, e := range c.localCIDs {
		if e.seq == 1 {
			has1 = true
		}
		if e.seq == 4 {
			has4 = true
		}
	}
	c.mu.Unlock()
	if locals != 4 || has1 || !has4 {
		t.Errorf("after retiring 1: %d active, seq1=%v seq4=%v — want 4 active, 1 gone, 4 issued", locals, has1, has4)
	}
	if reg.has(seq1CID) {
		t.Error("the retired identifier still routes")
	}

	// Legal too: retiring sequence 0 once alternatives exist.
	if _, err := c.frames(spaceApplication, appendFrame(nil, frameRetireConnectionID, 0), time.Now()); err != nil {
		t.Fatalf("retiring sequence 0 with alternatives available failed: %v", err)
	}
	c.mu.Lock()
	for _, e := range c.localCIDs {
		if e.seq == 0 {
			t.Error("sequence 0 is still active after being retired")
		}
	}
	c.mu.Unlock()

	// A duplicate retirement is a retransmission, not a violation.
	if _, err := c.frames(spaceApplication, appendFrame(nil, frameRetireConnectionID, 1), time.Now()); err != nil {
		t.Fatalf("a duplicate retirement failed: %v", err)
	}
}

// A connection without a registrar never issued anything, so the only
// retirable sequence is 0 — the identifier in use, which stays forbidden.
func TestRetireOnAConnectionThatIssuedNothing(t *testing.T) {
	c := newConn(&fuzzSink{addr: "bare:self"}, fakeFuzzAddr("bare:peer"),
		[]byte("peercid0"), []byte("selfcid0"), true, DefaultParameters())
	defer c.Close()
	_, err := c.frames(spaceApplication, appendFrame(nil, frameRetireConnectionID, 0), time.Now())
	wantViolation(t, err, transportProtocolViolation, "retiring sequence 0 on a single-identifier connection")
	_, err = c.frames(spaceApplication, appendFrame(nil, frameRetireConnectionID, 3), time.Now())
	wantViolation(t, err, transportProtocolViolation, "retiring a sequence never issued")
}

// --- B. Peer identifier consumption ----------------------------------------

// Retire Prior To retires every pooled sequence below it — RETIRE frames
// queued for each — and when the identifier in use is among them, sending
// switches to a fresh one at once.
func TestRetirePriorToRetiresAndSwitches(t *testing.T) {
	c := newConn(&fuzzSink{addr: "pool:self"}, fakeFuzzAddr("pool:peer"),
		[]byte("initial0"), []byte("selfcid0"), true, DefaultParameters())
	defer c.Close()
	c.adoptPeerCID([]byte("srv-cid0"))

	tok := make([]byte, 16)
	if _, err := c.frames(spaceApplication, ncidFrame(1, 0, []byte("srv-cid1"), tok), time.Now()); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	pool := len(c.peerCIDs)
	c.mu.Unlock()
	if pool != 2 {
		t.Fatalf("pool holds %d identifiers after one NEW_CONNECTION_ID, want 2", pool)
	}

	// Sequence 2, Retire Prior To 2: sequences 0 and 1 go, and 0 is in use.
	if _, err := c.frames(spaceApplication, ncidFrame(2, 2, []byte("srv-cid2"), tok), time.Now()); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	dcid := append([]byte(nil), c.dcid...)
	cur := c.currentPeerSeq
	pool = len(c.peerCIDs)
	c.mu.Unlock()
	if string(dcid) != "srv-cid2" || cur != 2 {
		t.Errorf("after Retire Prior To 2, sending under %q (seq %d), want srv-cid2 (seq 2)", dcid, cur)
	}
	if pool != 1 {
		t.Errorf("pool holds %d identifiers, want 1", pool)
	}
	var retired []uint64
	for _, f := range controlFrames(t, c) {
		if f.Type == frameRetireConnectionID {
			retired = append(retired, f.value)
		}
	}
	if len(retired) != 2 {
		t.Fatalf("%d RETIRE_CONNECTION_ID frames queued (%v), want 2", len(retired), retired)
	}

	// An identifier arriving already below the threshold is retired at
	// once, never pooled.
	if _, err := c.frames(spaceApplication, ncidFrame(1, 0, []byte("srv-cid1"), tok), time.Now()); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	pool = len(c.peerCIDs)
	c.mu.Unlock()
	if pool != 1 {
		t.Errorf("a below-threshold identifier was pooled (%d entries)", pool)
	}
}

func TestNewConnectionIDViolations(t *testing.T) {
	tok := make([]byte, 16)
	newClient := func(name string) *Conn {
		c := newConn(&fuzzSink{addr: fakeFuzzAddr(name + ":self")}, fakeFuzzAddr(name+":peer"),
			[]byte("initial0"), []byte("selfcid0"), true, DefaultParameters())
		t.Cleanup(func() { _ = c.Close() })
		c.adoptPeerCID([]byte("srv-cid0"))
		return c
	}

	t.Run("same sequence, different content", func(t *testing.T) {
		c := newClient("dup")
		if _, err := c.frames(spaceApplication, ncidFrame(1, 0, []byte("srv-cid1"), tok), time.Now()); err != nil {
			t.Fatal(err)
		}
		// The identical repeat is a retransmission, tolerated.
		if _, err := c.frames(spaceApplication, ncidFrame(1, 0, []byte("srv-cid1"), tok), time.Now()); err != nil {
			t.Fatalf("an identical repeat was refused: %v", err)
		}
		_, err := c.frames(spaceApplication, ncidFrame(1, 0, []byte("srv-cidX"), tok), time.Now())
		wantViolation(t, err, transportProtocolViolation, "sequence 1 reissued with different content")
	})

	t.Run("more active identifiers than advertised", func(t *testing.T) {
		c := newClient("over")
		for seq := uint64(1); seq <= 3; seq++ {
			cid := []byte("srv-cid0")
			cid[7] = byte('0' + seq)
			if _, err := c.frames(spaceApplication, ncidFrame(seq, 0, cid, tok), time.Now()); err != nil {
				t.Fatalf("sequence %d, within the limit of 4: %v", seq, err)
			}
		}
		_, err := c.frames(spaceApplication, ncidFrame(4, 0, []byte("srv-cid4"), tok), time.Now())
		wantViolation(t, err, transportConnectionIDLimit, "a fifth simultaneously-active identifier")
	})

	t.Run("zero-length peer identifier", func(t *testing.T) {
		// A server whose client chose a zero-length source identifier: the
		// peer has nothing to rotate, and §19.15 forbids it the frame.
		c := newConn(&fuzzSink{addr: "zero:self"}, fakeFuzzAddr("zero:peer"),
			nil, []byte("selfcid0"), false, DefaultParameters())
		t.Cleanup(func() { _ = c.Close() })
		_, err := c.frames(spaceApplication, ncidFrame(1, 0, []byte("srv-cid1"), tok), time.Now())
		wantViolation(t, err, transportProtocolViolation, "NEW_CONNECTION_ID under a zero-length peer identifier")
	})
}

// --- C. Server-side passive migration --------------------------------------

// newMigratableServer is a confirmed server-side Conn whose sends are
// recorded: the smallest thing the §9.3 decision can be observed on.
func newMigratableServer(t *testing.T, name string) (*Conn, *recordingPC) {
	t.Helper()
	pc := &recordingPC{addr: fakeFuzzAddr(name + ":self")}
	c := newConn(pc, fakeFuzzAddr(name+":peerA"), []byte("peercid0"), []byte("selfcid0"), false, DefaultParameters())
	t.Cleanup(func() { _ = c.Close() })
	installAppSealer(t, c)
	c.mu.Lock()
	c.handshakeConfirmed = true
	c.mu.Unlock()
	return c, pc
}

// rxFromNewAddress stamps the per-datagram state receiveAt would have set
// for an authenticated 1-RTT packet from addr.
func rxFromNewAddress(c *Conn, addr net.Addr, size int, highest bool) {
	c.mu.Lock()
	c.rxFrom = addr
	c.rxDatagramLen = size
	c.rxHighest = highest
	c.mu.Unlock()
}

// §9.1/§9.3: a packet of nothing but probing frames does not move the path,
// and its PATH_CHALLENGE is answered to the address it came from — not to
// the peer's current address.
func TestProbingOnlyPacketDoesNotMovePath(t *testing.T) {
	c, pc := newMigratableServer(t, "probe")
	newAddr := fakeFuzzAddr("probe:peerB")
	rxFromNewAddress(c, newAddr, 1200, true)

	challenge := AppendVarint(nil, framePathChallenge)
	challenge = append(challenge, 1, 2, 3, 4, 5, 6, 7, 8)
	if _, err := c.frames(spaceApplication, challenge, time.Now()); err != nil {
		t.Fatal(err)
	}

	c.mu.Lock()
	peer := c.peer
	migrations := c.migrations
	probing := c.pathProbe.pending
	c.mu.Unlock()
	if !sameAddr(peer, fakeFuzzAddr("probe:peerA")) || migrations != 0 {
		t.Errorf("a probing-only packet moved the path to %v (migrations %d)", peer, migrations)
	}
	if probing {
		t.Error("a probing-only packet started this end's own path validation")
	}
	writes := pc.recorded()
	if len(writes) != 1 {
		t.Fatalf("%d datagrams left, want 1 (the PATH_RESPONSE)", len(writes))
	}
	if !sameAddr(writes[0].to, newAddr) {
		t.Errorf("the PATH_RESPONSE went to %v, want the challenge's source %v (§9.3)", writes[0].to, newAddr)
	}
	if len(writes[0].data) < 1200 {
		t.Errorf("the PATH_RESPONSE left in a %d-byte datagram, want 1200 (§8.2.2)", len(writes[0].data))
	}

	// §8.2.2's exception: a challenge that arrived in a small datagram from
	// an off-path address earns no expansion — the response must not be a
	// 1200-byte reflection of a few bytes aimed at a spoofed source.
	rxFromNewAddress(c, fakeFuzzAddr("probe:peerC"), 60, true)
	if _, err := c.frames(spaceApplication, challenge, time.Now()); err != nil {
		t.Fatal(err)
	}
	writes = pc.recorded()
	if len(writes) != 2 {
		t.Fatalf("%d datagrams left, want 2", len(writes))
	}
	if len(writes[1].data) >= 1200 {
		t.Errorf("a 60-byte off-path challenge got a %d-byte answer — amplification (§8.2.2)", len(writes[1].data))
	}
}

// §9.3/§9.4/§9.5: a non-probing packet from a new address — if it is the
// highest-numbered received — moves the path: amplification re-armed and
// seeded with the datagram, congestion and round-trip state reset, a fresh
// pooled identifier adopted, a padded PATH_CHALLENGE probing the new path.
// The peer's echo then validates it; a mismatched echo validates nothing.
func TestNonProbingPacketFromNewAddressMigrates(t *testing.T) {
	c, pc := newMigratableServer(t, "mig")
	tok := make([]byte, 16)
	if _, err := c.frames(spaceApplication, ncidFrame(1, 0, []byte("cli-cid1"), tok), time.Now()); err != nil {
		t.Fatal(err)
	}

	// Pollute the path state so the reset is observable.
	c.mu.Lock()
	c.rtt.sample(50*time.Millisecond, 0)
	c.cc.cwnd = 999_999
	c.ptoCount = 2
	c.mu.Unlock()

	// A stale packet (not the highest-numbered) must not move the path.
	rxFromNewAddress(c, fakeFuzzAddr("mig:peerB"), 1200, false)
	if _, err := c.frames(spaceApplication, []byte{framePing}, time.Now()); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	moved := !sameAddr(c.peer, fakeFuzzAddr("mig:peerA"))
	c.mu.Unlock()
	if moved {
		t.Fatal("a non-highest-numbered packet moved the path")
	}

	// The highest-numbered one does.
	newAddr := fakeFuzzAddr("mig:peerB")
	rxFromNewAddress(c, newAddr, 1200, true)
	if _, err := c.frames(spaceApplication, []byte{framePing}, time.Now()); err != nil {
		t.Fatal(err)
	}

	c.mu.Lock()
	peer := c.peer
	migrations := c.migrations
	ampl, amplRecv := c.amplActive, c.amplRecv
	rttReset := !c.rtt.sampled
	cwnd := c.cc.cwnd
	ptoCount := c.ptoCount
	probing := c.pathProbe.pending
	dcid := append([]byte(nil), c.dcid...)
	challengeData := c.pathProbe.data
	c.mu.Unlock()

	if !sameAddr(peer, newAddr) || migrations != 1 {
		t.Fatalf("the path did not move: peer %v, migrations %d", peer, migrations)
	}
	if !ampl || amplRecv != 1200 {
		t.Errorf("amplification not re-armed on the new path: active=%v recv=%d", ampl, amplRecv)
	}
	if !rttReset || ptoCount != 0 {
		t.Errorf("round-trip state survived the migration: sampled=%v ptoCount=%d (§9.4)", !rttReset, ptoCount)
	}
	if want := newNewReno(1200).cwnd; cwnd != want {
		t.Errorf("congestion window is %d after migration, want the initial %d (§9.4)", cwnd, want)
	}
	if string(dcid) != "cli-cid1" {
		t.Errorf("sending under %q after migration, want the unused pooled cli-cid1 (§9.5)", dcid)
	}
	if !probing {
		t.Fatal("no path validation in flight after migration")
	}
	writes := pc.recorded()
	if len(writes) == 0 {
		t.Fatal("no PATH_CHALLENGE left for the new path")
	}
	last := writes[len(writes)-1]
	if !sameAddr(last.to, newAddr) || len(last.data) < 1200 {
		t.Errorf("the challenge went to %v in %d bytes, want %v in ≥1200 (§8.2.1)", last.to, len(last.data), newAddr)
	}

	// A response echoing the wrong data validates nothing (an old probe's
	// answer, perhaps), and is not fatal.
	wrong := AppendVarint(nil, framePathResponse)
	wrong = append(wrong, 9, 9, 9, 9, 9, 9, 9, 9)
	rxFromNewAddress(c, newAddr, 100, true)
	if _, err := c.frames(spaceApplication, wrong, time.Now()); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	stillArmed := c.amplActive
	c.mu.Unlock()
	if !stillArmed {
		t.Error("a mismatched PATH_RESPONSE validated the path")
	}

	// The right echo validates — from any address (§8.2.3).
	right := AppendVarint(nil, framePathResponse)
	right = append(right, challengeData[:]...)
	rxFromNewAddress(c, fakeFuzzAddr("mig:peerC"), 100, true)
	if _, err := c.frames(spaceApplication, right, time.Now()); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	validated := !c.amplActive && !c.pathProbe.pending
	c.mu.Unlock()
	if !validated {
		t.Error("the echoed challenge did not validate the path")
	}
}

// §9: an address change before the handshake confirms is a spoof signal —
// the datagram is counted and dropped, the path untouched.
func TestPreConfirmationAddressChangeIsIgnored(t *testing.T) {
	pc := &recordingPC{addr: fakeFuzzAddr("pre:self")}
	c := newConn(pc, fakeFuzzAddr("pre:peerA"), []byte("peercid0"), []byte("selfcid0"), false, DefaultParameters())
	defer c.Close()
	installAppSealer(t, c)

	if err := c.receive([]byte{0x40, 1, 2, 3, 4, 5, 6, 7, 8, 9}, fakeFuzzAddr("pre:peerB")); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	peer := c.peer
	discarded := c.discarded
	c.mu.Unlock()
	if !sameAddr(peer, fakeFuzzAddr("pre:peerA")) {
		t.Errorf("a pre-confirmation address change moved the path to %v", peer)
	}
	if discarded == 0 {
		t.Error("the refused datagram was not counted")
	}
	// A client never follows anyone, confirmed or not.
	cl := newConn(&fuzzSink{addr: "pre:cself"}, fakeFuzzAddr("pre:cpeerA"),
		[]byte("peercid0"), []byte("selfcid0"), true, DefaultParameters())
	defer cl.Close()
	cl.mu.Lock()
	cl.handshakeConfirmed = true
	cl.mu.Unlock()
	if err := cl.receive([]byte{0x40, 1, 2, 3, 4, 5, 6, 7, 8, 9}, fakeFuzzAddr("pre:cpeerB")); err != nil {
		t.Fatal(err)
	}
	cl.mu.Lock()
	clPeer := cl.peer
	cl.mu.Unlock()
	if !sameAddr(clPeer, fakeFuzzAddr("pre:cpeerA")) {
		t.Errorf("a client followed an address change to %v", clPeer)
	}
}

// --- End to end over loopback ----------------------------------------------

// A listener-managed connection issues identifiers after confirmation; the
// client pools them, and a packet sent under one of the new identifiers —
// same address — still routes to the same connection.
func TestListenerIssuedIdentifiersRoute(t *testing.T) {
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	l, err := NewListener(pc, ServerTLSForTest(t), DefaultParameters(), ListenerConfig{})
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

	clientPC, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	client, err := Dial(clientPC, pc.LocalAddr(), ClientTLSForTest(), DefaultParameters())
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

	// The client should see the issued identifiers arrive.
	deadline := time.Now().Add(5 * time.Second)
	var alt []byte
	for time.Now().Before(deadline) {
		client.mu.Lock()
		for _, e := range client.peerCIDs {
			if e.seq != 0 {
				alt = append([]byte(nil), e.cid...)
			}
		}
		client.mu.Unlock()
		if alt != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if alt == nil {
		t.Fatal("no NEW_CONNECTION_ID reached the client after confirmation")
	}

	// Every identifier the server holds active routes to it in the demux.
	server.mu.Lock()
	locals := make([][]byte, 0, len(server.localCIDs))
	for _, e := range server.localCIDs {
		locals = append(locals, append([]byte(nil), e.cid...))
	}
	server.mu.Unlock()
	if len(locals) < 2 {
		t.Fatalf("the server holds %d active identifiers, want at least 2", len(locals))
	}
	l.mu.Lock()
	for _, cid := range locals {
		if l.byDCID[string(cid)] != server {
			t.Errorf("identifier %x is not registered to its connection", cid)
		}
	}
	l.mu.Unlock()

	// Switch the client's sends to the issued identifier and prove the
	// packets still reach the same connection: data written under it must
	// arrive.
	client.mu.Lock()
	client.dcid = alt
	client.mu.Unlock()

	got := make(chan []byte, 16)
	server.OnStreamFrames(func(fs []Frame) error {
		for _, f := range fs {
			if f.IsStream() && len(f.Data) > 0 {
				got <- append([]byte(nil), f.Data...)
			}
		}
		return nil
	})
	s, err := client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write([]byte("under the new identifier")); err != nil {
		t.Fatal(err)
	}
	select {
	case data := <-got:
		if string(data) != "under the new identifier" {
			t.Errorf("the server read %q", data)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("data sent under the issued identifier never arrived — it did not route")
	}
}

// rebindablePC is a client-side packet connection that can change which
// underlying UDP socket its writes leave from — a NAT rebind, from the
// server's point of view — while reading from all of them.
type rebindablePC struct {
	mu       sync.Mutex
	socks    []*net.UDPConn
	active   int
	deadline time.Time
	bump     chan struct{}
	incoming chan rebindDatagram
	closed   chan struct{}
	once     sync.Once
}

type rebindDatagram struct {
	data []byte
	from net.Addr
}

func newRebindablePC(t *testing.T, sockets int) *rebindablePC {
	t.Helper()
	r := &rebindablePC{
		bump:     make(chan struct{}),
		incoming: make(chan rebindDatagram, 64),
		closed:   make(chan struct{}),
	}
	for range sockets {
		s, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
		if err != nil {
			t.Fatal(err)
		}
		r.socks = append(r.socks, s)
		go r.pump(s)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

func (r *rebindablePC) pump(s *net.UDPConn) {
	buf := make([]byte, 2048)
	for {
		n, from, err := s.ReadFrom(buf)
		if err != nil {
			return
		}
		select {
		case r.incoming <- rebindDatagram{data: append([]byte(nil), buf[:n]...), from: from}:
		case <-r.closed:
			return
		}
	}
}

func (r *rebindablePC) rebind(i int) {
	r.mu.Lock()
	r.active = i
	r.mu.Unlock()
}

func (r *rebindablePC) ReadFrom(p []byte) (int, net.Addr, error) {
	for {
		r.mu.Lock()
		dl, bump := r.deadline, r.bump
		r.mu.Unlock()
		var timeout <-chan time.Time
		var timer *time.Timer
		if !dl.IsZero() {
			timer = time.NewTimer(time.Until(dl))
			timeout = timer.C
		}
		select {
		case d := <-r.incoming:
			if timer != nil {
				timer.Stop()
			}
			return copy(p, d.data), d.from, nil
		case <-timeout:
			return 0, nil, os.ErrDeadlineExceeded
		case <-bump:
			if timer != nil {
				timer.Stop()
			}
		case <-r.closed:
			if timer != nil {
				timer.Stop()
			}
			return 0, nil, net.ErrClosed
		}
	}
}

func (r *rebindablePC) WriteTo(p []byte, addr net.Addr) (int, error) {
	r.mu.Lock()
	s := r.socks[r.active]
	r.mu.Unlock()
	return s.WriteTo(p, addr)
}

func (r *rebindablePC) Close() error {
	r.once.Do(func() {
		close(r.closed)
		for _, s := range r.socks {
			_ = s.Close()
		}
	})
	return nil
}

func (r *rebindablePC) LocalAddr() net.Addr {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.socks[r.active].LocalAddr()
}

func (r *rebindablePC) SetDeadline(t time.Time) error      { return r.SetReadDeadline(t) }
func (r *rebindablePC) SetWriteDeadline(t time.Time) error { return nil }

func (r *rebindablePC) SetReadDeadline(t time.Time) error {
	r.mu.Lock()
	r.deadline = t
	close(r.bump) // wake a blocked ReadFrom so it re-reads the deadline
	r.bump = make(chan struct{})
	r.mu.Unlock()
	return nil
}

// The NAT-rebind scenario end to end: a real client keeps its connection
// state but its datagrams start leaving from a second socket. The server
// follows — new peer address, a migration counted, a PATH_CHALLENGE the
// client's machinery answers on its own — and once the path validates, the
// amplification limit disarms and data keeps flowing.
func TestServerFollowsClientRebind(t *testing.T) {
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	l, err := NewListener(pc, ServerTLSForTest(t), DefaultParameters(), ListenerConfig{})
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

	rp := newRebindablePC(t, 2)
	client, err := Dial(rp, pc.LocalAddr(), ClientTLSForTest(), DefaultParameters())
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

	var recvMu sync.Mutex
	var received int
	server.OnStreamFrames(func(fs []Frame) error {
		recvMu.Lock()
		for _, f := range fs {
			if f.IsStream() {
				received += len(f.Data)
			}
		}
		recvMu.Unlock()
		return nil
	})
	waitReceived := func(n int, what string) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			recvMu.Lock()
			got := received
			recvMu.Unlock()
			if got >= n {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("%s: the server never received the %d bytes", what, n)
	}

	s, err := client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(make([]byte, 100)); err != nil {
		t.Fatal(err)
	}
	waitReceived(100, "before the rebind")

	oldAddr := rp.socks[0].LocalAddr()
	newAddr := rp.socks[1].LocalAddr()
	server.mu.Lock()
	peerBefore := server.peer
	server.mu.Unlock()
	if !sameAddr(peerBefore, oldAddr) {
		t.Fatalf("the server's peer is %v before the rebind, want %v", peerBefore, oldAddr)
	}

	// The rebind: same connection state, new source socket, and a small
	// write — the migrating datagram funds far less than a 1200-byte probe,
	// which is the NAT-rebind common case: the probe must fit the budget,
	// and no extra traffic may be needed to get the path validated.
	rp.rebind(1)
	if _, err := s.Write(make([]byte, 20)); err != nil {
		t.Fatal(err)
	}
	waitReceived(120, "across the rebind")
	waitFor(t, "the server to follow and validate the new path", func() bool {
		server.mu.Lock()
		defer server.mu.Unlock()
		return sameAddr(server.peer, newAddr) && server.migrations == 1 &&
			!server.amplActive && !server.pathProbe.pending
	})

	// Validated: data still flows on the new path.
	recvMu.Lock()
	mark := received
	recvMu.Unlock()
	if _, err := s.Write(make([]byte, 300)); err != nil {
		t.Fatal(err)
	}
	waitReceived(mark+300, "after validation")
	if got := server.Migrations(); got != 1 {
		t.Errorf("Migrations() = %d, want 1", got)
	}
}

// --- D. Client-initiated migration: Rebind ---------------------------------

// The deliberate move end to end: a real client swaps its socket with Rebind,
// closes the old one, and keeps writing. The server follows the new address
// (one migration counted on each side), the client's own probe validates,
// and the connection is in no error state afterwards.
func TestClientRebindMovesThePath(t *testing.T) {
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	l, err := NewListener(pc, ServerTLSForTest(t), DefaultParameters(), ListenerConfig{})
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

	first, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	client, err := Dial(first, pc.LocalAddr(), ClientTLSForTest(), DefaultParameters())
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
	got := make(chan []byte, 8)
	server.OnStreamFrames(func(fs []Frame) error {
		for _, f := range fs {
			if f.IsStream() {
				got <- append([]byte(nil), f.Data...)
			}
		}
		return nil
	})

	// The client confirms on HANDSHAKE_DONE, which may still be in flight
	// when Dial returns; Rebind refuses until then, so wait for it.
	waitFor(t, "the client to confirm the handshake", func() bool {
		client.mu.Lock()
		defer client.mu.Unlock()
		return client.handshakeConfirmed
	})

	second, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if err := client.Rebind(second); err != nil {
		t.Fatalf("Rebind: %v", err)
	}
	_ = first.Close() // the old socket is the caller's, and the caller is done with it

	s, err := client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write([]byte("from the second socket")); err != nil {
		t.Fatal(err)
	}
	select {
	case data := <-got:
		if string(data) != "from the second socket" {
			t.Errorf("the server read %q", data)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("data written after Rebind never reached the server")
	}

	waitFor(t, "the server to follow the client", func() bool {
		server.mu.Lock()
		defer server.mu.Unlock()
		return sameAddr(server.peer, second.LocalAddr())
	})
	if n := server.Migrations(); n != 1 {
		t.Errorf("server migrations = %d, want 1", n)
	}
	if n := client.Migrations(); n != 1 {
		t.Errorf("client migrations = %d, want 1", n)
	}
	waitFor(t, "the client's path validation", func() bool {
		client.mu.Lock()
		defer client.mu.Unlock()
		return !client.pathProbe.pending
	})
	if err := client.Err(); err != nil {
		t.Fatalf("the client ended with %v after a Rebind the server answered", err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
}

// waitFor polls cond until it holds or the test's patience runs out.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("waiting for %s: still not the case after 10s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Rebind's refusals, each on the smallest Conn that exhibits it: a server, a
// client before confirmation, a closed connection, a nil socket.
func TestRebindRefusals(t *testing.T) {
	server := newConn(&recordingPC{addr: fakeFuzzAddr("rb:sself")}, fakeFuzzAddr("rb:speer"), []byte("peercid0"), []byte("selfcid0"), false, DefaultParameters())
	defer server.Close()
	server.mu.Lock()
	server.handshakeConfirmed = true
	server.mu.Unlock()
	if err := server.Rebind(&recordingPC{}); err == nil {
		t.Error("a server accepted Rebind")
	}

	client := newConn(&recordingPC{addr: fakeFuzzAddr("rb:cself")}, fakeFuzzAddr("rb:cpeer"), []byte("peercid0"), []byte("selfcid0"), true, DefaultParameters())
	defer client.Close()
	if err := client.Rebind(&recordingPC{}); err == nil {
		t.Error("a client accepted Rebind before its handshake confirmed")
	}
	if err := client.Rebind(nil); err == nil {
		t.Error("Rebind(nil) was accepted")
	}
	client.mu.Lock()
	client.handshakeConfirmed = true
	client.mu.Unlock()
	installAppSealer(t, client)
	_ = client.Close()
	if err := client.Rebind(&recordingPC{}); !errors.Is(err, errClosed) {
		t.Errorf("Rebind on a closed connection: %v, want errClosed", err)
	}
}

// A Rebind the server never answers: the PATH_CHALLENGE retransmits on its
// backoff, and after the last unanswered one the connection ends with an
// error naming the cause — not the silent idle timeout a server-side probe
// settles for, because here the old path is gone and nothing else will
// ever arrive. Driven through the timer's own entry point with a clock
// advanced by hand, so no real seconds pass.
func TestRebindToADeadPathEndsTheConnection(t *testing.T) {
	old := &recordingPC{addr: fakeFuzzAddr("dead:old")}
	c := newConn(old, fakeFuzzAddr("dead:peer"), []byte("peercid0"), []byte("selfcid0"), true, DefaultParameters())
	defer c.Close()
	installAppSealer(t, c)
	c.mu.Lock()
	c.handshakeConfirmed = true
	c.mu.Unlock()

	fresh := &recordingPC{addr: fakeFuzzAddr("dead:new")}
	if err := c.Rebind(fresh); err != nil {
		t.Fatal(err)
	}
	if n := len(fresh.recorded()); n != 1 {
		t.Fatalf("%d datagrams left on the new socket right after Rebind, want 1 (the PATH_CHALLENGE)", n)
	}
	if n := len(old.recorded()); n != 0 {
		t.Fatalf("%d datagrams left on the old socket after Rebind", n)
	}
	c.mu.Lock()
	if !c.pathProbe.pending || !c.pathProbe.fatal {
		t.Fatalf("probe pending=%v fatal=%v after Rebind, want both true", c.pathProbe.pending, c.pathProbe.fatal)
	}
	now := c.pathProbe.sentAt
	for i := 0; i <= pathProbeMaxRetransmits; i++ {
		now = now.Add(time.Hour) // past any backoff
		c.pathProbeDeadlineLocked(now)
	}
	c.mu.Unlock()
	select {
	case <-c.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the connection did not end after the last unanswered PATH_CHALLENGE")
	}
	if err := c.Err(); err == nil {
		t.Fatal("the connection ended without an error naming the dead path")
	}
	if n := len(fresh.recorded()); n != 1+pathProbeMaxRetransmits+1 {
		// The first challenge, its retransmissions, and the CONNECTION_CLOSE.
		t.Errorf("%d datagrams on the new socket, want %d", n, 1+pathProbeMaxRetransmits+1)
	}
}

// The same probe answered in time is not fatal: the response clears the
// flag along with the probe, and the connection stays up.
func TestRebindValidatedByAResponseIsNotFatal(t *testing.T) {
	c := newConn(&recordingPC{addr: fakeFuzzAddr("ok:old")}, fakeFuzzAddr("ok:peer"), []byte("peercid0"), []byte("selfcid0"), true, DefaultParameters())
	defer c.Close()
	installAppSealer(t, c)
	c.mu.Lock()
	c.handshakeConfirmed = true
	c.mu.Unlock()
	if err := c.Rebind(&recordingPC{addr: fakeFuzzAddr("ok:new")}); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	resp := AppendVarint(nil, framePathResponse)
	resp = append(resp, c.pathProbe.data[:]...)
	c.mu.Unlock()
	if _, err := c.frames(spaceApplication, resp, time.Now()); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	pending, fatal := c.pathProbe.pending, c.pathProbe.fatal
	c.mu.Unlock()
	if pending || fatal {
		t.Fatalf("pending=%v fatal=%v after the PATH_RESPONSE, want both false", pending, fatal)
	}
	select {
	case <-c.Done():
		t.Fatalf("the connection ended: %v", c.Err())
	default:
	}
}

// pathResponse encodes a PATH_RESPONSE echoing data.
func pathResponse(data [8]byte) []byte {
	return append(AppendVarint(nil, framePathResponse), data[:]...)
}

// writesTo filters recorded datagrams by destination.
func writesTo(ws []recordedWrite, addr net.Addr) []recordedWrite {
	var out []recordedWrite
	for _, w := range ws {
		if sameAddr(w.to, addr) {
			out = append(out, w)
		}
	}
	return out
}

// RFC 9000 §8.2.1: a migration announced by a 40-byte datagram funds a
// 120-byte budget. The PATH_CHALLENGE used to be padded to 1200 regardless,
// withheld by the limit on every attempt, and the path stayed unvalidated
// and throttled to three times the client's bytes for the connection's
// life. It must go out within the budget, validate the address, earn the
// full-size second validation, and leave the connection unthrottled.
func TestSmallDatagramMigrationValidatesWithinTheBudget(t *testing.T) {
	c, pc := newMigratableServer(t, "small")
	newAddr := fakeFuzzAddr("small:peerB")
	rxFromNewAddress(c, newAddr, 40, true)
	if _, err := c.frames(spaceApplication, []byte{framePing}, time.Now()); err != nil {
		t.Fatal(err)
	}
	probes := writesTo(pc.recorded(), newAddr)
	if len(probes) != 1 {
		t.Fatalf("%d datagrams to the new path after a 40-byte migration, want the PATH_CHALLENGE", len(probes))
	}
	if n := len(probes[0].data); n > 120 {
		t.Fatalf("the challenge is %d bytes, over the 120-byte budget", n)
	}

	c.mu.Lock()
	first := c.pathProbe.data
	c.mu.Unlock()
	if _, err := c.frames(spaceApplication, pathResponse(first), time.Now()); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	ampl, pending, second := c.amplActive, c.pathProbe.pending, c.pathProbe.data
	c.mu.Unlock()
	if ampl {
		t.Fatal("the answered challenge did not lift the amplification limit")
	}
	if !pending || second == first {
		t.Fatal("no second, full-size validation after a small probe (§8.2.1)")
	}
	probes = writesTo(pc.recorded(), newAddr)
	if last := probes[len(probes)-1]; len(last.data) < 1200 {
		t.Fatalf("the second validation left in %d bytes, want ≥1200", len(last.data))
	}
	if _, err := c.frames(spaceApplication, pathResponse(second), time.Now()); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	pending = c.pathProbe.pending
	c.mu.Unlock()
	if pending {
		t.Fatal("the full-size validation's answer did not complete it")
	}

	// Unthrottled: ten 1000-byte sends all leave.
	before := len(writesTo(pc.recorded(), newAddr))
	payload := append([]byte{framePing}, make([]byte, 999)...)
	c.mu.Lock()
	for range 10 {
		if err := c.sendPacketLocked(spaceApplication, payload, sendOpts{}); err != nil {
			c.mu.Unlock()
			t.Fatal(err)
		}
	}
	c.mu.Unlock()
	if got := len(writesTo(pc.recorded(), newAddr)) - before; got != 10 {
		t.Errorf("%d of 10 datagrams left after validation, want all", got)
	}
}

// RFC 9000 §9.3.2: a migrated path whose validation fails sends the
// connection back to the last validated peer address — limit disarmed,
// identifier restored — where it used to stay on the dead path,
// amplification-limited, for good. §9.3.3: the old path is challenged at
// migration.
func TestUnvalidatedMigrationRevertsToTheLastValidatedPath(t *testing.T) {
	c, pc := newMigratableServer(t, "revert")
	tok := make([]byte, 16)
	if _, err := c.frames(spaceApplication, ncidFrame(1, 0, []byte("cli-cid1"), tok), time.Now()); err != nil {
		t.Fatal(err)
	}
	oldAddr, newAddr := fakeFuzzAddr("revert:peerA"), fakeFuzzAddr("revert:peerB")
	rxFromNewAddress(c, newAddr, 1200, true)
	if _, err := c.frames(spaceApplication, []byte{framePing}, time.Now()); err != nil {
		t.Fatal(err)
	}
	old := writesTo(pc.recorded(), oldAddr)
	if len(old) != 1 || len(old[0].data) < 1200 || string(old[0].data[1:9]) != "peercid0" {
		t.Fatalf("want one ≥1200-byte challenge on the old path under its own identifier (§9.3.3), got %d datagrams", len(old))
	}

	c.mu.Lock()
	if !sameAddr(c.peer, newAddr) || string(c.dcid) != "cli-cid1" {
		c.mu.Unlock()
		t.Fatalf("the path did not move: peer %v dcid %q", c.peer, c.dcid)
	}
	now := c.pathProbe.sentAt
	for i := 0; i <= pathProbeMaxRetransmits; i++ {
		now = now.Add(time.Hour)
		c.pathProbeDeadlineLocked(now)
	}
	peer, dcid, ampl, pending, migrations := c.peer, string(c.dcid), c.amplActive, c.pathProbe.pending, c.migrations
	c.mu.Unlock()
	if !sameAddr(peer, oldAddr) || dcid != "peercid0" {
		t.Errorf("after the failed validation: peer %v dcid %q, want %v under peercid0 (§9.3.2)", peer, dcid, oldAddr)
	}
	if ampl || pending {
		t.Errorf("after reverting: amplActive=%v probing=%v, want both false", ampl, pending)
	}
	if migrations != 1 {
		t.Errorf("migrations = %d, want 1: a revert undoes a move, it is not one", migrations)
	}
	select {
	case <-c.Done():
		t.Fatalf("the connection ended: %v", c.Err())
	default:
	}
	if n := len(writesTo(pc.recorded(), oldAddr)); n < 1+pathProbeMaxRetransmits {
		t.Errorf("%d challenges on the old path, want it retransmitted with the new path's probe", n)
	}

	before := len(writesTo(pc.recorded(), oldAddr))
	payload := append([]byte{framePing}, make([]byte, 999)...)
	c.mu.Lock()
	for range 10 {
		_ = c.sendPacketLocked(spaceApplication, payload, sendOpts{})
	}
	c.mu.Unlock()
	if got := len(writesTo(pc.recorded(), oldAddr)) - before; got != 10 {
		t.Errorf("%d of 10 datagrams left on the reverted path, want all", got)
	}
}

// RFC 9000 §9.3.3: a migration forged from a copied packet is undone as
// soon as the legitimate peer's non-probing packets arrive again from the
// last validated path — no probe, no amplification limit on the way back.
func TestReturnToTheValidatedPathEndsAnUnprovenMigration(t *testing.T) {
	c, _ := newMigratableServer(t, "spoof")
	oldAddr := fakeFuzzAddr("spoof:peerA")
	rxFromNewAddress(c, fakeFuzzAddr("spoof:attacker"), 1200, true)
	if _, err := c.frames(spaceApplication, []byte{framePing}, time.Now()); err != nil {
		t.Fatal(err)
	}
	rxFromNewAddress(c, oldAddr, 50, true)
	if _, err := c.frames(spaceApplication, []byte{framePing}, time.Now()); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	peer, ampl, pending := c.peer, c.amplActive, c.pathProbe.pending
	c.mu.Unlock()
	if !sameAddr(peer, oldAddr) || ampl || pending {
		t.Errorf("peer %v amplActive=%v probing=%v, want back on %v unthrottled", peer, ampl, pending, oldAddr)
	}
}

// §5.1.2: NEW_CONNECTION_ID with Retire Prior To equal to its own sequence,
// sent over and over, makes this end queue one RETIRE_CONNECTION_ID per
// frame. While nothing drains the control queue — here no 1-RTT sealer, on
// the wire a peer withholding ACKs to keep the window shut — that queue
// used to grow without bound; past twice the advertised limit it is now
// CONNECTION_ID_LIMIT_ERROR.
func TestRetirementFloodIsBounded(t *testing.T) {
	c := newConn(&fuzzSink{addr: "flood:self"}, fakeFuzzAddr("flood:peer"),
		[]byte("initial0"), []byte("selfcid0"), true, DefaultParameters())
	defer c.Close()
	c.adoptPeerCID([]byte("srv-cid0"))
	tok := make([]byte, 16)

	limit := int(max(2*c.params.ActiveConnIDLimit, 8))
	var err error
	seq := uint64(1)
	for ; seq <= 1000 && err == nil; seq++ {
		cid := []byte{'f', 'l', 'o', 'o', 'd', byte(seq >> 16), byte(seq >> 8), byte(seq)}
		_, err = c.frames(spaceApplication, ncidFrame(seq, seq, cid, tok), time.Now())
	}
	wantViolation(t, err, transportConnectionIDLimit, "an unending retirement flood")
	if retired := len(controlFrames(t, c)); retired > limit {
		t.Errorf("%d frames queued, over the bound of %d", retired, limit)
	}
	if seq > uint64(limit)+2 {
		t.Errorf("the flood ran %d frames before being refused, want about %d", seq-1, limit+1)
	}
}

// Draining the control queue resets the count: retirements a peer forces
// at the pace they leave are not a flood.
func TestRetirementsThatLeaveDoNotAccumulate(t *testing.T) {
	c := newConn(&fuzzSink{addr: "drain:self"}, fakeFuzzAddr("drain:peer"),
		[]byte("initial0"), []byte("selfcid0"), true, DefaultParameters())
	defer c.Close()
	installAppSealer(t, c)
	c.adoptPeerCID([]byte("srv-cid0"))
	tok := make([]byte, 16)
	for seq := uint64(1); seq <= 100; seq++ {
		cid := []byte{'d', 'r', 'a', 'i', 'n', byte(seq >> 16), byte(seq >> 8), byte(seq)}
		if _, err := c.frames(spaceApplication, ncidFrame(seq, seq, cid, tok), time.Now()); err != nil {
			t.Fatalf("sequence %d: %v", seq, err)
		}
		c.mu.Lock()
		c.flushControlLocked()
		c.mu.Unlock()
	}
}
