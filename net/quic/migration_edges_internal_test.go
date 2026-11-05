package quic

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/net/quic/udp"
)

// The spin bit is drawn, both values seen.
func TestSpinBitIsDrawn(t *testing.T) {
	seen := map[byte]bool{}
	c := &Conn{}
	for range 64 {
		c.rollSpinLocked()
		seen[c.spin] = true
	}
	if !seen[0] || !seen[0x20] {
		t.Errorf("spin values seen: %v", seen)
	}
}

// Settings at the edge of what assembleConn normalises are kept.
func TestAssembleKeepsSettingsAtTheirEdges(t *testing.T) {
	pcA, pcB := newLoopbackPair(t)
	params := DefaultConfig()
	params.ActiveConnIDLimit = 2
	params.Congestion.Algorithm = CongestionBBR
	params.Congestion.SlowStart = SlowStartHyStart
	c := assembleConn(pcA, pcB.LocalAddr(), []byte("dcid0000"), []byte("scid0000"), false, params)
	t.Cleanup(func() { _ = c.Close() })
	p := c.params
	if p.ActiveConnIDLimit != 2 || p.Congestion.Algorithm != CongestionBBR || p.Congestion.SlowStart != SlowStartHyStart {
		t.Errorf("limit %d, algorithm %v, slow start %v", p.ActiveConnIDLimit, p.Congestion.Algorithm, p.Congestion.SlowStart)
	}
}

// A preferred_address of the minimum length is read, and refused for its
// empty identifier; a parameter with an identifier of 64 is caught twice.
func TestParametersAtTheirEdges(t *testing.T) {
	_, err := parsePreferredAddress(make([]byte, preferredAddressLen))
	if err == nil || !strings.Contains(err.Error(), "with a 0-byte connection identifier") {
		t.Errorf("a minimal preferred_address: %v", err)
	}
	_, err = parseParameters(appendParam(appendParam(nil, 64, nil), 64, nil))
	if err == nil || !strings.Contains(err.Error(), "appears twice") {
		t.Errorf("parameter 64 twice: %v", err)
	}
}

// A listener takes its bounds themselves.
func TestListenerConfigAtItsBounds(t *testing.T) {
	c := ListenerConfig{MaxPendingConns: maxPendingConnsLimit, ConnQueueLen: maxConnQueueLen, StatelessResetsPerSecond: maxResetsPerSecond}
	if err := c.Validate(); err != nil {
		t.Errorf("Validate at the bounds: %v", err)
	}
}

// Packet information is asked for only on a socket bound to the unspecified
// address.
func TestPacketInfoOnlyOnUnspecified(t *testing.T) {
	uc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer uc.Close()
	if packetInfoSocket(uc) != nil {
		t.Error("packet information on a socket bound to 127.0.0.1")
	}
}

// closeFails is a socket whose Close reports an error.
type closeFails struct{ net.PacketConn }

var errCloseFails = errors.New("close fails")

func (c closeFails) Close() error { _ = c.PacketConn.Close(); return errCloseFails }

// Close reports the preferred-address socket's failure.
func TestListenerCloseReportsThePreferredSocket(t *testing.T) {
	pc, other := newLoopbackPair(t)
	l, err := NewListener(pc, ServerTLSForTest(t), DefaultConfig(), ListenerConfig{PreferredAddress: closeFails{other}})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); !errors.Is(err, errCloseFails) {
		t.Errorf("Close = %v, want the preferred socket's error", err)
	}
}

// foreignVersionCIDs reads identifiers to the last byte and no further.
func TestForeignVersionCIDsAtTheirEdges(t *testing.T) {
	for _, c := range []struct {
		name string
		d    []byte
		ok   bool
	}{
		{"seven bytes, empty identifiers", []byte{0xc0, 0, 0, 0, 2, 0, 0}, true},
		{"no source length", []byte{0xc0, 0, 0, 0, 2, 3, 1, 2, 3}, false},
		{"source one byte short", []byte{0xc0, 0, 0, 0, 2, 0, 2, 9}, false},
	} {
		_, _, ok := foreignVersionCIDs(c.d)
		if ok != c.ok {
			t.Errorf("%s: ok %v", c.name, ok)
		}
	}
}

// listenerWithConn is a listener and a connection of its, the connection's
// peer token filed under image.
func listenerFor(t *testing.T) *Listener {
	t.Helper()
	pc, _ := newLoopbackPair(t)
	l, err := NewListener(pc, ServerTLSForTest(t), DefaultConfig(), ListenerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

// A 21-byte reset is found; a retired token leaves its connection's list,
// the list kept while it holds others.
func TestListenerTokensAtTheirEdges(t *testing.T) {
	l := listenerFor(t)
	c := newIdleServerConn(t)
	d := append([]byte{0x40}, make([]byte, 20)...)
	for i := range d[1:] {
		d[1+i] = byte(i + 1)
	}
	resetWith(c, d)
	var token [16]byte
	copy(token[:], d[len(d)-16:])
	l.mu.Lock()
	l.keysOf[c] = nil // a connection of this listener's
	l.mu.Unlock()
	l.peerTokenAdded(c, token)
	other := token
	other[0] ^= 0xff
	l.peerTokenAdded(c, other)
	if got := l.resetFor(d, c.peer); got != c {
		t.Errorf("a 21-byte reset found %v", got)
	}
	l.peerTokenRetired(c, token)
	l.mu.Lock()
	defer l.mu.Unlock()
	if images := l.tokensOf[c]; len(images) != 1 || images[0] != l.tokenImage(other) {
		t.Errorf("tokens left after retiring the first of two: %d", len(images))
	}
}

// The reset budget rolls over at one second exactly, not within it.
func TestResetBudgetWindow(t *testing.T) {
	l := listenerFor(t)
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.resetWindow, l.resetsSent = now.Add(-time.Second), l.lcfg.StatelessResetsPerSecond
	if !l.resetBudgetLocked(now, "a") {
		t.Error("no budget a second after the window opened")
	}
	l.resetWindow, l.resetsSent = now.Add(-100*time.Millisecond), 1
	l.resetBudgetLocked(now, "a")
	if l.resetsSent != 1 {
		t.Error("the window rolled within its second")
	}
}

// resetSink counts what is written through it.
type resetSink struct {
	net.PacketConn
	n int
}

func (w *resetSink) WriteTo(b []byte, _ net.Addr) (int, error) { w.n++; return len(b), nil }

// A 22-byte datagram is answered with a reset.
func TestStatelessResetForATwentyTwoByteDatagram(t *testing.T) {
	l := listenerFor(t)
	via := &resetSink{}
	l.maybeSendStatelessReset(make([]byte, 22), make([]byte, listenerCIDLen), &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9}, via)
	if via.n != 1 {
		t.Errorf("%d resets for a 22-byte datagram", via.n)
	}
}

// stubRegistrar mints nothing.
type stubRegistrar struct{}

func (stubRegistrar) addLocalCID(*Conn) ([]byte, error) { return nil, errors.New("full") }
func (stubRegistrar) removeLocalCID([]byte)             {}
func (stubRegistrar) resetTokenFor([]byte) [16]byte     { return [16]byte{} }
func (stubRegistrar) peerTokenAdded(*Conn, [16]byte)    {}
func (stubRegistrar) peerTokenRetired(*Conn, [16]byte)  {}

// An unused identifier at the retire threshold is a spare: no retirement.
func TestSpareAtTheRetireThreshold(t *testing.T) {
	c := boundsConn(t, false)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.registrar, c.handshakeConfirmed = stubRegistrar{}, true
	c.localCIDs = []localCID{{seq: 2}, {seq: 3, used: true}}
	c.localRetirePrior, c.peerLocalSeq = 2, 3
	c.issueLocalCIDsLocked()
	if c.localRetirePrior != 2 {
		t.Errorf("localRetirePrior = %d with a spare at the threshold", c.localRetirePrior)
	}
}

// Identifier bookkeeping at its edges.
func TestPeerIdentifiersAtTheirEdges(t *testing.T) {
	c := boundsConn(t, false)
	c.mu.Lock()
	c.nextLocalSeq = 3
	c.mu.Unlock()
	if _, err := c.handleRetireConnectionID(3); err == nil {
		t.Error("retiring sequence 3, never issued, accepted")
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.params.ActiveConnIDLimit, c.controlRetires = 8, 8
	if err := c.queueRetireLocked(99); err != nil {
		t.Errorf("a ninth retirement under a limit of 8: %v", err)
	}

	c.currentPeerSeq = 0
	c.peerCIDs = []peerCID{{seq: 2, cid: []byte("cid2"), used: true}, {seq: 1, cid: []byte("cid1"), used: true}, {seq: 0, cid: []byte("cid0")}}
	if got := c.pickPeerCIDLocked(false); got != 1 {
		t.Errorf("pick among used identifiers = %d, want the lowest sequence (index 1)", got)
	}

	c.peerCIDs = []peerCID{{seq: 1, cid: []byte("cid1")}, {seq: 0, cid: []byte("cid0")}}
	c.rxLocalCIDSeq, c.pathLocalSeq = 1, 0
	if !c.mayFollowLocked(&net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9}, c.pc) {
		t.Error("no following with an unused identifier at index 0")
	}
	if _, seq, ok := c.takeFreshPeerCIDLocked(); !ok || seq != 1 {
		t.Errorf("takeFresh = %d, %v", seq, ok)
	}
}

// Reverting to the validated path takes an unused identifier at index 0.
func TestRevertTakesTheFirstUnusedIdentifier(t *testing.T) {
	c, _ := appConn(t)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastValidatedPeer, c.lastValidatedPC, c.lastValidatedSeq = c.peer, c.pc, 5
	c.peerCIDs = []peerCID{{seq: 1, cid: []byte("cid1")}, {seq: 0, cid: []byte("cid0"), used: true}}
	c.currentPeerSeq = 0
	c.revertToValidatedLocked(time.Now())
	if c.closeErr != nil || string(c.dcid) != "cid1" {
		t.Errorf("revert: closeErr %v, dcid %q", c.closeErr, c.dcid)
	}
}

// A path challenge is full-size when the budget allows 1200 bytes exactly,
// and the budget is what was received thrice less what was sent.
func TestPathChallengeFullAtTheBudget(t *testing.T) {
	for _, tc := range []struct {
		recv, sent int64
		full       bool
	}{
		{400, 0, true},
		{400, 100, false},
	} {
		c, _ := appConn(t)
		c.mu.Lock()
		c.amplActive, c.amplRecv, c.amplSent = true, tc.recv, tc.sent
		c.sendPathChallengeLocked()
		if c.pathProbe.full != tc.full {
			t.Errorf("received %d, sent %d: full %v", tc.recv, tc.sent, c.pathProbe.full)
		}
		c.mu.Unlock()
	}
}

// recordPC keeps a copy of every datagram written through it.
type recordPC struct {
	net.PacketConn
	got [][]byte
}

func (r *recordPC) WriteTo(b []byte, _ net.Addr) (int, error) {
	r.got = append(r.got, append([]byte(nil), b...))
	return len(b), nil
}

// PATH_RESPONSEs leave whole, as many per packet as the chunk holds: every
// packet opens to whole frames, and every response arrives.
func TestPathResponsesPackWholeFrames(t *testing.T) {
	s, err := initialSecrets([]byte("dcid0000"))
	if err != nil {
		t.Fatal(err)
	}
	opener, err := newPacketOpener(s.Server)
	if err != nil {
		t.Fatal(err)
	}
	const n = 300
	c, _ := appConn(t)
	c.mu.Lock()
	defer c.mu.Unlock()
	// 1300 bytes: a chunk of 1234, which 9 does not divide (1200 and 1452
	// give 1134 and 1386, multiples of 63).
	c.maxDatagram = 1300
	rec := &recordPC{PacketConn: c.pc}
	c.pc = rec
	responses := make([]byte, 0, 9*n)
	for range n {
		responses = append(responses, framePathResponse, 1, 2, 3, 4, 5, 6, 7, 8)
	}
	c.answerPathChallengesLocked(responses, time.Now())
	got := 0
	for i, d := range rec.got {
		payload, _, _, err := opener.openPhased(d, 1+len(c.dcid), 0)
		if err != nil {
			t.Fatalf("datagram %d: %v", i, err)
		}
		frames, err := parseFrames(nil, payload)
		if err != nil {
			t.Fatalf("datagram %d does not parse to whole frames: %v", i, err)
		}
		for _, f := range frames {
			if f.Type == framePathResponse {
				got++
			}
		}
	}
	if got != n || len(rec.got) < 2 {
		t.Errorf("%d responses in %d datagrams, want %d in more than one", got, len(rec.got), n)
	}
}

// After the padded first answer, the next packets wait for 64 bytes of
// window beyond them.
func TestPathResponsesWaitForTheWindow(t *testing.T) {
	c, cc := appConn(t)
	c.mu.Lock()
	defer c.mu.Unlock()
	per := c.maxStreamChunk() / 9
	responses := make([]byte, 0, 9*(per+1))
	for range per + 1 {
		responses = append(responses, framePathResponse, 1, 2, 3, 4, 5, 6, 7, 8)
	}
	cc.inFlight = cc.cwnd - 1200 - (9 + 63)
	sp := &c.spaces[spaceApplication]
	before := sp.nextPN
	c.answerPathChallengesLocked(responses, time.Now())
	if got := sp.nextPN - before; got != 1 {
		t.Errorf("%d packets; the second, 63 bytes short of its window, left", got)
	}
}

// A client with zero-length identifiers answers from any path.
func TestZeroLengthIdentifiersAnswerOffPath(t *testing.T) {
	c, _ := appConn(t)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.dcid = nil
	c.rxFrom = &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9}
	c.rxLocalCIDSeq, c.pathLocalSeq = 1, 0
	c.offPath.addr, c.offPath.recv = c.rxFrom, 1000
	c.answerPathChallengesLocked([]byte{framePathResponse, 1, 2, 3, 4, 5, 6, 7, 8}, time.Now())
	if c.offPath.sent == 0 {
		t.Error("no answer off-path under zero-length identifiers")
	}

	other, _ := newLoopbackPair(t)
	c.rxFrom, c.rxVia = nil, other
	before := c.spaces[spaceApplication].nextPN
	c.answerPathChallengesLocked([]byte{framePathResponse, 1, 2, 3, 4, 5, 6, 7, 8}, time.Now())
	if c.spaces[spaceApplication].nextPN == before {
		t.Error("no answer from another socket under zero-length identifiers")
	}
}

// The preferred-address socket answers the current peer; a probe toward a
// target answers under the target's identifier.
func TestPreferredSocketAndPathIdentifiers(t *testing.T) {
	c, _ := appConn(t)
	other, _ := newLoopbackPair(t)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rxVia, c.rxFrom = other, nil
	c.peerCIDs = append(c.peerCIDs, peerCID{seq: 9, cid: []byte("cid9")})
	if frame, _ := c.preferredPathChallengeLocked(time.Now()); frame == nil {
		t.Error("no challenge from the preferred socket for the current peer")
	}

	target := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9}
	c.pathProbe.target, c.pathProbe.targetCID, c.rxFrom = target, []byte("tcid"), target
	if cid, ok := c.newPathCIDLocked(); !ok || string(cid) != "tcid" {
		t.Errorf("newPathCID toward the probe's target = %q, %v", cid, ok)
	}
}

// A candidate's challenge is full-size when its address's budget allows
// 1200 bytes exactly.
func TestCandidateChallengeFullAtTheBudget(t *testing.T) {
	for _, tc := range []struct {
		recv, sent int64
		full       bool
	}{
		{400, 0, true},
		{400, 100, false},
	} {
		c, _ := appConn(t)
		c.mu.Lock()
		from := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9}
		c.rxFrom = from
		c.offPath.addr, c.offPath.recv, c.offPath.sent = from, tc.recv, tc.sent
		if c.candidateChallengeLocked() == nil {
			t.Fatal("no challenge")
		}
		if got := c.answered[len(c.answered)-1].full; got != tc.full {
			t.Errorf("received %d, sent %d: full %v", tc.recv, tc.sent, got)
		}
		c.mu.Unlock()
	}
}

// A client's Rebind that never validated ends with the count of challenges
// sent.
func TestRebindNeverValidatedSaysHowManyChallenges(t *testing.T) {
	pcA, pcB := newLoopbackPair(t)
	c := assembleConn(pcA, pcB.LocalAddr(), []byte("dcid0000"), []byte("scid0000"), true, DefaultConfig())
	t.Cleanup(func() { _ = c.Close() })
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pathProbe.pending, c.pathProbe.fatal = true, true
	c.pathProbe.retransmits, c.pathProbe.sentAt = pathProbeMaxRetransmits, now.Add(-time.Hour)
	c.pathProbeDeadlineLocked(now)
	if want := fmt.Sprintf("(%d PATH_CHALLENGEs unanswered)", pathProbeMaxRetransmits+1); c.closeErr == nil || !strings.Contains(c.closeErr.Error(), want) {
		t.Errorf("closeErr = %v, want %q", c.closeErr, want)
	}
}

// Moving to the preferred address counts a migration.
func TestPreferredMoveCountsAMigration(t *testing.T) {
	c, _ := appConn(t)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.peerCIDs = append(c.peerCIDs, peerCID{seq: 9, cid: []byte("cid9")})
	c.preferredCandidate = preferredCandidate{cid: []byte("cid9"), to: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9}}
	c.adoptPreferredCandidateLocked(time.Now())
	if c.migrations != 1 {
		t.Errorf("migrations = %d", c.migrations)
	}
}

// A packet from another socket of this end, with no probe from it, moves
// nothing under an identifier, and does under zero-length ones.
func TestMigrationFromAnotherSocket(t *testing.T) {
	for _, zero := range []bool{false, true} {
		c, _ := appConn(t)
		other, _ := newLoopbackPair(t)
		c.mu.Lock()
		if zero {
			c.dcid = nil
		}
		c.rxVia, c.rxFrom, c.rxHighest = other, nil, true
		c.pathProbe.viaNonProbing = false
		c.mu.Unlock()
		c.maybeMigrate([]Frame{{Type: framePing}}, time.Now())
		c.mu.Lock()
		moved := c.pathProbe.viaNonProbing
		c.mu.Unlock()
		if moved != zero {
			t.Errorf("zero-length identifiers %v: moved %v", zero, moved)
		}
	}
}

// A migration adopts an unused identifier found at index 0.
func TestMigrationAdoptsTheFirstUnusedIdentifier(t *testing.T) {
	c, _ := appConn(t)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.peerCIDs = []peerCID{{seq: 1, cid: []byte("cid1")}, {seq: 0, cid: []byte("cid0"), used: true}}
	c.currentPeerSeq = 0
	c.migrateLocked(&net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9}, c.pc, time.Now())
	if c.currentPeerSeq != 1 {
		t.Errorf("currentPeerSeq = %d after the move", c.currentPeerSeq)
	}
}

// The table of sourced vias stops at its bound.
func TestSourcedViasStopAtTheirBound(t *testing.T) {
	uc, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero})
	if err != nil {
		t.Fatal(err)
	}
	defer uc.Close()
	if packetInfoSocket(uc) == nil {
		t.Skip("no packet information on this socket")
	}
	s := &sourcedVias{uc: uc, oob: make([]byte, udp.PacketInfoLen), byLocal: make(map[localKey]*sourcedConn)}
	for i := range maxSourcedVias {
		s.byLocal[localKey{addr: netip.AddrFrom4([4]byte{10, 0, byte(i >> 8), byte(i)})}] = &sourcedConn{}
	}
	sender, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	port := uc.LocalAddr().(*net.UDPAddr).Port
	if _, err := sender.WriteTo([]byte("x"), &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port}); err != nil {
		t.Fatal(err)
	}
	_ = uc.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, _, _, _, err := s.read(make([]byte, 64)); err != nil {
		t.Fatal(err)
	}
	if len(s.byLocal) != maxSourcedVias {
		t.Errorf("%d sourced vias, past the bound of %d", len(s.byLocal), maxSourcedVias)
	}
}

// A valid Retry token is verified, and spent, when the listener would not
// have asked for one itself.
func TestRetryTokenVerifiedWithoutForcedRetry(t *testing.T) {
	srv, cli := newLoopbackPair(t)
	l, err := NewListener(srv, ServerTLSForTest(t), DefaultConfig(), ListenerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	newSCID := []byte("newscid1")
	token := appendRetryToken(nil, l.lcfg.TokenKey, cli.LocalAddr(), []byte("odcid000"), newSCID)
	if _, err := cli.WriteTo(sealedClientInitialWithToken(t, newSCID, []byte("clientsc"), token), srv.LocalAddr()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		l.mu.Lock()
		_, spent := l.spent.byID[string(newSCID)]
		l.mu.Unlock()
		if spent {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the token was never spent: it reached the direct path unexamined")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
