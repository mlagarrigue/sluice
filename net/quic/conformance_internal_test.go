package quic

import (
	"crypto/tls"
	"errors"
	"fmt"
	"testing"
	"time"
)

// Conformance regression tests for the 2026-10-03 audit's findings: each one
// pins a specific RFC requirement the implementation used to miss, and each
// fails against the pre-fix behaviour it documents.

// A duplicate ACK — one that newly acknowledges nothing — used to disarm a
// pending time-threshold loss deadline for good (RFC 9002 §6.1.2/A.10): the
// timer was cleared unconditionally and re-armed only when the ACK carried
// news, so the loss was then found only by a later new ACK or the PTO.
func TestDuplicateAckKeepsTheLossTimerArmed(t *testing.T) {
	var s sentTracker
	var rtt rttEstimator
	rtt.sample(100*time.Millisecond, 0)
	now := time.Now()
	s.record(sentPacket{pn: 0, sentAt: now.Add(-10 * time.Millisecond), size: 50, ackEliciting: true, payload: []byte{0}})
	s.record(sentPacket{pn: 1, sentAt: now, size: 50, ackEliciting: true, payload: []byte{1}})

	var peer ackTracker
	peer.record(1, now)
	f := ackFrameFor(t, &peer)
	if _, err := s.processAck(now, f.largest, f.Data, &rtt, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if s.lossTime.IsZero() {
		t.Fatal("precondition: the first ACK should have armed the loss timer for packet 0")
	}

	// The same ACK again: no news, and the deadline must survive it.
	out, err := s.processAck(now.Add(time.Millisecond), f.largest, f.Data, &rtt, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if out.newlyAckedCount != 0 {
		t.Fatalf("the duplicate newly acked %d packets; the test needs it to ack none", out.newlyAckedCount)
	}
	if s.lossTime.IsZero() {
		t.Error("a duplicate ACK disarmed the loss timer; packet 0's loss now waits for the PTO")
	}
}

// RFC 9000 §14.1: a client expands *every* datagram carrying an Initial
// packet, ACK-only ones included — a conformant server discards the smaller
// ones, and a discarded Initial ACK is a whole flight spuriously
// retransmitted.
func TestClientAckOnlyInitialIsPadded(t *testing.T) {
	pcA, pcB := newLoopbackPair(t)
	c := newConn(pcA, pcB.LocalAddr(), []byte("dcid0000"), []byte("scid0000"), true, DefaultParameters())
	defer c.Close()
	if err := c.installInitial([]byte("dcid0000")); err != nil {
		t.Fatal(err)
	}

	c.mu.Lock()
	sp := &c.spaces[spaceInitial]
	err := c.writePacketLocked(sp, spaceInitial, []byte{framePing}, nil, sendOpts{ackOnly: true}, time.Now())
	c.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}

	if err := pcB.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 2048)
	n, _, err := pcB.ReadFrom(buf)
	if err != nil {
		t.Fatal(err)
	}
	if n < 1200 {
		t.Errorf("a client's ack-only Initial left in a %d-byte datagram; §14.1 requires 1200", n)
	}
}

// RFC 9001 §6.6: a key that has sealed its 2^23 packets is spent, and with
// key update out of scope the connection must end with AEAD_LIMIT_REACHED
// rather than keep degrading the cipher.
func TestAEADConfidentialityLimitClosesTheConnection(t *testing.T) {
	pcA, pcB := newLoopbackPair(t)
	c := newConn(pcA, pcB.LocalAddr(), []byte("dcid0000"), []byte("scid0000"), false, DefaultParameters())
	defer c.Close()
	if err := c.installInitial([]byte("dcid0000")); err != nil {
		t.Fatal(err)
	}

	c.mu.Lock()
	c.spaces[spaceInitial].sealer.sealed = c.spaces[spaceInitial].sealer.k.confLimit - 1
	err := c.sendPacketLocked(spaceInitial, []byte{framePing}, sendOpts{})
	closeSent := c.closeSent
	c.mu.Unlock()

	if err == nil {
		t.Fatal("a send past the confidentiality limit succeeded")
	}
	if code := closeCodeFor(err); code != transportAEADLimitReached {
		t.Errorf("the refusal carries code %#x, want AEAD_LIMIT_REACHED (0x0f)", code)
	}
	if !closeSent {
		t.Error("no CONNECTION_CLOSE went out")
	}
	select {
	case <-c.closed:
	case <-time.After(2 * time.Second):
		t.Error("the connection did not close")
	}
}

// RFC 9001 §6.6's other half: past 2^52 packets that fail authentication
// under one key, the key's integrity bound is spent — the one case where
// unauthenticated bytes are allowed to end a connection.
func TestAEADIntegrityLimitClosesTheConnection(t *testing.T) {
	pcA, pcB := newLoopbackPair(t)
	c := newConn(pcA, pcB.LocalAddr(), []byte("dcid0000"), []byte("scid0000"), true, DefaultParameters())
	defer c.Close()
	if err := c.installInitial([]byte("dcid0000")); err != nil {
		t.Fatal(err)
	}

	c.mu.Lock()
	c.spaces[spaceInitial].opener.failed = c.spaces[spaceInitial].opener.k.intLimit - 1
	c.mu.Unlock()

	// A forged Initial addressed to this connection: parses, never
	// authenticates.
	garbage := syntheticInitial(t, []byte("scid0000"), []byte("attacker"))
	if err := c.receive(garbage, c.peer); err == nil {
		t.Fatal("the forgery that crossed the integrity limit was tolerated")
	}
	if code := closeCodeFor(c.Err()); code != transportAEADLimitReached {
		t.Errorf("the close carries code %#x, want AEAD_LIMIT_REACHED (0x0f)", code)
	}
}

// RFC 9001 §4.8: a TLS alert ends the connection with 0x0100 plus the
// alert's own code — ALPN failure must arrive as 0x0178, not as the
// INTERNAL_ERROR every TLS-derived close used to collapse into.
func TestCloseCodeForTLSAlerts(t *testing.T) {
	if got := closeCodeFor(&transportError{code: transportFlowControl}); got != transportFlowControl {
		t.Errorf("transportError code = %#x, want %#x", got, transportFlowControl)
	}
	// Alert 120 is no_application_protocol (RFC 9001 §8.1's example).
	if got := closeCodeFor(fmt.Errorf("quic: the handshake refused data: %w", tls.AlertError(120))); got != 0x0178 {
		t.Errorf("an ALPN alert maps to %#x, want 0x0178", got)
	}
	// Alert 109 is missing_extension (§8.2: missing transport parameters).
	if got := closeCodeFor(fmt.Errorf("wrapped: %w", tls.AlertError(109))); got != 0x016d {
		t.Errorf("a missing-extension alert maps to %#x, want 0x016d", got)
	}
	if got := closeCodeFor(errors.New("an io error")); got != transportInternalError {
		t.Errorf("an unclassified error maps to %#x, want INTERNAL_ERROR", got)
	}
}

// RFC 9000 §7.5: CRYPTO reassembly is bounded in bytes, not only in chunk
// count — 256 chunks of ~1380 bytes each used to park ~350 KB per
// never-completing handshake.
func TestCryptoBufferIsBounded(t *testing.T) {
	pcA, pcB := newLoopbackPair(t)
	c := newConn(pcA, pcB.LocalAddr(), []byte("dcid0000"), []byte("scid0000"), true, DefaultParameters())
	defer c.Close()

	var err error
	for i := range uint64(100) {
		// Disjoint fragments, never contiguous with offset zero: nothing
		// is ever delivered, everything parks.
		err = c.handleCrypto(spaceInitial, Frame{Type: frameCrypto, offset: 1 + i*2000, Data: make([]byte, 1000)})
		if err != nil {
			break
		}
	}
	if err == nil {
		t.Fatal("100 KB of disjoint CRYPTO fragments were parked without complaint")
	}
	te := &transportError{}
	if !errors.As(err, &te) || te.code != transportCryptoBufferExceeded {
		t.Errorf("the refusal is %v, want CRYPTO_BUFFER_EXCEEDED (0x0d)", err)
	}
}

// RFC 9000 §12.4: Initial and Handshake packets admit only PING, ACK,
// CRYPTO, PADDING and the transport CONNECTION_CLOSE. Initial keys are
// public, so a HANDSHAKE_DONE admitted there is a HANDSHAKE_DONE anyone can
// forge — and it used to make a mid-handshake client discard its Handshake
// keys for good.
func TestHandshakeDoneInAnInitialIsRefused(t *testing.T) {
	pcA, pcB := newLoopbackPair(t)
	c := newConn(pcA, pcB.LocalAddr(), []byte("dcid0000"), []byte("scid0000"), true, DefaultParameters())
	defer c.Close()
	if err := c.installInitial([]byte("dcid0000")); err != nil {
		t.Fatal(err)
	}

	payload := AppendVarint(nil, frameHandshakeDone)
	_, err := c.frames(spaceInitial, payload, time.Now())
	te := &transportError{}
	if !errors.As(err, &te) || te.code != transportProtocolViolation {
		t.Fatalf("HANDSHAKE_DONE at the Initial level returned %v, want PROTOCOL_VIOLATION", err)
	}
	c.mu.Lock()
	confirmed := c.handshakeConfirmed
	discarded := c.spaces[spaceHandshake].discarded
	c.mu.Unlock()
	if confirmed || discarded {
		t.Error("the forged HANDSHAKE_DONE still confirmed the handshake")
	}
}

// RFC 9000 §3.2/§3.5: frames addressing the direction a unidirectional
// stream does not have are STREAM_STATE_ERROR. A STOP_SENDING on the peer's
// own uni stream used to make this end answer with a RESET_STREAM on a
// stream it never sends on — which a conformant peer must kill the
// connection over.
func TestStreamDirectionIsEnforced(t *testing.T) {
	pcA, pcB := newLoopbackPair(t)
	c := newConn(pcA, pcB.LocalAddr(), []byte("dcid0000"), []byte("scid0000"), true, DefaultParameters())
	defer c.Close()
	// Application keys so the pre-fix behaviour (sending the RESET_STREAM)
	// had somewhere to go; the fix refuses before any send.
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

	// Stream 3 is, from this client's side, the peer's unidirectional
	// stream: this end never sends on it, so STOP_SENDING cannot name it.
	payload := appendFrame(nil, frameStopSending, 3, 0)
	_, err = c.frames(spaceApplication, payload, time.Now())
	te := &transportError{}
	if !errors.As(err, &te) || te.code != transportStreamState {
		t.Errorf("STOP_SENDING on the peer's uni stream returned %v, want STREAM_STATE_ERROR", err)
	}

	// Stream 2 is this client's own unidirectional stream: the peer never
	// sends on it, so STREAM frames cannot name it either — even once the
	// bookkeeping for it exists.
	_ = c.Stream(2)
	frame := AppendVarint(nil, FrameStream|streamOFF|streamLEN)
	frame = AppendVarint(frame, 2)
	frame = AppendVarint(frame, 0)
	frame = AppendVarint(frame, 3)
	frame = append(frame, "abc"...)
	_, err = c.frames(spaceApplication, frame, time.Now())
	if !errors.As(err, &te) || te.code != transportStreamState {
		t.Errorf("STREAM on this end's own uni stream returned %v, want STREAM_STATE_ERROR", err)
	}
}

// RFC 9000 §10.1: the effective idle timeout is floored at three probe
// timeouts — on a lossy, high-RTT path the recovery probes legitimately
// outstanding are the opposite of idleness.
func TestIdleTimeoutIsFlooredAtThreePTOs(t *testing.T) {
	pcA, pcB := newLoopbackPair(t)
	params := DefaultParameters()
	params.MaxIdleTimeout = 100
	c := newConn(pcA, pcB.LocalAddr(), []byte("dcid0000"), []byte("scid0000"), true, params)
	defer c.Close()

	c.mu.Lock()
	c.rtt.sample(2*time.Second, 0) // 3×PTO lands far past the 100 ms announcement
	c.idleDeadline = time.Now().Add(-time.Second)
	c.mu.Unlock()

	c.actOnDeadlines(time.Now())
	select {
	case <-c.closed:
		t.Error("the idle timeout fired inside the 3×PTO floor, with recovery probes still legitimate")
	default:
	}
}

// RFC 9000 §14.1-14.2: without path MTU discovery, nothing proves the path
// carries more than the 1200 bytes the protocol guarantees — whatever the
// peer's max_udp_payload_size announces, datagrams stay at 1200 until
// DPLPMTUD exists.
func TestMaxDatagramStaysAt1200WithoutPMTUD(t *testing.T) {
	pcA, pcB := newLoopbackPair(t)
	c := newConn(pcA, pcB.LocalAddr(), []byte("server00"), []byte("client00"), true, DefaultParameters())
	defer c.Close()
	c.initialDCID = []byte("origdcid")

	raw := appendParameters(nil, TransportParameters{
		initialSourceCID:  []byte("server00"),
		originalDCID:      []byte("origdcid"),
		MaxUDPPayloadSize: 1452,
	})
	if err := c.handlePeerParameters(raw); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	got := c.maxDatagram
	c.mu.Unlock()
	if got != 1200 {
		t.Errorf("maxDatagram = %d after the peer announced 1452; §14.2 caps it at 1200 until PMTUD exists", got)
	}
}

// RFC 9000 §8.2.2: a PATH_RESPONSE leaves in a 1200-byte datagram — the
// challenge doubles as an MTU probe — and exactly once, never replayed by
// loss recovery.
func TestPathResponseIsExpandedAndSentOnce(t *testing.T) {
	pcA, pcB := newLoopbackPair(t)
	c := newConn(pcA, pcB.LocalAddr(), []byte("dcid0000"), []byte("scid0000"), true, DefaultParameters())
	defer c.Close()
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

	payload := AppendVarint(nil, framePathChallenge)
	payload = append(payload, 1, 2, 3, 4, 5, 6, 7, 8)
	if _, err := c.frames(spaceApplication, payload, time.Now()); err != nil {
		t.Fatal(err)
	}

	if err := pcB.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 2048)
	n, _, err := pcB.ReadFrom(buf)
	if err != nil {
		t.Fatal(err)
	}
	if n < 1200 {
		t.Errorf("the PATH_RESPONSE left in a %d-byte datagram; §8.2.2 requires 1200", n)
	}
	c.mu.Lock()
	sent := c.spaces[spaceApplication].sent.packets
	var replayable bool
	for _, p := range sent {
		if len(p.payload) > 0 {
			replayable = true
		}
	}
	c.mu.Unlock()
	if len(sent) == 0 {
		t.Fatal("the response was not tracked at all")
	}
	if replayable {
		t.Error("the PATH_RESPONSE is queued for retransmission; §8.2.2 says it goes once")
	}
}

// RFC 9002 B.8: a congestion event is gated on the *latest* lost packet's
// send time — one pre-recovery straggler in a loss batch must not shield a
// genuinely new loss epoch from its window reduction.
func TestCongestionEventGatesOnTheLatestLoss(t *testing.T) {
	cc := newNewReno(1200)
	now := time.Now()
	cc.onSent(4800)
	cc.onLost(1200, now.Add(-time.Second), now.Add(-500*time.Millisecond))
	before := cc.cwnd

	// A batch mixing a straggler from before recovery with a loss from
	// after it: the latest is what decides, and it says new epoch.
	cc.onLost(1200, now, now)
	if cc.cwnd >= before {
		t.Errorf("cwnd = %d after a post-recovery loss, want below %d — the straggler shielded the batch", cc.cwnd, before)
	}
}

// The aggregation feeding that gate: processAck reports the newest lost
// send time, not the oldest.
func TestProcessAckReportsTheLatestLossSendTime(t *testing.T) {
	var s sentTracker
	var rtt rttEstimator
	now := time.Now()
	oldSent := now.Add(-2 * time.Second)
	newSent := now.Add(-10 * time.Millisecond)
	s.record(sentPacket{pn: 0, sentAt: oldSent, size: 50, ackEliciting: true, payload: []byte{0}})
	s.record(sentPacket{pn: 1, sentAt: newSent, size: 50, ackEliciting: true, payload: []byte{1}})
	s.record(sentPacket{pn: 4, sentAt: now, size: 50, ackEliciting: true, payload: []byte{4}})

	var peer ackTracker
	peer.record(4, now)
	f := ackFrameFor(t, &peer)
	out, err := s.processAck(now, f.largest, f.Data, &rtt, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.lost) != 2 {
		t.Fatalf("%d losses, want 2 (packets 0 and 1 by count threshold)", len(out.lost))
	}
	if !out.latestLossSent.Equal(newSent) {
		t.Errorf("latestLossSent = %v, want the newer packet's %v", out.latestLossSent, newSent)
	}
}

// RFC 9002 §6.2.1: no application-space probe timeout before the handshake
// confirms — the server may not be able to read 1-RTT packets yet, and the
// Handshake space's own PTO covers the interval.
func TestApplicationPTOWaitsForHandshakeConfirmation(t *testing.T) {
	pcA, pcB := newLoopbackPair(t)
	c := newConn(pcA, pcB.LocalAddr(), []byte("dcid0000"), []byte("scid0000"), true, DefaultParameters())
	defer c.Close()

	c.mu.Lock()
	defer c.mu.Unlock()
	sp := &c.spaces[spaceApplication]
	sp.lastElicit = time.Now()
	sp.sent.record(sentPacket{pn: 0, sentAt: time.Now(), size: 100, ackEliciting: true})
	if d := c.ptoDeadlineLocked(sp, spaceApplication); !d.IsZero() {
		t.Error("the application-space PTO armed before the handshake confirmed")
	}
	c.handshakeConfirmed = true
	if d := c.ptoDeadlineLocked(sp, spaceApplication); d.IsZero() {
		t.Error("the application-space PTO did not arm after confirmation")
	}
}

// RFC 9002 §6.2.2.1: a client whose flight is fully acknowledged keeps the
// PTO armed until the server provably validated its address — the server
// may be amplification-blocked, unable to send until this end does, and two
// endpoints each waiting on the other was a 10-second deadlock.
func TestClientAntiDeadlockPTOStaysArmed(t *testing.T) {
	pcA, pcB := newLoopbackPair(t)
	c := newConn(pcA, pcB.LocalAddr(), []byte("dcid0000"), []byte("scid0000"), true, DefaultParameters())
	defer c.Close()
	if err := c.installInitial([]byte("dcid0000")); err != nil {
		t.Fatal(err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	sp := &c.spaces[spaceInitial]
	sp.lastElicit = time.Now()
	// Nothing in flight, nothing queued: exactly the fully-acked flight.
	if d := c.ptoDeadlineLocked(sp, spaceInitial); d.IsZero() {
		t.Error("the anti-deadlock PTO is not armed while the server may still be amplification-blocked")
	}
	c.peerAddrValidated = true
	if d := c.ptoDeadlineLocked(sp, spaceInitial); !d.IsZero() {
		t.Error("the PTO stayed armed on an empty flight after the address question was settled")
	}
}

// RFC 9002 §6.2.1: the PTO timer is not set while a time-threshold loss
// timer is — the question a probe would ask is one loss detection is
// already about to answer, sooner.
func TestPTOIsSuppressedWhileALossTimerIsArmed(t *testing.T) {
	pcA, pcB := newLoopbackPair(t)
	c := newConn(pcA, pcB.LocalAddr(), []byte("dcid0000"), []byte("scid0000"), false, DefaultParameters())
	defer c.Close()
	if err := c.installInitial([]byte("dcid0000")); err != nil {
		t.Fatal(err)
	}

	now := time.Now()
	c.mu.Lock()
	sp := &c.spaces[spaceInitial]
	sp.lastElicit = now.Add(-time.Minute) // the PTO deadline is long past
	sp.sent.record(sentPacket{pn: 0, sentAt: now.Add(-time.Minute), size: 100, ackEliciting: true, payload: []byte{0}})
	sp.sent.lossTime = now.Add(time.Minute) // but loss detection already owns the question
	c.mu.Unlock()

	c.actOnDeadlines(now)

	c.mu.Lock()
	ptoCount, nextPN := c.ptoCount, c.spaces[spaceInitial].nextPN
	c.mu.Unlock()
	if ptoCount != 0 || nextPN != 0 {
		t.Errorf("a probe fired (ptoCount %d, nextPN %d) while the loss timer was armed", ptoCount, nextPN)
	}
}

// RFC 9002 §7: CRYPTO obeys the congestion window like everything else —
// what the window cannot take queues for retransmitLocked instead of
// leaving as an unpaced, window-ignoring burst.
func TestCryptoObeysTheCongestionWindow(t *testing.T) {
	pcA, pcB := newLoopbackPair(t)
	c := newConn(pcA, pcB.LocalAddr(), []byte("dcid0000"), []byte("scid0000"), false, DefaultParameters())
	defer c.Close()
	if err := c.installInitial([]byte("dcid0000")); err != nil {
		t.Fatal(err)
	}

	c.mu.Lock()
	c.cc.cwnd = 2000
	c.mu.Unlock()

	if err := c.sendCrypto(tls.QUICEncryptionLevelInitial, make([]byte, 8192)); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	inFlight, cwnd := c.cc.inFlight, c.cc.cwnd
	queued := len(c.spaces[spaceInitial].retrans)
	c.mu.Unlock()
	if inFlight > cwnd+c.maxDatagram {
		t.Errorf("CRYPTO put %d bytes in flight against a %d-byte window", inFlight, cwnd)
	}
	if queued == 0 {
		t.Error("the remainder was not queued for when the window reopens")
	}
}

// RFC 9002 §6.2.1's exception: a client does not reset its PTO backoff on
// Initial ACKs — the server may still be amplification-blocked, and that is
// exactly the stall the backoff is pacing.
func TestClientKeepsBackoffOnInitialAcks(t *testing.T) {
	pcA, pcB := newLoopbackPair(t)
	c := newConn(pcA, pcB.LocalAddr(), []byte("dcid0000"), []byte("scid0000"), true, DefaultParameters())
	defer c.Close()
	if err := c.installInitial([]byte("dcid0000")); err != nil {
		t.Fatal(err)
	}

	now := time.Now()
	c.mu.Lock()
	c.ptoCount = 2
	in := &c.spaces[spaceInitial]
	in.sent.record(sentPacket{pn: 0, sentAt: now, size: 100, ackEliciting: true})
	in.nextPN = 1
	hs := &c.spaces[spaceHandshake]
	hs.sent.record(sentPacket{pn: 0, sentAt: now, size: 100, ackEliciting: true})
	hs.nextPN = 1
	c.mu.Unlock()

	var peer ackTracker
	peer.record(0, now)
	f := ackFrameFor(t, &peer)
	if err := c.handleAck(spaceInitial, f, time.Now()); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	got := c.ptoCount
	c.mu.Unlock()
	if got != 2 {
		t.Errorf("an Initial ACK reset the client's backoff (ptoCount %d, want 2)", got)
	}

	// A Handshake ACK is the proof that ends the exception.
	if err := c.handleAck(spaceHandshake, f, time.Now()); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	got, validated := c.ptoCount, c.peerAddrValidated
	c.mu.Unlock()
	if got != 0 || !validated {
		t.Errorf("a Handshake ACK left ptoCount %d, validated %v; want 0, true", got, validated)
	}
}

// RFC 9002 §7: control frames other than flow-control grants obey the
// congestion window. Grants alone may exceed it — they are how a blocked
// peer resumes sending, and a peer that cannot send cannot produce the ACKs
// that would reopen this end's window.
func TestControlFramesObeyTheCongestionWindowExceptGrants(t *testing.T) {
	pcA, pcB := newLoopbackPair(t)
	c := newConn(pcA, pcB.LocalAddr(), []byte("dcid0000"), []byte("scid0000"), false, DefaultParameters())
	defer c.Close()
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
	c.cc.cwnd = 1200
	c.cc.inFlight = 1200 // the window is spent
	c.control = appendFrame(c.control, frameRetireConnectionID, 0)
	c.flushControlLocked()
	deferred := len(c.control)
	c.mu.Unlock()
	if deferred == 0 {
		t.Error("a non-grant control frame was flushed against a spent congestion window")
	}

	c.mu.Lock()
	c.grants = appendFrame(c.grants, frameMaxData, uint64(1<<20))
	c.flushControlLocked()
	grantsLeft, controlLeft := len(c.grants), len(c.control)
	c.mu.Unlock()
	if grantsLeft != 0 {
		t.Error("a window grant waited for the congestion window — the deadlock the bypass exists to break")
	}
	if controlLeft == 0 {
		t.Error("the deferred control frame rode a grant flush past the spent window")
	}

	// The window reopens: the deferred frame goes on the next flush.
	c.mu.Lock()
	c.cc.inFlight = 0
	c.flushControlLocked()
	controlLeft = len(c.control)
	c.mu.Unlock()
	if controlLeft != 0 {
		t.Error("the deferred control frame did not flush once the window reopened")
	}
}

// RFC 9000 §4.1, second half: a sender parked at a flow-control limit
// re-announces *_BLOCKED periodically, so the wait cannot be mistaken for
// idleness by either end's idle timer. The announcement's once-per-limit
// flag governs the first send, not the keepalive.
func TestBlockedFramesResendPeriodicallyWhileParked(t *testing.T) {
	pcA, pcB := newLoopbackPair(t)
	c := newConn(pcA, pcB.LocalAddr(), []byte("dcid0000"), []byte("scid0000"), false, DefaultParameters())
	defer c.Close()

	now := time.Now()
	c.mu.Lock()
	// Connection-level credit spent, with the immediate announcement already
	// out — exactly the state a parked writer leaves behind.
	c.connSend.max = 1000
	c.connSend.sent = 1000
	c.connSend.blockedSent = true
	c.blockedResend = now // matured
	c.resendBlockedFramesLocked(now)
	queued := len(c.control) // no app sealer: the frame stays queued
	rearmed := !c.blockedResend.IsZero() && c.blockedResend.After(now)
	c.mu.Unlock()
	if queued == 0 {
		t.Error("a matured blocked-resend deadline queued no DATA_BLOCKED while still parked")
	}
	if !rearmed {
		t.Error("the blocked-resend deadline did not re-arm while still parked")
	}

	// Credit arrives: the next firing finds nothing parked and disarms.
	c.mu.Lock()
	c.control = nil
	c.connSend.grant(2000)
	c.resendBlockedFramesLocked(now)
	queued = len(c.control)
	disarmed := c.blockedResend.IsZero()
	c.mu.Unlock()
	if queued != 0 {
		t.Error("DATA_BLOCKED queued after credit arrived")
	}
	if !disarmed {
		t.Error("the blocked-resend deadline stayed armed with nothing parked")
	}
}

// RFC 9000 §10.3.1: a datagram that routes nowhere but ends in a reset
// token the peer announced is the peer's stateless reset — the connection
// ends silently, now, instead of grinding to the idle timeout against a
// peer that provably has no state left.
func TestStatelessResetIsDetectedByItsToken(t *testing.T) {
	pcA, pcB := newLoopbackPair(t)
	c := newConn(pcA, pcB.LocalAddr(), []byte("dcid0000"), []byte("scid0000"), true, DefaultParameters())
	defer c.Close()

	token := [16]byte{9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 1, 2, 3, 4, 5, 6}
	c.mu.Lock()
	c.peerCIDs = []peerCID{{seq: 0, cid: []byte("dcid0000"), used: true, token: token}}
	c.mu.Unlock()

	// Looks like a short-header packet addressed to nobody, ends in the
	// token — exactly what §10.3 says a reset must look like.
	reset := make([]byte, 40)
	reset[0] = 0x40
	for i := 1; i < len(reset)-16; i++ {
		reset[i] = byte(i * 7)
	}
	copy(reset[len(reset)-16:], token[:])

	err := c.receive(reset, pcB.LocalAddr())
	if err == nil || !errors.Is(err, ErrQUIC) {
		t.Fatalf("receive returned %v, want the stateless-reset teardown error", err)
	}
	select {
	case <-c.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("the connection did not close on a stateless reset")
	}

	// The same bytes with a wrong token are noise, not a reset.
	c2 := newConn(pcA, pcB.LocalAddr(), []byte("dcid0001"), []byte("scid0001"), true, DefaultParameters())
	defer c2.Close()
	c2.mu.Lock()
	c2.peerCIDs = []peerCID{{seq: 0, cid: []byte("dcid0001"), used: true, token: token}}
	c2.mu.Unlock()
	reset[len(reset)-1] ^= 0xff
	if err := c2.receive(reset, pcB.LocalAddr()); err != nil {
		t.Fatalf("a near-miss token ended the connection: %v", err)
	}
}
