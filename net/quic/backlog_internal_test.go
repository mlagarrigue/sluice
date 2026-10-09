package quic

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"
)

// Regression tests for the 2026-10 backlog items: each pins an RFC 9000/9001/
// 9002 requirement (or a flow-control accounting invariant) the
// implementation used to miss, and each fails against the pre-fix behaviour
// it documents.

// A STREAM frame and the RESET_STREAM that ends the same stream can share a
// packet. The reset's "bytes the application will never consume" used to be
// counted from the stream's consumption mark — which, in batch mode, the
// packet's own delta had not reached yet — so settleBatchConsumption then
// credited the same delta a second time, over-granting the connection window
// by exactly one packet's worth.
func TestStreamAndResetInOnePacketSettleFlowControlOnce(t *testing.T) {
	c := newConn(&fuzzSink{addr: "backlog:self"}, fakeFuzzAddr("backlog:peer"),
		[]byte{1, 2, 3, 4}, []byte{5, 6, 7, 8}, false, DefaultParameters())
	defer c.Close()
	c.OnStreamFrames(func([]Frame) error { return nil })

	data := []byte("hello")
	payload := AppendVarint(nil, FrameStream|streamOFF|streamLEN)
	payload = AppendVarint(payload, 0) // the peer's first bidi stream
	payload = AppendVarint(payload, 0) // offset
	payload = AppendVarint(payload, uint64(len(data)))
	payload = append(payload, data...)
	payload = appendFrame(payload, FrameResetStream, 0, 7, uint64(len(data)))

	if _, err := c.frames(spaceApplication, payload, time.Now()); err != nil {
		t.Fatal(err)
	}

	c.mu.Lock()
	consumed, highest := c.connRecv.consumed, c.connRecv.highest
	c.mu.Unlock()
	if highest != uint64(len(data)) {
		t.Fatalf("connection high-water mark = %d, want %d", highest, len(data))
	}
	if consumed != uint64(len(data)) {
		t.Errorf("connection-level consumption = %d for a %d-byte stream; the delta was settled twice",
			consumed, len(data))
	}
}

// RFC 9000 §10.1: a send restarts the idle timer only when no ack-eliciting
// packet has been sent since the last receive. An endpoint retransmitting
// into silence used to extend its own idle clock with every attempt, so the
// timeout that exists to detect a dead peer never fired while this end kept
// talking at it.
func TestIdleTimerRestartsOnlyOnTheFirstSendAfterAReceive(t *testing.T) {
	pcA, pcB := newLoopbackPair(t)
	c := newConn(pcA, pcB.LocalAddr(), []byte("dcid0000"), []byte("scid0000"), true, DefaultParameters())
	defer c.Close()
	if err := c.installInitial([]byte("dcid0000")); err != nil {
		t.Fatal(err)
	}

	c.mu.Lock()
	stale := time.Now().Add(time.Second)
	c.idleDeadline = stale
	c.elicitingSinceRecv = false
	if err := c.sendPacketLocked(spaceInitial, []byte{framePing}, sendOpts{}); err != nil {
		c.mu.Unlock()
		t.Fatal(err)
	}
	first := c.idleDeadline
	if err := c.sendPacketLocked(spaceInitial, []byte{framePing}, sendOpts{}); err != nil {
		c.mu.Unlock()
		t.Fatal(err)
	}
	second := c.idleDeadline
	c.mu.Unlock()

	if !first.After(stale) {
		t.Fatal("the first ack-eliciting send after a receive did not restart the idle timer")
	}
	if !second.Equal(first) {
		t.Error("a second send into the same silence moved the idle deadline again; §10.1 restarts it once per receive")
	}
}

// RFC 9000 §13.2.1: an ack-eliciting packet that arrives out of order, or
// that opens a gap, should be acknowledged immediately — the peer's loss
// detection is waiting on exactly that signal — while the in-order first
// packet of a burst keeps the delayed-ACK timer.
func TestOutOfOrderPacketMakesTheAckImmediate(t *testing.T) {
	var a ackTracker
	now := time.Now()

	a.record(0, now)
	a.onAckEliciting(spaceApplication, now)
	if a.immediate {
		t.Fatal("the first in-order packet was answered immediately; it should ride the delayed-ACK timer")
	}
	a.appendAck(nil, now)

	a.record(2, now) // skips 1: a gap
	a.onAckEliciting(spaceApplication, now)
	if !a.immediate {
		t.Error("a packet that opened a gap did not make the ACK immediate")
	}
	a.appendAck(nil, now)

	a.record(1, now) // below the largest: out of order
	a.onAckEliciting(spaceApplication, now)
	if !a.immediate {
		t.Error("an out-of-order packet did not make the ACK immediate")
	}
	a.appendAck(nil, now)

	a.record(3, now) // extends the run: in order again
	a.onAckEliciting(spaceApplication, now)
	if a.immediate {
		t.Error("an in-order packet after the gap closed was answered immediately; the urgency should have cleared")
	}
}

// RFC 9000 §12.4: a packet whose payload carries no frames at all is a
// PROTOCOL_VIOLATION. A payload of nothing but PADDING contains frames and
// stays legal.
func TestFramelessPacketIsAProtocolViolation(t *testing.T) {
	c := newConn(&fuzzSink{addr: "backlog:self"}, fakeFuzzAddr("backlog:peer"),
		[]byte{1, 2, 3, 4}, []byte{5, 6, 7, 8}, false, DefaultParameters())
	defer c.Close()

	_, err := c.frames(spaceApplication, nil, time.Now())
	te := &transportError{}
	if !errors.As(err, &te) || te.code != transportProtocolViolation {
		t.Errorf("an empty payload returned %v, want PROTOCOL_VIOLATION", err)
	}

	if _, err := c.frames(spaceApplication, make([]byte, 16), time.Now()); err != nil {
		t.Errorf("a payload of nothing but PADDING was refused: %v", err)
	}
}

// RFC 9000 §17.2.2: only clients send tokens on Initial packets. A client
// receiving a server Initial with a non-zero token field discards it rather
// than processing it.
func TestClientDiscardsAServerInitialCarryingAToken(t *testing.T) {
	dcid, scid := []byte("dcid0000"), []byte("scid0000")
	c := newConn(&fuzzSink{addr: "backlog:self"}, fakeFuzzAddr("backlog:peer"), dcid, scid, true, DefaultParameters())
	defer c.Close()
	if err := c.installInitial(dcid); err != nil {
		t.Fatal(err)
	}

	// A genuine server Initial — sealed under the keys the client's own
	// destination identifier derives, addressed to the client's SCID — in
	// two variants differing only in the token field.
	build := func(token []byte) []byte {
		secrets, err := initialSecrets(dcid)
		if err != nil {
			t.Fatal(err)
		}
		sealer, err := newPacketSealer(secrets.Server)
		if err != nil {
			t.Fatal(err)
		}
		const pnLen = 4
		var header []byte
		header = append(header, 0xc0|byte(packetInitial)<<4|byte(pnLen-1))
		header = append(header, 0, 0, 0, 1) // version 1
		header = append(header, byte(len(scid)))
		header = append(header, scid...) // the client's own identifier
		header = append(header, byte(len(dcid)))
		header = append(header, dcid...)
		header = AppendVarint(header, uint64(len(token)))
		header = append(header, token...)
		payload := []byte{framePing}
		header = AppendVarint(header, uint64(pnLen+len(payload)+16))
		pnOffset := len(header)
		header = append(header, 0, 0, 0, 0) // packet number 0
		pkt, err := sealer.Seal(nil, header, payload, 0, pnOffset, pnLen)
		if err != nil {
			t.Fatal(err)
		}
		return pkt
	}

	if err := c.receive(build([]byte("bogus-token")), c.peer); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	ranges := len(c.spaces[spaceInitial].ack.ranges)
	discarded := c.discarded
	c.mu.Unlock()
	if ranges != 0 {
		t.Error("a server Initial carrying a token was processed; §17.2.2 says discard it")
	}
	if discarded != 1 {
		t.Errorf("discarded = %d, want 1", discarded)
	}

	// The control: the same packet without a token is processed.
	if err := c.receive(build(nil), c.peer); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	ranges = len(c.spaces[spaceInitial].ack.ranges)
	c.mu.Unlock()
	if ranges != 1 {
		t.Error("the token-free control packet was not processed; the test is checking the wrong thing")
	}
}

// RFC 9000 §18.2: transport-parameter refusals — active_connection_id_limit
// below its minimum of 2, and the server-only stateless_reset_token and
// preferred_address arriving from a client — are TRANSPORT_PARAMETER_ERROR.
func TestTransportParameterRefusals(t *testing.T) {
	appendParam := func(dst []byte, id uint64, val []byte) []byte {
		dst = AppendVarint(dst, id)
		dst = AppendVarint(dst, uint64(len(val)))
		return append(dst, val...)
	}

	t.Run("active_connection_id_limit below 2", func(t *testing.T) {
		raw := appendParam(nil, paramActiveConnIDLimit, AppendVarint(nil, 1))
		_, err := parseParameters(raw)
		te := &transportError{}
		if !errors.As(err, &te) || te.code != transportParameterError {
			t.Errorf("a limit of 1 returned %v, want TRANSPORT_PARAMETER_ERROR", err)
		}
		if _, err := parseParameters(appendParam(nil, paramActiveConnIDLimit, AppendVarint(nil, 2))); err != nil {
			t.Errorf("the minimum of 2 was refused: %v", err)
		}
	})

	clientParams := func(extra func([]byte) []byte) []byte {
		p := DefaultParameters()
		p.initialSourceCID = []byte{1, 2, 3, 4}
		return extra(appendParameters(nil, p))
	}
	serverConn := func() *Conn {
		c := newConn(&fuzzSink{addr: "backlog:self"}, fakeFuzzAddr("backlog:peer"),
			[]byte{1, 2, 3, 4}, []byte{5, 6, 7, 8}, false, DefaultParameters())
		t.Cleanup(func() { _ = c.Close() })
		return c
	}

	t.Run("client-sent stateless_reset_token", func(t *testing.T) {
		raw := clientParams(func(b []byte) []byte {
			return appendParam(b, paramStatelessResetToken, make([]byte, 16))
		})
		err := serverConn().handlePeerParameters(raw)
		te := &transportError{}
		if !errors.As(err, &te) || te.code != transportParameterError {
			t.Errorf("got %v, want TRANSPORT_PARAMETER_ERROR", err)
		}
	})

	t.Run("client-sent preferred_address", func(t *testing.T) {
		raw := clientParams(func(b []byte) []byte {
			return appendParam(b, paramPreferredAddress, make([]byte, 41))
		})
		err := serverConn().handlePeerParameters(raw)
		te := &transportError{}
		if !errors.As(err, &te) || te.code != transportParameterError {
			t.Errorf("got %v, want TRANSPORT_PARAMETER_ERROR", err)
		}
	})

	t.Run("the same announcements from a server are legal", func(t *testing.T) {
		if err := serverConn().handlePeerParameters(clientParams(func(b []byte) []byte { return b })); err != nil {
			t.Errorf("unadorned client parameters were refused: %v", err)
		}
	})
}

// RFC 9000 §19.7: a NEW_TOKEN frame with an empty token is a
// FRAME_ENCODING_ERROR.
func TestEmptyNewTokenIsAFrameEncodingError(t *testing.T) {
	if _, err := parseFrames(nil, []byte{frameNewToken, 0x00}); err == nil {
		t.Error("a NEW_TOKEN frame with an empty token parsed")
	}
	if _, err := parseFrames(nil, []byte{frameNewToken, 0x01, 0xaa}); err != nil {
		t.Errorf("a one-byte token was refused: %v", err)
	}
}

// RFC 9000 §10.2.3: before the handshake is confirmed, a CONNECTION_CLOSE
// goes out at every level that can still seal — the peer may not hold the
// newest keys — and an application close leaving at a lower level travels as
// the transport form with APPLICATION_ERROR, never with application state
// under handshake keys.
func TestPreConfirmationCloseIsSentAtEverySealableLevel(t *testing.T) {
	pcA, pcB := newLoopbackPair(t)
	dcid := []byte("dcid0000")
	c := newConn(pcA, pcB.LocalAddr(), dcid, []byte("scid0000"), false, DefaultParameters())
	defer c.Close()
	if err := c.installInitial(dcid); err != nil {
		t.Fatal(err)
	}
	hsSecrets, err := initialSecrets([]byte("hskeys00")) // stand-in handshake keys
	if err != nil {
		t.Fatal(err)
	}
	hsSealer, err := newPacketSealer(hsSecrets.Server)
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	c.spaces[spaceHandshake].sealer = hsSealer
	c.sendCloseLocked(0x1234, true) // an application close, mid-handshake
	c.mu.Unlock()

	if err := pcB.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	types := map[byte]bool{}
	buf := make([]byte, 2048)
	for range 2 {
		n, _, err := pcB.ReadFrom(buf)
		if err != nil {
			t.Fatalf("expected a close at both sealable levels, got %v after %v", err, types)
		}
		h, _, err := parseLongHeader(buf[:n])
		if err != nil {
			t.Fatal(err)
		}
		types[h.Type] = true
		if h.Type == packetInitial {
			// Initial keys are public: anyone can check what left in them.
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
				t.Fatal(err)
			}
			frames, err := parseFrames(nil, payload)
			if err != nil {
				t.Fatal(err)
			}
			if len(frames) != 1 || frames[0].Type != frameConnectionClose {
				t.Fatalf("the Initial-level close carries %+v, want one transport CONNECTION_CLOSE", frames)
			}
			if frames[0].value != transportApplicationError {
				t.Errorf("the lower-level close carries code %#x, want APPLICATION_ERROR (%#x)",
					frames[0].value, uint64(transportApplicationError))
			}
		}
	}
	if !types[packetInitial] || !types[packetHandshake] {
		t.Errorf("close packets left at levels %v, want both Initial and Handshake", types)
	}
}

// RFC 9000 §7.2: a client's first Initial names a destination identifier of
// at least eight bytes; Initial secrets must never be derived from a shorter
// one. Both server entry points refuse it.
func TestShortClientDCIDIsRefused(t *testing.T) {
	short, scid := []byte("dcid"), []byte("scid0000")

	t.Run("standalone Accept", func(t *testing.T) {
		pcA, pcB := newLoopbackPair(t)
		_ = pcB
		_, err := Accept(pcA, pcB.LocalAddr(), syntheticInitial(t, short, scid), ServerTLSForTest(t), DefaultParameters())
		if err == nil {
			t.Fatal("Accept took an Initial with a 4-byte destination identifier")
		}
	})

	t.Run("listener", func(t *testing.T) {
		serverPC, clientPC := newLoopbackPair(t)
		l, err := NewListener(serverPC, ServerTLSForTest(t), DefaultParameters(), ListenerConfig{})
		if err != nil {
			t.Fatal(err)
		}
		defer l.Close()
		l.dispatch(sealedClientInitial(t, short, scid), clientPC.LocalAddr(), l.pc)
		if got := l.DiscardedPackets(); got != 1 {
			t.Errorf("discarded = %d, want 1", got)
		}
		l.mu.Lock()
		pending := len(l.pendingConns)
		l.mu.Unlock()
		if pending != 0 {
			t.Errorf("%d pending connections were spawned from a too-short identifier", pending)
		}
	})
}

// RFC 9000 §4.5: a FIN that declares a final size below bytes already
// received retracts data the peer provably sent — FINAL_SIZE_ERROR, exactly
// as a RESET_STREAM below the high-water mark has always been.
func TestFinBelowReceivedBytesIsAFinalSizeError(t *testing.T) {
	c := newConn(&fuzzSink{addr: "backlog:self"}, fakeFuzzAddr("backlog:peer"),
		[]byte{1, 2, 3, 4}, []byte{5, 6, 7, 8}, false, DefaultParameters())
	defer c.Close()
	c.OnStreamFrames(func([]Frame) error { return nil })

	stream := func(offset uint64, data []byte, fin bool) []byte {
		typ := uint64(FrameStream | streamOFF | streamLEN)
		if fin {
			typ |= streamFIN
		}
		b := AppendVarint(nil, typ)
		b = AppendVarint(b, 0)
		b = AppendVarint(b, offset)
		b = AppendVarint(b, uint64(len(data)))
		return append(b, data...)
	}

	if _, err := c.frames(spaceApplication, stream(0, []byte("hello"), false), time.Now()); err != nil {
		t.Fatal(err)
	}
	_, err := c.frames(spaceApplication, stream(0, []byte("he"), true), time.Now())
	te := &transportError{}
	if !errors.As(err, &te) || te.code != transportFinalSize {
		t.Errorf("a FIN below bytes already received returned %v, want FINAL_SIZE_ERROR", err)
	}
}

// processAck releases exactly what the frame's ranges cover — exercised here
// with several disjoint ranges, since the two-pointer merge that replaced the
// per-range rescan is only as good as its handling of the gaps.
func TestProcessAckWithSeveralRanges(t *testing.T) {
	var s sentTracker
	var rtt rttEstimator
	rtt.sample(100*time.Millisecond, 0)
	now := time.Now()
	for pn := range uint64(10) {
		s.record(sentPacket{pn: pn, sentAt: now, size: 10, ackEliciting: true, payload: []byte{byte(pn)}})
	}

	var peer ackTracker
	for _, pn := range []uint64{0, 1, 2, 5, 6, 9} { // ranges 9, 5-6, 0-2
		peer.record(pn, now)
	}
	f := ackFrameFor(t, &peer)
	out, err := s.processAck(now, f.largest, f.Data, &rtt, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if out.newlyAckedCount != 6 || out.newlyAcked != 60 {
		t.Errorf("released %d packets (%d bytes), want 6 (60)", out.newlyAckedCount, out.newlyAcked)
	}
	if !out.largestWasNew || !out.largestEliciting {
		t.Error("the frame's largest (9) was not recognised as newly acknowledged and eliciting")
	}
	// Of the survivors 3, 4, 7, 8: packets 3 and 4 sit packetThreshold below
	// the largest acknowledged and are declared lost; 7 and 8 stay in flight.
	if len(out.lost) != 2 || out.lost[0][0] != 3 || out.lost[1][0] != 4 {
		t.Errorf("lost payloads = %v, want packets 3 and 4", out.lost)
	}
	var kept []uint64
	for _, p := range s.packets {
		kept = append(kept, p.pn)
	}
	if len(kept) != 2 || kept[0] != 7 || kept[1] != 8 {
		t.Errorf("still in flight: %v, want [7 8]", kept)
	}

	// The same frame again: everything it covers is already gone, and the
	// merge must newly acknowledge nothing.
	out, err = s.processAck(now, f.largest, f.Data, &rtt, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if out.newlyAckedCount != 0 {
		t.Errorf("a duplicate ACK newly released %d packets", out.newlyAckedCount)
	}
}

// RFC 9002 §5.3: the declared ACK delay is clamped to the peer's
// max_ack_delay only after the handshake is confirmed — before that the peer
// may legitimately hold ACKs longer, and clamping early folds genuine hold
// time into the round-trip estimate.
func TestAckDelayClampWaitsForHandshakeConfirmation(t *testing.T) {
	run := func(confirmed bool) time.Duration {
		c := newConn(&fuzzSink{addr: "backlog:self"}, fakeFuzzAddr("backlog:peer"),
			[]byte{1, 2, 3, 4}, []byte{5, 6, 7, 8}, false, DefaultParameters())
		defer c.Close()
		c.mu.Lock()
		c.handshakeConfirmed = confirmed
		c.hasPeerParams = true
		c.peerParams.MaxAckDelay = 25
		c.rtt.sample(10*time.Millisecond, 0) // the floor the subtraction is guarded by
		sp := &c.spaces[spaceApplication]
		sp.nextPN = 1
		sp.sent.record(sentPacket{pn: 0, sentAt: time.Now().Add(-100 * time.Millisecond), size: 100, ackEliciting: true})
		c.mu.Unlock()

		// An ACK declaring an 80 ms hold: raw delay 10000 × 2^3 µs.
		f := Frame{Type: frameACK, largest: 0, delay: 10000, Data: []byte{0, 0}}
		if err := c.handleAck(spaceApplication, f, time.Now()); err != nil {
			t.Fatal(err)
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.rtt.smoothed
	}

	// Unconfirmed, the full 80 ms is subtracted from the ~100 ms sample:
	// smoothed ≈ (7·10 + 20)/8 ≈ 11 ms. Clamped to 25 ms it would be ≈ 18 ms.
	if got := run(false); got >= 15*time.Millisecond {
		t.Errorf("pre-confirmation smoothed RTT = %v; the declared delay was clamped too early", got)
	}
	if got := run(true); got < 15*time.Millisecond {
		t.Errorf("post-confirmation smoothed RTT = %v; the max_ack_delay clamp did not apply", got)
	}
}

// RFC 9114 §6.1: an HTTP/3 server should allow at least 100 request streams,
// and this transport's default is what ServeH3 inherits.
func TestDefaultBidiStreamLimitCoversH3(t *testing.T) {
	if got := DefaultParameters().InitialMaxStreamsBidi; got < 100 {
		t.Errorf("InitialMaxStreamsBidi = %d, want at least RFC 9114 §6.1's 100", got)
	}
}

// RFC 9000 §5.2.2: a long-header packet in a version this package does not
// speak is answered with a Version Negotiation packet — identifiers echoed
// swapped, version 1 listed — provided the triggering datagram reaches the
// 1200-byte minimum (§14.1's amplification guard).
func TestListenerSendsVersionNegotiation(t *testing.T) {
	serverPC, clientPC := newLoopbackPair(t)
	l, err := NewListener(serverPC, ServerTLSForTest(t), DefaultParameters(), ListenerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	dcid, scid := []byte("future00"), []byte("cl-scid0")
	foreign := func(size int) []byte {
		b := []byte{0xc3}
		b = append(b, 0x0a, 0x0a, 0x0a, 0x0a) // a version this package does not speak
		b = append(b, byte(len(dcid)))
		b = append(b, dcid...)
		b = append(b, byte(len(scid)))
		b = append(b, scid...)
		return append(b, make([]byte, size-len(b))...)
	}

	l.dispatch(foreign(1200), clientPC.LocalAddr(), l.pc)
	if err := clientPC.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 2048)
	n, _, err := clientPC.ReadFrom(buf)
	if err != nil {
		t.Fatal("no Version Negotiation packet arrived:", err)
	}
	pkt := buf[:n]
	if pkt[0]&0x80 == 0 || pkt[1]|pkt[2]|pkt[3]|pkt[4] != 0 {
		t.Fatalf("the answer is not a Version Negotiation packet: % x", pkt[:5])
	}
	rest := pkt[5:]
	gotDCID, rest, err := connectionID(rest)
	if err != nil {
		t.Fatal(err)
	}
	gotSCID, rest, err := connectionID(rest)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotDCID, scid) || !bytes.Equal(gotSCID, dcid) {
		t.Errorf("identifiers echoed as dcid=%q scid=%q, want them swapped", gotDCID, gotSCID)
	}
	foundV1 := false
	for len(rest) >= 4 {
		if rest[0]|rest[1]|rest[2] == 0 && rest[3] == 1 {
			foundV1 = true
		}
		rest = rest[4:]
	}
	if !foundV1 {
		t.Error("the Version Negotiation packet does not list version 1")
	}

	// Below 1200 bytes nothing is sent — answering would amplify.
	before := l.DiscardedPackets()
	l.dispatch(foreign(600), clientPC.LocalAddr(), l.pc)
	if err := clientPC.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if n, _, err := clientPC.ReadFrom(buf); err == nil {
		t.Errorf("a %d-byte answer left for a 600-byte trigger; §14.1 forbids it", n)
	}
	if l.DiscardedPackets() != before+1 {
		t.Error("the undersized trigger was not counted as discarded")
	}
}

// RFC 9000 §8.1.2: an Initial whose Retry token cannot be validated is
// answered with an immediate INVALID_TOKEN close, so the client learns now
// instead of eating its whole handshake timeout.
func TestInvalidRetryTokenGetsAnImmediateClose(t *testing.T) {
	serverPC, clientPC := newLoopbackPair(t)
	l, err := NewListener(serverPC, ServerTLSForTest(t), DefaultParameters(), ListenerConfig{AlwaysRetry: true})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	// A sealed, padded Initial carrying a token this listener never issued.
	dcid, scid := []byte("post-ret"), []byte("cl-scid0")
	secrets, err := initialSecrets(dcid)
	if err != nil {
		t.Fatal(err)
	}
	sealer, err := newPacketSealer(secrets.Client)
	if err != nil {
		t.Fatal(err)
	}
	token := make([]byte, 64) // long enough to look like a token, verifies as nothing
	const pnLen = 4
	var header []byte
	header = append(header, 0xc0|byte(packetInitial)<<4|byte(pnLen-1))
	header = append(header, 0, 0, 0, 1)
	header = append(header, byte(len(dcid)))
	header = append(header, dcid...)
	header = append(header, byte(len(scid)))
	header = append(header, scid...)
	header = AppendVarint(header, uint64(len(token)))
	header = append(header, token...)
	payload := []byte{framePing}
	if pad := initialPadding(len(header), pnLen, len(payload)); pad > 0 {
		payload = append(payload, make([]byte, pad)...)
	}
	header = AppendVarint(header, uint64(pnLen+len(payload)+16))
	pnOffset := len(header)
	header = append(header, 0, 0, 0, 0)
	pkt, err := sealer.Seal(nil, header, payload, 0, pnOffset, pnLen)
	if err != nil {
		t.Fatal(err)
	}

	l.dispatch(pkt, clientPC.LocalAddr(), l.pc)

	if err := clientPC.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 2048)
	n, _, err := clientPC.ReadFrom(buf)
	if err != nil {
		t.Fatal("no answer arrived for the invalid token:", err)
	}
	h, _, err := parseLongHeader(buf[:n])
	if err != nil || h.Type != packetInitial {
		t.Fatalf("the answer is not an Initial packet: %v", err)
	}
	opener, err := newPacketOpener(secrets.Server)
	if err != nil {
		t.Fatal(err)
	}
	opened, _, err := opener.Open(h.Raw, h.PNOffset, 0)
	if err != nil {
		t.Fatal("the answer does not open under the client's Initial keys:", err)
	}
	frames, err := parseFrames(nil, opened)
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 1 || frames[0].Type != frameConnectionClose || frames[0].value != transportInvalidToken {
		t.Errorf("the answer carries %+v, want one CONNECTION_CLOSE with INVALID_TOKEN (0x0b)", frames)
	}
}

// Standalone [Accept] pays RFC 9000 §8.1's anti-amplification limit exactly
// as a Listener-managed connection does: the accounting is armed before the
// first datagram is processed and released by the first decrypted
// Handshake-level packet — after which the handshake completes as before.
func TestAcceptEnforcesTheAmplificationLimit(t *testing.T) {
	serverPC, clientPC := newLoopbackPair(t)
	serverCfg := ServerTLSForTest(t)

	type result struct {
		conn *Conn
		err  error
	}
	done := make(chan result, 1)
	go func() {
		buf := make([]byte, 2048)
		n, from, err := serverPC.ReadFrom(buf)
		if err != nil {
			done <- result{nil, err}
			return
		}
		c, err := Accept(serverPC, from, append([]byte(nil), buf[:n]...), serverCfg, DefaultParameters())
		done <- result{c, err}
	}()

	client, err := Dial(clientPC, serverPC.LocalAddr(), ClientTLSForTest(), DefaultParameters())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	r := <-done
	if r.err != nil {
		t.Fatal(r.err)
	}
	defer r.conn.Close()

	r.conn.mu.Lock()
	active, recv, sent := r.conn.amplActive, r.conn.amplRecv, r.conn.amplSent
	r.conn.mu.Unlock()
	if active {
		t.Error("the limit is still active after the handshake validated the address")
	}
	if recv == 0 || sent == 0 {
		t.Errorf("no amplification accounting ran (recv=%d, sent=%d); the limit was never armed", recv, sent)
	}
}

// streamFrameBytes encodes one STREAM frame on stream id at offset.
func streamFrameBytes(id, offset uint64, data []byte) []byte {
	b := AppendVarint(nil, FrameStream|streamOFF|streamLEN)
	b = AppendVarint(b, id)
	b = AppendVarint(b, offset)
	b = AppendVarint(b, uint64(len(data)))
	return append(b, data...)
}

// Deltas that arrive before OnStreamFrames registers are replayed to it. The
// hold used to stop at sixteen datagrams while the switch threw the read
// buffers away: everything past the sixteenth packet vanished — a request
// stream truncated mid-frame, nothing counted — after being credited.
func TestEveryDeltaBeforeRegistrationIsReplayed(t *testing.T) {
	c := newConn(&fuzzSink{addr: "backlog:self"}, fakeFuzzAddr("backlog:peer"),
		[]byte{1, 2, 3, 4}, []byte{5, 6, 7, 8}, false, DefaultParameters())
	defer c.Close()

	const packets, size = 48, 100
	var want []byte
	for i := range packets {
		data := make([]byte, size)
		for j := range data {
			data[j] = byte(i*size + j)
		}
		// Two streams interleaved, so the replay's order is tested too.
		id := uint64(i%2) * 4
		off := uint64(i/2) * size
		if _, err := c.frames(spaceApplication, streamFrameBytes(id, off, data), time.Now()); err != nil {
			t.Fatal(err)
		}
		want = append(want, data...)
	}

	var got []byte
	c.OnStreamFrames(func(fs []Frame) error {
		for _, f := range fs {
			if f.Type == FrameStream {
				got = append(got, f.Data...)
			}
		}
		return nil
	})
	if len(got) != len(want) {
		t.Fatalf("the callback was replayed %d bytes of the %d that arrived first", len(got), len(want))
	}
	if string(got) != string(want) {
		t.Fatal("the replay reordered or altered the deltas")
	}
	c.mu.Lock()
	consumed := c.connRecv.consumed
	c.mu.Unlock()
	if consumed != packets*size {
		t.Errorf("connection-level consumption = %d after replaying %d bytes, want each credited once", consumed, packets*size)
	}
}

// Under manual credit the replay credits nothing: the consumer releases
// bytes itself. The switch used to credit the read buffers regardless, and
// the consumer's own release credited the same bytes again — a peer could
// then exceed the connection window by what had arrived before
// registration.
func TestManualCreditReplayDoesNotReopenTheWindow(t *testing.T) {
	params := DefaultParameters()
	params.InitialMaxData = 6000
	c := newConn(&fuzzSink{addr: "backlog:self"}, fakeFuzzAddr("backlog:peer"),
		[]byte{1, 2, 3, 4}, []byte{5, 6, 7, 8}, false, params)
	defer c.Close()
	c.SetManualCredit(true)

	chunk := make([]byte, 100)
	var off uint64
	send := func(n int) error {
		for range n {
			if _, err := c.frames(spaceApplication, streamFrameBytes(0, off, chunk), time.Now()); err != nil {
				return err
			}
			off += uint64(len(chunk))
		}
		return nil
	}
	if err := send(50); err != nil { // 5000 bytes before registration
		t.Fatal(err)
	}
	var held int
	c.OnStreamFrames(func(fs []Frame) error {
		for _, f := range fs {
			held += len(f.Data)
		}
		return nil
	})
	if held != 5000 {
		t.Fatalf("replayed %d bytes, want 5000", held)
	}
	// The consumer still holds all 5000 bytes and has released none: only
	// the 1000 left in the window may arrive.
	if err := send(10); err != nil {
		t.Fatalf("data within the window refused: %v", err)
	}
	err := send(1)
	te := &transportError{}
	if !errors.As(err, &te) || te.code != transportFlowControl {
		t.Errorf("a byte past the 6000-byte window returned %v, want FLOW_CONTROL_ERROR", err)
	}
}

// Reading before registering is unsupported mixing, but it loses nothing:
// what no Read took is replayed, and credited once.
func TestRegistrationAfterAReadReplaysWhatWasNotRead(t *testing.T) {
	c := newConn(&fuzzSink{addr: "backlog:self"}, fakeFuzzAddr("backlog:peer"),
		[]byte{1, 2, 3, 4}, []byte{5, 6, 7, 8}, false, DefaultParameters())
	defer c.Close()
	for i := range 30 {
		if _, err := c.frames(spaceApplication, streamFrameBytes(0, uint64(i)*100, []byte(strings.Repeat("x", 100))), time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	s := <-c.accepted
	if n, err := s.Read(make([]byte, 250)); err != nil || n != 250 {
		t.Fatalf("Read = %d, %v", n, err)
	}
	var got int
	c.OnStreamFrames(func(fs []Frame) error {
		for _, f := range fs {
			got += len(f.Data)
		}
		return nil
	})
	if got != 3000-250 {
		t.Errorf("replayed %d bytes, want the %d no Read took", got, 3000-250)
	}
	c.mu.Lock()
	consumed := c.connRecv.consumed
	c.mu.Unlock()
	if consumed != 3000 {
		t.Errorf("connection-level consumption = %d, want 3000: every byte credited once", consumed)
	}
}

// RFC 9000 §18.2: max_idle_timeout is a varint of milliseconds, so a peer may
// announce up to 2^62-1 of them. Converted with a plain multiplication that
// wraps to -1ms, which this end then took as the smaller timeout and closed
// the connection as idle at once.
func TestHugePeerIdleTimeoutKeepsTheLocalOne(t *testing.T) {
	c := newConn(&fuzzSink{addr: "idle:self"}, fakeFuzzAddr("idle:peer"),
		[]byte{1, 2, 3, 4}, []byte{5, 6, 7, 8}, false, DefaultParameters())
	t.Cleanup(func() { _ = c.Close() })
	p := DefaultParameters()
	p.initialSourceCID = []byte{1, 2, 3, 4}
	p.MaxIdleTimeout = MaxVarint
	if err := c.handlePeerParameters(appendParameters(nil, p)); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	got := c.idleTimeout
	c.mu.Unlock()
	if want := 30 * time.Second; got != want {
		t.Errorf("idle timeout = %v after the peer announced 2^62-1 ms, want the local %v", got, want)
	}
	if got := c.blockedResendInterval(); got != 10*time.Second {
		t.Errorf("blocked resend interval = %v, want a third of the local 30s", got)
	}
}
