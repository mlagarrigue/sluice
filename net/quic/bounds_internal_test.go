package quic

import (
	"bytes"
	"crypto/tls"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/internal/varint"
)

// The bounds and presence checks RFC 9000 names: each case closes with the
// code the RFC gives it, and a parameter announced at zero is not mistaken
// for its absence.

func wantCode(t *testing.T, err error, code uint64, what string) {
	t.Helper()
	te := &transportError{}
	if !errors.As(err, &te) || te.code != code {
		t.Errorf("%s returned %v, want transport error %#x", what, err, code)
	}
}

func appendParam(dst []byte, id uint64, val []byte) []byte {
	dst = varint.Append(dst, id)
	dst = varint.Append(dst, uint64(len(val)))
	return append(dst, val...)
}

// clientParamsFor is a client's parameter block as the server conn below
// expects it, with extra appended.
func clientParamsFor(extra ...byte) []byte {
	p := DefaultParameters()
	p.initialSourceCID = []byte{1, 2, 3, 4}
	return append(appendParameters(nil, p), extra...)
}

func boundsConn(t *testing.T, isClient bool) *Conn {
	t.Helper()
	c := newConn(&fuzzSink{addr: "bounds:self"}, fakeFuzzAddr("bounds:peer"),
		[]byte{1, 2, 3, 4}, []byte{5, 6, 7, 8}, isClient, DefaultParameters())
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// RFC 9000 §4.6, §19.11: a MAX_STREAMS over 2^60 is FRAME_ENCODING_ERROR;
// 2^60 itself is the ceiling and legal.
func TestMaxStreamsOverTheVarintCeilingIsAFrameEncodingError(t *testing.T) {
	for _, typ := range []byte{frameMaxStreams, frameMaxStreams | 1} {
		c := boundsConn(t, false)
		err := c.frames(spaceApplication, varint.Append([]byte{typ}, maxStreamsLimit+1), time.Now())
		wantCode(t, err, transportFrameEncoding, "MAX_STREAMS 2^60+1")
		if err := c.frames(spaceApplication, varint.Append([]byte{typ}, maxStreamsLimit), time.Now()); err != nil {
			t.Errorf("MAX_STREAMS 2^60 was refused: %v", err)
		}
	}
}

// RFC 9000 §4.6: the same bound on initial_max_streams_* is
// TRANSPORT_PARAMETER_ERROR.
func TestInitialMaxStreamsOverTheVarintCeilingIsAParameterError(t *testing.T) {
	for _, id := range []uint64{paramInitialMaxStreamsBidi, paramInitialMaxStreamsUni} {
		_, err := parseParameters(appendParam(nil, id, varint.Append(nil, maxStreamsLimit+1)))
		wantCode(t, err, transportParameterError, "initial_max_streams 2^60+1")
		p, err := parseParameters(appendParam(nil, id, varint.Append(nil, maxStreamsLimit)))
		if err != nil || p.InitialMaxStreamsBidi+p.InitialMaxStreamsUni != maxStreamsLimit {
			t.Errorf("initial_max_streams 2^60 parsed as %+v, %v", p, err)
		}
	}
}

// RFC 9000 §19.7: a server that receives NEW_TOKEN closes with
// PROTOCOL_VIOLATION; a client reads the same frame without complaint.
func TestNewTokenReceivedByAServerIsAProtocolViolation(t *testing.T) {
	newToken := []byte{frameNewToken, 0x01, 0xaa}
	err := boundsConn(t, false).frames(spaceApplication, newToken, time.Now())
	wantCode(t, err, transportProtocolViolation, "NEW_TOKEN at a server")
	if err := boundsConn(t, true).frames(spaceApplication, newToken, time.Now()); err != nil {
		t.Errorf("a client refused NEW_TOKEN: %v", err)
	}
}

// RFC 9000 §7.4: an identifier twice is TRANSPORT_PARAMETER_ERROR, whether
// this package decodes it or not.
func TestDuplicateParameterIsAParameterError(t *testing.T) {
	twice := appendParam(appendParam(nil, paramInitialMaxData, varint.Append(nil, 1)), paramInitialMaxData, varint.Append(nil, 2))
	_, err := parseParameters(twice)
	wantCode(t, err, transportParameterError, "initial_max_data twice")

	const unknown = 0x1b // a §18.1 reserved identifier, skipped when seen once
	once := appendParam(nil, unknown, []byte{1})
	if _, err := parseParameters(once); err != nil {
		t.Errorf("an unknown identifier was refused: %v", err)
	}
	_, err = parseParameters(appendParam(once, unknown, []byte{1}))
	wantCode(t, err, transportParameterError, "an unknown identifier twice")

	const high = 0x4000 // above the 64 identifiers the bitmask tracks
	_, err = parseParameters(appendParam(appendParam(nil, high, nil), high, nil))
	wantCode(t, err, transportParameterError, "a high identifier twice")
}

// RFC 9000 §7.4: an integer parameter is exactly one varint. Bytes after
// it, or a length that cuts it short, are an invalid value; so is a block
// that stops mid-parameter.
func TestIntegerParameterMustBeExactlyOneVarint(t *testing.T) {
	trailing := appendParam(nil, paramInitialMaxData, append(varint.Append(nil, 1), 0))
	_, err := parseParameters(trailing)
	wantCode(t, err, transportParameterError, "a varint followed by a byte")

	short := appendParam(nil, paramInitialMaxData, []byte{0x80}) // a 4-byte varint, 1 byte given
	_, err = parseParameters(short)
	wantCode(t, err, transportParameterError, "a varint cut short")

	_, err = parseParameters([]byte{byte(paramInitialMaxData), 0x04, 0x00})
	wantCode(t, err, transportParameterError, "a value cut short by the block")

	_, err = parseParameters([]byte{0x80})
	wantCode(t, err, transportParameterError, "an identifier cut short")

	_, err = parseParameters([]byte{byte(paramInitialMaxData), 0x40}) // a 2-byte length, 1 byte given
	wantCode(t, err, transportParameterError, "a length cut short")

	if _, err := parseParameters(appendParam(nil, paramInitialMaxData, varint.Append(nil, 1))); err != nil {
		t.Errorf("one exact varint was refused: %v", err)
	}
}

// RFC 9000 §18.2: the defaults of 3 and 25 ms are for an absent parameter;
// an announced 0 is 0.
func TestAckDelayParametersAnnouncedAtZeroAreNotTheDefaults(t *testing.T) {
	c := boundsConn(t, false)
	if err := c.handlePeerParameters(clientParamsFor()); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	exp, delay := c.peerAckDelayExponent(), c.peerMaxAckDelayLocked()
	c.mu.Unlock()
	if exp != 3 || delay != 25*time.Millisecond {
		t.Errorf("absent parameters read as exponent %d, max_ack_delay %v; want the defaults 3 and 25ms", exp, delay)
	}

	c = boundsConn(t, false)
	zeros := appendParam(appendParam(clientParamsFor(), paramAckDelayExponent, varint.Append(nil, 0)), paramMaxAckDelay, varint.Append(nil, 0))
	if err := c.handlePeerParameters(zeros); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	exp, delay = c.peerAckDelayExponent(), c.peerMaxAckDelayLocked()
	c.mu.Unlock()
	if exp != 0 || delay != 0 {
		t.Errorf("parameters announced at 0 read as exponent %d, max_ack_delay %v; want 0 and 0", exp, delay)
	}
}

// RFC 9000 §17.2.1: the unused bits of a Version Negotiation packet are
// arbitrary, with the header form and (SHOULD) the fixed bit set. Drawn, so
// a run of packets is not all one value.
func TestVersionNegotiationDrawsItsUnusedBits(t *testing.T) {
	seen := map[byte]bool{}
	for range 32 {
		first := appendVersionNegotiation(nil, []byte{1}, []byte{2}, version1)[0]
		if first&0xc0 != 0xc0 {
			t.Fatalf("first byte %#x: header form or fixed bit clear", first)
		}
		seen[first&0x3f] = true
	}
	if len(seen) < 2 {
		t.Errorf("32 Version Negotiation packets all share one first byte: %v", seen)
	}
}

// RFC 9000 §17.2.5 gives a Retry's four unused bits the same status as the
// Version Negotiation's: arbitrary, ignored by the client. Drawn the same
// way, under a header that still reads as a long-header Retry whose tag
// verifies, since the tag covers the first byte too.
func TestRetryDrawsItsUnusedBits(t *testing.T) {
	odcid := []byte{9, 9, 9, 9, 9, 9, 9, 9}
	seen := map[byte]bool{}
	for range 32 {
		pkt := appendRetryPacket(nil, []byte{1}, []byte{2}, odcid, []byte("token"))
		if pkt[0]&0xf0 != 0xf0 {
			t.Fatalf("first byte %#x: not a long-header Retry with the fixed bit", pkt[0])
		}
		if !verifyRetryTag(odcid, pkt) {
			t.Fatalf("first byte %#x: the integrity tag does not verify", pkt[0])
		}
		seen[pkt[0]&0x0f] = true
	}
	if len(seen) < 2 {
		t.Errorf("32 Retry packets all share one first byte: %v", seen)
	}
}

// RFC 9000 §7.3: initial_source_connection_id is mandatory for both
// endpoints, and its absence is TRANSPORT_PARAMETER_ERROR — also against a
// peer whose own identifier is zero-length, where the absent parameter used
// to compare equal to the empty identifier. Present and empty is the legal
// announcement of such a peer, and stays accepted.
func TestMissingInitialSourceCIDIsRefusedEvenForAnEmptyPeerCID(t *testing.T) {
	// A client block with every identifier parameter left out.
	absent := appendParam(nil, paramInitialMaxData, varint.Append(nil, 1<<20))
	absent = appendParam(absent, paramInitialMaxStreamsBidi, varint.Append(nil, 100))

	c := boundsConn(t, false)
	c.mu.Lock()
	c.dcid = nil // the peer's source identifier is zero-length
	c.mu.Unlock()
	wantCode(t, c.handlePeerParameters(absent), transportParameterError, "an absent initial_source_connection_id against an empty peer identifier")

	c = boundsConn(t, false)
	c.mu.Lock()
	c.dcid = nil
	c.mu.Unlock()
	present := appendParam(absent, paramInitialSourceCID, nil)
	if err := c.handlePeerParameters(present); err != nil {
		t.Errorf("a present, empty initial_source_connection_id from a peer with an empty identifier was refused: %v", err)
	}

	// This end announces its own identifier even when it is empty.
	var p TransportParameters
	if got := appendParameters(nil, p); !bytes.Contains(got, []byte{byte(paramInitialSourceCID), 0}) {
		t.Errorf("an empty initial_source_connection_id was not announced: %x", got)
	}
}

// RFC 9000 §8.1: prior to validating the client address, a server MUST NOT
// send more than three times the bytes it has received. A 0-RTT packet held
// for its keys and replayed by releaseEarly must not raise that budget a
// second time: its bytes arrived once. Until 2026-10-26 the replay went back
// through admitSource, which counted them again — 36 bytes received counted
// as 72, so the allowance grew by three times the replayed bytes while the
// client's address was still unvalidated. A client that coalesces its 0-RTT
// with its Initial, the ordinary way to send it, reached it on every
// connection.
func TestDeferredEarlyReplayIsNotCountedTwice(t *testing.T) {
	zeroRTT := func(dcid []byte) []byte {
		b := []byte{0xc0 | packetZeroRTT<<4, 0, 0, 0, 1, byte(len(dcid))}
		b = append(b, dcid...)
		b = append(b, 0)
		b = varint.Append(b, 24)
		return append(b, make([]byte, 24)...)
	}

	server := boundsConn(t, false)
	server.mu.Lock()
	server.early = newEarlyGate(0, 0) // a server that admits 0-RTT holds the packet
	server.amplActive, server.amplRecv, server.amplSent = true, 0, 0
	server.mu.Unlock()

	datagram := zeroRTT(server.scid)
	if err := server.receiveAt(datagram, fakeFuzzAddr("bounds:peer"), time.Now()); err != nil {
		t.Fatal(err)
	}
	server.mu.Lock()
	held, afterArrival := len(server.deferredEarly), server.amplRecv
	server.mu.Unlock()
	if held != 1 {
		t.Fatalf("the 0-RTT packet was not held: %d deferred", held)
	}
	if afterArrival != int64(len(datagram)) {
		t.Fatalf("amplRecv = %d after arrival, want %d (the datagram, counted once)", afterArrival, len(datagram))
	}

	// The early secret arrives; the held packet is replayed.
	s, err := suiteFor(tls.TLS_AES_128_GCM_SHA256)
	if err != nil {
		t.Fatal(err)
	}
	opener, err := newOpenerSuite(make([]byte, 32), s)
	if err != nil {
		t.Fatal(err)
	}
	server.mu.Lock()
	server.earlyOpener = opener
	server.mu.Unlock()
	if err := server.releaseEarly(); err != nil {
		t.Fatal(err)
	}

	server.mu.Lock()
	afterReplay, active := server.amplRecv, server.amplActive
	server.mu.Unlock()
	if !active {
		t.Fatal("the amplification limit was lifted; the client address is not validated yet")
	}
	if afterReplay != int64(len(datagram)) {
		t.Errorf("amplRecv = %d after the replay, want %d: the replayed bytes were counted a second time, raising the 3x budget by %d",
			afterReplay, len(datagram), 3*(afterReplay-int64(len(datagram))))
	}
}

// RFC 9000 §10.1 floors the idle timeout at three probe timeouts, and says
// so as a MUST with no bound on the result. The PTO adds the peer's
// max_ack_delay whole, which §18.2 lets it announce up to 2^14-1 ms, plus a
// smoothed RTT its delayed acknowledgements inflate with no ceiling: the
// floor therefore passed DefaultParameters' MaxIdleTimeout by a factor the
// peer chose with one transport parameter, and a Listener's connection keeps
// its routing entry and its MaxConns and MaxConnsPerIP slots for the whole
// of it. maxIdleFloor caps it, the same deviation maxClosingPeriod takes
// against §10.2.
func TestTheIdleFloorIsCapped(t *testing.T) {
	c := boundsConn(t, false)
	c.mu.Lock()
	c.idleTimeout = 100 * time.Millisecond
	c.peerParams.MaxAckDelay = (1<<14 - 1) * time.Millisecond // §18.2's ceiling
	c.peerParams.sawMaxAckDelay = true
	c.hasPeerParams = true
	// Last activity long enough ago that the capped floor has passed and the
	// uncapped one has not: the cap is the only thing that decides.
	c.idleDeadline = time.Now().Add(-maxIdleFloor - time.Second)
	uncapped := 3 * c.rtt.pto(c.peerMaxAckDelayLocked())
	c.mu.Unlock()
	if uncapped <= maxIdleFloor {
		t.Fatalf("three PTOs come to %v, not over the cap of %v; the case under test is not reached", uncapped, maxIdleFloor)
	}

	c.actOnDeadlines(time.Now())
	select {
	case <-c.closed:
	default:
		t.Errorf("the connection is still live %v after its last activity, with an idle timeout of %v: three uncapped PTOs come to %v, which a peer chooses with one transport parameter, and a Listener holds its MaxConns slot for all of it",
			maxIdleFloor+time.Second, c.idleTimeout, uncapped)
	}
}

// The floor itself stays: a connection whose peer is slow enough that three
// PTOs exceed the idle timeout is not torn down at the timeout, which is
// what §10.1's MUST is for — room for several probes to be sent and lost.
func TestTheIdleFloorStillDefersTheTimeout(t *testing.T) {
	c := boundsConn(t, false)
	c.mu.Lock()
	// An idle timeout far under three PTOs, and a deadline already past it.
	c.idleTimeout = 10 * time.Millisecond
	c.peerParams.MaxAckDelay = 500 * time.Millisecond
	c.peerParams.sawMaxAckDelay = true
	c.hasPeerParams = true
	c.idleDeadline = time.Now().Add(-5 * time.Millisecond)
	floor := 3 * c.rtt.pto(c.peerMaxAckDelayLocked())
	c.mu.Unlock()
	if floor <= c.idleTimeout || floor > maxIdleFloor {
		t.Fatalf("three PTOs come to %v, want between the idle timeout %v and the cap %v; the case under test is not reached",
			floor, c.idleTimeout, maxIdleFloor)
	}
	c.actOnDeadlines(time.Now())
	select {
	case <-c.closed:
		t.Error("the connection was given up at its idle timeout although three PTOs had not passed: §10.1's floor is gone")
	default:
	}
}

// The floored deadline is also what the loops wait for. While the floor
// holds a connection past its raw idle deadline, actOnDeadlines returns the
// floored deadline as the next one pending — never the raw one, which is in
// the past: timerLoop would then compute wait = 0, re-arm at once and spin,
// taking c.mu at each turn, until the floor let go. readLoop and readLoopFed
// read the same deadline (idleDeadlineLocked) for the same reason.
func TestIdleFloorDoesNotSpinTheLoops(t *testing.T) {
	c := boundsConn(t, false)
	c.mu.Lock()
	c.idleTimeout = 10 * time.Millisecond
	c.peerParams.MaxAckDelay = 500 * time.Millisecond
	c.peerParams.sawMaxAckDelay = true
	c.hasPeerParams = true
	c.idleDeadline = time.Now().Add(-5 * time.Millisecond)
	floor := 3 * c.rtt.pto(c.peerMaxAckDelayLocked())
	floored := c.idleDeadline.Add(floor - c.idleTimeout)
	c.mu.Unlock()
	if floor <= c.idleTimeout || floor > maxIdleFloor {
		t.Fatalf("three PTOs come to %v, want between the idle timeout %v and the cap %v; the case under test is not reached",
			floor, c.idleTimeout, maxIdleFloor)
	}

	now := time.Now()
	next := c.actOnDeadlines(now)
	select {
	case <-c.closed:
		t.Fatal("the connection was given up at its idle timeout although three PTOs had not passed: §10.1's floor is gone")
	default:
	}
	if next.IsZero() {
		t.Fatal("actOnDeadlines returned no deadline while the idle floor still runs")
	}
	if !next.After(now) {
		t.Fatalf("actOnDeadlines returned a deadline %v before now: timerLoop waits 0 and spins until the floor lets go, %v later",
			now.Sub(next), floored.Sub(now))
	}
	if !next.Equal(floored) {
		t.Errorf("actOnDeadlines returned %v as the next deadline, want the floored idle deadline %v", next, floored)
	}
}

// Every number this package puts on the wire travels as a variable-length
// integer, bounded at 2^62-1 (RFC 9000 §16), and internal/varint.Append
// panics past it. A value the caller passes — a transport parameter, an
// error code — is refused at the API with an error naming it, before
// anything changes: TransportParameters.check used to let 2^62 through and
// appendParameters panicked on the first handshake, on a Listener's read
// goroutine; CloseWithError used to panic inside closeOnce with c.mu held
// and the sync.Once consumed, leaving a connection nothing could lock or
// close; Reset and StopSending used to panic after marking the stream.
func TestCallerValuesPastTheVarintCeilingAreRefused(t *testing.T) {
	t.Run("transport parameters", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			set  func(p *TransportParameters, v uint64)
		}{
			{"InitialMaxData", func(p *TransportParameters, v uint64) { p.InitialMaxData = v }},
			{"InitialMaxStreamDataUni", func(p *TransportParameters, v uint64) { p.InitialMaxStreamDataUni = v }},
			{"InitialMaxStreamDataBidiLocal", func(p *TransportParameters, v uint64) { p.InitialMaxStreamDataBidiLocal = v }},
			{"InitialMaxStreamDataBidiRemote", func(p *TransportParameters, v uint64) { p.InitialMaxStreamDataBidiRemote = v }},
			{"ActiveConnIDLimit", func(p *TransportParameters, v uint64) { p.ActiveConnIDLimit = v }},
		} {
			for _, v := range []uint64{1 << 62, ^uint64(0)} {
				p := DefaultParameters()
				tc.set(&p, v)
				err := p.check()
				if err == nil || !strings.Contains(err.Error(), tc.name) {
					t.Errorf("%s = %d: check returned %v, want an error naming the field", tc.name, v, err)
				}
			}
			p := DefaultParameters()
			tc.set(&p, varint.Max)
			if err := p.check(); err != nil {
				t.Errorf("%s = 2^62-1: check returned %v, want nil — the ceiling itself is encodable", tc.name, err)
			}
		}
		for _, tc := range []struct {
			name string
			set  func(p *TransportParameters, v uint64)
		}{
			{"InitialMaxStreamsBidi", func(p *TransportParameters, v uint64) { p.InitialMaxStreamsBidi = v }},
			{"InitialMaxStreamsUni", func(p *TransportParameters, v uint64) { p.InitialMaxStreamsUni = v }},
		} {
			p := DefaultParameters()
			tc.set(&p, maxStreamsLimit+1)
			if err := p.check(); err == nil || !strings.Contains(err.Error(), tc.name) {
				t.Errorf("%s = 2^60+1: check returned %v, want an error naming the field (§4.6)", tc.name, err)
			}
			tc.set(&p, maxStreamsLimit)
			if err := p.check(); err != nil {
				t.Errorf("%s = 2^60: check returned %v, want nil", tc.name, err)
			}
		}
		// What check accepts, appendParameters encodes without a panic.
		p := DefaultParameters()
		p.InitialMaxData, p.InitialMaxStreamsBidi = varint.Max, maxStreamsLimit
		if err := p.check(); err != nil {
			t.Fatal(err)
		}
		_ = appendParameters(nil, p)
	})

	t.Run("close codes", func(t *testing.T) {
		c := boundsConn(t, false)
		installAppSealer(t, c)
		if err := c.CloseWithError(1 << 62); err == nil {
			t.Fatal("CloseWithError(1<<62) returned nil, want a refusal")
		}
		if err := c.ShutdownWithError(t.Context(), 1<<62); err == nil {
			t.Fatal("ShutdownWithError(ctx, 1<<62) returned nil, want a refusal")
		}
		// The timer loop takes c.mu on its own schedule, so a TryLock
		// would fail by chance: a Lock that returns within a second is
		// the proof the refusal left the mutex free.
		locked := make(chan struct{})
		go func() {
			c.mu.Lock()
			defer c.mu.Unlock()
			close(locked)
		}()
		select {
		case <-locked:
		case <-time.After(time.Second):
			t.Fatal("c.mu is held after the refusals: the connection can no longer be locked")
		}
		select {
		case <-c.closed:
			t.Fatal("the connection closed on a refused code")
		default:
		}
		// The sync.Once was not consumed: a close with a legal code still
		// closes.
		if err := c.CloseWithError(1); err != nil {
			t.Fatal(err)
		}
		select {
		case <-c.closed:
		default:
			t.Fatal("CloseWithError(1) after a refused code did not close the connection: closeOnce was consumed by the refusal")
		}
	})

	t.Run("stream codes", func(t *testing.T) {
		c := boundsConn(t, false)
		installAppSealer(t, c)
		c.mu.Lock()
		c.peerMaxStreamsBidi = 1
		c.hasPeerParams = true
		c.mu.Unlock()
		s, err := c.OpenStream(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Reset(1 << 62); err == nil {
			t.Error("Reset(1<<62) returned nil, want a refusal")
		}
		if err := s.StopSending(1 << 62); err == nil {
			t.Error("StopSending(1<<62) returned nil, want a refusal")
		}
		c.mu.Lock()
		reset, sendClosed := s.writeReset, s.sendClosed
		c.mu.Unlock()
		s.mu.Lock()
		taken := s.endTaken
		s.mu.Unlock()
		if reset || sendClosed || taken {
			t.Errorf("after the refusals writeReset=%v sendClosed=%v endTaken=%v, want all false: a refused code changes nothing", reset, sendClosed, taken)
		}
	})
}

// peerRetiredSelf is a ring that never grows with the connection's age:
// every NEW_CONNECTION_ID scans it (retiredSelfLocked), so a list that kept
// one sequence per migration would make that scan grow for the connection's
// life. The newest retirement is remembered, the oldest past the ring is not.
func TestRetiredSelfRingIsBounded(t *testing.T) {
	c := boundsConn(t, false)
	c.mu.Lock()
	defer c.mu.Unlock()
	const retired = 10 * maxRetiredSelf
	for seq := uint64(1); seq <= retired; seq++ {
		c.noteRetiredSelfLocked(seq)
	}
	if got := len(c.peerRetiredSelf); got > maxRetiredSelf {
		t.Fatalf("%d sequences remembered after %d retirements, want at most %d", got, retired, maxRetiredSelf)
	}
	if !c.retiredSelfLocked(retired) {
		t.Error("the newest retirement is not remembered")
	}
	if c.retiredSelfLocked(1) {
		t.Error("the oldest retirement is still remembered past the ring")
	}
}

// One packet of 1 451 PINGs parses to as many frames; the parse scratch
// that held them is not what the connection keeps for the next packet.
// An ordinary packet's scratch still is, which is what the scratch is for.
func TestParseScratchDoesNotKeepAPacketOfPings(t *testing.T) {
	c := assembleConn(&fuzzSink{addr: "scratch:self"}, fakeFuzzAddr("scratch:peer"), []byte{1, 2, 3, 4}, []byte{5, 6, 7, 8}, false, DefaultParameters())
	t.Cleanup(func() { _ = c.Close() })
	pings := bytes.Repeat([]byte{framePing}, 1451)
	if err := c.frames(spaceApplication, pings, time.Now()); err != nil {
		t.Fatal(err)
	}
	if got := cap(c.parseScratch); got > maxKeptFrames {
		t.Errorf("after 1451 PINGs the connection keeps a scratch of %d frames, want at most %d", got, maxKeptFrames)
	}
	if err := c.frames(spaceApplication, pings[:3], time.Now()); err != nil {
		t.Fatal(err)
	}
	if c.parseScratch == nil {
		t.Error("an ordinary packet's scratch was not kept")
	}
}
