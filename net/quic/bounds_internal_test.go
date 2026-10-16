package quic

import (
	"errors"
	"testing"
	"time"
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
	dst = AppendVarint(dst, id)
	dst = AppendVarint(dst, uint64(len(val)))
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
		err := c.frames(spaceApplication, AppendVarint([]byte{typ}, maxStreamsLimit+1), time.Now())
		wantCode(t, err, transportFrameEncoding, "MAX_STREAMS 2^60+1")
		if err := c.frames(spaceApplication, AppendVarint([]byte{typ}, maxStreamsLimit), time.Now()); err != nil {
			t.Errorf("MAX_STREAMS 2^60 was refused: %v", err)
		}
	}
}

// RFC 9000 §4.6: the same bound on initial_max_streams_* is
// TRANSPORT_PARAMETER_ERROR.
func TestInitialMaxStreamsOverTheVarintCeilingIsAParameterError(t *testing.T) {
	for _, id := range []uint64{paramInitialMaxStreamsBidi, paramInitialMaxStreamsUni} {
		_, err := parseParameters(appendParam(nil, id, AppendVarint(nil, maxStreamsLimit+1)))
		wantCode(t, err, transportParameterError, "initial_max_streams 2^60+1")
		p, err := parseParameters(appendParam(nil, id, AppendVarint(nil, maxStreamsLimit)))
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
	twice := appendParam(appendParam(nil, paramInitialMaxData, AppendVarint(nil, 1)), paramInitialMaxData, AppendVarint(nil, 2))
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
	trailing := appendParam(nil, paramInitialMaxData, append(AppendVarint(nil, 1), 0))
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

	if _, err := parseParameters(appendParam(nil, paramInitialMaxData, AppendVarint(nil, 1))); err != nil {
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
	zeros := appendParam(appendParam(clientParamsFor(), paramAckDelayExponent, AppendVarint(nil, 0)), paramMaxAckDelay, AppendVarint(nil, 0))
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
