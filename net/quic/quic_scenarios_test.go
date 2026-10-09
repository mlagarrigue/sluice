package quic

import (
	"errors"
	"testing"
	"time"
)

// Scenario tests that exercise the transport at a level below a full
// handshake: a Conn built directly with newConn, fed frames through
// c.frames the way the read loop would after Open has already authenticated
// them. Most of the scenarios this handoff package asked for already live
// in transport_behaviour_test.go and reassembly_test.go — this file holds
// what was still missing.

// The peer-initiated stream limit is enforced at the QUIC transport layer,
// not only at HTTP/3's: streamFor (receive.go) checks a newly named stream's
// ordinal against localMaxStreamsBidi/Uni and refuses with
// STREAM_LIMIT_ERROR (RFC 9000 §4.6) before the stream is ever created. The
// stream this test opens last is the first one past [DefaultParameters]'s
// InitialMaxStreamsBidi (100, RFC 9114 §6.1's floor for request streams).
// This is the transport-level enforcement of the *advertised* limit itself,
// distinct from HTTP/3's own request accounting (covered separately in
// net/httpstream/).
func TestPeerExceedingStreamLimitIsRefused(t *testing.T) {
	c := newConn(&fuzzSink{addr: "scenario:self"}, fakeFuzzAddr("scenario:peer"),
		[]byte{1, 2, 3, 4}, []byte{5, 6, 7, 8}, false, DefaultParameters())
	defer c.Close()

	// Switch to batch mode so a peer-initiated stream never blocks on the
	// accept queue — this test cares about the limit check, not about who
	// drains AcceptStream.
	c.OnStreamFrames(func([]Frame) error { return nil })

	limit := DefaultParameters().InitialMaxStreamsBidi
	streamFrame := func(id uint64) []byte {
		// A zero-length, non-FIN STREAM frame: enough to name the stream,
		// nothing for flow control or reassembly to do.
		frame := AppendVarint(nil, FrameStream|streamOFF|streamLEN)
		frame = AppendVarint(frame, id)
		frame = AppendVarint(frame, 0) // offset
		frame = AppendVarint(frame, 0) // length
		return frame
	}

	// Client-initiated bidirectional stream identifiers are 0, 4, 8, ... —
	// the low two bits name bidi/client. Opening id N implicitly opens every
	// lower-numbered stream of the same kind (§3.2), so one frame per
	// identifier, in order, exercises the ordinal count the same way a real
	// peer would.
	for i := range limit {
		id := i * 4
		if _, err := c.frames(spaceApplication, streamFrame(id), time.Now()); err != nil {
			t.Fatalf("stream %d (within the %d-stream limit): %v", i, limit, err)
		}
	}

	// The next stream (ordinal limit+1) is past what this end granted and
	// none has been retired to earn a slot back.
	overID := limit * 4
	_, err := c.frames(spaceApplication, streamFrame(overID), time.Now())
	if err == nil {
		t.Fatalf("stream ordinal %d, past the %d-stream limit, was accepted", limit+1, limit)
	}
	var te *transportError
	if !errors.As(err, &te) {
		t.Fatalf("the refusal was %v (%T), want a *transportError", err, err)
	}
	if te.code != transportStreamLimit {
		t.Fatalf("the refusal carried code %#x, want STREAM_LIMIT_ERROR (%#x)", te.code, transportStreamLimit)
	}
}

// The unidirectional limit is enforced the same way, on the other bit
// pattern (id&0x02 != 0) and the other counter (localMaxStreamsUni).
func TestPeerExceedingUniStreamLimitIsRefused(t *testing.T) {
	c := newConn(&fuzzSink{addr: "scenario:self"}, fakeFuzzAddr("scenario:peer"),
		[]byte{1, 2, 3, 4}, []byte{5, 6, 7, 8}, false, DefaultParameters())
	defer c.Close()
	c.OnStreamFrames(func([]Frame) error { return nil })

	limit := DefaultParameters().InitialMaxStreamsUni
	streamFrame := func(id uint64) []byte {
		frame := AppendVarint(nil, FrameStream|streamOFF|streamLEN)
		frame = AppendVarint(frame, id)
		frame = AppendVarint(frame, 0)
		frame = AppendVarint(frame, 0)
		return frame
	}

	// Client-initiated unidirectional stream identifiers are 2, 6, 10, ...
	for i := range limit {
		id := i*4 + 2
		if _, err := c.frames(spaceApplication, streamFrame(id), time.Now()); err != nil {
			t.Fatalf("uni stream %d (within the %d-stream limit): %v", i, limit, err)
		}
	}

	overID := limit*4 + 2
	_, err := c.frames(spaceApplication, streamFrame(overID), time.Now())
	var te *transportError
	if !errors.As(err, &te) {
		t.Fatalf("the refusal was %v (%T), want a *transportError", err, err)
	}
	if te.code != transportStreamLimit {
		t.Fatalf("the refusal carried code %#x, want STREAM_LIMIT_ERROR (%#x)", te.code, transportStreamLimit)
	}
}
