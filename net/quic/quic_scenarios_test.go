package quic

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/internal/varint"
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
	c.OnStreamFrames(CreditOnDelivery, func([]Frame) error { return nil })

	limit := DefaultParameters().InitialMaxStreamsBidi
	streamFrame := func(id uint64) []byte {
		// A zero-length, non-FIN STREAM frame: enough to name the stream,
		// nothing for flow control or reassembly to do.
		frame := varint.Append(nil, FrameStream|streamOFF|streamLEN)
		frame = varint.Append(frame, id)
		frame = varint.Append(frame, 0) // offset
		frame = varint.Append(frame, 0) // length
		return frame
	}

	// Client-initiated bidirectional stream identifiers are 0, 4, 8, ... —
	// the low two bits name bidi/client. Opening id N implicitly opens every
	// lower-numbered stream of the same kind (§3.2), so one frame per
	// identifier, in order, exercises the ordinal count the same way a real
	// peer would.
	for i := range limit {
		id := i * 4
		if err := c.frames(spaceApplication, streamFrame(id), time.Now()); err != nil {
			t.Fatalf("stream %d (within the %d-stream limit): %v", i, limit, err)
		}
	}

	// The next stream (ordinal limit+1) is past what this end granted and
	// none has been retired to earn a slot back.
	overID := limit * 4
	err := c.frames(spaceApplication, streamFrame(overID), time.Now())
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
	c.OnStreamFrames(CreditOnDelivery, func([]Frame) error { return nil })

	limit := DefaultParameters().InitialMaxStreamsUni
	streamFrame := func(id uint64) []byte {
		frame := varint.Append(nil, FrameStream|streamOFF|streamLEN)
		frame = varint.Append(frame, id)
		frame = varint.Append(frame, 0)
		frame = varint.Append(frame, 0)
		return frame
	}

	// Client-initiated unidirectional stream identifiers are 2, 6, 10, ...
	for i := range limit {
		id := i*4 + 2
		if err := c.frames(spaceApplication, streamFrame(id), time.Now()); err != nil {
			t.Fatalf("uni stream %d (within the %d-stream limit): %v", i, limit, err)
		}
	}

	overID := limit*4 + 2
	err := c.frames(spaceApplication, streamFrame(overID), time.Now())
	var te *transportError
	if !errors.As(err, &te) {
		t.Fatalf("the refusal was %v (%T), want a *transportError", err, err)
	}
	if te.code != transportStreamLimit {
		t.Fatalf("the refusal carried code %#x, want STREAM_LIMIT_ERROR (%#x)", te.code, transportStreamLimit)
	}
}

// STREAM_DATA_BLOCKED names a stream the peer is sending on, so it goes
// through the same checks as a STREAM frame (RFC 9000 §3.2): one past the
// advertised limit is STREAM_LIMIT_ERROR (§4.6), one for a unidirectional
// stream this end opened — which the peer cannot be blocked on — is
// STREAM_STATE_ERROR (§19.13). It used to be inert with DATA_BLOCKED.
func TestStreamDataBlockedIsCheckedLikeAStreamFrame(t *testing.T) {
	for _, tt := range []struct {
		name string
		id   uint64
		code uint64
	}{
		{"past the bidirectional stream limit", DefaultParameters().InitialMaxStreamsBidi * 4, transportStreamLimit},
		{"on a send-only stream of this end's", 3, transportStreamState}, // server-initiated unidirectional
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := newConn(&fuzzSink{addr: "scenario:self"}, fakeFuzzAddr("scenario:peer"),
				[]byte{1, 2, 3, 4}, []byte{5, 6, 7, 8}, false, DefaultParameters())
			defer c.Close()
			c.OnStreamFrames(CreditOnDelivery, func([]Frame) error { return nil })

			err := c.frames(spaceApplication, appendFrame(nil, frameStreamDataBlocked, tt.id, 0), time.Now())
			var te *transportError
			if !errors.As(err, &te) {
				t.Fatalf("STREAM_DATA_BLOCKED for stream %d gave %v (%T), want a *transportError", tt.id, err, err)
			}
			if te.code != tt.code {
				t.Fatalf("the refusal carried code %#x, want %#x", te.code, tt.code)
			}
		})
	}
}

// RFC 9000 §3.5: STOP_SENDING is only sent for a stream the peer has not
// reset — once its RESET_STREAM is in there is nothing to stop, and the
// HTTP/3 server asks after its own Reset, when that is common. A send-only
// stream of this end's has no peer sending part at all: an error, no frame.
func TestStopSendingIsNotSentWhereItMeansNothing(t *testing.T) {
	c := newConn(&fuzzSink{addr: "scenario:self"}, fakeFuzzAddr("scenario:peer"),
		[]byte{1, 2, 3, 4}, []byte{5, 6, 7, 8}, false, DefaultParameters())
	defer c.Close()
	c.OnStreamFrames(CreditOnDelivery, func([]Frame) error { return nil })

	s, err := c.Stream(0) // client-initiated bidirectional, picked ahead of its first frame
	if err != nil {
		t.Fatal(err)
	}
	if err := c.frames(spaceApplication, appendFrame(nil, FrameResetStream, 0, 7, 0), time.Now()); err != nil {
		t.Fatalf("the peer's RESET_STREAM was refused: %v", err)
	}
	c.mu.Lock()
	before := c.spaces[spaceApplication].nextPN
	c.mu.Unlock()
	if err := s.StopSending(1); err != nil {
		t.Errorf("StopSending after the peer's reset = %v, want nil and no frame", err)
	}
	c.mu.Lock()
	after := c.spaces[spaceApplication].nextPN
	c.mu.Unlock()
	if after != before {
		t.Errorf("StopSending after the peer's reset sent a packet (pn %d → %d)", before, after)
	}

	uni := newStream(c, 3) // server-initiated unidirectional: this end sends, the peer only receives
	if err := uni.StopSending(1); err == nil || !errors.Is(err, ErrQUIC) || !strings.Contains(err.Error(), "send-only") {
		t.Errorf("StopSending on a send-only stream = %v, want an ErrQUIC naming the send-only stream", err)
	}
}
