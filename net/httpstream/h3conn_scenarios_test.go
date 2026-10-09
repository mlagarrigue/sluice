package httpstream

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/net/quic"
)

// resetFrame is the RESET_STREAM frame the transport delivers when the peer
// abandons a stream.
func resetFrame(id uint64) quic.Frame {
	return quic.Frame{Type: quic.FrameResetStream, StreamID: id}
}

// A body trickled across many small STREAM frames whose total runs past
// MaxBodyBytes must still be refused, not accumulated without bound. Frames
// checks the raw arriving bytes before they even reach drain's frame parser
// (h3conn.go:137), and drain checks the parsed body again as it grows
// (h3conn.go:221) — this exercises the first of those against a peer that
// never sends a frame big enough to trip a single-shot check, only many that
// add up.
func TestH3BodyOverManyDatagramsPastMaxBodyBytesIsRefused(t *testing.T) {
	const maxBody = 64
	cfg := Config{MaxBodyBytes: maxBody}.withDefaults()
	a := newH3Assembler(cfg)

	// HEADERS alone first, stream left open.
	batch, _, failed, err := a.Frames([]quic.Frame{
		streamFrame(0, h3Request("POST", "/upload"), false),
	})
	if err != nil {
		t.Fatalf("the HEADERS frame alone returned %v", err)
	}
	if batch.Len() != 0 || len(failed) != 0 {
		t.Fatalf("a request completed or failed before any body arrived: batch=%d failed=%v", batch.Len(), failed)
	}

	// Then the body, ten bytes at a time — many datagrams, each individually
	// well under the bound, whose sum is not. Each trickle is a whole,
	// validly framed DATA frame: the point is a peer that never sends a
	// single frame big enough to trip a one-shot check, only many that add
	// up, so each one has to parse as far as drain does before the running
	// total is what refuses it.
	payload := []byte("0123456789")
	sent := 0
	for sent < maxBody+3*len(payload) {
		fin := sent+len(payload) >= maxBody+3*len(payload)
		frame := appendH3Frame(nil, h3Data, payload)
		batch, _, failed, err = a.Frames([]quic.Frame{streamFrame(0, frame, fin)})
		if err != nil {
			t.Fatalf("Frames failed the connection over one stream's body: %v", err)
		}
		if len(failed) > 0 {
			if !errors.Is(failed[0].Err, ErrTooLarge) {
				t.Fatalf("the stream was refused with %v, want ErrTooLarge", failed[0].Err)
			}
			if failed[0].Code != h3ExcessiveLoad {
				t.Errorf("refused with code %#x, want H3_EXCESSIVE_LOAD (%#x)", failed[0].Code, h3ExcessiveLoad)
			}
			return // refused, as it must be — the scenario is satisfied
		}
		if batch.Len() != 0 {
			t.Fatal("a request over the body bound was accepted rather than refused")
		}
		sent += len(payload)
	}
	t.Fatalf("a body of %d bytes was accepted whole, over the %d-byte bound", sent, maxBody)
}

// The 65th concurrently open request stream on one connection: RFC 9114
// §5.2 wants a request refused rather than the connection lost once a server
// is at its limit, and H3Assembler enforces it as
// [Config.MaxConcurrentStreams] — concurrently open, unfinished request
// streams, not a lifetime total. The default is 64 (httpstream.go's
// withDefaults, matching the transport's MAX_STREAMS grant), so the 65th
// distinct stream opened while the other 64 are still unfinished is the one
// that must be refused. MaxRequestsPerBatch is deliberately set to its
// documented sweet spot of 8 here: a batching knob, it must not influence
// admission, and before the two were separated it silently did.
func TestH3The65thConcurrentStreamIsRefused(t *testing.T) {
	a := newH3Assembler(Config{MaxRequestsPerBatch: 8})
	if a.cfg.MaxConcurrentStreams != 64 {
		t.Fatalf("MaxConcurrentStreams defaults to %d, this test assumes 64", a.cfg.MaxConcurrentStreams)
	}

	var frames []quic.Frame
	for i := range a.cfg.MaxConcurrentStreams {
		id := uint64(4 * i) // client-initiated bidirectional
		// HEADERS with no FIN: every stream stays open, occupying a slot.
		frames = append(frames, streamFrame(id, h3Request("GET", "/x"), false))
	}
	batch, _, failed, err := a.Frames(frames)
	if err != nil {
		t.Fatalf("filling the connection to its limit returned %v", err)
	}
	if batch.Len() != 0 || len(failed) != 0 {
		t.Fatalf("the first %d streams should all still be open: batch=%d failed=%v",
			a.cfg.MaxConcurrentStreams, batch.Len(), failed)
	}

	// The 65th distinct stream.
	id65 := uint64(4 * a.cfg.MaxConcurrentStreams)
	_, _, failed, err = a.Frames([]quic.Frame{streamFrame(id65, h3Request("GET", "/over"), false)})
	if err != nil {
		t.Fatalf("the 65th stream failed the connection rather than being refused on its own: %v", err)
	}
	if len(failed) != 1 || failed[0].StreamID != id65 {
		t.Fatalf("failed = %v, want stream %d refused", failed, id65)
	}
	if !errors.Is(failed[0].Err, ErrH3Protocol) {
		t.Errorf("refusal error = %v, want it to wrap ErrH3", failed[0].Err)
	}
	if failed[0].Code != h3RequestRejected {
		t.Errorf("refused with code %#x, want H3_REQUEST_REJECTED (%#x)", failed[0].Code, h3RequestRejected)
	}
}

// maxRefusedRemembered (h3conn.go) bounds the tombstones kept for streams
// this assembler already refused, at 256 — the other number the 257-stream
// scenario in the handoff doc points at once MaxConcurrentStreams' own limit
// turns out to be 64 rather than a round 256. Refusing 257 distinct streams
// must still hold: no panic, no unbounded growth of the refused set, and the
// FIFO eviction promised by the comment on maxRefusedRemembered actually
// drops the oldest tombstone once the 257th arrives.
func TestH3The257thRefusalEvictsTheOldestTombstone(t *testing.T) {
	if maxRefusedRemembered != 256 {
		t.Fatalf("maxRefusedRemembered is %d, this test assumes 256", maxRefusedRemembered)
	}
	// A malformed request refuses its stream outright and cheaply: no
	// :method at all.
	badRequest := appendH3Frame(nil, h3Headers, appendQPACK(nil, []Header{
		{Name: []byte(":path"), Value: []byte("/x")},
		{Name: []byte(":scheme"), Value: []byte("https")},
	}))

	a := newH3Assembler(Config{})
	for i := range maxRefusedRemembered + 1 {
		id := uint64(4 * i)
		_, _, failed, err := a.Frames([]quic.Frame{streamFrame(id, badRequest, true)})
		if err != nil {
			t.Fatalf("stream %d (i=%d) failed the connection: %v", id, i, err)
		}
		if len(failed) != 1 {
			t.Fatalf("stream %d (i=%d): %d failures, want 1", id, i, len(failed))
		}
	}
	if len(a.refused) != maxRefusedRemembered {
		t.Fatalf("%d tombstones remembered, want the bound of %d held exactly", len(a.refused), maxRefusedRemembered)
	}
	// Stream 0 was the first refused and must be the one evicted; stream
	// (maxRefusedRemembered)*4 was the 257th and must still be a tombstone.
	if _, gone := a.refused[0]; gone {
		t.Error("the oldest tombstone (stream 0) is still remembered past the 256-entry bound")
	}
	lastID := uint64(4 * maxRefusedRemembered)
	if _, gone := a.refused[lastID]; !gone {
		t.Errorf("the 257th refused stream (%d) was not remembered", lastID)
	}
}

// A datagram that carries a stream's deltas and then its RESET_STREAM — the
// ordinary packet order for a peer cancelling mid-upload — must leave no
// assembly state behind. ServeH3 used to run every Forget in a separate pass
// before Frames re-walked the same slice, so the deltas recreated an entry
// the transport had already abandoned: never completable, never evicted, one
// admission slot burnt per occurrence. MaxConcurrentStreams such datagrams
// wedged all new requests for the connection's lifetime.
func TestH3ResetInPacketOrderFreesTheAdmissionSlot(t *testing.T) {
	a := newH3Assembler(Config{})
	partial := h3Request("POST", "/cancelled")[:8] // mid-frame, never completable

	for i := range a.cfg.MaxConcurrentStreams {
		id := uint64(4 * i)
		_, _, failed, err := a.Frames([]quic.Frame{
			streamFrame(id, partial, false),
			resetFrame(id),
		})
		if err != nil {
			t.Fatalf("datagram %d failed the connection: %v", i, err)
		}
		if len(failed) != 0 {
			t.Fatalf("datagram %d refused a stream the peer already reset: %v", i, failed)
		}
	}
	if len(a.streams) != 0 {
		t.Fatalf("%d cancelled streams still hold assembly state", len(a.streams))
	}

	// Every slot must be free again: a fresh request on the next stream.
	id := uint64(4 * a.cfg.MaxConcurrentStreams)
	batch, _, failed, err := a.Frames([]quic.Frame{streamFrame(id, h3Request("GET", "/after"), true)})
	if err != nil {
		t.Fatal(err)
	}
	if len(failed) != 0 {
		t.Fatalf("a request after %d cancellations was refused: %v", a.cfg.MaxConcurrentStreams, failed)
	}
	if batch.Len() != 1 {
		t.Fatalf("%d requests completed, want 1", batch.Len())
	}
}

// Deltas on the other side of the reset — later in the same datagram — must
// not resurrect the stream either: Forget leaves a tombstone now, exactly as
// a refusal does.
func TestH3DeltasAfterAResetLandOnATombstone(t *testing.T) {
	a := newH3Assembler(Config{})
	req := h3Request("GET", "/ghost")

	batch, _, failed, err := a.Frames([]quic.Frame{
		streamFrame(0, req[:4], false),
		resetFrame(0),
		streamFrame(0, req[4:], true), // stragglers, already in flight
	})
	if err != nil {
		t.Fatal(err)
	}
	if batch.Len() != 0 || len(failed) != 0 {
		t.Fatalf("a cancelled stream completed or was re-refused: batch=%d failed=%v", batch.Len(), failed)
	}
	if len(a.streams) != 0 {
		t.Fatal("the cancelled stream's stragglers recreated assembly state")
	}
}

// RFC 9114 §4.1.2: a content-length that disagrees with the DATA sum is a
// malformed request — the same integrity check the h1 parser and the h2 side
// make, at the first moment the sum is final.
func TestH3ContentLengthMustMatchTheBody(t *testing.T) {
	build := func(cl string, body []byte) []byte {
		stream := h3Request("POST", "/upload", [2]string{"content-length", cl})
		return appendH3Frame(stream, h3Data, body)
	}

	// Agreement is served.
	a := newH3Assembler(Config{})
	batch, _, failed, err := a.Frames([]quic.Frame{streamFrame(0, build("3", []byte("abc")), true)})
	if err != nil {
		t.Fatal(err)
	}
	if batch.Len() != 1 || len(failed) != 0 {
		t.Fatalf("a request with a matching content-length was refused: batch=%d failed=%v", batch.Len(), failed)
	}

	// Disagreement is one stream's refusal.
	for _, tt := range []struct {
		name string
		cl   string
		body []byte
	}{
		{"short body", "10", []byte("abc")},
		{"long body", "1", []byte("abc")},
		{"length on an empty body", "10", nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			a := newH3Assembler(Config{})
			_, _, failed, err := a.Frames([]quic.Frame{streamFrame(0, build(tt.cl, tt.body), true)})
			if err != nil {
				t.Fatalf("one malformed request failed the connection: %v", err)
			}
			if len(failed) != 1 || failed[0].Code != h3MessageError {
				t.Fatalf("failed = %v, want one H3_MESSAGE_ERROR refusal", failed)
			}
		})
	}

	// Two lengths on one request is the smuggling family's opening move.
	t.Run("a repeated content-length", func(t *testing.T) {
		a := newH3Assembler(Config{})
		stream := h3Request("POST", "/u", [2]string{"content-length", "3"}, [2]string{"content-length", "3"})
		stream = appendH3Frame(stream, h3Data, []byte("abc"))
		_, _, failed, err := a.Frames([]quic.Frame{streamFrame(0, stream, true)})
		if err != nil {
			t.Fatal(err)
		}
		if len(failed) != 1 || failed[0].Code != h3MessageError {
			t.Fatalf("failed = %v, want one H3_MESSAGE_ERROR refusal", failed)
		}
	})
}

// RFC 9114 §7.1: a clean FIN inside a frame means the declared tail never
// arrived — H3_FRAME_ERROR, not a truncated body served as an intact one.
func TestH3FinMidFrameIsAFrameError(t *testing.T) {
	stream := h3Request("POST", "/cut")
	stream = quic.AppendVarint(stream, h3Data)
	stream = quic.AppendVarint(stream, 10)
	stream = append(stream, []byte("abc")...) // 3 of the 10 declared bytes

	a := newH3Assembler(Config{})
	batch, _, failed, err := a.Frames([]quic.Frame{streamFrame(0, stream, true)})
	if err != nil {
		t.Fatal(err)
	}
	if batch.Len() != 0 {
		t.Fatal("a request truncated mid-frame was served")
	}
	if len(failed) != 1 || failed[0].Code != h3FrameError {
		t.Fatalf("failed = %v, want one H3_FRAME_ERROR refusal", failed)
	}
}

// Trailers are not served, but their bytes answer to the same octet rules as
// everything else that could be re-serialised toward an HTTP/1.1 hop — and a
// frame after them is a second request trying to ride the stream.
func TestH3TrailersAreValidatedThenDropped(t *testing.T) {
	head := h3Request("POST", "/t")
	head = appendH3Frame(head, h3Data, []byte("x"))

	trailers := func(fields ...Header) []byte {
		return appendH3Frame(nil, h3Headers, appendQPACK(nil, fields))
	}

	// Clean trailers: the request completes, the trailers do not reach it.
	a := newH3Assembler(Config{})
	stream := append(append([]byte(nil), head...), trailers(Header{Name: []byte("x-sum"), Value: []byte("ok")})...)
	batch, _, failed, err := a.Frames([]quic.Frame{streamFrame(0, stream, true)})
	if err != nil {
		t.Fatal(err)
	}
	if batch.Len() != 1 || len(failed) != 0 {
		t.Fatalf("a request with clean trailers was refused: batch=%d failed=%v", batch.Len(), failed)
	}
	if got := batch.Items[0].Get("x-sum"); got != nil {
		t.Errorf("a trailer field reached the request as %q", got)
	}

	for _, tt := range []struct {
		name  string
		bytes []byte
		code  uint64
	}{
		{"a pseudo-header in the trailers", trailers(Header{Name: []byte(":status"), Value: []byte("200")}), h3MessageError},
		{"a CR in a trailer value", trailers(Header{Name: []byte("x-sum"), Value: []byte("a\rb")}), h3MessageError},
		{"a trailer name that is not a token", trailers(Header{Name: []byte("x sum"), Value: []byte("v")}), h3MessageError},
		// A frame after the trailers is an invalid sequence, which RFC 9114
		// §4.1 answers with H3_FRAME_UNEXPECTED — the same refusal the h2
		// side makes for DATA after END_HEADERS trailers.
		{"DATA after the trailers", append(trailers(Header{Name: []byte("x-sum"), Value: []byte("v")}), appendH3Frame(nil, h3Data, []byte("y"))...), h3FrameUnexpected},
		{"HEADERS after the trailers", append(trailers(Header{Name: []byte("x-sum"), Value: []byte("v")}), trailers(Header{Name: []byte("x-more"), Value: []byte("v")})...), h3FrameUnexpected},
	} {
		t.Run(tt.name, func(t *testing.T) {
			a := newH3Assembler(Config{})
			stream := append(append([]byte(nil), head...), tt.bytes...)
			batch, _, failed, err := a.Frames([]quic.Frame{streamFrame(0, stream, true)})
			if err != nil {
				t.Fatalf("one malformed stream failed the connection: %v", err)
			}
			if batch.Len() != 0 {
				t.Fatal("a malformed stream's request was served")
			}
			if len(failed) != 1 || failed[0].Code != tt.code {
				t.Fatalf("failed = %v, want one refusal with code %#x", failed, tt.code)
			}
		})
	}
}

// RFC 9114 §4.1 again, from the other side of the request: DATA arriving
// before any HEADERS is an invalid frame sequence, refused with
// H3_FRAME_UNEXPECTED rather than treated as a malformed message.
func TestH3DataBeforeHeadersIsFrameUnexpected(t *testing.T) {
	a := newH3Assembler(Config{})
	stream := appendH3Frame(nil, h3Data, []byte("early"))
	batch, _, failed, err := a.Frames([]quic.Frame{streamFrame(0, stream, true)})
	if err != nil {
		t.Fatalf("one malformed stream failed the connection: %v", err)
	}
	if batch.Len() != 0 {
		t.Fatal("a stream with DATA before HEADERS was served")
	}
	if len(failed) != 1 || failed[0].Code != h3FrameUnexpected {
		t.Fatalf("failed = %v, want one H3_FRAME_UNEXPECTED refusal", failed)
	}
}

// uniOpen builds the bytes a client unidirectional stream opens with: its
// type varint, then whatever follows.
func uniOpen(typ uint64, rest ...byte) []byte {
	return append(quic.AppendVarint(nil, typ), rest...)
}

func settingsFrame(id, value uint64) []byte {
	payload := quic.AppendVarint(nil, id)
	payload = quic.AppendVarint(payload, value)
	return appendH3Frame(nil, h3Settings, payload)
}

// connErr asserts err is an H3ConnectionError carrying code.
func connErr(t *testing.T, err error, code uint64) {
	t.Helper()
	var ce h3ConnectionError
	if !errors.As(err, &ce) {
		t.Fatalf("err = %v, want an H3ConnectionError", err)
	}
	if ce.Code != code {
		t.Fatalf("connection error code %#x, want %#x", ce.Code, code)
	}
	if !errors.Is(err, ErrH3Protocol) {
		t.Fatalf("err = %v, want it to wrap ErrH3", err)
	}
}

// RFC 9114 §6.2.1 and RFC 9204 §4.2: the client's unidirectional streams are
// read far enough to take its SETTINGS and to enforce the critical-stream
// error regime — which used to be entirely unenforced because every client
// unidirectional stream was skipped unread.
func TestH3ClientUnidirectionalStreams(t *testing.T) {
	t.Run("SETTINGS on the control stream are parsed", func(t *testing.T) {
		a := newH3Assembler(Config{})
		open := append(uniOpen(h3StreamControl), settingsFrame(h3SettingMaxFieldSection, 4096)...)
		if _, _, _, err := a.Frames([]quic.Frame{streamFrame(2, open, false)}); err != nil {
			t.Fatalf("a conformant control stream was refused: %v", err)
		}
		if a.peerMaxFieldSection() != 4096 {
			t.Fatalf("MAX_FIELD_SECTION_SIZE = %d, want 4096", a.peerMaxFieldSection())
		}
	})

	t.Run("a control stream split across datagrams", func(t *testing.T) {
		a := newH3Assembler(Config{})
		open := append(uniOpen(h3StreamControl), settingsFrame(h3SettingMaxFieldSection, 512)...)
		cut := 2
		if _, _, _, err := a.Frames([]quic.Frame{streamFrame(2, open[:cut], false)}); err != nil {
			t.Fatal(err)
		}
		if _, _, _, err := a.Frames([]quic.Frame{streamFrame(2, open[cut:], false)}); err != nil {
			t.Fatal(err)
		}
		if a.peerMaxFieldSection() != 512 {
			t.Fatalf("MAX_FIELD_SECTION_SIZE = %d after a split delivery", a.peerMaxFieldSection())
		}
	})

	t.Run("first control frame not SETTINGS", func(t *testing.T) {
		a := newH3Assembler(Config{})
		open := append(uniOpen(h3StreamControl), appendH3Frame(nil, h3GoAway, []byte{0x00})...)
		_, _, _, err := a.Frames([]quic.Frame{streamFrame(2, open, false)})
		connErr(t, err, h3MissingSettings)
	})

	t.Run("a GREASE frame before SETTINGS", func(t *testing.T) {
		// RFC 9114 §6.2.1: SETTINGS must be the very first frame. A reserved
		// frame type is skippable everywhere else (§7.2.8), but not where
		// SETTINGS has to stand.
		a := newH3Assembler(Config{})
		open := append(uniOpen(h3StreamControl), appendH3Frame(nil, 0x21, []byte{0xde, 0xad})...)
		open = append(open, settingsFrame(h3SettingMaxFieldSection, 64)...)
		_, _, _, err := a.Frames([]quic.Frame{streamFrame(2, open, false)})
		connErr(t, err, h3MissingSettings)
	})

	t.Run("a reserved HTTP/2 setting identifier", func(t *testing.T) {
		// §7.2.4.1: HTTP/2's settings with no HTTP/3 meaning are reserved,
		// and receiving one is H3_SETTINGS_ERROR rather than a skip.
		for _, id := range []uint64{0x00, 0x02, 0x03, 0x04, 0x05} {
			a := newH3Assembler(Config{})
			open := append(uniOpen(h3StreamControl), settingsFrame(id, 1)...)
			_, _, _, err := a.Frames([]quic.Frame{streamFrame(2, open, false)})
			connErr(t, err, h3SettingsError)
		}
	})

	t.Run("an insert on the QPACK encoder stream", func(t *testing.T) {
		// RFC 9204 §3.2.3: a capacity of zero was advertised, so the encoder
		// must not send instructions that use the dynamic table. An Insert
		// With Name Reference (leading bit 1) is refused as
		// QPACK_ENCODER_STREAM_ERROR.
		a := newH3Assembler(Config{})
		_, _, _, err := a.Frames([]quic.Frame{streamFrame(2, uniOpen(h3StreamQPACKEncode, 0xc1, 0x00), false)})
		connErr(t, err, qpackEncoderStreamError)
	})

	t.Run("a non-zero capacity on the QPACK encoder stream", func(t *testing.T) {
		// Set Dynamic Table Capacity (001xxxxx) above the advertised zero is
		// the error RFC 9204 §4.3.1 names.
		a := newH3Assembler(Config{})
		_, _, _, err := a.Frames([]quic.Frame{streamFrame(2, uniOpen(h3StreamQPACKEncode, 0x21), false)})
		connErr(t, err, qpackEncoderStreamError)
	})

	t.Run("a capacity of zero on the QPACK encoder stream is tolerated", func(t *testing.T) {
		// Setting the capacity to the advertised zero changes nothing, and
		// §4.3.1 only refuses a value that exceeds the limit.
		a := newH3Assembler(Config{})
		if _, _, _, err := a.Frames([]quic.Frame{streamFrame(2, uniOpen(h3StreamQPACKEncode, 0x20), false)}); err != nil {
			t.Fatalf("a capacity of zero was refused: %v", err)
		}
		// And an insert after it is still refused: the capacity is still zero.
		_, _, _, err := a.Frames([]quic.Frame{streamFrame(2, []byte{0x80}, false)})
		connErr(t, err, qpackEncoderStreamError)
	})

	t.Run("a second SETTINGS on the control stream", func(t *testing.T) {
		a := newH3Assembler(Config{})
		open := append(uniOpen(h3StreamControl), settingsFrame(h3SettingMaxFieldSection, 64)...)
		open = append(open, settingsFrame(h3SettingMaxFieldSection, 64)...)
		_, _, _, err := a.Frames([]quic.Frame{streamFrame(2, open, false)})
		connErr(t, err, h3FrameUnexpected)
	})

	t.Run("a request frame on the control stream", func(t *testing.T) {
		a := newH3Assembler(Config{})
		open := append(uniOpen(h3StreamControl), settingsFrame(h3SettingMaxFieldSection, 64)...)
		open = append(open, appendH3Frame(nil, h3Data, []byte("x"))...)
		_, _, _, err := a.Frames([]quic.Frame{streamFrame(2, open, false)})
		connErr(t, err, h3FrameUnexpected)
	})

	t.Run("a second control stream", func(t *testing.T) {
		a := newH3Assembler(Config{})
		open := append(uniOpen(h3StreamControl), settingsFrame(h3SettingMaxFieldSection, 64)...)
		if _, _, _, err := a.Frames([]quic.Frame{streamFrame(2, open, false)}); err != nil {
			t.Fatal(err)
		}
		_, _, _, err := a.Frames([]quic.Frame{streamFrame(6, uniOpen(h3StreamControl), false)})
		connErr(t, err, h3StreamCreationError)
	})

	t.Run("a second QPACK encoder stream", func(t *testing.T) {
		a := newH3Assembler(Config{})
		if _, _, _, err := a.Frames([]quic.Frame{streamFrame(2, uniOpen(h3StreamQPACKEncode), false)}); err != nil {
			t.Fatal(err)
		}
		_, _, _, err := a.Frames([]quic.Frame{streamFrame(6, uniOpen(h3StreamQPACKEncode), false)})
		connErr(t, err, h3StreamCreationError)
	})

	t.Run("a second QPACK decoder stream", func(t *testing.T) {
		a := newH3Assembler(Config{})
		if _, _, _, err := a.Frames([]quic.Frame{streamFrame(2, uniOpen(h3StreamQPACKDecode), false)}); err != nil {
			t.Fatal(err)
		}
		_, _, _, err := a.Frames([]quic.Frame{streamFrame(6, uniOpen(h3StreamQPACKDecode), false)})
		connErr(t, err, h3StreamCreationError)
	})

	t.Run("a client-opened push stream", func(t *testing.T) {
		a := newH3Assembler(Config{})
		_, _, _, err := a.Frames([]quic.Frame{streamFrame(2, uniOpen(h3StreamPush), false)})
		connErr(t, err, h3StreamCreationError)
	})

	t.Run("the control stream closed by FIN", func(t *testing.T) {
		a := newH3Assembler(Config{})
		open := append(uniOpen(h3StreamControl), settingsFrame(h3SettingMaxFieldSection, 64)...)
		_, _, _, err := a.Frames([]quic.Frame{streamFrame(2, open, true)})
		connErr(t, err, h3ClosedCriticalStream)
	})

	t.Run("the control stream reset", func(t *testing.T) {
		a := newH3Assembler(Config{})
		open := append(uniOpen(h3StreamControl), settingsFrame(h3SettingMaxFieldSection, 64)...)
		if _, _, _, err := a.Frames([]quic.Frame{streamFrame(2, open, false)}); err != nil {
			t.Fatal(err)
		}
		_, _, _, err := a.Frames([]quic.Frame{resetFrame(2)})
		connErr(t, err, h3ClosedCriticalStream)
	})

	t.Run("a QPACK stream closed by FIN", func(t *testing.T) {
		a := newH3Assembler(Config{})
		_, _, _, err := a.Frames([]quic.Frame{streamFrame(2, uniOpen(h3StreamQPACKEncode), true)})
		connErr(t, err, h3ClosedCriticalStream)
	})

	t.Run("an unknown stream type is discarded without error", func(t *testing.T) {
		a := newH3Assembler(Config{})
		if _, _, _, err := a.Frames([]quic.Frame{streamFrame(2, uniOpen(0x21, 0xde, 0xad), false)}); err != nil {
			t.Fatalf("a reserved unidirectional stream type was refused: %v", err)
		}
		// Closed or reset, still nobody's business but the peer's (§6.2).
		if _, _, _, err := a.Frames([]quic.Frame{streamFrame(2, nil, true)}); err != nil {
			t.Fatalf("closing an unknown unidirectional stream was refused: %v", err)
		}
	})

	t.Run("a stream reset before its type byte is tolerated", func(t *testing.T) {
		a := newH3Assembler(Config{})
		if _, _, _, err := a.Frames([]quic.Frame{resetFrame(2)}); err != nil {
			t.Fatalf("a uni stream reset before its type was refused: %v", err)
		}
	})
}

// The transport re-credits every delivered byte while the assembler still
// retains it, so flow control does not bound what a connection's unfinished
// uploads hold together — the 4×MaxBodyBytes aggregate does, the same factor
// the h2 side uses for the same reason.
func TestH3AggregateAssemblyBytesAreBounded(t *testing.T) {
	// Sized so no single stream ever trips its own per-stream bound: each
	// parks well under MaxBodyBytes, and only their sum crosses the line.
	const maxBody = 256
	cfg := Config{MaxBodyBytes: maxBody}.withDefaults()
	a := newH3Assembler(cfg)

	head := h3Request("POST", "/park")
	body := appendH3Frame(nil, h3Data, bytes.Repeat([]byte("x"), 150))

	refused := false
	for i := range 8 {
		id := uint64(4 * i)
		stream := append(append([]byte(nil), head...), body...)
		_, _, failed, err := a.Frames([]quic.Frame{streamFrame(id, stream, false)}) // no FIN: parked
		if err != nil {
			t.Fatalf("stream %d failed the connection: %v", id, err)
		}
		if a.assembling > 4*maxBody {
			t.Fatalf("%d bytes retained across streams, over the %d aggregate bound", a.assembling, 4*maxBody)
		}
		if len(failed) > 0 {
			if !errors.Is(failed[0].Err, ErrTooLarge) || failed[0].Code != h3ExcessiveLoad {
				t.Fatalf("refused with %v (code %#x), want ErrTooLarge and H3_EXCESSIVE_LOAD", failed[0].Err, failed[0].Code)
			}
			refused = true
			break
		}
	}
	if !refused {
		t.Fatalf("8 streams parked %d bytes without one refusal, against a %d aggregate bound", a.assembling, 4*maxBody)
	}
}

// A request that assembles nothing complete within ReadTimeout is reset —
// the h3 twin of the h2 side's assembly deadline. Without it, a peer
// trickling a byte now and then parks its buffers for as long as it keeps
// the connection's idle timer alive.
func TestH3StalledAssemblyExpires(t *testing.T) {
	cfg := Config{ReadTimeout: time.Minute}.withDefaults()
	a := newH3Assembler(cfg)

	// A request that never finishes.
	before := time.Now()
	if _, _, failed, err := a.Frames([]quic.Frame{streamFrame(0, h3Request("POST", "/stall")[:6], false)}); err != nil || len(failed) != 0 {
		t.Fatalf("the partial request was refused on arrival: failed=%v err=%v", failed, err)
	}
	after := time.Now()

	// The clock is armed at ReadTimeout from arrival...
	st := a.streams[0]
	if st == nil {
		t.Fatal("the partial request holds no assembly state")
	}
	if st.deadline.Before(before.Add(cfg.ReadTimeout)) || st.deadline.After(after.Add(cfg.ReadTimeout)) {
		t.Fatalf("deadline = %v, want ReadTimeout (%v) after arrival", st.deadline, cfg.ReadTimeout)
	}
	// ...and is made to have run out rather than waited out: the subject is
	// what the sweep does with an expired stream, and a sleep would only
	// guess at when one is.
	st.deadline = time.Now().Add(-time.Nanosecond)

	// The next datagram — any datagram — is the sweep's clock.
	_, _, failed, err := a.Frames([]quic.Frame{streamFrame(4, h3Request("GET", "/live"), true)})
	if err != nil {
		t.Fatal(err)
	}
	if len(failed) != 1 || failed[0].StreamID != 0 {
		t.Fatalf("failed = %v, want the stalled stream 0 expired", failed)
	}
	if failed[0].Code != h3RequestIncomplete {
		t.Errorf("expired with code %#x, want H3_REQUEST_INCOMPLETE (%#x)", failed[0].Code, h3RequestIncomplete)
	}
	if len(a.streams) != 0 {
		t.Error("the expired stream still holds assembly state")
	}
	if a.assembling != 0 {
		t.Errorf("assembling = %d after everything expired or completed, want 0", a.assembling)
	}
}

// A HEADERS frame answers to MaxHeaderBytes, not MaxBodyBytes — and to its
// *declared* length, before the payload is buffered: otherwise a peer parks
// a body-sized buffer and buys a QPACK decode 32× the advertised
// MAX_FIELD_SECTION_SIZE.
func TestH3HeadersFramesAreBoundedByMaxHeaderBytes(t *testing.T) {
	cfg := Config{MaxHeaderBytes: 128, MaxBodyBytes: 4096}.withDefaults()

	// A complete HEADERS frame between the two bounds.
	big := h3Request("GET", "/big", [2]string{"x-pad", string(bytes.Repeat([]byte("p"), 200))})
	a := newH3Assembler(cfg)
	batch, _, failed, err := a.Frames([]quic.Frame{streamFrame(0, big, true)})
	if err != nil {
		t.Fatal(err)
	}
	if batch.Len() != 0 {
		t.Fatal("a HEADERS frame over MaxHeaderBytes was served")
	}
	if len(failed) != 1 || !errors.Is(failed[0].Err, ErrTooLarge) || failed[0].Code != h3ExcessiveLoad {
		t.Fatalf("failed = %v, want one ErrTooLarge refusal with H3_EXCESSIVE_LOAD", failed)
	}

	// A declared-oversized HEADERS whose payload has barely begun to arrive:
	// refused on the declaration, not buffered while it trickles in.
	declared := quic.AppendVarint(nil, h3Headers)
	declared = quic.AppendVarint(declared, 2000)
	declared = append(declared, bytes.Repeat([]byte("h"), 10)...)
	a = newH3Assembler(cfg)
	_, _, failed, err = a.Frames([]quic.Frame{streamFrame(0, declared, false)})
	if err != nil {
		t.Fatal(err)
	}
	if len(failed) != 1 || !errors.Is(failed[0].Err, ErrTooLarge) {
		t.Fatalf("failed = %v, want the declared length refused before the payload arrives", failed)
	}
	if len(a.streams) != 0 {
		t.Error("the refused stream still parks its partial HEADERS")
	}
}

// A body of exactly MaxBodyBytes is within the bound, and must be admitted
// however it is delivered — including as its HEADERS and one DATA frame in a
// single STREAM frame, which the raw-buffer check used to refuse: it held
// the frame headers and the head against the body's bound. One byte more is
// still refused, by the frame and body bounds drain applies.
func TestH3BodyOfExactlyMaxBodyBytesInOneDelivery(t *testing.T) {
	const maxBody = 4096
	for _, tc := range []struct {
		size int
		ok   bool
	}{{maxBody, true}, {maxBody + 1, false}} {
		a := newH3Assembler(Config{MaxBodyBytes: maxBody}.withDefaults())
		body := bytes.Repeat([]byte("b"), tc.size)
		stream := appendH3Frame(h3Request("POST", "/upload"), h3Data, body)
		batch, _, failed, err := a.Frames([]quic.Frame{streamFrame(0, stream, true)})
		if err != nil {
			t.Fatalf("%d bytes: %v", tc.size, err)
		}
		if tc.ok {
			if len(failed) != 0 || batch.Len() != 1 || len(batch.Items[0].Body) != tc.size {
				t.Errorf("a %d-byte body: %d requests, failed %v", tc.size, batch.Len(), failed)
			}
			continue
		}
		if len(failed) != 1 || batch.Len() != 0 {
			t.Errorf("a %d-byte body: %d requests, failed %v; want it refused", tc.size, batch.Len(), failed)
		}
	}
}

// RFC 9114 §7.2.4: unknown settings are ignored. The assembler keeps the one
// value it acts on and nothing else; it used to keep a map of every
// identifier the client sent — thousands, in a SETTINGS frame of
// MaxHeaderBytes — for the connection's life.
func TestH3AssemblerKeepsOnlyTheSettingItUses(t *testing.T) {
	control := func(unknown int) []byte {
		var payload []byte
		for i := range unknown {
			payload = quic.AppendVarint(payload, 0x21+0x1f*uint64(i)) // reserved, unknown
			payload = quic.AppendVarint(payload, 1)
		}
		payload = quic.AppendVarint(payload, h3SettingMaxFieldSection)
		payload = quic.AppendVarint(payload, 4096)
		return append(uniOpen(h3StreamControl), appendH3Frame(nil, h3Settings, payload)...)
	}
	cost := func(stream []byte) float64 {
		return testing.AllocsPerRun(20, func() {
			a := newH3Assembler(Config{})
			if _, _, _, err := a.Frames([]quic.Frame{streamFrame(2, stream, false)}); err != nil {
				t.Fatal(err)
			}
			if a.peerMaxFieldSection() != 4096 {
				t.Fatalf("MAX_FIELD_SECTION_SIZE = %d, want 4096", a.peerMaxFieldSection())
			}
		})
	}
	few, many := cost(control(1)), cost(control(2000))
	if many > few+2 {
		t.Errorf("2000 unknown settings cost %.0f allocations against %.0f for one", many, few)
	}

	t.Run("a repeated known identifier is refused", func(t *testing.T) {
		a := newH3Assembler(Config{})
		payload := quic.AppendVarint(nil, h3SettingMaxFieldSection)
		payload = quic.AppendVarint(payload, 1)
		payload = quic.AppendVarint(payload, h3SettingMaxFieldSection)
		payload = quic.AppendVarint(payload, 2)
		open := append(uniOpen(h3StreamControl), appendH3Frame(nil, h3Settings, payload)...)
		_, _, _, err := a.Frames([]quic.Frame{streamFrame(2, open, false)})
		connErr(t, err, h3SettingsError)
	})
}

// RFC 9204 §4.4: the decoder stream reports on this server's encoder's
// dynamic table, which is never used. Stream Cancellation is legal — a
// decoder may send one for any reset stream — and the two instructions that
// acknowledge table state are QPACK_DECODER_STREAM_ERROR. The stream used to
// be discarded unread, accepting both.
func TestH3QPACKDecoderStreamIsRead(t *testing.T) {
	for _, tc := range []struct {
		name string
		data [][]byte // successive deliveries after the stream type
		code uint64   // zero: accepted
	}{
		{"stream cancellation", [][]byte{{0x44}}, 0},
		{"stream cancellation split across deliveries", [][]byte{{0x7f, 0x80}, {0x01, 0x48}}, 0},
		{"section acknowledgment", [][]byte{{0x84}}, qpackDecoderStreamError},
		{"insert count increment", [][]byte{{0x01}}, qpackDecoderStreamError},
		{"insert count increment of zero", [][]byte{{0x00}}, qpackDecoderStreamError},
		{"after a cancellation", [][]byte{{0x44, 0x84}}, qpackDecoderStreamError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := newH3Assembler(Config{})
			if _, _, _, err := a.Frames([]quic.Frame{streamFrame(6, uniOpen(h3StreamQPACKDecode), false)}); err != nil {
				t.Fatalf("opening the decoder stream: %v", err)
			}
			var err error
			for _, d := range tc.data {
				if _, _, _, err = a.Frames([]quic.Frame{streamFrame(6, d, false)}); err != nil {
					break
				}
			}
			if tc.code == 0 {
				if err != nil {
					t.Fatalf("refused: %v", err)
				}
				if n := len(a.unis[6].buf); n != 0 {
					t.Errorf("%d bytes of whole instructions still parked", n)
				}
				return
			}
			connErr(t, err, tc.code)
		})
	}
}
