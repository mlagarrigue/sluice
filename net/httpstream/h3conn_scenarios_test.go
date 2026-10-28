package httpstream

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/internal/varint"
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
	cfg := Config{MaxBodyBytes: maxBody}.mustResolve()
	a := testAssembler(cfg)

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
			if failed[0].Status != 413 || failed[0].Code != h3NoError {
				t.Errorf("refused with status %d, code %#x; want 413 then H3_NO_ERROR", failed[0].Status, failed[0].Code)
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

// The stream over the limit on one connection: RFC 9114 §5.2 wants a
// request refused rather than the connection lost once a server is at its
// limit, and H3Assembler enforces it as [Config.MaxConcurrentStreams] —
// concurrently open, unfinished request streams, not a lifetime total. The
// default is 100 (httpstream.go's withDefaults, the floor RFC 9114 §6.1
// recommends and the transport's own MAX_STREAMS grant), so the 101st
// distinct stream opened while the other 100 are still unfinished is the
// one that must be refused. MaxRequestsPerBatch is deliberately set to its
// documented sweet spot of 8 here: a batching knob, it must not influence
// admission, and before the two were separated it silently did.
func TestH3TheStreamOverTheLimitIsRefused(t *testing.T) {
	a := testAssembler(Config{MaxRequestsPerBatch: 8})
	if a.cfg.MaxConcurrentStreams != defaultMaxConcurrency {
		t.Fatalf("MaxConcurrentStreams defaults to %d, not the %d HTTP/2 advertises", a.cfg.MaxConcurrentStreams, defaultMaxConcurrency)
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

	// The first stream past the limit.
	idOver := uint64(4 * a.cfg.MaxConcurrentStreams)
	_, _, failed, err = a.Frames([]quic.Frame{streamFrame(idOver, h3Request("GET", "/over"), false)})
	if err != nil {
		t.Fatalf("the stream over the limit failed the connection rather than being refused on its own: %v", err)
	}
	if len(failed) != 1 || failed[0].StreamID != idOver {
		t.Fatalf("failed = %v, want stream %d refused", failed, idOver)
	}
	if !errors.Is(failed[0].Err, ErrH3Protocol) {
		t.Errorf("refusal error = %v, want it to wrap ErrH3Protocol", failed[0].Err)
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

	a := testAssembler(Config{})
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
// assembly state behind. ServeH3Conn used to run every Forget in a separate pass
// before Frames re-walked the same slice, so the deltas recreated an entry
// the transport had already abandoned: never completable, never evicted, one
// admission slot burnt per occurrence. MaxConcurrentStreams such datagrams
// wedged all new requests for the connection's lifetime.
func TestH3ResetInPacketOrderFreesTheAdmissionSlot(t *testing.T) {
	a := testAssembler(Config{})
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
	a := testAssembler(Config{})
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
	a := testAssembler(Config{})
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
			a := testAssembler(Config{})
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
		a := testAssembler(Config{})
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

// RFC 9114 §7.1: "When a stream terminates cleanly, if the last frame on the
// stream was truncated, this MUST be treated as a connection error of type
// H3_FRAME_ERROR" — the connection's, not the stream's: the declared tail
// never arrived, and a framer that ends streams mid-frame cannot be trusted
// on the next one. It used to reset the stream alone with that code. The
// same holds while an unknown frame is being skipped unread: its bytes are
// owed all the same.
func TestH3FinMidFrameIsAConnectionError(t *testing.T) {
	inData := h3Request("POST", "/cut")
	inData = varint.Append(inData, h3Data)
	inData = varint.Append(inData, 10)
	inData = append(inData, []byte("abc")...) // 3 of the 10 declared bytes

	inReserved := h3Request("POST", "/grease")
	inReserved = varint.Append(inReserved, 0x21+0x1f) // a reserved type
	inReserved = varint.Append(inReserved, 10)
	inReserved = append(inReserved, []byte("abc")...)

	for _, tt := range []struct {
		name   string
		stream []byte
	}{
		{"inside a DATA frame", inData},
		{"inside a reserved frame being skipped", inReserved},
		{"inside a frame header", append(h3Request("POST", "/h"), byte(h3Data))},
	} {
		t.Run(tt.name, func(t *testing.T) {
			a := testAssembler(Config{})
			batch, _, failed, err := a.Frames([]quic.Frame{streamFrame(0, tt.stream, true)})
			if batch.Len() != 0 || len(failed) != 0 {
				t.Fatalf("served or refused on the stream (batch=%d failed=%v); the connection is at fault", batch.Len(), failed)
			}
			connErr(t, err, h3FrameError)
		})
	}
}

// RFC 9114 §4.1: a stream that ends "without enough of the HTTP message to
// provide a complete response" is aborted with H3_REQUEST_INCOMPLETE — the
// message is short, not malformed. A FIN before any HEADERS used to be
// H3_MESSAGE_ERROR, the code for a request that is wrong rather than
// missing.
func TestH3FinBeforeHeadersIsRequestIncomplete(t *testing.T) {
	a := testAssembler(Config{})
	batch, _, failed, err := a.Frames([]quic.Frame{streamFrame(0, nil, true)})
	if err != nil {
		t.Fatal(err)
	}
	if batch.Len() != 0 {
		t.Fatal("an empty stream was served")
	}
	if len(failed) != 1 || failed[0].Code != h3RequestIncomplete {
		t.Fatalf("failed = %v, want one H3_REQUEST_INCOMPLETE refusal", failed)
	}
}

// The one-identifier frames of the control stream — GOAWAY, MAX_PUSH_ID,
// CANCEL_PUSH — are inert for a server that never pushes, and were not read
// at all until 2026-10-21. RFC 9114 still binds their layout (§7.1, §10.8:
// exactly one varint, else H3_FRAME_ERROR) and their identifiers: a GOAWAY
// may not carry a larger push ID than the previous one (§5.2), a
// MAX_PUSH_ID may not shrink (§7.2.7), and a CANCEL_PUSH names a push this
// server never promised (§7.2.3) — each H3_ID_ERROR.
func TestH3ControlIdentifiersAreChecked(t *testing.T) {
	settings := settingsFrame(h3SettingMaxFieldSection, 64)
	id := func(typ uint64, v uint64) []byte { return appendH3Frame(nil, typ, varint.Append(nil, v)) }
	for _, tt := range []struct {
		name   string
		frames [][]byte
		code   uint64 // zero: no error
	}{
		{"GOAWAY then a smaller GOAWAY", [][]byte{id(h3GoAway, 8), id(h3GoAway, 4)}, 0},
		{"GOAWAY then the same GOAWAY", [][]byte{id(h3GoAway, 8), id(h3GoAway, 8)}, 0},
		{"GOAWAY then a larger GOAWAY", [][]byte{id(h3GoAway, 4), id(h3GoAway, 8)}, h3IDError},
		{"a first GOAWAY at the maximum", [][]byte{id(h3GoAway, 1<<62-1)}, 0},
		{"MAX_PUSH_ID then a larger one", [][]byte{id(h3MaxPushID, 4), id(h3MaxPushID, 8)}, 0},
		{"MAX_PUSH_ID then the same one", [][]byte{id(h3MaxPushID, 4), id(h3MaxPushID, 4)}, 0},
		{"MAX_PUSH_ID then a smaller one", [][]byte{id(h3MaxPushID, 8), id(h3MaxPushID, 4)}, h3IDError},
		{"CANCEL_PUSH", [][]byte{id(h3CancelPush, 0)}, h3IDError},
		{"GOAWAY with an empty payload", [][]byte{appendH3Frame(nil, h3GoAway, nil)}, h3FrameError},
		{"GOAWAY with a byte after the identifier", [][]byte{appendH3Frame(nil, h3GoAway, []byte{0x04, 0x00})}, h3FrameError},
		{"MAX_PUSH_ID cut inside the identifier", [][]byte{appendH3Frame(nil, h3MaxPushID, []byte{0x40})}, h3FrameError},
	} {
		t.Run(tt.name, func(t *testing.T) {
			a := testAssembler(Config{})
			open := append(uniOpen(h3StreamControl), settings...)
			for _, fr := range tt.frames {
				open = append(open, fr...)
			}
			_, _, _, err := a.Frames([]quic.Frame{streamFrame(2, open, false)})
			if tt.code == 0 {
				if err != nil {
					t.Fatalf("a legal control frame failed the connection: %v", err)
				}
				return
			}
			connErr(t, err, tt.code)
		})
	}
}

// RFC 9114 §5.2: the GOAWAY promises every request stream below its
// identifier an answer, and ServeH3Conn finishes a draining connection the
// moment nothing is in flight. The two agree only if admitting a stream and
// counting it in flight cannot interleave with the drain's reading of the
// count: a stream admitted but not yet counted when the drain looked was
// served into a connection already finishing. Here streams are admitted on
// one goroutine while the drain is decided on another, many times over; if
// the drain ever finds nothing in flight, nothing may be in flight after it.
func TestH3DrainNeverFinishesOverAnAdmittedStream(t *testing.T) {
	head := h3Request("GET", "/x") // no FIN: the stream stays in flight
	for round := range 2000 {
		a := testAssembler(Config{MaxConcurrentStreams: 1 << 16})
		a.Finish = func() {}
		start := make(chan struct{})
		done := make(chan struct{})
		go func() {
			defer close(done)
			<-start
			for id := uint64(0); id < 16*4; id += 4 {
				if _, _, _, err := a.Frames([]quic.Frame{streamFrame(id, head, false)}); err != nil {
					t.Error(err)
					return
				}
			}
		}()
		close(start)
		_, idle := a.beginDrain()
		<-done
		if n := a.inFlight.Load(); idle && n != 0 {
			t.Fatalf("round %d: the drain found nothing in flight, yet %d request streams are in flight after it", round, n)
		}
	}
}

// Trailers reach the handler in Request.Trailers, apart from the headers,
// and their bytes answer to the same octet rules as everything else that
// could be re-serialised toward an HTTP/1.1 hop — and a frame after them is
// a second request trying to ride the stream.
func TestH3RequestTrailersReachTheHandler(t *testing.T) {
	head := h3Request("POST", "/t")
	head = appendH3Frame(head, h3Data, []byte("x"))

	trailers := func(fields ...Header) []byte {
		return appendH3Frame(nil, h3Headers, appendQPACK(nil, fields))
	}

	// Clean trailers: the request completes and carries them, apart.
	a := testAssembler(Config{})
	stream := append(append([]byte(nil), head...), trailers(
		Header{Name: []byte("x-sum"), Value: []byte("ok")},
		Header{Name: []byte("x-sig"), Value: []byte("a b")},
	)...)
	batch, _, failed, err := a.Frames([]quic.Frame{streamFrame(0, stream, true)})
	if err != nil {
		t.Fatal(err)
	}
	if batch.Len() != 1 || len(failed) != 0 {
		t.Fatalf("a request with clean trailers was refused: batch=%d failed=%v", batch.Len(), failed)
	}
	r := batch.Items[0]
	if got := r.Get("x-sum"); got != nil {
		t.Errorf("a trailer field reached the headers as %q", got)
	}
	if len(r.Trailers) != 2 ||
		string(r.Trailers[0].Name) != "x-sum" || string(r.Trailers[0].Value) != "ok" ||
		string(r.Trailers[1].Name) != "x-sig" || string(r.Trailers[1].Value) != "a b" {
		t.Errorf("Trailers = %q, want x-sum: ok, x-sig: a b", r.Trailers)
	}
	if len(r.Headers) != cap(r.Headers) {
		t.Errorf("Headers has room for %d more fields: an append would overwrite the trailers", cap(r.Headers)-len(r.Headers))
	}

	// Malformed trailers are the request's fault: a stream error, the
	// connection goes on. A frame *after* them is a framing fault, tested
	// with the other invalid sequences below.
	for _, tt := range []struct {
		name  string
		bytes []byte
	}{
		{"a pseudo-header in the trailers", trailers(Header{Name: []byte(":status"), Value: []byte("200")})},
		{"a CR in a trailer value", trailers(Header{Name: []byte("x-sum"), Value: []byte("a\rb")})},
		{"a trailer name that is not a token", trailers(Header{Name: []byte("x sum"), Value: []byte("v")})},
		// RFC 9114 §4.2 holds for the trailer section as for the head.
		{"a connection-specific trailer", trailers(Header{Name: []byte("transfer-encoding"), Value: []byte("chunked")})},
		{"a TE trailer that is not trailers", trailers(Header{Name: []byte("te"), Value: []byte("gzip")})},
	} {
		t.Run(tt.name, func(t *testing.T) {
			a := testAssembler(Config{})
			stream := append(append([]byte(nil), head...), tt.bytes...)
			batch, _, failed, err := a.Frames([]quic.Frame{streamFrame(0, stream, true)})
			if err != nil {
				t.Fatalf("one malformed stream failed the connection: %v", err)
			}
			if batch.Len() != 0 {
				t.Fatal("a malformed stream's request was served")
			}
			if len(failed) != 1 || failed[0].Code != h3MessageError {
				t.Fatalf("failed = %v, want one H3_MESSAGE_ERROR refusal", failed)
			}
		})
	}
}

// What RFC 9114 and RFC 9204 make the connection's fault on a request stream,
// each a MUST for a connection error with the code named: an invalid frame
// sequence (§4.1), a control-stream frame off the control stream (§7.2.3,
// §7.2.4, §7.2.6, §7.2.7), a PUSH_PROMISE from a client (§7.2.5), a frame
// type HTTP/2 used and HTTP/3 reserved (§7.2.8), and a field section the
// QPACK state cannot decode (RFC 9204 §2.2.3, §3.1). Every one used to be
// answered on the stream alone — RESET_STREAM, the connection kept — which
// let a peer whose framer is broken keep sending, one stream after another.
func TestH3FramingFaultsAreConnectionErrors(t *testing.T) {
	head := h3Request("POST", "/t")
	section := func(fields ...Header) []byte {
		return appendH3Frame(nil, h3Headers, appendQPACK(nil, fields))
	}
	trailer := section(Header{Name: []byte("x-sum"), Value: []byte("v")})
	for _, tt := range []struct {
		name   string
		stream []byte
		code   uint64
	}{
		{"DATA before HEADERS", appendH3Frame(nil, h3Data, []byte("early")), h3FrameUnexpected},
		{"DATA after the trailers", append(append(append([]byte(nil), head...), trailer...), appendH3Frame(nil, h3Data, []byte("y"))...), h3FrameUnexpected},
		{"HEADERS after the trailers", append(append(append([]byte(nil), head...), trailer...), trailer...), h3FrameUnexpected},
		{"SETTINGS on a request stream", append(append([]byte(nil), head...), settingsFrame(h3SettingMaxFieldSection, 64)...), h3FrameUnexpected},
		{"GOAWAY on a request stream", append(append([]byte(nil), head...), appendH3Frame(nil, h3GoAway, []byte{0x00})...), h3FrameUnexpected},
		{"MAX_PUSH_ID on a request stream", append(append([]byte(nil), head...), appendH3Frame(nil, h3MaxPushID, []byte{0x00})...), h3FrameUnexpected},
		{"CANCEL_PUSH on a request stream", append(append([]byte(nil), head...), appendH3Frame(nil, h3CancelPush, []byte{0x00})...), h3FrameUnexpected},
		{"PUSH_PROMISE from the client", append(append([]byte(nil), head...), appendH3Frame(nil, h3PushPromise, []byte{0x00})...), h3FrameUnexpected},
		{"a dynamic-table reference", appendH3Frame(nil, h3Headers, []byte{0x00, 0x00, 0x80}), qpackDecompressionFailed},
		{"a static index past the table", appendH3Frame(nil, h3Headers, []byte{0x00, 0x00, 0xff, 0x7f}), qpackDecompressionFailed},
		{"a required insert count of one", appendH3Frame(nil, h3Headers, []byte{0x01, 0x00}), qpackDecompressionFailed},
	} {
		t.Run(tt.name, func(t *testing.T) {
			a := testAssembler(Config{})
			batch, _, failed, err := a.Frames([]quic.Frame{streamFrame(0, tt.stream, true)})
			if batch.Len() != 0 || len(failed) != 0 {
				t.Fatalf("served or refused on the stream (batch=%d failed=%v); the connection is at fault", batch.Len(), failed)
			}
			connErr(t, err, tt.code)
		})
	}

	// RFC 9114 §7.1: a SETTINGS payload that ends inside a pair fails the
	// frame's layout, H3_FRAME_ERROR — not H3_SETTINGS_ERROR, which is for
	// what whole pairs say. It used to be the latter.
	t.Run("a SETTINGS payload truncated inside a pair", func(t *testing.T) {
		a := testAssembler(Config{})
		open := append(uniOpen(h3StreamControl), appendH3Frame(nil, h3Settings, varint.Append(nil, h3SettingMaxFieldSection))...)
		_, _, _, err := a.Frames([]quic.Frame{streamFrame(2, open, false)})
		connErr(t, err, h3FrameError)
	})
}

// RFC 9204 §7.4: a value wider than the decoder can decode — past the 62 bits
// §4.1.1 requires — is a *stream* error of type QPACK_DECOMPRESSION_FAILED
// on a request stream, where the other decoding failures are the
// connection's. Wherever the integer sits — a string length, the prefix's
// Required Insert Count or its Delta Base — the connection goes on.
func TestH3QPACKIntegerPastSixtyTwoBitsIsAStreamError(t *testing.T) {
	nine := []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	for _, tt := range []struct {
		name    string
		section []byte
	}{
		// Prefix, then a literal with a literal name whose 3-bit-prefix name
		// length is 2^62: 0x27 then nine continuation octets.
		{"a string length of 2^62", []byte{0x00, 0x00, 0x27, 0xf9, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x3f}},
		{"a delta base of 2^62", append(append([]byte{0x00, 0x7f}, nine...), 0x3f)},
		{"a required insert count of 2^62", append(append([]byte{0xff}, nine...), 0x3f, 0x00)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			a := testAssembler(Config{})
			batch, _, failed, err := a.Frames([]quic.Frame{streamFrame(0, appendH3Frame(nil, h3Headers, tt.section), true)})
			if err != nil {
				t.Fatalf("one stream's oversized integer failed the connection: %v", err)
			}
			if batch.Len() != 0 || len(failed) != 1 || failed[0].Code != qpackDecompressionFailed {
				t.Fatalf("batch=%d failed=%v, want one QPACK_DECOMPRESSION_FAILED refusal on the stream", batch.Len(), failed)
			}
		})
	}
}

// uniOpen builds the bytes a client unidirectional stream opens with: its
// type varint, then whatever follows.
func uniOpen(typ uint64, rest ...byte) []byte {
	return append(varint.Append(nil, typ), rest...)
}

func settingsFrame(id, value uint64) []byte {
	payload := varint.Append(nil, id)
	payload = varint.Append(payload, value)
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
		t.Fatalf("err = %v, want it to wrap ErrH3Protocol", err)
	}
}

// RFC 9114 §6.2.1 and RFC 9204 §4.2: the client's unidirectional streams are
// read far enough to take its SETTINGS and to enforce the critical-stream
// error regime — which used to be entirely unenforced because every client
// unidirectional stream was skipped unread.
func TestH3ClientUnidirectionalStreams(t *testing.T) {
	t.Run("SETTINGS on the control stream are parsed", func(t *testing.T) {
		a := testAssembler(Config{})
		open := append(uniOpen(h3StreamControl), settingsFrame(h3SettingMaxFieldSection, 4096)...)
		if _, _, _, err := a.Frames([]quic.Frame{streamFrame(2, open, false)}); err != nil {
			t.Fatalf("a conformant control stream was refused: %v", err)
		}
		if a.peerMaxFieldSection() != 4096 {
			t.Fatalf("MAX_FIELD_SECTION_SIZE = %d, want 4096", a.peerMaxFieldSection())
		}
	})

	t.Run("a control stream split across datagrams", func(t *testing.T) {
		a := testAssembler(Config{})
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
		a := testAssembler(Config{})
		open := append(uniOpen(h3StreamControl), appendH3Frame(nil, h3GoAway, []byte{0x00})...)
		_, _, _, err := a.Frames([]quic.Frame{streamFrame(2, open, false)})
		connErr(t, err, h3MissingSettings)
	})

	t.Run("a GREASE frame before SETTINGS", func(t *testing.T) {
		// RFC 9114 §6.2.1: SETTINGS must be the very first frame. A reserved
		// frame type is skippable everywhere else (§7.2.8), but not where
		// SETTINGS has to stand.
		a := testAssembler(Config{})
		open := append(uniOpen(h3StreamControl), appendH3Frame(nil, 0x21, []byte{0xde, 0xad})...)
		open = append(open, settingsFrame(h3SettingMaxFieldSection, 64)...)
		_, _, _, err := a.Frames([]quic.Frame{streamFrame(2, open, false)})
		connErr(t, err, h3MissingSettings)
	})

	t.Run("a reserved HTTP/2 setting identifier", func(t *testing.T) {
		// §7.2.4.1: HTTP/2's settings with no HTTP/3 meaning are reserved,
		// and receiving one is H3_SETTINGS_ERROR rather than a skip.
		for _, id := range []uint64{0x00, 0x02, 0x03, 0x04, 0x05} {
			a := testAssembler(Config{})
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
		a := testAssembler(Config{})
		_, _, _, err := a.Frames([]quic.Frame{streamFrame(2, uniOpen(h3StreamQPACKEncode, 0xc1, 0x00), false)})
		connErr(t, err, qpackEncoderStreamError)
	})

	t.Run("a non-zero capacity on the QPACK encoder stream", func(t *testing.T) {
		// Set Dynamic Table Capacity (001xxxxx) above the advertised zero is
		// the error RFC 9204 §4.3.1 names.
		a := testAssembler(Config{})
		_, _, _, err := a.Frames([]quic.Frame{streamFrame(2, uniOpen(h3StreamQPACKEncode, 0x21), false)})
		connErr(t, err, qpackEncoderStreamError)
	})

	t.Run("a capacity of zero on the QPACK encoder stream is tolerated", func(t *testing.T) {
		// Setting the capacity to the advertised zero changes nothing, and
		// §4.3.1 only refuses a value that exceeds the limit.
		a := testAssembler(Config{})
		if _, _, _, err := a.Frames([]quic.Frame{streamFrame(2, uniOpen(h3StreamQPACKEncode, 0x20), false)}); err != nil {
			t.Fatalf("a capacity of zero was refused: %v", err)
		}
		// And an insert after it is still refused: the capacity is still zero.
		_, _, _, err := a.Frames([]quic.Frame{streamFrame(2, []byte{0x80}, false)})
		connErr(t, err, qpackEncoderStreamError)
	})

	t.Run("a second SETTINGS on the control stream", func(t *testing.T) {
		a := testAssembler(Config{})
		open := append(uniOpen(h3StreamControl), settingsFrame(h3SettingMaxFieldSection, 64)...)
		open = append(open, settingsFrame(h3SettingMaxFieldSection, 64)...)
		_, _, _, err := a.Frames([]quic.Frame{streamFrame(2, open, false)})
		connErr(t, err, h3FrameUnexpected)
	})

	t.Run("a request frame on the control stream", func(t *testing.T) {
		a := testAssembler(Config{})
		open := append(uniOpen(h3StreamControl), settingsFrame(h3SettingMaxFieldSection, 64)...)
		open = append(open, appendH3Frame(nil, h3Data, []byte("x"))...)
		_, _, _, err := a.Frames([]quic.Frame{streamFrame(2, open, false)})
		connErr(t, err, h3FrameUnexpected)
	})

	t.Run("a second control stream", func(t *testing.T) {
		a := testAssembler(Config{})
		open := append(uniOpen(h3StreamControl), settingsFrame(h3SettingMaxFieldSection, 64)...)
		if _, _, _, err := a.Frames([]quic.Frame{streamFrame(2, open, false)}); err != nil {
			t.Fatal(err)
		}
		_, _, _, err := a.Frames([]quic.Frame{streamFrame(6, uniOpen(h3StreamControl), false)})
		connErr(t, err, h3StreamCreationError)
	})

	t.Run("a second QPACK encoder stream", func(t *testing.T) {
		a := testAssembler(Config{})
		if _, _, _, err := a.Frames([]quic.Frame{streamFrame(2, uniOpen(h3StreamQPACKEncode), false)}); err != nil {
			t.Fatal(err)
		}
		_, _, _, err := a.Frames([]quic.Frame{streamFrame(6, uniOpen(h3StreamQPACKEncode), false)})
		connErr(t, err, h3StreamCreationError)
	})

	t.Run("a second QPACK decoder stream", func(t *testing.T) {
		a := testAssembler(Config{})
		if _, _, _, err := a.Frames([]quic.Frame{streamFrame(2, uniOpen(h3StreamQPACKDecode), false)}); err != nil {
			t.Fatal(err)
		}
		_, _, _, err := a.Frames([]quic.Frame{streamFrame(6, uniOpen(h3StreamQPACKDecode), false)})
		connErr(t, err, h3StreamCreationError)
	})

	t.Run("a client-opened push stream", func(t *testing.T) {
		a := testAssembler(Config{})
		_, _, _, err := a.Frames([]quic.Frame{streamFrame(2, uniOpen(h3StreamPush), false)})
		connErr(t, err, h3StreamCreationError)
	})

	t.Run("the control stream closed by FIN", func(t *testing.T) {
		a := testAssembler(Config{})
		open := append(uniOpen(h3StreamControl), settingsFrame(h3SettingMaxFieldSection, 64)...)
		_, _, _, err := a.Frames([]quic.Frame{streamFrame(2, open, true)})
		connErr(t, err, h3ClosedCriticalStream)
	})

	t.Run("the control stream reset", func(t *testing.T) {
		a := testAssembler(Config{})
		open := append(uniOpen(h3StreamControl), settingsFrame(h3SettingMaxFieldSection, 64)...)
		if _, _, _, err := a.Frames([]quic.Frame{streamFrame(2, open, false)}); err != nil {
			t.Fatal(err)
		}
		_, _, _, err := a.Frames([]quic.Frame{resetFrame(2)})
		connErr(t, err, h3ClosedCriticalStream)
	})

	t.Run("a QPACK stream closed by FIN", func(t *testing.T) {
		a := testAssembler(Config{})
		_, _, _, err := a.Frames([]quic.Frame{streamFrame(2, uniOpen(h3StreamQPACKEncode), true)})
		connErr(t, err, h3ClosedCriticalStream)
	})

	t.Run("an unknown stream type is discarded without error", func(t *testing.T) {
		a := testAssembler(Config{})
		if _, _, _, err := a.Frames([]quic.Frame{streamFrame(2, uniOpen(0x21, 0xde, 0xad), false)}); err != nil {
			t.Fatalf("a reserved unidirectional stream type was refused: %v", err)
		}
		// Closed or reset, still nobody's business but the peer's (§6.2).
		if _, _, _, err := a.Frames([]quic.Frame{streamFrame(2, nil, true)}); err != nil {
			t.Fatalf("closing an unknown unidirectional stream was refused: %v", err)
		}
	})

	t.Run("a stream reset before its type byte is tolerated", func(t *testing.T) {
		a := testAssembler(Config{})
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
	cfg := Config{MaxBodyBytes: maxBody}.mustResolve()
	a := testAssembler(cfg)

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
	cfg := Config{ReadTimeout: time.Minute}.mustResolve()
	a := testAssembler(cfg)

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
	// guess at when one is. The sweep's own clock, the earliest deadline it
	// knows of, was armed at the same instant and is moved with it.
	if !a.nextExpiry.Equal(st.deadline) {
		t.Fatalf("the sweep's earliest deadline is %v, want the one stream's %v", a.nextExpiry, st.deadline)
	}
	st.deadline = time.Now().Add(-time.Nanosecond)
	a.nextExpiry = st.deadline

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
	cfg := Config{MaxHeaderBytes: 128, MaxBodyBytes: 4096}.mustResolve()

	// A complete HEADERS frame between the two bounds.
	big := h3Request("GET", "/big", [2]string{"x-pad", string(bytes.Repeat([]byte("p"), 200))})
	a := testAssembler(cfg)
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
	declared := varint.Append(nil, h3Headers)
	declared = varint.Append(declared, 2000)
	declared = append(declared, bytes.Repeat([]byte("h"), 10)...)
	a = testAssembler(cfg)
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
		a := testAssembler(Config{MaxBodyBytes: maxBody}.mustResolve())
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
			payload = varint.Append(payload, 0x21+0x1f*uint64(i)) // reserved, unknown
			payload = varint.Append(payload, 1)
		}
		payload = varint.Append(payload, h3SettingMaxFieldSection)
		payload = varint.Append(payload, 4096)
		return append(uniOpen(h3StreamControl), appendH3Frame(nil, h3Settings, payload)...)
	}
	cost := func(stream []byte) float64 {
		return testing.AllocsPerRun(20, func() {
			a := testAssembler(Config{})
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
		a := testAssembler(Config{})
		payload := varint.Append(nil, h3SettingMaxFieldSection)
		payload = varint.Append(payload, 1)
		payload = varint.Append(payload, h3SettingMaxFieldSection)
		payload = varint.Append(payload, 2)
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
		// RFC 9204 §4.1.1: integers up to 62 bits decode, and a QUIC stream
		// identifier is 62 bits wide. Stream 2^40 is one a long-lived client
		// reaches; the decoder used to stop at 28 bits and close the
		// connection over it.
		{"stream cancellation of stream 2^40", [][]byte{{0x7f, 0xc1, 0xff, 0xff, 0xff, 0xff, 0x1f}}, 0},
		{"stream cancellation of stream 2^62-1", [][]byte{{0x7f, 0xc0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x3f}}, 0},
		{"stream cancellation of stream 2^62-1 split across deliveries", [][]byte{{0x7f, 0xc0, 0xff, 0xff}, {0xff, 0xff, 0xff, 0xff, 0xff, 0x3f}}, 0},
		{"stream cancellation past 62 bits", [][]byte{{0x7f, 0xc1, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x3f}}, qpackDecoderStreamError},
		{"section acknowledgment", [][]byte{{0x84}}, qpackDecoderStreamError},
		{"insert count increment", [][]byte{{0x01}}, qpackDecoderStreamError},
		{"insert count increment of zero", [][]byte{{0x00}}, qpackDecoderStreamError},
		{"after a cancellation", [][]byte{{0x44, 0x84}}, qpackDecoderStreamError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := testAssembler(Config{})
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

// A request leaves the assembler when its FIN arrives, not the connection:
// it is the handler's until its response is written and the stream retired,
// and the limit counts it there too. Before 2026-10-20 the limit was read on
// the streams still assembling, so a request the peer completed — then
// cancelled, RFC 9114 §4.1.1 — freed a slot its handler call still used.
func TestH3ACompletedRequestHoldsItsSlotUntilRetired(t *testing.T) {
	a := testAssembler(Config{MaxConcurrentStreams: 2})
	batch, _, failed, err := a.Frames([]quic.Frame{
		streamFrame(0, h3Request("GET", "/a"), true),
		streamFrame(4, h3Request("GET", "/b"), true),
	})
	if err != nil || batch.Len() != 2 || len(failed) != 0 {
		t.Fatalf("two complete requests on a limit of two: batch=%d failed=%v err=%v", batch.Len(), failed, err)
	}
	batch, _, failed, err = a.Frames([]quic.Frame{streamFrame(8, h3Request("GET", "/c"), true)})
	if err != nil || batch.Len() != 0 || len(failed) != 1 || failed[0].Code != h3RequestRejected {
		t.Fatalf("the third request, with two unanswered: batch=%d failed=%v err=%v, want one H3_REQUEST_REJECTED", batch.Len(), failed, err)
	}
	a.retire() // one response written
	batch, _, failed, err = a.Frames([]quic.Frame{streamFrame(12, h3Request("GET", "/d"), true)})
	if err != nil || batch.Len() != 1 || len(failed) != 0 {
		t.Fatalf("the request after a retire: batch=%d failed=%v err=%v, want it assembled", batch.Len(), failed, err)
	}
}

// RFC 9114 §4.2.2, §7.2.4.2: before the client's SETTINGS arrive its
// MAX_FIELD_SECTION_SIZE is unlimited; a SETTINGS that announces zero is a
// limit of zero, under which no response fits — not the absence of a limit,
// which is what zero used to mean on this side.
func TestH3AnnouncedFieldSectionLimitOfZeroIsALimit(t *testing.T) {
	a := testAssembler(Config{})
	res := Response{Status: 200}
	if _, err := appendH3Response(nil, res, nil, a.peerMaxFieldSection(), &h3Scratch{}, testDate); err != nil {
		t.Fatalf("before any SETTINGS the response was refused: %v", err)
	}
	open := append(uniOpen(h3StreamControl), settingsFrame(h3SettingMaxFieldSection, 0)...)
	if _, _, _, err := a.Frames([]quic.Frame{streamFrame(2, open, false)}); err != nil {
		t.Fatalf("the control stream was refused: %v", err)
	}
	if got := a.peerMaxFieldSection(); got != 0 {
		t.Fatalf("peerMaxFieldSection = %d after SETTINGS_MAX_FIELD_SECTION_SIZE 0, want 0", got)
	}
	if _, err := appendH3Response(nil, res, nil, a.peerMaxFieldSection(), &h3Scratch{}, testDate); err == nil {
		t.Error("a response was encoded for a peer that announced a field-section limit of zero")
	}
}

// RFC 9114 §4.1.1: a client that cancels a request sends RESET_STREAM for
// its direction and STOP_SENDING for the server's, often in one packet, and
// the transport hands both up. A cancellation is charged once per stream
// against the rapid-reset allowance, whatever the order of its two frames:
// charged once per frame, the allowance countCancel documents (four times
// MaxConcurrentStreams) was half that for every cancelled request.
func TestH3ATwoFrameCancellationIsOneCharge(t *testing.T) {
	reset := quic.Frame{Type: quic.FrameResetStream, StreamID: 0}
	stop := quic.Frame{Type: quic.FrameStopSending, StreamID: 0}
	for _, tc := range []struct {
		name   string
		head   []byte
		fin    bool
		frames [][]quic.Frame
	}{
		{"reset then stop, one packet", h3Request("POST", "/u", [2]string{"content-length", "16"}), false, [][]quic.Frame{{reset, stop}}},
		{"stop then reset, one packet", h3Request("POST", "/u", [2]string{"content-length", "16"}), false, [][]quic.Frame{{stop, reset}}},
		{"stop then reset, two packets", h3Request("POST", "/u", [2]string{"content-length", "16"}), false, [][]quic.Frame{{stop}, {reset}}},
		// Complete, so in the handler's hands and out of assembly: the
		// charge is remembered past the stream's assembly state.
		{"a complete request, stop then reset", h3Request("GET", "/g"), true, [][]quic.Frame{{stop}, {reset}}},
		{"a complete request, reset then stop", h3Request("GET", "/g"), true, [][]quic.Frame{{reset}, {stop}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := testAssembler(Config{BodyDelivery: BodyWhole})
			if _, _, failed, err := a.Frames([]quic.Frame{streamFrame(0, tc.head, tc.fin)}); err != nil || len(failed) != 0 {
				t.Fatalf("the request's head: %v, failed %v", err, failed)
			}
			for _, frames := range tc.frames {
				if _, _, _, err := a.Frames(frames); err != nil {
					t.Fatalf("the cancellation: %v", err)
				}
			}
			if a.cancelled != 1 {
				t.Fatalf("one cancelled request charged %d times, want once", a.cancelled)
			}
		})
	}
}

// RFC 9114 §4.1, §7.2.3, §7.2.4, §7.2.5, §7.2.6, §7.2.7: SETTINGS, GOAWAY,
// MAX_PUSH_ID and CANCEL_PUSH belong on the control stream, and a client
// never sends PUSH_PROMISE; on a request stream each is a connection error
// of type H3_FRAME_UNEXPECTED, whatever length it declares. Declared over
// the frame bound, each used to be refused for its size on the stream alone
// (H3_EXCESSIVE_LOAD): the length was judged before the type. Only the
// frame header is sent — the refusal is on the declaration.
func TestH3AMisplacedControlFrameOverTheBoundIsAConnectionError(t *testing.T) {
	cfg := Config{MaxBodyBytes: 1 << 10, BodyDelivery: BodyWhole}.mustResolve()
	for _, c := range []struct {
		name string
		typ  uint64
	}{
		{"SETTINGS", h3Settings},
		{"GOAWAY", h3GoAway},
		{"MAX_PUSH_ID", h3MaxPushID},
		{"CANCEL_PUSH", h3CancelPush},
		{"PUSH_PROMISE", h3PushPromise},
	} {
		t.Run(c.name, func(t *testing.T) {
			a := testAssembler(cfg)
			frame := varint.Append(nil, c.typ)
			frame = varint.Append(frame, uint64(cfg.MaxBodyBytes)+1)
			_, _, failed, err := a.Frames([]quic.Frame{streamFrame(0, frame, false)})
			ce, ok := errors.AsType[h3ConnectionError](err)
			if !ok || ce.Code != h3FrameUnexpected {
				t.Fatalf("a %s frame declaring %d bytes on a request stream gave err=%v, failed=%v; want a connection error of type H3_FRAME_UNEXPECTED (%#x)",
					c.name, cfg.MaxBodyBytes+1, err, failed, h3FrameUnexpected)
			}
		})
	}
}
