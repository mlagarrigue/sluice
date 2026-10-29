package httpstream

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
	"time"
)

// The tests in this file drive the reader half of a connection directly —
// consume, frame batch by frame batch — and read back what it queued for the
// writer. Each pins an edge the end-to-end tests cross without stopping on:
// a bound taken at exactly its value, a state reached only after an
// eviction, a header block cut by a reset. A socket would add a peer's own
// flow control and timing to every one of them; none needs it.

func headersFrame(id uint32, flags byte, block []byte) h2Frame {
	return h2Frame{Type: frameHeaders, Flags: flags, StreamID: id, Payload: block}
}

func continuationFrame(id uint32, flags byte, block []byte) h2Frame {
	return h2Frame{Type: frameContinuation, Flags: flags, StreamID: id, Payload: block}
}

func dataFrame(id uint32, flags byte, n int) h2Frame {
	return h2Frame{Type: frameData, Flags: flags, StreamID: id, Payload: bytes.Repeat([]byte{'x'}, n)}
}

func rstFrame(id uint32) h2Frame {
	return h2Frame{Type: frameRSTStream, StreamID: id, Payload: []byte{0, 0, 0, 8}} // CANCEL
}

func h2SettingsFrame(entries ...[2]uint32) h2Frame {
	var p []byte
	for _, e := range entries {
		p = binary.BigEndian.AppendUint16(p, uint16(e[0])) //nolint:gosec // G115: setting identifiers are 16 bits
		p = binary.BigEndian.AppendUint32(p, e[1])
	}
	return h2Frame{Type: frameSettings, Payload: p}
}

const endBoth = flagEndHeaders | flagEndStream

// feed hands frames to consume as one batch and fails the test on a
// connection error.
func feed(t *testing.T, s *h2Conn, frames ...h2Frame) []Request {
	t.Helper()
	ready, _, err := s.consume(frames)
	if err != nil {
		t.Fatalf("connection error: %v", err)
	}
	return ready
}

// queued returns what the reader queued for the writer since the last call,
// and empties the queue: nothing writes it out here, and a full queue blocks
// the reader.
func queued(t *testing.T, s *h2Conn) []h2Frame {
	t.Helper()
	s.w.mu.Lock()
	buf := append([]byte(nil), s.w.ctrl...)
	s.w.ctrl = s.w.ctrl[:0]
	s.w.mu.Unlock()
	var out []h2Frame
	for len(buf) > 0 {
		f, n, err := parseFrame(buf, maxFrameSize)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, f)
		buf = buf[n:]
	}
	return out
}

// framesOn keeps the frames of one type on one stream.
func framesOn(frames []h2Frame, typ byte, id uint32) []h2Frame {
	var out []h2Frame
	for _, f := range frames {
		if f.Type == typ && f.StreamID == id {
			out = append(out, f)
		}
	}
	return out
}

// The rapid-reset allowance is four batches' worth of frames plus two for
// each completed request, and that many resets are allowed — not one fewer.
func TestH2ResetAllowanceIsExact(t *testing.T) {
	s := newBenchH2Server(Config{BodyDelivery: BodyWhole, MaxFramesPerBatch: 2})
	s.served = 3
	allowance := 2*4 + 2*3
	for i := range allowance {
		if err := s.countReset(); err != nil {
			t.Fatalf("reset %d of an allowance of %d refused: %v", i+1, allowance, err)
		}
	}
	if err := s.countReset(); !errors.Is(err, errCalm) {
		t.Fatalf("reset %d past an allowance of %d: %v, want ENHANCE_YOUR_CALM", allowance+1, allowance, err)
	}
}

// CVE-2023-44487 itself: HEADERS with END_STREAM, then RST_STREAM at once.
// The request counted as served gives its credit back when it is cancelled,
// so the pairs spend the flat allowance and trip it at its end.
func TestH2CancelledRequestsEarnNoResetAllowance(t *testing.T) {
	s := newBenchH2Server(Config{BodyDelivery: BodyWhole, MaxFramesPerBatch: 1, MaxConcurrentStreams: 1000})
	const flat = 1 * 4
	for i := range 50 {
		id := uint32(2*i + 1) //nolint:gosec // G115: i < 50
		_, _, err := s.consume([]h2Frame{headersFrame(id, endBoth, headBlock("GET", "/")), rstFrame(id)})
		queued(t, s)
		if err != nil {
			if !errors.Is(err, errCalm) {
				t.Fatalf("pair %d: %v, want ENHANCE_YOUR_CALM", i+1, err)
			}
			if i != flat {
				t.Fatalf("the bound tripped at pair %d, want %d", i+1, flat+1)
			}
			return
		}
	}
	t.Fatal("50 cancelled requests never tripped the reset bound")
}

// A request handed over with its body still streaming is a request served,
// for the rapid-reset bound, as one whose body arrived whole is.
func TestH2StreamedRequestCountsAsServed(t *testing.T) {
	s := newBenchH2Server(Config{BodyDelivery: BodyStreamed})
	for i := range 3 {
		id := uint32(2*i + 1) //nolint:gosec // G115: i < 3
		if ready := feed(t, s, headersFrame(id, flagEndHeaders, headBlock("POST", "/"))); len(ready) != 1 {
			t.Fatalf("stream %d: %d requests handed over, want 1", id, len(ready))
		}
	}
	if s.served != 3 {
		t.Fatalf("served = %d after three streamed requests, want 3", s.served)
	}
}

// An even identifier is idle whatever lastID says (RFC 9113 §5.1.1): this
// server opens none, so no frame on one is a late frame, and HEADERS on one
// is not a request.
func TestH2EvenStreamsAreAlwaysIdle(t *testing.T) {
	for _, f := range []h2Frame{
		rstFrame(2),
		{Type: frameWindowUpdate, StreamID: 2, Payload: windowUpdatePayload(1)},
		headersFrame(2, endBoth, headBlock("GET", "/")),
	} {
		s := newBenchH2Server(Config{BodyDelivery: BodyWhole})
		feed(t, s, headersFrame(3, endBoth, headBlock("GET", "/")))
		_, _, err := s.consume([]h2Frame{f})
		if _, stream := errors.AsType[streamError](err); err == nil || stream {
			t.Errorf("frame type %d on stream 2 below lastID 3: %v, want a connection error", f.Type, err)
		}
	}
}

// A streamed request whose handler has returned is forgotten at the next
// batch, even when no other stream is open to make the reader look.
func TestH2ReturnedStreamedRequestIsForgotten(t *testing.T) {
	s := newBenchH2Server(Config{BodyDelivery: BodyStreamed})
	ready := feed(t, s, headersFrame(1, flagEndHeaders, headBlock("POST", "/")))
	if len(ready) != 1 {
		t.Fatalf("%d requests handed over, want 1", len(ready))
	}
	ready[0].body.q.close() // the handler returned without reading
	feed(t, s)
	if _, kept := s.streams[1]; kept {
		t.Fatal("stream 1 is still held after its handler returned")
	}
}

// Every stream past its assembly deadline is reset in the same batch, not the
// first of them.
func TestH2EveryStalledStreamIsReset(t *testing.T) {
	s := newBenchH2Server(Config{BodyDelivery: BodyWhole})
	feed(t, s,
		headersFrame(1, flagEndHeaders, headBlock("POST", "/")),
		headersFrame(3, flagEndHeaders, headBlock("POST", "/")))
	for _, st := range s.streams {
		st.deadline = time.Now().Add(-time.Second)
	}
	feed(t, s)
	got := queued(t, s)
	for _, id := range []uint32{1, 3} {
		if len(framesOn(got, frameRSTStream, id)) != 1 {
			t.Errorf("stalled stream %d was not reset", id)
		}
	}
}

// A RST_STREAM on a stream the ring has already forgotten adds nothing to
// what the connection remembers: the bound on closed streams holds against a
// peer that resets old identifiers.
func TestH2ResetOfAForgottenStreamIsNotRemembered(t *testing.T) {
	s := newBenchH2Server(Config{BodyDelivery: BodyWhole, MaxConcurrentStreams: 1})
	feed(t, s, headersFrame(1, endBoth, headBlock("GET", "/")))
	s.inFlight.Add(-1) // answered: the slot is free
	feed(t, s, headersFrame(3, endBoth, headBlock("GET", "/")))
	feed(t, s, rstFrame(1))
	if len(s.closed) > s.cfg.MaxConcurrentStreams {
		t.Fatalf("%d closed streams remembered, bound %d", len(s.closed), s.cfg.MaxConcurrentStreams)
	}
}

// A header block cut by the assembly deadline still reaches the HPACK
// decoder whole: the stream is reset, its fragments wait in late, and the
// CONTINUATION that ends them completes the block — whichever order the other
// stalled streams were reset in, which is a map's.
func TestH2BlockCutByTheDeadlineStillReachesTheDecoder(t *testing.T) {
	trailer := hpackIncIndexed("x-t", "v") // enters the dynamic table
	for range 3 {
		s := newBenchH2Server(Config{BodyDelivery: BodyWhole})
		frames := []h2Frame{headersFrame(1, flagEndHeaders, headBlock("POST", "/"))}
		for id := uint32(3); id <= 63; id += 2 {
			frames = append(frames, headersFrame(id, flagEndHeaders, headBlock("POST", "/")))
		}
		feed(t, s, frames...)
		for _, st := range s.streams {
			st.deadline = time.Now().Add(-time.Second)
		}
		feed(t, s, headersFrame(1, flagEndStream, trailer[:3]))
		queued(t, s)
		ready := feed(t, s,
			continuationFrame(1, flagEndHeaders, trailer[3:]),
			headersFrame(65, endBoth, append(headBlock("GET", "/"), hpackIndexed(staticTableLen+1)...)))
		if len(ready) != 1 || string(ready[0].Get("x-t")) != "v" {
			t.Fatalf("the request after the cut block: %d requests, x-t %q; want x-t: v", len(ready), firstGet(ready, "x-t"))
		}
	}
}

func firstGet(reqs []Request, name string) []byte {
	if len(reqs) == 0 {
		return nil
	}
	return reqs[0].Get(name)
}

// The opened bitmap: an identifier opened is remembered across a gap well
// inside the window, a skipped one reads as never opened at any distance in
// it, and a bit left by an identifier one window back is cleared when the
// identifier sharing it is skipped.
func TestH2OpenedBitmapTellsOpenedFromSkipped(t *testing.T) {
	s := newBenchH2Server(Config{BodyDelivery: BodyWhole})
	s.openID(4001)
	s.openID(5201)
	if !s.wasOpened(4001) {
		t.Error("4001, opened 600 identifiers back, reads as never opened")
	}
	if s.wasOpened(4003) {
		t.Error("4003, skipped, reads as opened")
	}

	s = newBenchH2Server(Config{BodyDelivery: BodyWhole})
	s.openID(1)
	s.openID(2001)
	if !s.wasOpened(1) || s.wasOpened(3) {
		t.Errorf("after 1 and 2001: wasOpened(1) = %v, wasOpened(3) = %v; want true, false", s.wasOpened(1), s.wasOpened(3))
	}

	s = newBenchH2Server(Config{BodyDelivery: BodyWhole})
	for _, id := range []uint32{1, 2001, 4001, 4099} {
		s.openID(id)
	}
	// 4097 shares its bit with 1, which was opened; 4097 was skipped.
	if s.wasOpened(4097) {
		t.Error("4097, skipped, reads as opened through the bit stream 1 left")
	}
	if !s.wasOpened(4001) {
		t.Error("4001 reads as never opened")
	}
}

// Padding or the priority fields may take a HEADERS frame's whole payload:
// the block then starts in the CONTINUATION.
func TestH2HeadersMayCarryNoFragment(t *testing.T) {
	s := newBenchH2Server(Config{BodyDelivery: BodyWhole})
	ready := feed(t, s,
		headersFrame(1, flagPadded|flagEndStream, []byte{3, 0, 0, 0}),
		continuationFrame(1, flagEndHeaders, headBlock("GET", "/")))
	if len(ready) != 1 {
		t.Errorf("padding to the end of HEADERS: %d requests, want 1", len(ready))
	}
	ready = feed(t, s,
		headersFrame(3, flagPriority|flagEndStream, []byte{0, 0, 0, 0, 15}),
		continuationFrame(3, flagEndHeaders, headBlock("GET", "/")))
	if len(ready) != 1 {
		t.Errorf("priority fields alone in HEADERS: %d requests, want 1", len(ready))
	}
}

// A block decoded on a closed stream leaves no buffer behind in late, which
// lives as long as the connection.
func TestH2LateBlockKeepsNoBuffer(t *testing.T) {
	s := newBenchH2Server(Config{BodyDelivery: BodyWhole})
	feed(t, s, headersFrame(1, flagEndHeaders, headBlock("POST", "/")), rstFrame(1))
	feed(t, s, headersFrame(1, endBoth, literal(nil, "x-t", "v")))
	if cap(s.late.block) != 0 {
		t.Fatalf("late holds a %d-byte block buffer", cap(s.late.block))
	}
}

// An empty trailer section is no trailers: Request.Trailers is nil, as its
// documentation says for a request that sent none.
func TestH2EmptyTrailerSectionIsNoTrailers(t *testing.T) {
	s := newBenchH2Server(Config{BodyDelivery: BodyWhole})
	feed(t, s, headersFrame(1, flagEndHeaders, headBlock("POST", "/")))
	ready := feed(t, s, headersFrame(1, endBoth, nil))
	if len(ready) != 1 {
		t.Fatalf("%d requests, want 1", len(ready))
	}
	if tr := ready[0].Trailers(); tr != nil {
		t.Fatalf("Trailers() = %#v, want nil", tr)
	}
}

// The 400 answering a malformed HEAD request has no body: the method is read
// back out of the fields buildRequest refused.
func TestH2MalformedHeadRequestIsAnsweredWithoutABody(t *testing.T) {
	s := newBenchH2Server(Config{BodyDelivery: BodyWhole})
	feed(t, s, headersFrame(1, endBoth, headBlock("HEAD", "/", [2]string{"connection", "close"})))
	got := queued(t, s)
	heads := framesOn(got, frameHeaders, 1)
	if len(heads) != 1 || h2Status(heads[0].Payload) != 400 || heads[0].Flags&flagEndStream == 0 {
		t.Fatalf("want one 400 head ending the stream, got %d heads", len(heads))
	}
	if n := len(framesOn(got, frameData, 1)); n != 0 {
		t.Fatalf("the 400 to a HEAD request carried %d DATA frames", n)
	}
}

// Every body bound is inclusive: a content-length of MaxBodyBytes, a body of
// MaxBodyBytes, and unfinished bodies summing to four times it are all
// accepted. Past the aggregate, the reason names the bound, and reaches the
// caller once the reset allowance is spent.
func TestH2BodyBoundsAreInclusive(t *testing.T) {
	s := newBenchH2Server(Config{BodyDelivery: BodyWhole, MaxBodyBytes: 10})
	ready := feed(t, s,
		headersFrame(1, flagEndHeaders, headBlock("POST", "/", [2]string{"content-length", "10"})),
		dataFrame(1, flagEndStream, 10))
	if len(ready) != 1 || len(ready[0].Body) != 10 {
		t.Fatalf("a body of exactly MaxBodyBytes: %d requests", len(ready))
	}
	var frames []h2Frame
	for id := uint32(3); id <= 9; id += 2 {
		frames = append(frames, headersFrame(id, flagEndHeaders, headBlock("POST", "/")), dataFrame(id, 0, 10))
	}
	feed(t, s, frames...)
	if got := queued(t, s); len(framesOn(got, frameRSTStream, 9)) != 0 {
		t.Fatal("four unfinished bodies of MaxBodyBytes were refused: the aggregate bound is 4×")
	}
	s.resets = s.cfg.MaxFramesPerBatch*4 + 2*s.served
	_, _, err := s.consume([]h2Frame{headersFrame(11, flagEndHeaders, headBlock("POST", "/")), dataFrame(11, 0, 1)})
	if err == nil || !strings.Contains(err.Error(), "over 40 bytes") {
		t.Fatalf("past the aggregate bound with no reset left: %v, want the 40-byte bound named", err)
	}
}

// A writer that has failed ends the read loop at the next frame that writes:
// the interim 100 and a stream's WINDOW_UPDATE both report it.
func TestH2WriterFailureEndsTheRead(t *testing.T) {
	failed := func(s *h2Conn) {
		s.w.mu.Lock()
		s.w.setErr(errors.New("the socket is gone"))
		s.w.mu.Unlock()
	}

	s := newBenchH2Server(Config{BodyDelivery: BodyWhole})
	failed(s)
	if _, _, err := s.consume([]h2Frame{headersFrame(1, flagEndHeaders,
		headBlock("POST", "/", [2]string{"expect", "100-continue"}))}); err == nil {
		t.Error("the interim 100 met a failed writer and the read went on")
	}

	s = newBenchH2Server(Config{BodyDelivery: BodyWhole, InitialWindowSize: 1000})
	feed(t, s, headersFrame(1, flagEndHeaders, headBlock("POST", "/")))
	failed(s)
	if _, _, err := s.consume([]h2Frame{dataFrame(1, 0, 600)}); err == nil {
		t.Error("the WINDOW_UPDATE met a failed writer and the read went on")
	}
}

// The highest identifier the client used can itself be forgotten: refused at
// the concurrency limit, then pushed out of the ring by an older stream's
// retirement. A frame on it is still a late frame, not a new stream nor one
// that never existed.
func TestH2ForgottenLastStreamIsStillClosed(t *testing.T) {
	s := newBenchH2Server(Config{BodyDelivery: BodyWhole, MaxConcurrentStreams: 1})
	feed(t, s, headersFrame(1, flagEndHeaders, headBlock("POST", "/")))
	feed(t, s, headersFrame(3, flagEndHeaders, headBlock("POST", "/"))) // refused: 1 holds the slot
	if ready := feed(t, s, dataFrame(1, flagEndStream, 1)); len(ready) != 1 {
		t.Fatalf("stream 1: %d requests, want 1", len(ready))
	}
	s.inFlight.Add(-1) // answered: the slot is free
	queued(t, s)
	if _, ok := s.closed[3]; ok || s.lastID != 3 {
		t.Fatalf("setup: stream 3 still remembered (%v) or lastID %d", ok, s.lastID)
	}

	feed(t, s, dataFrame(3, 0, 5))
	if ready := feed(t, s, headersFrame(3, endBoth, headBlock("GET", "/"))); len(ready) != 0 {
		t.Fatal("HEADERS on the forgotten stream 3 opened it again")
	}
	if got := queued(t, s); len(got) != 0 {
		t.Fatalf("late frames on the forgotten stream 3 were answered: %d frames", len(got))
	}
}

// A late header block on a forgotten stream may span a CONTINUATION; its
// fragments accumulate in late even when late already stood for that stream.
func TestH2LateBlockOnAForgottenStreamSpansFrames(t *testing.T) {
	s := newBenchH2Server(Config{BodyDelivery: BodyWhole, MaxConcurrentStreams: 1})
	feed(t, s, headersFrame(1, endBoth, headBlock("GET", "/")))
	s.inFlight.Add(-1)
	feed(t, s, rstFrame(1), dataFrame(1, 0, 1)) // late now stands for stream 1
	feed(t, s, headersFrame(3, endBoth, headBlock("GET", "/")))
	s.inFlight.Add(-1)
	if _, ok := s.closed[1]; ok || s.late.id != 1 {
		t.Fatalf("setup: stream 1 still remembered (%v), late.id %d", ok, s.late.id)
	}

	trailer := hpackIncIndexed("x-t", "v")
	feed(t, s, headersFrame(1, flagEndStream, trailer[:3]))
	ready := feed(t, s,
		continuationFrame(1, flagEndHeaders, trailer[3:]),
		headersFrame(5, endBoth, append(headBlock("GET", "/"), hpackIndexed(staticTableLen+1)...)))
	if len(ready) != 1 || string(ready[0].Get("x-t")) != "v" {
		t.Fatalf("the request after the late block: %d requests, x-t %q; want x-t: v", len(ready), firstGet(ready, "x-t"))
	}
}

// A stream's window is spent exactly, and granted back once less than half
// of it is left — not at half.
func TestH2StreamWindowEdges(t *testing.T) {
	s := newBenchH2Server(Config{BodyDelivery: BodyWhole, InitialWindowSize: 1000, MaxBodyBytes: 4000})
	feed(t, s, h2Frame{Type: frameSettings, Flags: flagAck}) // no grace past the advertised window
	feed(t, s, headersFrame(1, flagEndHeaders, headBlock("POST", "/")), dataFrame(1, 0, 1000))
	if got := queued(t, s); len(framesOn(got, frameRSTStream, 1)) != 0 {
		t.Fatal("a DATA frame of exactly the stream window was refused")
	}

	feed(t, s, headersFrame(3, flagEndHeaders, headBlock("POST", "/")), dataFrame(3, 0, 500))
	if got := queued(t, s); len(framesOn(got, frameWindowUpdate, 3)) != 0 {
		t.Fatal("half the window left, and it was granted back already")
	}
	feed(t, s, dataFrame(3, 0, 1))
	wu := framesOn(queued(t, s), frameWindowUpdate, 3)
	if len(wu) != 1 || be32(wu[0].Payload) != 501 {
		t.Fatalf("one byte under half the window: %d WINDOW_UPDATEs, want one of 501", len(wu))
	}
}

// The connection's window is spent exactly, granted back once less than half
// is left, and by what was spent.
func TestH2ConnectionWindowEdges(t *testing.T) {
	s := newBenchH2Server(Config{BodyDelivery: BodyWhole, MaxConcurrentStreams: 1})
	win := int(s.connWind)
	feed(t, s, headersFrame(1, endBoth, headBlock("GET", "/")), rstFrame(1)) // DATA on 1 is now counted and dropped
	queued(t, s)

	feed(t, s, dataFrame(1, 0, win))
	wu := framesOn(queued(t, s), frameWindowUpdate, 0)
	if len(wu) != 1 || int(be32(wu[0].Payload)) != win {
		t.Fatalf("the whole connection window spent: %d WINDOW_UPDATEs, want one of %d", len(wu), win)
	}
	feed(t, s, dataFrame(1, 0, win/2))
	if wu := framesOn(queued(t, s), frameWindowUpdate, 0); len(wu) != 0 {
		t.Fatal("half the connection window left, and it was granted back already")
	}
	feed(t, s, dataFrame(1, 0, 1))
	wu = framesOn(queued(t, s), frameWindowUpdate, 0)
	if len(wu) != 1 || int(be32(wu[0].Payload)) != win/2+1 {
		t.Fatalf("one byte under half: %d WINDOW_UPDATEs, want one of %d", len(wu), win/2+1)
	}
}

// A content-length of zero is a declared length like any other: a body
// against it is malformed.
func TestH2ZeroContentLengthRefusesABody(t *testing.T) {
	s := newBenchH2Server(Config{BodyDelivery: BodyWhole})
	ready := feed(t, s,
		headersFrame(1, flagEndHeaders, headBlock("POST", "/", [2]string{"content-length", "0"})),
		dataFrame(1, flagEndStream, 5))
	if len(ready) != 0 {
		t.Fatal("a 5-byte body against content-length: 0 was served")
	}
	if heads := framesOn(queued(t, s), frameHeaders, 1); len(heads) != 1 || h2Status(heads[0].Payload) != 400 {
		t.Fatal("no 400 for a body against content-length: 0")
	}
}

// Before the peer acknowledges a smaller initial window, it may send up to
// the protocol's default (RFC 9113 §6.9.3) and not a byte more.
func TestH2GraceBeforeTheSettingsAckIsTheDefaultWindow(t *testing.T) {
	s := newBenchH2Server(Config{BodyDelivery: BodyStreamed, InitialWindowSize: 1000, MaxBodyBytes: 1 << 20})
	const frame = 16384
	feed(t, s, headersFrame(1, flagEndHeaders, headBlock("POST", "/")),
		dataFrame(1, 0, frame), dataFrame(1, 0, frame), dataFrame(1, 0, frame), dataFrame(1, 0, frame-1))
	if got := queued(t, s); len(framesOn(got, frameRSTStream, 1)) != 0 {
		t.Fatal("65535 bytes before the SETTINGS ACK were refused")
	}
	feed(t, s, headersFrame(3, flagEndHeaders, headBlock("POST", "/")),
		dataFrame(3, 0, frame), dataFrame(3, 0, frame), dataFrame(3, 0, frame), dataFrame(3, 0, frame))
	rst := framesOn(queued(t, s), frameRSTStream, 3)
	if len(rst) != 1 || be32(rst[0].Payload) != errFlowControlError {
		t.Fatal("65536 bytes before the SETTINGS ACK were not refused with FLOW_CONTROL_ERROR")
	}
}

// A content-length that fits is held to the body bound (413); one that
// overflows is malformed (400).
func TestH2ContentLengthOverflowIsMalformed(t *testing.T) {
	s := newBenchH2Server(Config{BodyDelivery: BodyWhole})
	feed(t, s,
		headersFrame(1, endBoth, headBlock("GET", "/", [2]string{"content-length", "2147483640"})),
		headersFrame(3, endBoth, headBlock("GET", "/", [2]string{"content-length", "21474836480"})))
	got := queued(t, s)
	for id, want := range map[uint32]int{1: 413, 3: 400} {
		heads := framesOn(got, frameHeaders, id)
		if len(heads) != 1 || h2Status(heads[0].Payload) != want {
			t.Errorf("stream %d: want one %d", id, want)
		}
	}
}

// ENABLE_PUSH of 1 is a legal value for a client to send (RFC 9113 §6.5.2);
// only past 1 is it an error.
func TestH2EnablePushOfOneIsAccepted(t *testing.T) {
	s := newBenchH2Server(Config{BodyDelivery: BodyWhole})
	feed(t, s, h2SettingsFrame([2]uint32{settingEnablePush, 1}))
}

// A SETTINGS frame applies every entry it carries: the frame size beside an
// initial window.
func TestH2SettingsApplyEveryEntry(t *testing.T) {
	s := newBenchH2Server(Config{BodyDelivery: BodyWhole, MaxFrameSize: 32768})
	feed(t, s, h2SettingsFrame(
		[2]uint32{settingInitialWindowSize, 1000},
		[2]uint32{settingMaxFrameSize, 32768}))
	s.w.mu.Lock()
	got := s.w.maxFrame
	s.w.mu.Unlock()
	if got != 32768 {
		t.Fatalf("frame size %d after SETTINGS, want 32768", got)
	}
}
