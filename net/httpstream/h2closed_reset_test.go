package httpstream

import (
	"bytes"
	"context"
	"errors"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
)

// RFC 9113 §5.1: "An endpoint MUST NOT send frames other than PRIORITY on a
// closed stream". A stream the peer reset is closed; a stream error found on
// it afterwards — a WINDOW_UPDATE of zero, §6.9 — is noted, not answered
// with a RST_STREAM of this end's.
func TestH2NoResetOnAStreamThePeerClosed(t *testing.T) {
	addr, _ := serveTest(t, Config{}, echoTarget)
	withReset := func(reset bool) (resets int, goAway bool) {
		c := dialH2(t, addr)
		c.frame(h2Frame{Type: frameHeaders, Flags: flagEndHeaders, StreamID: 1, Payload: headBlock("POST", "/open")})
		if reset {
			var cancel [4]byte
			be32put(cancel[:], errCancel)
			c.frame(h2Frame{Type: frameRSTStream, StreamID: 1, Payload: cancel[:]})
		}
		c.frame(h2Frame{Type: frameWindowUpdate, StreamID: 1, Payload: []byte{0, 0, 0, 0}})
		c.frame(h2Frame{Type: framePing, Payload: []byte("roundtrp")})
		c.flush()
		deadline := time.Now().Add(3 * time.Second)
		c.pump(func() bool { return c.pings > 0 || c.sawGoAway || time.Now().After(deadline) })
		return c.resetCount[1], c.sawGoAway
	}
	if n, ga := withReset(true); n != 0 || ga {
		t.Errorf("after the peer's RST_STREAM: %d RST_STREAM from the server (GOAWAY %v), want none on a closed stream", n, ga)
	}
	if n, ga := withReset(false); n != 1 || ga {
		t.Errorf("control, no peer reset: %d RST_STREAM (GOAWAY %v), want exactly one for the zero WINDOW_UPDATE", n, ga)
	}
}

// §5.1 again: a stream enters "closed" once an endpoint "both sends and
// receives a frame with an END_STREAM flag set". A request ended by its
// HEADERS and answered to its END_STREAM is closed on both sides, and a
// late HEADERS or DATA on it — the peer's fault — is answered with nothing,
// where a stream still being answered ("half-closed (remote)") gets the
// STREAM_CLOSED reset the same paragraph asks for.
func TestH2NoResetOnAStreamClosedByBothEndStreams(t *testing.T) {
	for _, late := range []struct {
		name  string
		frame func() h2Frame
	}{
		{"HEADERS", func() h2Frame {
			return h2Frame{Type: frameHeaders, Flags: flagEndHeaders | flagEndStream, StreamID: 1, Payload: headBlock("GET", "/y")}
		}},
		{"DATA", func() h2Frame {
			return h2Frame{Type: frameData, Flags: flagEndStream, StreamID: 1, Payload: []byte("late")}
		}},
	} {
		t.Run(late.name, func(t *testing.T) {
			addr, _ := serveTest(t, Config{}, echoTarget)
			c := dialH2(t, addr)
			c.request(1, "GET", "/x")
			c.flush()
			c.bodies(1)
			if !c.ended[1] {
				t.Fatal("the answer on stream 1 did not end; the case under test is not reached")
			}
			c.frame(late.frame())
			// A second request after it: its answer is the sign the late
			// frame was read, and a RST_STREAM for it would have come first.
			c.request(3, "GET", "/z")
			c.flush()
			c.bodies(3)
			if code, seen := c.resets[1]; seen {
				t.Fatalf("the server sent RST_STREAM(%#x) on stream 1, closed by both END_STREAM flags: §5.1 forbids any frame but PRIORITY there", code)
			}
			if c.sawGoAway {
				t.Fatal("the late frame on a closed stream ended the connection")
			}
		})
	}
}

// A stream closed by both END_STREAM flags stays closed however many answers
// follow it: whether its answer ended is known for as long as this end
// remembers the stream at all. It used to be read from a ring of the last 256
// answers, and a late frame on a stream answered 300 requests earlier drew
// the RST_STREAM that §5.1 forbids on a closed stream. MaxConcurrentStreams
// is raised so the reader still remembers stream 1 after 300 retirements.
func TestH2NoResetOnAClosedStreamAnsweredLongAgo(t *testing.T) {
	addr, _ := serveH2(t, Config{MaxConcurrentStreams: 512}, echoTarget)
	c := dialH2(t, addr)
	c.request(1, "GET", "/first")
	c.flush()
	c.bodies(1)
	if !c.ended[1] {
		t.Fatal("the answer on stream 1 did not end; the case under test is not reached")
	}
	const between = 300
	for i := range between {
		id := uint32(3 + 2*i) //nolint:gosec // G115: a small loop index
		c.request(id, "GET", "/next")
		c.flush()
		c.bodies(2 + i)
	}
	c.frame(h2Frame{Type: frameData, Flags: flagEndStream, StreamID: 1, Payload: []byte("late")})
	last := uint32(3 + 2*between)
	c.request(last, "GET", "/last")
	c.flush()
	c.bodies(2 + between)
	if !c.ended[last] {
		t.Fatal("the last request was not answered; the late frame's effect is unread")
	}
	if code, seen := c.resets[1]; seen {
		t.Fatalf("the server sent RST_STREAM(%#x) on stream 1, closed by both END_STREAM flags %d answers earlier", code, between)
	}
	if c.sawGoAway {
		t.Fatal("the late frame on a closed stream ended the connection")
	}
}

// RFC 9113 §6.4: "After receiving a RST_STREAM on a stream, the receiver
// MUST NOT send additional frames for that stream, except for PRIORITY";
// §5.1 "closed": "An endpoint MUST NOT send frames other than PRIORITY on a
// closed stream". The reader processes the peer's RST_STREAM (fail, retire,
// forget) on its own goroutine while the handler's goroutine, past its
// check of the body's state, queues the 100 Continue (bodyQueue.next ->
// h2Writer.interim) or a WINDOW_UPDATE (sendGrant -> control32); both now
// ask the writer, under its lock, whether the stream is still alive, as
// deliver, extend and reset do.
//
// The race is reproduced without timing: the writer has forgotten the
// stream (what the RST_STREAM branch leaves behind) and the handler pulls
// a body that holds nothing, which is the first wait that sends the 100.
func TestH2NothingQueuedForAStreamTheWriterForgot(t *testing.T) {
	cfg, err := Config{}.resolve()
	if err != nil {
		t.Fatal(err)
	}
	a, b := net.Pipe()
	t.Cleanup(func() { _ = a.Close(); _ = b.Close() })
	w := newH2Writer(a, cfg) // run is not started: the control queue is inspected, never written
	s := &h2Conn{cfg: cfg, w: w, streams: map[uint32]*h2Stream{}, closed: map[uint32]h2State{}}
	st := &h2Stream{id: 1, declared: -1, recvWind: int32(cfg.InitialWindowSize)}
	st.req = Request{Headers: []Header{{Name: []byte("expect"), Value: []byte("100-continue")}}}
	w.open(st.id)
	body := s.newH2Body(st)

	// The peer's RST_STREAM reached the reader: the stream is retired and the
	// writer told to forget it (h2read.go, frame, case frameRSTStream).
	w.forget(st.id)

	// The handler pulls: nothing is held, so next sends the interim 100 and
	// then waits for the peer. The wait is ended by the body failing, as the
	// reader's fail would have done an instant earlier.
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = body.next()
	}()
	time.Sleep(200 * time.Millisecond)
	body.fail(errBodyReset)
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("next did not return")
	}
	// The other frame the handler's goroutine queues: the stream-level
	// WINDOW_UPDATE its reads earn.
	if err := body.sendGrant(1 << 14); err != nil {
		t.Fatal(err)
	}

	w.mu.Lock()
	ctrl := append([]byte(nil), w.ctrl...)
	w.mu.Unlock()
	for len(ctrl) > 0 {
		f, n, err := parseFrame(ctrl, cfg.MaxFrameSize)
		if err != nil {
			t.Fatalf("control queue: %v", err)
		}
		if f.StreamID == st.id {
			t.Fatalf("frame type %#x queued on stream %d after the writer forgot it (RFC 9113 §6.4)", f.Type, st.id)
		}
		ctrl = ctrl[n:]
	}
}

// What 73d0cd7 said of an empty DATA holds for a HEADERS: one on a stream
// both END_STREAM flags closed is decoded and dropped with no answer, and it
// must not count as use of the connection either, or one such frame per
// half IdleTimeout held a connection with no request in flight (RFC 9113
// §10.5; the godoc of Config.IdleTimeout).
func TestH2HeadersOnAClosedStreamDoNotHoldTheIdleClock(t *testing.T) {
	const idle = 200 * time.Millisecond
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	served := make(chan error, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			served <- err
			return
		}
		cfg := Config{Logger: discardLogger(), ReadTimeout: 5 * time.Second, IdleTimeout: idle, LingerTimeout: 100 * time.Millisecond}
		served <- ServeH2Conn(t.Context(), c, cfg, echoTarget)
	}()
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	c := h2clientOn(t, conn)
	c.request(1, "GET", "/once")
	c.flush()
	c.pump(func() bool { return c.ended[1] })
	if !c.ended[1] {
		t.Fatal("the request was not answered")
	}
	late, _ := appendH2Frame(nil, h2Frame{Type: frameHeaders, Flags: flagEndHeaders | flagEndStream, StreamID: 1})
	deadline := time.Now().Add(10 * idle)
	for time.Now().Before(deadline) {
		if _, err := conn.Write(late); err != nil {
			break
		}
		select {
		case err := <-served:
			if errors.Is(err, ErrShutdownForced) {
				t.Fatal(err)
			}
			return // closed under the trickle: the clock did not restart
		case <-time.After(idle / 2):
		}
	}
	select {
	case <-served:
	case <-time.After(idle / 4):
		t.Fatalf("the connection outlived %v of empty HEADERS frames on a closed stream with no request in flight", 10*idle)
	}
}

// A handler's grant may be computed before the peer's RST_STREAM is read and
// sent after: streamGrant asks whether the stream is still in the writer's
// books. It asked a ring of the last 64 forgotten streams, and a stream
// forgotten longer ago got its WINDOW_UPDATE (RFC 9113 §6.4: no frame but
// PRIORITY after a RST_STREAM). It asks the send windows now, which interim
// asks too: exact for every stream.
func TestH2NoGrantOnAStreamForgottenLongAgo(t *testing.T) {
	cfg, err := Config{}.resolve()
	if err != nil {
		t.Fatal(err)
	}
	w := newH2Writer(nil, cfg) // run is not started: the control queue is inspected, never written
	w.open(1)
	w.forget(1)
	for id := uint32(3); id < 3+2*64; id += 2 {
		w.open(id)
		w.forget(id)
	}
	if err := w.streamGrant(1, 100); err != nil {
		t.Fatal(err)
	}
	for ctrl := w.ctrl; len(ctrl) > 0; {
		f, n, err := parseFrame(ctrl, cfg.MaxFrameSize)
		if err != nil {
			t.Fatalf("control queue: %v", err)
		}
		if f.StreamID == 1 {
			t.Fatalf("frame type %#x queued on stream 1, forgotten 65 forgets ago (RFC 9113 §6.4)", f.Type)
		}
		ctrl = ctrl[n:]
	}
}

// A stream that has left the closed-stream ring is still closed: its
// identifier is at or below the highest the client opened, and no answer of
// this end is being written on it. A stream error found on it — a
// WINDOW_UPDATE of zero — is noted, as on a stream the ring still holds,
// not answered with the RST_STREAM RFC 9113 §5.1 forbids on a closed stream
// and charged to the reset allowance.
func TestH2NoResetOnAClosedStreamOutOfTheRing(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		_ = ServeH2Conn(t.Context(), c, Config{Logger: discardLogger(), MaxConcurrentStreams: 1}, echoTarget)
	}()
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	c := h2clientOn(t, conn)
	// With MaxConcurrentStreams 1 the ring holds one stream: stream 1 has
	// left it once stream 3 is answered — or refused, while stream 1 still
	// counts against the limit, which retires stream 3 into the ring all
	// the same.
	c.request(1, "GET", "/a")
	c.flush()
	c.pump(func() bool { return c.ended[1] })
	if !c.ended[1] {
		t.Fatal("stream 1 was not answered")
	}
	c.request(3, "GET", "/a")
	c.flush()
	c.pump(func() bool { return c.ended[3] || c.resets[3] != 0 })
	if !c.ended[3] && c.resets[3] == 0 {
		t.Fatal("nothing came back for stream 3")
	}
	c.frame(h2Frame{Type: frameWindowUpdate, StreamID: 1, Payload: []byte{0, 0, 0, 0}})
	// Answered, or refused while stream 3 still counts against the limit of
	// one: either way after the frame before it was applied.
	c.request(5, "GET", "/c")
	c.flush()
	c.pump(func() bool { return c.ended[5] || c.resets[5] != 0 })
	if !c.ended[5] && c.resets[5] == 0 {
		t.Fatal("nothing came back for the request behind the frame")
	}
	if n := c.resetCount[1]; n > 0 {
		t.Fatalf("RST_STREAM(%#x) sent on stream 1, closed by both END_STREAM flags and answered (RFC 9113 §5.1)", c.resets[1])
	}
}

// A HEADERS on a stream half-closed by the client's END_STREAM is a stream
// error of type STREAM_CLOSED (RFC 9113 §5.1), however many fields it
// decodes to: the field-count bound came first, and such a block drew a 431
// head and RST_STREAM(NO_ERROR), as if it were a request.
func TestH2OversizedHeadersOnAHalfClosedStreamIsStreamClosed(t *testing.T) {
	release := make(chan struct{})
	defer close(release) // before serveTest's cleanup, which waits for the handler
	addr, _ := serveTest(t, Config{MaxHeaders: 64}, func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
		<-release
		return sluice.Batch[Response]{Items: make([]Response, b.Len())}
	})
	c := dialH2(t, addr)
	c.request(1, "GET", "/held")
	var block []byte
	for i := range 65 {
		block = literal(block, "x-f"+strconv.Itoa(i), "v")
	}
	c.frame(h2Frame{Type: frameHeaders, Flags: flagEndHeaders | flagEndStream, StreamID: 1, Payload: block})
	c.flush()
	if code := c.waitReset(1); code != errStreamClosed {
		t.Errorf("stream 1 was reset with %#x, want STREAM_CLOSED (%#x)", code, errStreamClosed)
	}
	if got := h2Status(c.heads[1]); got != 0 {
		t.Errorf("a %d head answered a block on a half-closed stream", got)
	}
}

// The same, when the block's decoded list is past MaxHeaderBytes rather than
// past MaxHeaders: the decoder's bound is judged in blockRefused, which
// answered 431 on any stream not yet closed. 1,000 one-byte references to
// the static table's ":method GET" are 1,000 bytes on the wire, inside every
// frame bound, and 42,000 bytes decoded, past the default 32 KiB list.
func TestH2OverlongHeadersOnAHalfClosedStreamIsStreamClosed(t *testing.T) {
	release := make(chan struct{})
	defer close(release) // before serveTest's cleanup, which waits for the handler
	addr, _ := serveTest(t, Config{}, func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
		<-release
		return sluice.Batch[Response]{Items: make([]Response, b.Len())}
	})
	c := dialH2(t, addr)
	c.request(1, "GET", "/held")
	block := bytes.Repeat([]byte{0x82}, 1000)
	c.frame(h2Frame{Type: frameHeaders, Flags: flagEndHeaders | flagEndStream, StreamID: 1, Payload: block})
	c.flush()
	if code := c.waitReset(1); code != errStreamClosed {
		t.Errorf("stream 1 was reset with %#x, want STREAM_CLOSED (%#x)", code, errStreamClosed)
	}
	if got := h2Status(c.heads[1]); got != 0 {
		t.Errorf("a %d head answered a block on a half-closed stream", got)
	}
}

// A streamed answer whose producer fails — a panic, a length it misses, a
// refused trailer section — long after the peer reset its stream, with any
// number of streams forgotten since, sends no RST_STREAM to the closed
// stream (RFC 9113 §5.1, §6.4): reset asks the send windows, exact for every
// stream, where a ring of the last 64 forgotten let one through. A stream
// still answering is reset.
func TestH2NoResetOnAStreamForgottenLongAgo(t *testing.T) {
	cfg := Config{}.mustResolve()
	w := newH2Writer(nil, cfg) // run is not started: the control queue is read, never written
	w.open(1)
	w.forget(1) // the peer's RST_STREAM
	for id := uint32(3); id < 3+2*64; id += 2 {
		w.open(id)
		w.forget(id)
	}
	w.open(201)
	for _, id := range []uint32{1, 201} {
		if err := w.reset(id, errInternalError); err != nil {
			t.Fatal(err)
		}
	}
	reset := map[uint32]bool{}
	for ctrl := w.ctrl; len(ctrl) > 0; {
		f, n, err := parseFrame(ctrl, cfg.MaxFrameSize)
		if err != nil {
			t.Fatalf("control queue: %v", err)
		}
		reset[f.StreamID] = f.Type == frameRSTStream
		ctrl = ctrl[n:]
	}
	if reset[1] {
		t.Error("RST_STREAM queued on stream 1, forgotten 65 forgets ago")
	}
	if !reset[201] {
		t.Error("no RST_STREAM on stream 201, still answering")
	}
}
