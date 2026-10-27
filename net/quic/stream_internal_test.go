package quic

import (
	"bytes"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/internal/varint"
)

// Tests for the sending direction of a stream that drive a Conn's internals
// directly: what a writer may and may not put on the wire once the peer, the
// window or the connection has moved under it.

// shutController holds the congestion window shut until a test opens it, so
// a writer parks on the first packet it tries to send, with its frame built
// and its credit charged. Read and written under conn.mu, like the
// controller it wraps.
type shutController struct {
	congestionController
	open bool
}

func (s *shutController) canSend(n int) bool {
	return s.open && s.congestionController.canSend(n)
}

// RFC 9000 §3.3 forbids a STREAM frame after a RESET_STREAM on the same
// stream, and §4.5 has a receiver answer bytes past the declared final size
// with FINAL_SIZE_ERROR — which ends the connection, and every other request
// in flight on it.
//
// A writer parked on the congestion window had already built its STREAM
// frame at s.writeOff and charged its credit. The wait releases conn.mu, and
// until 2026-10-26 it re-read nothing of the stream on waking. A peer's
// STOP_SENDING arriving meanwhile is acted on by the receive goroutine: it
// abandons this end's sending direction and answers RESET_STREAM with a
// final size of exactly that s.writeOff (§3.5), then broadcasts — and the
// writer woke and sent its frame at the offset just declared final. Two
// sluice ends closed the connection on it; quic-go and ngtcp2 do the same.
//
// The write must fail instead, and nothing must leave after the reset.
func TestParkedWriterDropsItsFrameWhenTheStreamIsReset(t *testing.T) {
	pc := &recordingPC{addr: fakeFuzzAddr("parked02:self")}
	peerPC := &recordingPC{addr: fakeFuzzAddr("parked02:peer")}
	c, _ := newKeyedConnOn(t, pc, peerPC)

	const sid = 1
	c.mu.Lock()
	c.cc = &shutController{congestionController: c.cc}
	c.connSend.grant(1 << 20)
	s := newStream(c, sid)
	c.streams[sid] = s
	s.sendQ.grant(1 << 20)
	c.mu.Unlock()

	type result struct {
		n   int
		err error
	}
	done := make(chan result, 1)
	go func() {
		n, err := s.Write(make([]byte, 4096))
		done <- result{n, err}
	}()

	// Parked: the frame is built and its credit charged — that happens
	// before the window is asked — and no datagram has left, because the
	// window is shut.
	parked := false
	for range 20_000 {
		c.mu.Lock()
		charged := c.connSend.sent > 0
		c.mu.Unlock()
		if charged && len(pc.recorded()) == 0 {
			parked = true
			break
		}
		select {
		case r := <-done:
			t.Fatalf("the write returned (%d bytes, %v) without parking; the sequence under test was not reached", r.n, r.err)
		default:
		}
		time.Sleep(100 * time.Microsecond)
	}
	if !parked {
		t.Fatal("the writer never parked on the congestion window; the sequence under test was not reached")
	}

	// The peer cancels the request. RESET_STREAM leaves ungated, declaring
	// the writer's own offset final.
	stop := varint.Append(nil, FrameStopSending)
	stop = varint.Append(stop, sid)
	stop = varint.Append(stop, 7)
	if err := c.frames(spaceApplication, stop, time.Now()); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	finalSize, reset := s.writeOff, s.writeReset
	c.mu.Unlock()
	if !reset {
		t.Fatal("the STOP_SENDING did not abandon this end's sending direction; the sequence under test was not reached")
	}
	afterReset := len(pc.recorded())
	if afterReset == 0 {
		t.Fatal("no RESET_STREAM left; the sequence under test was not reached")
	}

	// The window opens, as an acknowledgement would open it.
	c.mu.Lock()
	c.cc.(*shutController).open = true
	c.cond.Broadcast()
	c.mu.Unlock()

	var r result
	select {
	case r = <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("the parked write never returned")
	}
	if r.err == nil {
		t.Errorf("the write returned %d bytes and no error on a stream the peer had cancelled", r.n)
	}
	if n := len(pc.recorded()); n != afterReset {
		t.Errorf("%d datagrams left after the RESET_STREAM: the parked writer sent its frame at offset %d, the final size the reset declared (RFC 9000 §3.3, §4.5)",
			n-afterReset, finalSize)
	}
	c.mu.Lock()
	off := s.writeOff
	c.mu.Unlock()
	if off != finalSize {
		t.Errorf("s.writeOff moved to %d past the final size %d the reset declared", off, finalSize)
	}
}

// A stream whose bytes reached the read buffer, were partly read, then
// refused with StopSending and settled by the peer's RESET_STREAM.
//
// The connection window is given back as bytes are consumed, and abandon
// credits only what was never delivered to the application (finalSize minus
// what the reassembly handed over). Bytes already in the read buffer are
// credited by whoever reads them — and after StopSending nobody does: the
// application has said it will not read the rest. Until 2026-10-26 every
// stream refused that way took its unread bytes off the connection window
// for good: sixteen streams of 64 KiB with sixteen bytes read from each
// exhausted a 1 MiB window, and the peer's next byte on a live connection
// answered FLOW_CONTROL_ERROR.
func TestStopSendingGivesBackTheConnectionWindow(t *testing.T) {
	const (
		streams  = 16
		perBytes = 1 << 16 // 64 KiB, under the 256 KiB stream window
		readN    = 16
	)
	params := DefaultParameters()
	c := newConn(&fuzzSink{addr: "quict06:self"}, fakeFuzzAddr("quict06:peer"),
		[]byte{1, 2, 3, 4}, []byte{5, 6, 7, 8}, false, params)
	defer c.Close()

	data := bytes.Repeat([]byte("x"), perBytes)
	for i := range streams {
		id := uint64(i) * 4 // the peer is the client: bidi ids 0, 4, 8, …
		if err := c.frames(spaceApplication, streamFrameBytes(id, 0, data), time.Now()); err != nil {
			t.Fatalf("stream %d: delivering %d bytes: %v", id, perBytes, err)
		}
		s, err := c.AcceptStream(t.Context())
		if err != nil {
			t.Fatalf("stream %d: accept: %v", id, err)
		}
		buf := make([]byte, readN)
		if n, err := s.Read(buf); err != nil || n != readN {
			t.Fatalf("stream %d: read %d bytes, %v", id, n, err)
		}
		// The application says it will not read the rest.
		if err := s.StopSending(0); err != nil {
			t.Logf("stream %d: StopSending: %v (no keys on this bare conn)", id, err)
		}
		// The peer answers, as §3.5 requires, with the final size it sent.
		if err := c.frames(spaceApplication, appendFrame(nil, FrameResetStream, id, 0, perBytes), time.Now()); err != nil {
			t.Fatalf("stream %d: RESET_STREAM: %v", id, err)
		}
	}

	c.mu.Lock()
	maxData, highest, consumed := c.connRecv.max, c.connRecv.highest, c.connRecv.consumed
	c.mu.Unlock()

	var left uint64
	if maxData > highest {
		left = maxData - highest
	}
	t.Logf("connection window: granted %d, received %d, credited back %d, left for the peer %d",
		maxData, highest, consumed, left)
	// What the shut window costs the peer: one more byte on a fresh stream,
	// on a connection that is otherwise live, is a FLOW_CONTROL_ERROR.
	err := c.frames(spaceApplication, streamFrameBytes(streams*4, 0, []byte("y")), time.Now())
	t.Logf("one more byte from the peer: %v", err)

	if left < params.InitialMaxStreamDataBidiRemote {
		t.Fatalf("after %d streams refused with StopSending the peer has %d bytes of connection credit left, less than one stream window (%d); the next byte it sends answers %v",
			streams, left, params.InitialMaxStreamDataBidiRemote, err)
	}
}

// RFC 9000 §3.5: "STREAM frames received after sending a STOP_SENDING frame
// are still counted toward connection and stream flow control, even though
// these frames can be discarded upon receipt." A conformant peer has at
// least a round trip of data in flight when the STOP_SENDING reaches it.
// Until 2026-10-27 those bytes were counted, delivered into a read buffer
// nobody would drain (the application had said so), and never credited; the
// peer's RESET_STREAM settles only what was never delivered. Twenty-four
// streams of 32 KiB, half of it in flight at the STOP_SENDING, left 256 KiB
// of a 1 MiB window (measured). Now they are dropped on delivery and the
// window gets them back at once, with the read buffer staying empty.
func TestBytesAfterStopSendingAreGivenBack(t *testing.T) {
	c := boundsConn(t, false) // a server: client-initiated bidi streams are 0, 4, 8...
	installAppSealer(t, c)

	const half = 1 << 14 // bytes the peer delivers before, and again after, the STOP_SENDING
	const read = 16
	const streams = 24
	for i := range streams {
		id := uint64(i) * 4
		if err := c.frames(spaceApplication, streamFrame(nil, id, 0, string(make([]byte, half)), false), time.Now()); err != nil {
			t.Fatalf("stream %d first half: %v", id, err)
		}
		s, err := c.Stream(id)
		if err != nil {
			t.Fatalf("stream %d: %v", id, err)
		}
		if _, err := s.Read(make([]byte, read)); err != nil {
			t.Fatalf("stream %d read: %v", id, err)
		}
		if err := s.StopSending(0x10c); err != nil {
			t.Fatalf("stream %d StopSending: %v", id, err)
		}
		// In flight when the STOP_SENDING left: the second half arrives.
		if err := c.frames(spaceApplication, streamFrame(nil, id, half, string(make([]byte, half)), false), time.Now()); err != nil {
			t.Fatalf("stream %d second half: %v", id, err)
		}
		s.mu.Lock()
		buffered := len(s.buf)
		s.mu.Unlock()
		if buffered != 0 {
			t.Errorf("stream %d holds %d bytes in its read buffer after StopSending: nobody will read them", id, buffered)
		}
		// Then the peer's RESET_STREAM at the final size, §3.5's answer.
		if err := c.frames(spaceApplication, appendFrame(nil, FrameResetStream, id, 0x10c, 2*half), time.Now()); err != nil {
			t.Fatalf("stream %d reset: %v", id, err)
		}
	}
	c.mu.Lock()
	highest, consumed, maxData := c.connRecv.highest, c.connRecv.consumed, c.connRecv.max
	c.mu.Unlock()
	if highest != streams*2*half {
		t.Errorf("received %d bytes on the connection, want %d: every byte counts, §3.5", highest, streams*2*half)
	}
	if consumed != highest {
		t.Errorf("credited %d bytes of the %d received: %d bytes are counted and never given back", consumed, highest, highest-consumed)
	}
	if left := maxData - highest; left < 1<<19 {
		t.Errorf("after %d streams abandoned with %d bytes in flight each, the peer has %d bytes of connection credit left (granted %d), under half the window",
			streams, half, left, maxData)
	}
}
