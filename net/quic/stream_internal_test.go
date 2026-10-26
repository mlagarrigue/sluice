package quic

import (
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
