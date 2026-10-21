package quic

import (
	"errors"
	"io"
	"testing"
	"time"
)

// deliverAs mimics the read loop's two steps for one frame: deliver under the
// stream's lock, then — later, outside it — keepForRead. The gap between the
// two is where a Read may run.
func deliverAs(t *testing.T, s *Stream, off uint64, data string, fin bool) (delta []byte, finished bool) {
	t.Helper()
	grown, finished, _, err := s.deliver(Frame{Type: FrameStream, StreamID: s.id, offset: off, Data: []byte(data), Fin: fin}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return grown, finished
}

type readResult struct {
	n   int
	err error
}

func readAsync(s *Stream, p []byte) <-chan readResult {
	out := make(chan readResult, 1)
	go func() {
		n, err := s.Read(p)
		out <- readResult{n, err}
	}()
	return out
}

// Read must not report the end of a stream before the last delta has reached
// its buffer. deliver marks the stream finished under the lock on the read
// loop; the bytes follow in keepForRead, after the lock is released. A Read
// looping between the two used to find an empty buffer on a finished stream
// and return io.EOF — the last frame's bytes lost. quic-go's client caught it:
// one echo in twenty came back a frame short (interop/quicgo_test.go).
func TestReadWaitsForTheLastDeltaBeforeEOF(t *testing.T) {
	c, _ := newBenchConn(t)
	defer c.Close()
	s := newStream(c, 0)

	delta, finished := deliverAs(t, s, 0, "hello, ", false)
	s.keepForRead(delta, finished)
	last, finished := deliverAs(t, s, 7, "world", true)
	if !finished {
		t.Fatal("the FIN frame did not finish the stream")
	}

	buf := make([]byte, 64)
	n, err := s.Read(buf)
	if err != nil || string(buf[:n]) != "hello, " {
		t.Fatalf("first Read: %q, %v", buf[:n], err)
	}
	pending := readAsync(s, buf)
	select {
	case r := <-pending:
		t.Fatalf("Read returned (%d, %v) before the last delta was buffered", r.n, r.err)
	case <-time.After(50 * time.Millisecond):
	}
	s.keepForRead(last, finished)
	r := <-pending
	if r.err != nil || string(buf[:r.n]) != "world" {
		t.Fatalf("second Read: %q, %v", buf[:r.n], r.err)
	}
	if _, err := s.Read(buf); !errors.Is(err, io.EOF) {
		t.Fatalf("third Read: %v, want io.EOF", err)
	}
}

// The same gap exists for RESET_STREAM: abandon records the reset under the
// lock while the deltas of the same packet are still on their way to the
// buffer. The reset must be reported behind them, not instead of them.
func TestReadDrainsDeltasBeforeReportingReset(t *testing.T) {
	c, _ := newBenchConn(t)
	defer c.Close()
	s := newStream(c, 0)

	delta, finished := deliverAs(t, s, 0, "partial", false)
	if _, _, err := s.abandon(0x7, 7); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 64)
	pending := readAsync(s, buf)
	select {
	case r := <-pending:
		t.Fatalf("Read returned (%d, %v) before the delta was buffered", r.n, r.err)
	case <-time.After(50 * time.Millisecond):
	}
	s.keepForRead(delta, finished)
	s.endForRead()
	r := <-pending
	if r.err != nil || string(buf[:r.n]) != "partial" {
		t.Fatalf("first Read: %q, %v", buf[:r.n], r.err)
	}
	if _, err := s.Read(buf); err == nil || !errors.Is(err, ErrQUIC) {
		t.Fatalf("second Read: %v, want the reset error", err)
	}
}

// RFC 9000 §4.5: "Once a final size for a stream is known, it cannot
// change", whether a FIN or a RESET_STREAM declared it. After a reset, data
// at or past the final size, a FIN at another offset and a second reset to
// another size are each FINAL_SIZE_ERROR (§20.1's three cases). Until
// 2026-10-21 only a FIN recorded the final size, so a reset stream took a
// larger reset silently and data beyond its end as ordinary.
func TestResetFinalSizeIsHeldLikeAFin(t *testing.T) {
	finalSizeErr := func(t *testing.T, err error, what string) {
		t.Helper()
		var te *transportError
		if !errors.As(err, &te) || te.code != transportFinalSize {
			t.Fatalf("%s: %v, want FINAL_SIZE_ERROR", what, err)
		}
	}
	c, _ := newBenchConn(t)
	defer c.Close()

	s := newStream(c, 0)
	deliverAs(t, s, 0, "partial", false)
	if _, _, err := s.abandon(0x7, 10); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.abandon(0x7, 10); err != nil {
		t.Fatalf("the same reset again: %v, want it tolerated", err)
	}
	_, _, err := s.abandon(0x7, 12)
	finalSizeErr(t, err, "a second reset to a larger final size")
	_, _, _, err = s.deliver(Frame{Type: FrameStream, StreamID: s.id, offset: 10, Data: []byte("x")}, nil)
	finalSizeErr(t, err, "data at the final size after a reset")
	_, _, _, err = s.deliver(Frame{Type: FrameStream, StreamID: s.id, offset: 7, Data: []byte("abcd"), Fin: true}, nil)
	finalSizeErr(t, err, "a FIN past the final size after a reset")
	_, _, _, err = s.deliver(Frame{Type: FrameStream, StreamID: s.id, offset: 8, Fin: true}, nil)
	finalSizeErr(t, err, "a FIN below the final size after a reset")
	if _, _, _, err := s.deliver(Frame{Type: FrameStream, StreamID: s.id, offset: 7, Data: []byte("abc"), Fin: true}, nil); err != nil {
		t.Fatalf("a FIN at the reset's final size: %v, want it accepted", err)
	}
}
