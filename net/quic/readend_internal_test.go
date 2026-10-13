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
