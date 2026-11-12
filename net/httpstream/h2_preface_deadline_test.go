package httpstream

import (
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

// pieceConn hands its bytes out a few at a time, then io.EOF.
type pieceConn struct {
	net.Conn // nil: only Read and SetReadDeadline are called
	data     []byte
	piece    int
}

func (c *pieceConn) Read(p []byte) (int, error) {
	if len(c.data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, c.data[:min(len(c.data), c.piece)])
	c.data = c.data[n:]
	return n, nil
}

func (c *pieceConn) SetReadDeadline(time.Time) error { return nil }

// frameReader.deadline is the bound on the frame begun, held from its first
// byte and dropped when the frame is consumed. The preface consumes bytes
// without producing a frame, and its deadline stayed: a preface that came in
// pieces handed what was left of its ReadTimeout to the first frame after
// it. The idle clock, ReadTimeout from the first silent round, is the nearer
// of the two until a stream is used, so no path reaches the stale deadline
// today; the reader's rule is that a consumed frame leaves none behind.
func TestH2PrefaceDeadlineEndsWithThePreface(t *testing.T) {
	cfg := Config{ReadTimeout: time.Second}.mustResolve()
	r := &frameReader{conn: &pieceConn{data: []byte(Preface), piece: 1}, cfg: cfg}
	if batch, ok := r.next(); ok {
		t.Fatalf("a batch %+v from the preface alone", batch)
	}
	if !errors.Is(r.err, io.EOF) {
		t.Fatalf("the reader ended with %v, want io.EOF after the preface", r.err)
	}
	if !r.deadline.IsZero() {
		t.Errorf("the preface's read deadline %v is still held once it was consumed", r.deadline)
	}
}
