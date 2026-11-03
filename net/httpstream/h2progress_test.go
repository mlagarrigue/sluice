package httpstream

import (
	"net"
	"os"
	"testing"
	"time"
)

// slowReaderConn is a client that reads steadily but slower than one whole
// write: each deadline lets grant bytes through, and a longer write reports
// what it wrote with os.ErrDeadlineExceeded, as a net.Conn does when its
// deadline passes mid-write.
type slowReaderConn struct {
	net.Conn
	grant   int
	left    int
	written int
}

func (c *slowReaderConn) SetWriteDeadline(time.Time) error { c.left = c.grant; return nil }

func (c *slowReaderConn) Write(p []byte) (int, error) {
	n := min(len(p), c.left)
	c.left -= n
	c.written += n
	if n < len(p) {
		return n, os.ErrDeadlineExceeded
	}
	return n, nil
}

func (c *slowReaderConn) Close() error { return nil }

// WriteProgressBytes moves WriteTimeout on every protocol. HTTP/1.1 and
// HTTP/3 wrote through writeBound; the HTTP/2 writer set one deadline for a
// whole round — up to 64 KiB at the defaults — and closed the connection
// when it passed, so a client reading at 8 KiB per timeout, twenty times the
// floor the field promises, was cut where HTTP/1.1 serves it.
func TestH2SocketWriteKeepsTheProgressRule(t *testing.T) {
	cfg := Config{WriteTimeout: time.Second, WriteProgressBytes: 4 << 10, MaxWriteBufferBytes: 64 << 10}
	buf := make([]byte, 64<<10) // one round at the default budget
	const grant = 8 << 10       // twice the progress per timeout: a client that reads

	h1 := &slowReaderConn{grant: grant}
	if err := writeAll(h1, cfg, buf); err != nil || h1.written != len(buf) {
		t.Fatalf("control: writeBound gave up on a reading client: err=%v written=%d", err, h1.written)
	}
	h2c := &slowReaderConn{grant: grant}
	w := &h2Writer{conn: h2c, cfg: cfg}
	if err := w.writeAll(buf); err != nil || h2c.written != len(buf) {
		t.Fatalf("h2Writer.writeAll gave up after %d of %d bytes, each timeout seeing %d >= WriteProgressBytes %d: %v",
			h2c.written, len(buf), grant, cfg.WriteProgressBytes, err)
	}
	// And a client under the floor is still cut, as on HTTP/1.1.
	stalled := &slowReaderConn{grant: 1 << 10}
	w = &h2Writer{conn: stalled, cfg: cfg}
	if err := w.writeAll(buf); err == nil {
		t.Fatal("a client reading 1 KiB per timeout, under WriteProgressBytes, was kept")
	}
}
