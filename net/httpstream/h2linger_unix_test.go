//go:build unix

package httpstream

import (
	"net"
	"syscall"
	"testing"
	"time"
)

// RFC 9113 §5.4.1: the GOAWAY of a connection error reaches a peer that is
// still writing. Closing a socket with unread bytes in it makes TCP answer
// with a reset, which throws away what the socket had not yet sent; so
// ServeH2Conn closes its write side first and reads what the peer still
// sends before it closes (linger). Here the GOAWAY cannot have left when
// the connection fails: the client reads nothing, and its receive buffer,
// set small before the connection opens so that the window it announces is
// small too, is full of the acknowledgements of its PINGs. Behind the frame
// that fails, more frames the server never reads.
func TestH2ErrorGoAwayOutlivesUnreadFrames(t *testing.T) {
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
		served <- ServeH2Conn(t.Context(), c, Config{Logger: discardLogger(), LingerTimeout: 100 * time.Millisecond}, echoTarget)
	}()

	d := net.Dialer{Control: func(_, _ string, rc syscall.RawConn) error {
		var err error
		if cerr := rc.Control(func(fd uintptr) {
			err = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, 4<<10)
		}); cerr != nil {
			return cerr
		}
		return err
	}}
	conn, err := d.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	c := h2clientOn(t, conn)
	const pings = 2000 // 34 kB of acknowledgements, past the window announced
	ping := h2Frame{Type: framePing, Payload: make([]byte, 8)}
	for range pings {
		c.frame(ping)
	}
	c.frame(h2Frame{Type: frameWindowUpdate, Payload: windowUpdatePayload(0)})
	for range pings {
		c.frame(ping)
	}
	c.flush()
	select {
	case <-served:
	case <-time.After(5 * time.Second):
		t.Fatal("ServeH2Conn did not return after the connection error")
	}
	if code := goAwayCode(c.waitGoAway()); code != 0x1 {
		t.Fatalf("GOAWAY code = %#x, want 0x1 PROTOCOL_ERROR", code)
	}
	if c.pings != pings {
		t.Errorf("%d PINGs acknowledged before the GOAWAY, want %d", c.pings, pings)
	}
}
