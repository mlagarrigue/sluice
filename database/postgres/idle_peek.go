//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package postgres

import (
	"errors"
	"io"
	"net"
	"syscall"
)

// peekIdle looks at the socket under rw without reading from it and without
// waiting: one recv with MSG_PEEK and MSG_DONTWAIT, which takes nothing off
// the socket, so a TLS stream above it is left as it was. It reports whether
// the socket is quiet — nothing arrived, the peer has not closed — and
// whether it could look at all: a transport that is not a socket, or one
// this build cannot reach, is not judged here.
//
// Measured against a read with a deadline a moment ahead, which needs no
// system call of its own: that wait cost half a millisecond per lend, the
// timer's wake-up rather than the read (BenchmarkPoolLendIdle, 2026-11-19).
func peekIdle(rw io.ReadWriteCloser) (quiet, looked bool) {
	var nc any = rw
	if t, ok := rw.(interface{ NetConn() net.Conn }); ok {
		nc = t.NetConn()
	}
	sc, ok := nc.(syscall.Conn)
	if !ok {
		return false, false
	}
	raw, err := sc.SyscallConn()
	if err != nil {
		return false, true
	}
	var buf [1]byte
	var rerr error
	// One look: the callback reports done, so the runtime never waits.
	if err := raw.Read(func(fd uintptr) bool {
		_, _, rerr = syscall.Recvfrom(int(fd), buf[:], syscall.MSG_PEEK|syscall.MSG_DONTWAIT) //nolint:gosec // G115: a socket descriptor fits an int
		return true
	}); err != nil {
		return false, true
	}
	if errors.Is(rerr, syscall.EAGAIN) || errors.Is(rerr, syscall.EWOULDBLOCK) {
		return true, true
	}
	// Bytes waiting, the peer's end (a peek of none) or a socket error.
	return false, true
}
