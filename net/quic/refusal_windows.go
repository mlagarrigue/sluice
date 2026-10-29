//go:build windows

package quic

import (
	"errors"
	"syscall"
)

// wsaeConnRefused is Winsock's WSAECONNREFUSED, which package syscall does
// not name.
const wsaeConnRefused syscall.Errno = 10061

// isRefusal reports whether a socket error is an ICMP refusal surfacing.
// Winsock reports a port unreachable as WSAECONNRESET, or WSAECONNREFUSED on
// a connected socket; syscall's ECONNRESET and ECONNREFUSED are values of its
// own on Windows that no Winsock error matches, so the codes are named here.
//
// Package net turns SIO_UDP_CONNRESET off on every UDP socket it opens, and
// udp.Configure does too, so on those sockets the refusal never surfaces: a
// Dial to a closed port waits out the handshake deadline rather than failing
// at the first flight. A PacketConn that keeps the option on gets the early
// failure.
func isRefusal(err error) bool {
	return errors.Is(err, syscall.WSAECONNRESET) || errors.Is(err, wsaeConnRefused)
}
