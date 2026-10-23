//go:build windows

package httpstream

import (
	"errors"
	"syscall"
)

// isConnReset reports a connection the peer reset or the host aborted, an
// ordinary end (reportConnEnd). Windows reports its own codes, which
// errors.Is does not match against syscall.ECONNRESET: a client that closes
// with the server's last frames unread is a WSAECONNRESET there.
func isConnReset(err error) bool {
	return errors.Is(err, syscall.WSAECONNRESET) || errors.Is(err, syscall.WSAECONNABORTED)
}
