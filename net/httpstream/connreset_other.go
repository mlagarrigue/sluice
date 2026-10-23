//go:build !windows

package httpstream

import (
	"errors"
	"syscall"
)

// isConnReset reports a connection the peer reset, the host aborted, or
// that broke under a write, an ordinary end (reportConnEnd).
func isConnReset(err error) bool {
	return errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNABORTED) ||
		errors.Is(err, syscall.EPIPE)
}
