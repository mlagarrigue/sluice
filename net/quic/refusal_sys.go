//go:build unix

package quic

import (
	"errors"
	"syscall"
)

// isRefusal reports whether a socket error is an ICMP refusal surfacing:
// port unreachable as ECONNREFUSED, or ECONNRESET.
func isRefusal(err error) bool {
	return errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET)
}
