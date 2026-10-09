//go:build unix || windows

package quic

import (
	"errors"
	"syscall"
)

// isRefusal reports whether a socket error is an ICMP refusal surfacing:
// port unreachable as ECONNREFUSED, or the reset Windows reports for it.
func isRefusal(err error) bool {
	return errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET)
}
