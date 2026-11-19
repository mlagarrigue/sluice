//go:build !(linux || darwin || freebsd || netbsd || openbsd || dragonfly)

package postgres

import "io"

// peekIdle cannot look at a socket without reading from it on this build:
// an idle connection is lent as it is, and the call that finds it ended
// fails, as it would without a probe.
func peekIdle(io.ReadWriteCloser) (quiet, looked bool) { return false, false }
