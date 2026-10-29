//go:build unix

package quic

import "syscall"

// platformRefusal is the error a refused send or read surfaces with here,
// the one isRefusal recognises.
var platformRefusal error = syscall.ECONNREFUSED
