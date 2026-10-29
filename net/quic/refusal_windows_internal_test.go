//go:build windows

package quic

import (
	"fmt"
	"os"
	"syscall"
	"testing"
)

// The Winsock codes a refusal arrives as are recognised, wrapped as a read
// error wraps them; syscall's own ECONNRESET, which no Winsock error
// matches on Windows, is not what a socket returns.
func TestIsRefusalWindowsCodes(t *testing.T) {
	for _, code := range []syscall.Errno{syscall.WSAECONNRESET, wsaeConnRefused} {
		err := fmt.Errorf("read: %w", os.NewSyscallError("wsarecvfrom", code))
		if !isRefusal(err) {
			t.Errorf("Errno(%d) is not recognised as a refusal", uint32(code))
		}
	}
	if isRefusal(syscall.ERROR_FILE_NOT_FOUND) {
		t.Error("another Winsock error is taken for a refusal")
	}
}
