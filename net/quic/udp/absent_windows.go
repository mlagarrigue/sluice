package udp

import (
	"errors"
	"syscall"
)

// Winsock error codes (winerror.h). Package syscall's Windows build gives
// the POSIX names invented values that Winsock never returns, so the real
// ones are spelled here.
const (
	wsaEINVAL      syscall.Errno = 10022
	wsaENOPROTOOPT syscall.Errno = 10042
	wsaEOPNOTSUPP  syscall.Errno = 10045
)

// ignoreOptionAbsent treats the errors Winsock returns for an option it
// does not have, or for the wrong address family, as "not applied" rather
// than failure. It is used only for the best-effort options.
func ignoreOptionAbsent(err error) error {
	if errors.Is(err, wsaENOPROTOOPT) || errors.Is(err, wsaEINVAL) || errors.Is(err, wsaEOPNOTSUPP) {
		return nil
	}
	return err
}
