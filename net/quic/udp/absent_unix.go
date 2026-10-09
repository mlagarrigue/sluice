//go:build unix

package udp

import (
	"errors"
	"syscall"
)

// ignoreOptionAbsent treats the errors a kernel returns for an option it
// does not have, or for the wrong address family, as "not applied" rather
// than failure. It is used only for the best-effort options.
func ignoreOptionAbsent(err error) error {
	if errors.Is(err, syscall.ENOPROTOOPT) || errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.EOPNOTSUPP) {
		return nil
	}
	return err
}
