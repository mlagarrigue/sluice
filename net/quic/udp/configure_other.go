//go:build !linux && !darwin && !windows

package udp

import (
	"errors"
	"fmt"
)

// errUnsupported is what configure returns where no Don't Fragment option
// is known; only this build needs it.
var errUnsupported = fmt.Errorf("don't-fragment socket option: %w", errors.ErrUnsupported)

// configure on a platform with no known Don't Fragment option: nothing is
// changed and the caller learns so. The BSDs do have IP_DONTFRAG /
// IPV6_DONTFRAG (FreeBSD: 67 / 62) and would slot in like Darwin; they
// wait for a machine to verify them on.
func configure(fd uintptr, v6 bool) error {
	return errUnsupported
}
