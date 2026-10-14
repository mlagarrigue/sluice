//go:build !linux && !darwin && !windows

package udp

import (
	"errors"
	"fmt"
)

// The ECN field is read and set on Linux, Darwin and Windows. The BSDs
// report it through IP_RECVTOS / IPV6_RECVTCLASS like Darwin and would
// slot in the same way; they wait for a machine to verify them on. Without
// it this endpoint neither reads nor sets the field (RFC 9000 §13.4.1).
var errNoECN = fmt.Errorf("ecn socket option: %w", errors.ErrUnsupported)

func enableECN(fd uintptr, v6 bool) error { return errNoECN }

func packetECN(oob []byte) (uint8, bool) { return 0, false }

func appendECN(oob []byte, ecn uint8) []byte { return oob }
