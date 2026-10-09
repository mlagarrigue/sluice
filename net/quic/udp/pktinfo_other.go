//go:build !linux

package udp

import (
	"errors"
	"fmt"
	"net/netip"
)

// Packet information is wired on Linux only. Darwin has IP_PKTINFO and
// IPV6_RECVPKTINFO too, with a 4-byte-aligned cmsghdr, and Windows has
// IP_PKTINFO with its own WSACMSGHDR layout; both wait for a machine to
// verify them on. Here the listener keeps letting the kernel pick a
// reply's source.
var errNoPacketInfo = fmt.Errorf("packet-info socket option: %w", errors.ErrUnsupported)

func enablePacketInfo(fd uintptr, v6 bool) error { return errNoPacketInfo }

func packetDestination(oob []byte) (netip.Addr, int, bool) { return netip.Addr{}, 0, false }

func appendPacketSource(oob []byte, src netip.Addr, ifindex int) []byte { return oob }
