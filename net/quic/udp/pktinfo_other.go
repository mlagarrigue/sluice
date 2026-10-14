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

// The ECN field goes the same way: Darwin reports it through IP_RECVTOS
// and IPV6_RECVTCLASS too, Windows through IP_ECN / IPV6_ECN with its own
// layout; both wait for the same machine. Without it this endpoint
// neither reads nor sets the field (RFC 9000 §13.4.1).
var errNoECN = fmt.Errorf("ecn socket option: %w", errors.ErrUnsupported)

func enablePacketInfo(fd uintptr, v6 bool) error { return errNoPacketInfo }

func packetDestination(oob []byte) (netip.Addr, int, bool) { return netip.Addr{}, 0, false }

func appendPacketSource(oob []byte, src netip.Addr, ifindex int) []byte { return oob }

func enableECN(fd uintptr, v6 bool) error { return errNoECN }

func packetECN(oob []byte) (uint8, bool) { return 0, false }

func appendECN(oob []byte, ecn uint8) []byte { return oob }
