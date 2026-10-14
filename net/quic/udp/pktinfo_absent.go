//go:build !linux

package udp

import (
	"errors"
	"fmt"
	"net/netip"
)

// Packet information is wired on Linux only. Darwin has IP_PKTINFO (26) and
// IPV6_RECVPKTINFO (61) too, Windows IP_PKTINFO / IPV6_PKTINFO (19 / 19);
// both wait for a slice of their own. Here the listener keeps letting the
// kernel pick a reply's source.
var errNoPacketInfo = fmt.Errorf("packet-info socket option: %w", errors.ErrUnsupported)

func enablePacketInfo(fd uintptr, v6 bool) error { return errNoPacketInfo }

func packetDestination(oob []byte) (netip.Addr, int, bool) { return netip.Addr{}, 0, false }

func appendPacketSource(oob []byte, src netip.Addr, ifindex int) []byte { return oob }
