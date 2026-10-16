package udp

import (
	"encoding/binary"
	"net/netip"
	"syscall"
)

// Winsock names (ws2ipdef.h), read from the SDK header, not from memory:
// package syscall's Windows tables carry none of them. IP_ECN and
// IPV6_ECN are both the option that asks for the field (the documentation
// spells it IP_RECVECN / IPV6_RECVECN, the same value) and the type of
// the message that carries it, an INT, on both directions.
const (
	ipECN       = 50 // IP_ECN       (IPPROTO_IP)
	ipv6ECN     = 50 // IPV6_ECN     (IPPROTO_IPV6)
	ipPktinfo   = 19 // IP_PKTINFO   (IPPROTO_IP), the option and the message
	ipv6Pktinfo = 19 // IPV6_PKTINFO (IPPROTO_IPV6), the option and the message
)

// enablePacketInfo sets IP_PKTINFO on an AF_INET socket and IPV6_PKTINFO
// on an AF_INET6 one. A dual-stack socket reports IPv4 traffic through its
// own IP_PKTINFO message, so that option is set too, as for IP_ECN and as
// msquic does.
func enablePacketInfo(fd uintptr, v6 bool) error {
	h := syscall.Handle(fd)
	if v6 {
		if err := syscall.SetsockoptInt(h, syscall.IPPROTO_IPV6, ipv6Pktinfo, 1); err != nil {
			return err
		}
		return ignoreOptionAbsent(syscall.SetsockoptInt(h, syscall.IPPROTO_IP, ipPktinfo, 1))
	}
	return syscall.SetsockoptInt(h, syscall.IPPROTO_IP, ipPktinfo, 1)
}

// enableECN sets IP_ECN on an AF_INET socket and IPV6_ECN on an AF_INET6
// one. On a dual-stack socket bound to the wildcard Winsock documents
// that both must be set, each family's option covering its own traffic.
func enableECN(fd uintptr, v6 bool) error {
	h := syscall.Handle(fd)
	if v6 {
		if err := syscall.SetsockoptInt(h, syscall.IPPROTO_IPV6, ipv6ECN, 1); err != nil {
			return err
		}
		return ignoreOptionAbsent(syscall.SetsockoptInt(h, syscall.IPPROTO_IP, ipECN, 1))
	}
	return syscall.SetsockoptInt(h, syscall.IPPROTO_IP, ipECN, 1)
}

// packetECN finds the IP_ECN or IPV6_ECN message, an INT holding the two
// ECN bits alone.
func packetECN(oob []byte) (uint8, bool) {
	for {
		level, typ, data, rest, ok := nextCmsg(oob)
		if !ok {
			return 0, false
		}
		switch {
		case level == syscall.IPPROTO_IP && typ == ipECN:
			return ecnOfInt(data)
		case level == syscall.IPPROTO_IPV6 && typ == ipv6ECN:
			return ecnOfInt(data)
		}
		oob = rest
	}
}

// appendECN appends the IP_ECN message for an IPv4 destination and the
// IPV6_ECN one otherwise, each carrying ecn as an INT. One message only:
// WSASendMsg answers WSAEINVAL to a message of the other family (seen on
// an AF_INET socket handed both), and a dual-stack socket takes the IPv4
// one for a mapped destination, as msquic sends it.
func appendECN(oob []byte, ecn uint8, v4 bool) []byte {
	if v4 {
		return appendIntCmsg(oob, syscall.IPPROTO_IP, ipECN, uint32(ecn))
	}
	return appendIntCmsg(oob, syscall.IPPROTO_IPV6, ipv6ECN, uint32(ecn))
}

// packetDestination finds the packet-information message. IN_PKTINFO is
// not the BSD in_pktinfo: it is { IN_ADDR ipi_addr; ULONG ipi_ifindex; },
// eight bytes, ipi_addr holding the header's destination. IN6_PKTINFO
// has the RFC 3542 layout.
func packetDestination(oob []byte) (netip.Addr, int, bool) {
	for {
		level, typ, data, rest, ok := nextCmsg(oob)
		if !ok {
			return netip.Addr{}, 0, false
		}
		switch {
		case level == syscall.IPPROTO_IP && typ == ipPktinfo && len(data) >= 8:
			ifindex := int(binary.NativeEndian.Uint32(data[4:8]))
			return netip.AddrFrom4([4]byte(data[0:4])), ifindex, true
		case level == syscall.IPPROTO_IPV6 && typ == ipv6Pktinfo && len(data) >= 20:
			ifindex := int(binary.NativeEndian.Uint32(data[16:20]))
			return netip.AddrFrom16([16]byte(data[0:16])), ifindex, true
		}
		oob = rest
	}
}

// appendPacketSource writes IP_PKTINFO for an IPv4 or IPv4-mapped src and
// IPV6_PKTINFO otherwise: on send ipi_addr is the source and ipi_ifindex
// the interface to leave by. A dual-stack socket takes the IPv4 form for a
// mapped destination, the one family rule appendECN follows, and as
// msquic sends it.
func appendPacketSource(oob []byte, src netip.Addr, ifindex int) []byte {
	var data []byte
	if src.Unmap().Is4() {
		oob, data = appendCmsg(oob, syscall.IPPROTO_IP, ipPktinfo, 8)
		a := src.Unmap().As4()
		copy(data[0:4], a[:])
		binary.NativeEndian.PutUint32(data[4:8], uint32(ifindex)) //nolint:gosec // G115: an interface index Winsock gave as a ULONG
		return oob
	}
	oob, data = appendCmsg(oob, syscall.IPPROTO_IPV6, ipv6Pktinfo, 20)
	a := src.As16()
	copy(data[0:16], a[:])
	binary.NativeEndian.PutUint32(data[16:20], uint32(ifindex)) //nolint:gosec // G115: an interface index Winsock gave as a ULONG
	return oob
}
