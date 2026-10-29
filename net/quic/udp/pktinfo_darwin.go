package udp

import (
	"encoding/binary"
	"net/netip"
	"syscall"
)

// Darwin names (bsd/netinet/in.h, bsd/netinet6/in6.h), read from XNU's
// headers, not from memory: package syscall's Darwin tables do not carry
// them.
const (
	ipPktinfo       = 26 // IP_PKTINFO, also IP_RECVPKTINFO: the option and both messages
	ipv6RecvPktinfo = 61 // IPV6_RECVPKTINFO (RFC 3542)
	ipv6Pktinfo     = 46 // IPV6_PKTINFO (RFC 3542; 19 is the RFC 2292 one)
	ipTOS           = 3  // IP_TOS, the control message on send (int)
	ipRecvTOS       = 27 // IP_RECVTOS, the option and the message received (u_char)
	ipv6TClass      = 36 // IPV6_TCLASS, the control message on both directions (int)
	ipv6RecvTClass  = 35 // IPV6_RECVTCLASS
)

// enablePacketInfo sets IP_RECVPKTINFO on an AF_INET socket and
// IPV6_RECVPKTINFO on an AF_INET6 one. XNU reports IPv4 traffic on a
// dual-stack socket through the IPv6 path (ip6_savecontrol_v4 writes an
// IPV6_PKTINFO message carrying the mapped destination), so the IPv6
// option alone covers both families.
func enablePacketInfo(fd uintptr, v6 bool) error {
	if v6 {
		return syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IPV6, ipv6RecvPktinfo, 1)
	}
	return syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IP, ipPktinfo, 1)
}

// enableECN sets IP_RECVTOS on an AF_INET socket and IPV6_RECVTCLASS on an
// AF_INET6 one. XNU reports IPv4 traffic on a dual-stack socket through
// the IPv6 path (ip6_savecontrol_v4 copies the TOS byte into an
// IPV6_TCLASS message), so the IPv6 option alone covers both families.
func enableECN(fd uintptr, v6 bool) error {
	if v6 {
		return syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IPV6, ipv6RecvTClass, 1)
	}
	return syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IP, ipRecvTOS, 1)
}

// packetECN finds the IP_RECVTOS or IPV6_TCLASS message and returns the two
// ECN bits of the byte it carries. ip_savecontrol writes the TOS as one
// u_char under the IP_RECVTOS type — not IP_TOS as Linux does — and
// ip6_savecontrol writes the traffic class as a C int.
func packetECN(oob []byte) (uint8, bool) {
	for {
		level, typ, data, rest, ok := nextCmsg(oob)
		if !ok {
			return 0, false
		}
		switch {
		case level == syscall.IPPROTO_IP && typ == ipRecvTOS && len(data) >= 1:
			return data[0] & 3, true
		case level == syscall.IPPROTO_IPV6 && typ == ipv6TClass:
			return ecnOfInt(data)
		}
		oob = rest
	}
}

// appendECN appends the IP_TOS message for an IPv4 destination and the
// IPV6_TCLASS one otherwise, each carrying ecn as a C int, the one length
// ip_tos_from_control (bsd/netinet/in_tclass.c) accepts. A dual-stack
// socket sends to a mapped address through udp_output, which reads either
// kind, and to an IPv6 one through ip6_setpktopts, which reads IPV6_TCLASS.
func appendECN(oob []byte, ecn uint8, v4 bool) []byte {
	if v4 {
		return appendIntCmsg(oob, syscall.IPPROTO_IP, ipTOS, uint32(ecn))
	}
	return appendIntCmsg(oob, syscall.IPPROTO_IPV6, ipv6TClass, uint32(ecn))
}

// packetDestination finds the packet-information message.
//
// For IPv4 it returns in_pktinfo's ipi_addr, the header's destination:
// ip_savecontrol zeroes ipi_spec_dst, which Linux fills instead.
func packetDestination(oob []byte) (netip.Addr, int, bool) {
	for {
		level, typ, data, rest, ok := nextCmsg(oob)
		if !ok {
			return netip.Addr{}, 0, false
		}
		switch {
		case level == syscall.IPPROTO_IP && typ == ipPktinfo && len(data) >= 12:
			// struct in_pktinfo { unsigned int ifindex; in_addr spec_dst; in_addr addr; }
			ifindex := int(binary.NativeEndian.Uint32(data[0:4]))
			return netip.AddrFrom4([4]byte(data[8:12])), ifindex, true
		case level == syscall.IPPROTO_IPV6 && typ == ipv6Pktinfo && len(data) >= 20:
			// struct in6_pktinfo { in6_addr addr; unsigned int ifindex; }
			ifindex := int(binary.NativeEndian.Uint32(data[16:20]))
			// IPv4 traffic on a dual-stack socket arrives here, mapped;
			// it leaves as plain IPv4, the one form Windows can give too.
			return netip.AddrFrom16([16]byte(data[0:16])).Unmap(), ifindex, true
		}
		oob = rest
	}
}

// appendPacketSource writes IP_PKTINFO for an IPv4 or IPv4-mapped src and
// IPV6_PKTINFO otherwise. A dual-stack socket hands a datagram for a
// mapped destination to udp_output (udp6_send), whose udp_check_pktinfo
// reads IP_PKTINFO only, so a mapped source must travel in the IPv4 form.
// There ifindex is left out: an index takes precedence over ipi_spec_dst
// and resets the source to the unspecified address, which would undo the
// message's one purpose.
func appendPacketSource(oob []byte, src netip.Addr, ifindex int) []byte {
	var data []byte
	if src.Unmap().Is4() {
		oob, data = appendCmsg(oob, syscall.IPPROTO_IP, ipPktinfo, 12)
		a := src.Unmap().As4()
		copy(data[4:8], a[:]) // ipi_spec_dst; ipi_ifindex and ipi_addr stay zero
		return oob
	}
	oob, data = appendCmsg(oob, syscall.IPPROTO_IPV6, ipv6Pktinfo, 20)
	a := src.As16()
	copy(data[0:16], a[:])
	binary.NativeEndian.PutUint32(data[16:20], uint32(ifindex)) //nolint:gosec // G115: an interface index the kernel gave as a 32-bit int
	return oob
}
