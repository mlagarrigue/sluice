package udp

import (
	"encoding/binary"
	"net/netip"
	"syscall"
)

// Linux names (include/uapi/linux/in.h, in6.h); the same on every
// architecture, spelled here so the file does not depend on which of
// package syscall's generated tables carry them.
const (
	ipPktinfo       = 0x8  // IP_PKTINFO
	ipv6RecvPktinfo = 0x31 // IPV6_RECVPKTINFO
	ipv6Pktinfo     = 0x32 // IPV6_PKTINFO
	ipTOS           = 0x1  // IP_TOS, the control message on both directions
	ipRecvTOS       = 0xd  // IP_RECVTOS
	ipv6TClass      = 0x43 // IPV6_TCLASS, the control message on both directions
	ipv6RecvTClass  = 0x42 // IPV6_RECVTCLASS
)

// enablePacketInfo sets IP_PKTINFO on an AF_INET socket and
// IPV6_RECVPKTINFO on an AF_INET6 one. The kernel reports IPv4 traffic on
// a dual-stack socket through IPV6_PKTINFO as a mapped address, so one
// option per family covers it.
func enablePacketInfo(fd uintptr, v6 bool) error {
	if v6 {
		return syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IPV6, ipv6RecvPktinfo, 1)
	}
	return syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IP, ipPktinfo, 1)
}

// enableECN sets IP_RECVTOS on an AF_INET socket and IPV6_RECVTCLASS on an
// AF_INET6 one. A dual-stack socket receives IPv4 traffic through the IPv4
// stack, which reads the IPv4 option, so that one is set too, best effort.
func enableECN(fd uintptr, v6 bool) error {
	if v6 {
		if err := syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IPV6, ipv6RecvTClass, 1); err != nil {
			return err
		}
		return ignoreOptionAbsent(syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IP, ipRecvTOS, 1))
	}
	return syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IP, ipRecvTOS, 1)
}

// packetDestination finds the packet-information message.
//
// For IPv4 it returns in_pktinfo's ipi_spec_dst, the local address the
// kernel would itself answer from — equal to the header's destination for
// unicast, and the interface's own address for a broadcast, which is the
// one a reply can actually use as its source.
func packetDestination(oob []byte) (netip.Addr, int, bool) {
	for {
		level, typ, data, rest, ok := nextCmsg(oob)
		if !ok {
			return netip.Addr{}, 0, false
		}
		switch {
		case level == syscall.IPPROTO_IP && typ == ipPktinfo && len(data) >= 12:
			// struct in_pktinfo { int ifindex; in_addr spec_dst; in_addr addr; }
			ifindex := int(int32(binary.NativeEndian.Uint32(data[0:4]))) //nolint:gosec // G115: the kernel ABI field is a C int
			return netip.AddrFrom4([4]byte(data[4:8])), ifindex, true
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

// packetECN finds the IP_TOS or IPV6_TCLASS message and returns the two
// ECN bits of the byte it carries. The kernel reports IP_TOS as one byte
// and IPV6_TCLASS as a C int; the ECN field is the low two bits of either.
func packetECN(oob []byte) (uint8, bool) {
	for {
		level, typ, data, rest, ok := nextCmsg(oob)
		if !ok {
			return 0, false
		}
		switch {
		case level == syscall.IPPROTO_IP && typ == ipTOS && len(data) >= 1:
			return data[0] & 3, true
		case level == syscall.IPPROTO_IPV6 && typ == ipv6TClass:
			return ecnOfInt(data)
		}
		oob = rest
	}
}

// appendECN appends the IP_TOS message for an IPv4 destination and the
// IPV6_TCLASS one otherwise, each carrying ecn as a C int. A dual-stack
// socket sends to a mapped address through the IPv4 stack (udp_sendmsg),
// which reads IP_TOS, and to an IPv6 one through udpv6_sendmsg, which
// reads IPV6_TCLASS.
func appendECN(oob []byte, ecn uint8, v4 bool) []byte {
	if v4 {
		return appendIntCmsg(oob, syscall.IPPROTO_IP, ipTOS, uint32(ecn))
	}
	return appendIntCmsg(oob, syscall.IPPROTO_IPV6, ipv6TClass, uint32(ecn))
}

func appendPacketSource(oob []byte, src netip.Addr, ifindex int) []byte {
	level, typ, dataLen := syscall.IPPROTO_IPV6, ipv6Pktinfo, 20
	if src.Is4() {
		level, typ, dataLen = syscall.IPPROTO_IP, ipPktinfo, 12
	}
	var data []byte
	oob, data = appendCmsg(oob, level, typ, dataLen)
	if src.Is4() {
		// ipi_spec_dst is the source the kernel uses; ipi_addr is ignored
		// on send.
		binary.NativeEndian.PutUint32(data[0:4], uint32(ifindex)) //nolint:gosec // G115: an interface index the kernel gave as a 32-bit int
		a := src.As4()
		copy(data[4:8], a[:])
	} else {
		a := src.As16()
		copy(data[0:16], a[:])
		binary.NativeEndian.PutUint32(data[16:20], uint32(ifindex)) //nolint:gosec // G115: an interface index the kernel gave as a 32-bit int
	}
	return oob
}
