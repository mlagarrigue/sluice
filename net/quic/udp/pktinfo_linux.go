package udp

import (
	"encoding/binary"
	"net/netip"
	"slices"
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

// cmsghdr is { size_t len; int level; int type; }: the length field is a
// word wide, the two ints follow, and the data starts at CmsgLen(0).
const cmsgLenSize = syscall.SizeofCmsghdr - 8

func cmsgHeader(b []byte) (length, level, typ int) {
	if cmsgLenSize == 8 {
		length = int(binary.NativeEndian.Uint64(b)) //nolint:gosec // G115: a wrapped length fails the bounds check that follows
	} else {
		length = int(binary.NativeEndian.Uint32(b))
	}
	level = int(int32(binary.NativeEndian.Uint32(b[cmsgLenSize:]))) //nolint:gosec // G115: the kernel ABI field is a C int
	typ = int(int32(binary.NativeEndian.Uint32(b[cmsgLenSize+4:]))) //nolint:gosec // G115: the kernel ABI field is a C int
	return length, level, typ
}

// nextCmsg splits the first control message off oob: its level and type,
// its data, and what follows. ok is false when no well-formed message is
// left. Walking by hand, not with syscall.ParseSocketControlMessage, which
// would allocate a slice per datagram.
func nextCmsg(oob []byte) (level, typ int, data, rest []byte, ok bool) {
	hdr := syscall.CmsgLen(0)
	if len(oob) < hdr {
		return 0, 0, nil, nil, false
	}
	length, level, typ := cmsgHeader(oob)
	if length < hdr || length > len(oob) {
		return 0, 0, nil, nil, false
	}
	data = oob[hdr:length]
	if next := syscall.CmsgSpace(length - hdr); next < len(oob) {
		rest = oob[next:]
	}
	return level, typ, data, rest, true
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
			return netip.AddrFrom16([16]byte(data[0:16])), ifindex, true
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
		case level == syscall.IPPROTO_IPV6 && typ == ipv6TClass && len(data) >= 4:
			return uint8(binary.NativeEndian.Uint32(data[0:4]) & 3), true
		}
		oob = rest
	}
}

// appendCmsg appends one control message header for dataLen bytes of
// data, zeroed, and returns the grown oob and the data slice to fill.
func appendCmsg(oob []byte, level, typ, dataLen int) (grown, data []byte) {
	start := len(oob)
	oob = slices.Grow(oob, syscall.CmsgSpace(dataLen))
	oob = oob[:start+syscall.CmsgSpace(dataLen)]
	b := oob[start:]
	clear(b)
	if cmsgLenSize == 8 {
		binary.NativeEndian.PutUint64(b, uint64(syscall.CmsgLen(dataLen))) //nolint:gosec // G115: CmsgLen of a small fixed payload
	} else {
		binary.NativeEndian.PutUint32(b, uint32(syscall.CmsgLen(dataLen))) //nolint:gosec // G115: CmsgLen of a small fixed payload
	}
	binary.NativeEndian.PutUint32(b[cmsgLenSize:], uint32(level)) //nolint:gosec // G115: a socket level, a small positive constant
	binary.NativeEndian.PutUint32(b[cmsgLenSize+4:], uint32(typ)) //nolint:gosec // G115: an option name, a small positive constant
	return oob, b[syscall.CmsgLen(0):]
}

// appendECN appends the IP_TOS and IPV6_TCLASS messages, both carrying
// ecn as a C int. Both at once: the kernel sends to an IPv4 peer through
// the IPv4 stack, which reads the first and skips the second, and to an
// IPv6 peer through the IPv6 stack, which does the reverse — so one
// datagram's marking does not depend on which family its address is.
func appendECN(oob []byte, ecn uint8) []byte {
	var data []byte
	oob, data = appendCmsg(oob, syscall.IPPROTO_IP, ipTOS, 4)
	binary.NativeEndian.PutUint32(data, uint32(ecn))
	oob, data = appendCmsg(oob, syscall.IPPROTO_IPV6, ipv6TClass, 4)
	binary.NativeEndian.PutUint32(data, uint32(ecn))
	return oob
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
