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

// packetDestination walks the control messages by hand:
// syscall.ParseSocketMessage would allocate a slice per datagram.
//
// For IPv4 it returns in_pktinfo's ipi_spec_dst, the local address the
// kernel would itself answer from — equal to the header's destination for
// unicast, and the interface's own address for a broadcast, which is the
// one a reply can actually use as its source.
func packetDestination(oob []byte) (netip.Addr, int, bool) {
	hdr := syscall.CmsgLen(0)
	for len(oob) >= hdr {
		length, level, typ := cmsgHeader(oob)
		if length < hdr || length > len(oob) {
			break
		}
		data := oob[hdr:length]
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
		next := syscall.CmsgSpace(length - hdr)
		if next >= len(oob) {
			break
		}
		oob = oob[next:]
	}
	return netip.Addr{}, 0, false
}

func appendPacketSource(oob []byte, src netip.Addr, ifindex int) []byte {
	level, typ, dataLen := syscall.IPPROTO_IPV6, ipv6Pktinfo, 20
	if src.Is4() {
		level, typ, dataLen = syscall.IPPROTO_IP, ipPktinfo, 12
	}
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
	binary.NativeEndian.PutUint32(b[cmsgLenSize:], uint32(level))
	binary.NativeEndian.PutUint32(b[cmsgLenSize+4:], uint32(typ))
	data := b[syscall.CmsgLen(0):]
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
