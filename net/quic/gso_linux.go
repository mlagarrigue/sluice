package quic

import (
	"encoding/binary"
	"errors"
	"net"
	"syscall"

	"github.com/mlagarrigue/sluice/net/quic/udp"
)

// Linux names (include/uapi/linux/udp.h), spelled here because package
// syscall carries neither.
const (
	solUDP     = 17  // SOL_UDP
	udpSegment = 103 // UDP_SEGMENT
)

// cmsgLenSize is the width of cmsghdr's length field: a size_t, followed
// by two C ints, with the data at CmsgLen(0).
const cmsgLenSize = syscall.SizeofCmsghdr - 8

// writeSegments hands b to the kernel as consecutive datagrams of seg bytes
// each, the last possibly shorter, in one sendmsg: UDP segmentation offload
// (UDP_SEGMENT, Linux 4.18). The kernel — or the network card — cuts b
// where a loop of WriteTo would have, so the peer receives the same
// datagrams; one system call carries a whole flight instead of one per
// packet. ok is false when pc is not a socket this can drive, or when the
// kernel refused the option itself (an older kernel, a device without
// checksum offload): the caller then falls back to one write per datagram.
// ecn, when not Not-ECT, marks every segment (§13.4.2). oob is scratch for
// the control messages, returned grown for reuse.
func writeSegments(pc net.PacketConn, b []byte, seg int, to net.Addr, ecn uint8, oob []byte) (scratch []byte, ok bool, err error) {
	ua, isUDP := to.(*net.UDPAddr)
	if !isUDP {
		return oob, false, nil
	}
	var uc *net.UDPConn
	switch pc := pc.(type) {
	case *net.UDPConn:
		uc, oob = pc, oob[:0]
	case *sourcedConn:
		// The listener's packet-information socket: the source address
		// travels in its own control message, which the segment size joins.
		uc, oob = pc.UDPConn, append(oob[:0], pc.oob...)
	default:
		return oob, false, nil
	}
	oob = appendSegmentSize(oob, seg)
	ap := ua.AddrPort()
	if ecn != ecnNotECT {
		oob = udp.AppendECN(oob, ecn, ap.Addr())
	}
	_, _, err = uc.WriteMsgUDPAddrPort(b, oob, ap)
	if refusedSegmentation(err) {
		return oob, false, nil
	}
	return oob, true, err
}

// refusedSegmentation tells the kernel declining the option from a write
// that failed for its own reasons: EIO when the device cannot checksum the
// segments, EINVAL or ENOPROTOOPT when the kernel predates the option,
// EOPNOTSUPP when the socket family does not offer it. Nothing left in
// those cases, and the datagrams go one by one. Any other failure — a full
// buffer, an ICMP refusal reported on this send — is the write's, counted
// like any write's; loss recovery resends what it lost.
func refusedSegmentation(err error) bool {
	return errors.Is(err, syscall.EIO) || errors.Is(err, syscall.EINVAL) ||
		errors.Is(err, syscall.ENOPROTOOPT) || errors.Is(err, syscall.EOPNOTSUPP)
}

// appendSegmentSize appends the UDP_SEGMENT control message, a u16.
func appendSegmentSize(oob []byte, seg int) []byte {
	const dataLen = 2
	start := len(oob)
	oob = append(oob, make([]byte, syscall.CmsgSpace(dataLen))...)
	b := oob[start:]
	if cmsgLenSize == 8 {
		binary.NativeEndian.PutUint64(b, uint64(syscall.CmsgLen(dataLen))) //nolint:gosec // G115: CmsgLen of a small fixed payload
	} else {
		binary.NativeEndian.PutUint32(b, uint32(syscall.CmsgLen(dataLen))) //nolint:gosec // G115: CmsgLen of a small fixed payload
	}
	binary.NativeEndian.PutUint32(b[cmsgLenSize:], solUDP)
	binary.NativeEndian.PutUint32(b[cmsgLenSize+4:], udpSegment)
	binary.NativeEndian.PutUint16(b[syscall.CmsgLen(0):], uint16(seg)) //nolint:gosec // G115: a datagram size, at most maxDatagram
	return oob
}
