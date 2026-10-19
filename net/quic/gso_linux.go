package quic

import (
	"encoding/binary"
	"errors"
	"net"
	"syscall"
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

// offersSegmentation is true on Linux: a kernel without UDP_SEGMENT says so
// on the write itself, which refusedSegmentation recognises.
func offersSegmentation(*net.UDPConn) bool { return true }

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
