package quic

import (
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"syscall"
	"unsafe"
)

// Winsock names (ws2ipdef.h) and error codes (winerror.h), spelled here
// because package syscall carries none of them: its Windows build gives the
// POSIX error names values Winsock never returns.
const (
	ipprotoUDP     = 17 // IPPROTO_UDP
	udpSendMsgSize = 2  // UDP_SEND_MSG_SIZE

	wsaEINVAL      syscall.Errno = 10022
	wsaENOPROTOOPT syscall.Errno = 10042
	wsaEOPNOTSUPP  syscall.Errno = 10045
)

// WSACMSGHDR (ws2def.h) is { SIZE_T cmsg_len; INT cmsg_level; INT
// cmsg_type; }, and WSA_CMSG_LEN / WSA_CMSG_SPACE align the header and the
// data to the pointer size.
const cmsgLenSize = int(unsafe.Sizeof(uintptr(0)))

// segmentation records, once per process, whether Winsock has
// UDP_SEND_MSG_SIZE, which older releases lack.
// Asking costs a system call, and the answer is the system's, not the
// socket's: msquic asks once, on the socket its datapath opens first.
var segmentation struct {
	once    sync.Once
	offered bool
}

// offersSegmentation asks Winsock, on the first socket it sees, whether it
// knows UDP_SEND_MSG_SIZE. Asking rather than trying: a system that does
// not know a control message might not say so on the write.
func offersSegmentation(uc *net.UDPConn) bool {
	segmentation.once.Do(func() {
		rc, err := uc.SyscallConn()
		if err != nil {
			return
		}
		var serr error
		if err := rc.Control(func(fd uintptr) {
			_, serr = syscall.GetsockoptInt(syscall.Handle(fd), ipprotoUDP, udpSendMsgSize)
		}); err != nil {
			return
		}
		segmentation.offered = serr == nil
	})
	return segmentation.offered
}

// refusedSegmentation tells Winsock declining the option from a write that
// failed for its own reasons: WSAEINVAL, WSAENOPROTOOPT or WSAEOPNOTSUPP,
// the codes it gives an option it does not apply. The datagrams then go one
// by one. Any other failure is the write's, counted like any write's; loss
// recovery resends what it lost.
func refusedSegmentation(err error) bool {
	return errors.Is(err, wsaEINVAL) || errors.Is(err, wsaENOPROTOOPT) || errors.Is(err, wsaEOPNOTSUPP)
}

// appendSegmentSize appends the UDP_SEND_MSG_SIZE control message, a DWORD.
func appendSegmentSize(oob []byte, seg int) []byte {
	const dataLen = 4
	hdr := cmsgLenSize + 8
	space := hdr + (dataLen+cmsgLenSize-1)&^(cmsgLenSize-1)
	start := len(oob)
	oob = append(oob, make([]byte, space)...)
	b := oob[start:]
	if cmsgLenSize == 8 {
		binary.NativeEndian.PutUint64(b, uint64(hdr+dataLen))
	} else {
		binary.NativeEndian.PutUint32(b, uint32(hdr+dataLen))
	}
	binary.NativeEndian.PutUint32(b[cmsgLenSize:], ipprotoUDP)
	binary.NativeEndian.PutUint32(b[cmsgLenSize+4:], udpSendMsgSize)
	binary.NativeEndian.PutUint32(b[hdr:], uint32(seg)) //nolint:gosec // G115: a datagram size, at most maxDatagram
	return oob
}
