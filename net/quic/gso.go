//go:build linux || windows

package quic

import (
	"net"

	"github.com/mlagarrigue/sluice/net/quic/udp"
)

// writeSegments hands b to the kernel as consecutive datagrams of seg bytes
// each, the last possibly shorter, in one sendmsg: UDP segmentation offload
// (UDP_SEGMENT on Linux 4.18, UDP_SEND_MSG_SIZE on Windows). The kernel —
// or the network card — cuts b where a loop of WriteTo would have, so the
// peer receives the same datagrams; one system call carries a whole flight
// instead of one per packet. ok is false when pc is not a socket this can
// drive, when the system does not offer the option, or when the kernel
// refused it on this write (an older kernel, a device without checksum
// offload): the caller then falls back to one write per datagram. ecn, when
// not Not-ECT, marks every segment (§13.4.2). oob is scratch for the control
// messages, returned grown for reuse.
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
	if !offersSegmentation(uc) {
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
