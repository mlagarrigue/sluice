//go:build !linux && !windows

package quic

import "net"

// writeSegments is UDP segmentation offload, which only the Linux and Windows
// builds drive: elsewhere every datagram is its own write.
func writeSegments(_ net.PacketConn, _ []byte, _ int, _ net.Addr, _ uint8, oob []byte) (scratch []byte, ok bool, err error) {
	return oob, false, nil
}
