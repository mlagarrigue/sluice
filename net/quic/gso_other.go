//go:build !linux

package quic

import "net"

// writeSegments is UDP segmentation offload, which only the Linux build
// drives: elsewhere every datagram is its own write.
func writeSegments(_ net.PacketConn, _ []byte, _ int, _ net.Addr, oob []byte) (scratch []byte, ok bool, err error) {
	return oob, false, nil
}
