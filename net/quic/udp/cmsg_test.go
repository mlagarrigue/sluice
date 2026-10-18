//go:build linux || darwin || windows

package udp

import "encoding/binary"

// forgeCmsg lays out one control message as the kernel does: header, then
// data, then padding to the platform's alignment.
func forgeCmsg(level, typ int, data []byte) []byte {
	b := make([]byte, cmsgSpace(len(data)))
	if cmsgLenSize == 8 {
		binary.NativeEndian.PutUint64(b, uint64(cmsgLen(len(data))))
	} else {
		binary.NativeEndian.PutUint32(b, uint32(cmsgLen(len(data))))
	}
	binary.NativeEndian.PutUint32(b[cmsgLenSize:], uint32(level))
	binary.NativeEndian.PutUint32(b[cmsgLenSize+4:], uint32(typ))
	copy(b[cmsgLen(0):], data)
	return b
}
