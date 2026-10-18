//go:build linux || darwin || windows

package udp

import (
	"encoding/binary"
	"slices"
)

// A control message is { len; int level; int type; data }. The length
// field is a size_t on Linux and Windows (WSACMSGHDR) and a socklen_t on
// Darwin; cmsgLenSize is its width, cmsgLen and cmsgSpace the platform's
// CMSG_LEN and CMSG_SPACE. The walk and the builder below are shared; the
// option names and data widths are each platform's own.

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
	hdr := cmsgLen(0)
	if len(oob) < hdr {
		return 0, 0, nil, nil, false
	}
	length, level, typ := cmsgHeader(oob)
	if length < hdr || length > len(oob) {
		return 0, 0, nil, nil, false
	}
	data = oob[hdr:length]
	if next := cmsgSpace(length - hdr); next < len(oob) {
		rest = oob[next:]
	}
	return level, typ, data, rest, true
}

// appendCmsg appends one control message header for dataLen bytes of
// data, zeroed, and returns the grown oob and the data slice to fill.
func appendCmsg(oob []byte, level, typ, dataLen int) (grown, data []byte) {
	start := len(oob)
	oob = slices.Grow(oob, cmsgSpace(dataLen))
	oob = oob[:start+cmsgSpace(dataLen)]
	b := oob[start:]
	clear(b)
	if cmsgLenSize == 8 {
		binary.NativeEndian.PutUint64(b, uint64(cmsgLen(dataLen))) //nolint:gosec // G115: CmsgLen of a small fixed payload
	} else {
		binary.NativeEndian.PutUint32(b, uint32(cmsgLen(dataLen))) //nolint:gosec // G115: CmsgLen of a small fixed payload
	}
	binary.NativeEndian.PutUint32(b[cmsgLenSize:], uint32(level)) //nolint:gosec // G115: a socket level, a small positive constant
	binary.NativeEndian.PutUint32(b[cmsgLenSize+4:], uint32(typ)) //nolint:gosec // G115: an option name, a small positive constant
	return oob, b[cmsgLen(0):]
}

// appendIntCmsg appends one control message carrying v as a C int, the
// width every ECN option on the three platforms takes on send.
func appendIntCmsg(oob []byte, level, typ int, v uint32) []byte {
	var data []byte
	oob, data = appendCmsg(oob, level, typ, 4)
	binary.NativeEndian.PutUint32(data, v)
	return oob
}

// ecnOfInt reads the two ECN bits of a control message carrying a C int.
func ecnOfInt(data []byte) (uint8, bool) {
	if len(data) < 4 {
		return 0, false
	}
	return uint8(binary.NativeEndian.Uint32(data[0:4]) & 3), true
}
