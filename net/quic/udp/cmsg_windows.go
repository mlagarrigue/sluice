package udp

import "strconv"

// WSACMSGHDR (ws2def.h) is { SIZE_T cmsg_len; INT cmsg_level; INT
// cmsg_type; }: the length is pointer-wide, and WSA_CMSG_LEN /
// WSA_CMSG_SPACE align the header and the data to MAX_NATURAL_ALIGNMENT,
// the pointer size too. Package syscall's Windows build has no CmsgLen, so
// the two macros are spelled here. The pointer size is the size of an int on
// every platform Go supports.
const cmsgLenSize = strconv.IntSize / 8

func cmsgLen(dataLen int) int { return cmsgLenSize + 8 + dataLen }

func cmsgSpace(dataLen int) int {
	return cmsgLen((dataLen + cmsgLenSize - 1) &^ (cmsgLenSize - 1))
}
