//go:build linux || darwin

package udp

import "syscall"

// cmsghdr is { size_t len; int level; int type; } on Linux and
// { socklen_t len; int level; int type; } on Darwin: the two ints follow
// the length field, and the data starts at CmsgLen(0). Package syscall's
// CmsgLen and CmsgSpace know each kernel's alignment (a word on Linux,
// four bytes on Darwin).
const cmsgLenSize = syscall.SizeofCmsghdr - 8

func cmsgLen(dataLen int) int { return syscall.CmsgLen(dataLen) }

func cmsgSpace(dataLen int) int { return syscall.CmsgSpace(dataLen) }
