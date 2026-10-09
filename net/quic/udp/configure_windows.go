package udp

import (
	"syscall"
	"unsafe"
)

// Winsock names (ws2ipdef.h). Package syscall's Windows tables carry
// neither, so they are spelled here. Both happen to be 14; the values are
// read back by configure_windows_test.go.
const (
	ipDontFragment = 14 // IP_DONTFRAGMENT (IPPROTO_IP)
	ipv6DontFrag   = 14 // IPV6_DONTFRAG   (IPPROTO_IPV6)

	// SIO_UDP_CONNRESET (mstcpip.h): _WSAIOW(IOC_VENDOR, 12).
	sioUDPConnReset = syscall.IOC_IN | syscall.IOC_VENDOR | 12 // 0x9800000C
)

// disableConnReset turns SIO_UDP_CONNRESET off. Left on — Winsock's
// default, which package net does not change — an ICMP port-unreachable
// answering any earlier send surfaces as WSAECONNRESET on the socket's
// next read: one peer that went away makes a socket shared by a whole
// listener report a read error, and a dialled connection's read loop
// fail outright. UDP has no connection to reset; the datagram that
// provoked the ICMP is simply lost, which QUIC already handles.
func disableConnReset(h syscall.Handle) error {
	enable := uint32(0)
	var returned uint32
	return syscall.WSAIoctl(h, sioUDPConnReset,
		(*byte)(unsafe.Pointer(&enable)), uint32(unsafe.Sizeof(enable)),
		nil, 0, &returned, nil, 0)
}

// configure disables SIO_UDP_CONNRESET, then sets IP_DONTFRAGMENT or
// IPV6_DONTFRAG. Windows has no socket-level flow-label control, so §9.7
// is left to the stack.
//
// On a dual-stack socket (IPV6_V6ONLY off, which is how the standard
// library opens network "udp") Winsock documents that IPv4-level options
// apply to mapped traffic, so the IPv4 option is attempted too, best-effort.
func configure(fd uintptr, v6 bool) error {
	h := syscall.Handle(fd)
	// Best-effort: the read loops already treat a stray reset as a
	// transient error, so a stack that refuses the ioctl costs a counted
	// discard per ICMP, not the socket.
	_ = disableConnReset(h)
	if v6 {
		if err := syscall.SetsockoptInt(h, syscall.IPPROTO_IPV6, ipv6DontFrag, 1); err != nil {
			return err
		}
		return ignoreOptionAbsent(syscall.SetsockoptInt(h, syscall.IPPROTO_IP, ipDontFragment, 1))
	}
	return syscall.SetsockoptInt(h, syscall.IPPROTO_IP, ipDontFragment, 1)
}
