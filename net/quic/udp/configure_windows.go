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
	// IP_MTU_DISCOVER and IPV6_MTU_DISCOVER, both 71, take a PMTUD_STATE;
	// IP_PMTUDISC_PROBE is its fourth value.
	ipMTUDiscover   = 71
	ipv6MTUDiscover = 71
	pmtudiscProbe   = 3

	// SIO_UDP_CONNRESET (mstcpip.h): _WSAIOW(IOC_VENDOR, 12).
	sioUDPConnReset = syscall.IOC_IN | syscall.IOC_VENDOR | 12 // 0x9800000C
)

// disableConnReset turns SIO_UDP_CONNRESET off. Package net already does
// on every UDP socket it opens (go.dev/issue/5834); turning it off again
// costs one ioctl and keeps the guarantee [Configure] documents from
// resting on that. Left on — Winsock's default — an ICMP port-unreachable
// answering any earlier send surfaces as WSAECONNRESET on the socket's
// next read: one peer that went away makes a socket shared by a whole
// listener report a read error, and a dialled connection's read loop
// fail outright. UDP has no connection to reset; the datagram that
// provoked the ICMP is simply lost, which QUIC already handles.
//
// It is the one use of unsafe in the module's production code, the named
// exception to the architecture's rule S4: syscall.WSAIoctl takes its
// input buffer as a *byte and a length, and the four bytes of enable are
// what it reads. The pointer does not outlive the call.
func disableConnReset(h syscall.Handle) error {
	enable := uint32(0)
	var returned uint32
	return syscall.WSAIoctl(h, sioUDPConnReset,
		(*byte)(unsafe.Pointer(&enable)), uint32(unsafe.Sizeof(enable)),
		nil, 0, &returned, nil, 0)
}

// configure disables SIO_UDP_CONNRESET, then sets DF through
// IP_MTU_DISCOVER or IPV6_MTU_DISCOVER at IP_PMTUDISC_PROBE, or through
// IP_DONTFRAGMENT or IPV6_DONTFRAG where the stack lacks the first
// (setDontFragment). Windows has no socket-level flow-label control, so
// §9.7 is left to the stack.
//
// On a dual-stack socket (IPV6_V6ONLY off, which is how the standard
// library opens network "udp") Winsock documents that IPv4-level options
// apply to mapped traffic, so the IPv4 level is attempted too, best-effort.
func configure(fd uintptr, v6 bool) error {
	h := syscall.Handle(fd)
	// Best-effort: the read loops already treat a stray reset as a
	// transient error, so a stack that refuses the ioctl costs a counted
	// discard per ICMP, not the socket.
	_ = disableConnReset(h)
	if v6 {
		if err := setDontFragment(h, syscall.IPPROTO_IPV6, ipv6MTUDiscover, ipv6DontFrag); err != nil {
			return err
		}
		return ignoreOptionAbsent(setDontFragment(h, syscall.IPPROTO_IP, ipMTUDiscover, ipDontFragment))
	}
	return setDontFragment(h, syscall.IPPROTO_IP, ipMTUDiscover, ipDontFragment)
}

// setDontFragment sets DF at level, asking for the probe mode first. The
// probe mode is Linux's, with the same reason: Winsock documents that, on a
// datagram socket, IP_PMTUDISC_PROBE sets DF and refuses only a datagram
// larger than the interface MTU, where IP_PMTUDISC_DO refuses one larger
// than the path MTU the stack holds for the destination — a value an
// unauthenticated ICMP writes, which RFC 9000 §14.2.1 forbids acting on
// below 1200 bytes. The two options exclude each other: once either is set,
// Winsock answers the other with WSAEINVAL (measured on the CI's Windows
// runners), and the probe mode alone reads back DONTFRAG as 1. A stack
// that refuses the probe mode gets DONTFRAG, as before the mode existed.
func setDontFragment(h syscall.Handle, level, discover, dontFrag int) error {
	if err := syscall.SetsockoptInt(h, level, discover, pmtudiscProbe); err == nil {
		return nil
	}
	return syscall.SetsockoptInt(h, level, dontFrag, 1)
}
