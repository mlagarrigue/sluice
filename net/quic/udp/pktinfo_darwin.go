package udp

import "syscall"

// Darwin names (bsd/netinet/in.h, bsd/netinet6/in6.h), read from XNU's
// headers, not from memory: package syscall's Darwin tables do not carry
// them.
const (
	ipTOS          = 3  // IP_TOS, the control message on send (int)
	ipRecvTOS      = 27 // IP_RECVTOS, the option and the message received (u_char)
	ipv6TClass     = 36 // IPV6_TCLASS, the control message on both directions (int)
	ipv6RecvTClass = 35 // IPV6_RECVTCLASS
)

// enableECN sets IP_RECVTOS on an AF_INET socket and IPV6_RECVTCLASS on an
// AF_INET6 one. XNU reports IPv4 traffic on a dual-stack socket through
// the IPv6 path (ip6_savecontrol_v4 copies the TOS byte into an
// IPV6_TCLASS message), so the IPv6 option alone covers both families.
func enableECN(fd uintptr, v6 bool) error {
	if v6 {
		return syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IPV6, ipv6RecvTClass, 1)
	}
	return syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IP, ipRecvTOS, 1)
}

// packetECN finds the IP_RECVTOS or IPV6_TCLASS message and returns the two
// ECN bits of the byte it carries. ip_savecontrol writes the TOS as one
// u_char under the IP_RECVTOS type — not IP_TOS as Linux does — and
// ip6_savecontrol writes the traffic class as a C int.
func packetECN(oob []byte) (uint8, bool) {
	for {
		level, typ, data, rest, ok := nextCmsg(oob)
		if !ok {
			return 0, false
		}
		switch {
		case level == syscall.IPPROTO_IP && typ == ipRecvTOS && len(data) >= 1:
			return data[0] & 3, true
		case level == syscall.IPPROTO_IPV6 && typ == ipv6TClass:
			return ecnOfInt(data)
		}
		oob = rest
	}
}

// appendECN appends the IP_TOS and IPV6_TCLASS messages, both carrying
// ecn as a C int: ip_tos_from_control (bsd/netinet/in_tclass.c) takes the
// first of either kind whose length is CMSG_LEN(sizeof(int)) and skips
// the rest, and ip6_setpktopts skips every message below IPPROTO_IPV6 —
// so one datagram's marking does not depend on which family its address
// is, as on Linux.
func appendECN(oob []byte, ecn uint8) []byte {
	oob = appendIntCmsg(oob, syscall.IPPROTO_IP, ipTOS, uint32(ecn))
	return appendIntCmsg(oob, syscall.IPPROTO_IPV6, ipv6TClass, uint32(ecn))
}
