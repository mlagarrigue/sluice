package udp

import "syscall"

// Winsock names (ws2ipdef.h), read from the SDK header, not from memory:
// package syscall's Windows tables carry none of them. IP_ECN and
// IPV6_ECN are both the option that asks for the field (the documentation
// spells it IP_RECVECN / IPV6_RECVECN, the same value) and the type of
// the message that carries it, an INT, on both directions.
const (
	ipECN   = 50 // IP_ECN   (IPPROTO_IP)
	ipv6ECN = 50 // IPV6_ECN (IPPROTO_IPV6)
)

// enableECN sets IP_ECN on an AF_INET socket and IPV6_ECN on an AF_INET6
// one. On a dual-stack socket bound to the wildcard Winsock documents
// that both must be set, each family's option covering its own traffic.
func enableECN(fd uintptr, v6 bool) error {
	h := syscall.Handle(fd)
	if v6 {
		if err := syscall.SetsockoptInt(h, syscall.IPPROTO_IPV6, ipv6ECN, 1); err != nil {
			return err
		}
		return ignoreOptionAbsent(syscall.SetsockoptInt(h, syscall.IPPROTO_IP, ipECN, 1))
	}
	return syscall.SetsockoptInt(h, syscall.IPPROTO_IP, ipECN, 1)
}

// packetECN finds the IP_ECN or IPV6_ECN message, an INT holding the two
// ECN bits alone.
func packetECN(oob []byte) (uint8, bool) {
	for {
		level, typ, data, rest, ok := nextCmsg(oob)
		if !ok {
			return 0, false
		}
		switch {
		case level == syscall.IPPROTO_IP && typ == ipECN:
			return ecnOfInt(data)
		case level == syscall.IPPROTO_IPV6 && typ == ipv6ECN:
			return ecnOfInt(data)
		}
		oob = rest
	}
}

// appendECN appends the IP_ECN message for an IPv4 destination and the
// IPV6_ECN one otherwise, each carrying ecn as an INT. One message only:
// WSASendMsg answers WSAEINVAL to a message of the other family (seen on
// an AF_INET socket handed both), and a dual-stack socket takes the IPv4
// one for a mapped destination, as msquic sends it.
func appendECN(oob []byte, ecn uint8, v4 bool) []byte {
	if v4 {
		return appendIntCmsg(oob, syscall.IPPROTO_IP, ipECN, uint32(ecn))
	}
	return appendIntCmsg(oob, syscall.IPPROTO_IPV6, ipv6ECN, uint32(ecn))
}
