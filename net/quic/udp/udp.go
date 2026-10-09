// Package udp opens UDP sockets configured the way a QUIC endpoint needs
// them, which [net] alone cannot express.
//
// RFC 9000 §14 forbids IP fragmentation of QUIC datagrams and asks that the
// IPv4 Don't Fragment bit be set "if possible"; §9.7 asks that IPv6 flow
// labels follow RFC 6437, one label per path. Both are socket options with
// a different name on every operating system and no portable spelling in
// the standard library, so the [quic] package — which takes a
// [net.PacketConn] and never opens one — leaves them to whoever does. This
// package is that helper: [Listen] opens a socket with the options applied,
// [Configure] applies them to a socket the caller already has.
//
// # What each platform does
//
//   - Linux: IP_MTU_DISCOVER / IPV6_MTU_DISCOVER set to the DO mode. The
//     kernel sets DF and also refuses, with EMSGSIZE at send time, a
//     datagram larger than the path MTU it has cached — the error reaches
//     the caller's WriteTo rather than the wire. IPV6_AUTOFLOWLABEL is set
//     on IPv6 sockets so the kernel assigns a flow label from the flow
//     hash of each datagram, which gives one label per peer address and a
//     fresh one when the peer moves (§9.7). Tested here.
//   - macOS: IP_DONTFRAG / IPV6_DONTFRAG. No flow-label control exists;
//     the label stays whatever the kernel chooses. Written from the
//     documented constants, NOT yet run on a Mac.
//   - Windows: IP_DONTFRAGMENT / IPV6_DONTFRAG. Winsock refuses, with
//     WSAEMSGSIZE at send time, a datagram larger than the outgoing
//     interface MTU. No flow-label control exists. SIO_UDP_CONNRESET is
//     turned off, so an ICMP port-unreachable for an earlier send no
//     longer surfaces as WSAECONNRESET on the next read (best-effort; the
//     test exists but no CI job runs it on Windows).
//   - Anything else: [Configure] returns [errors.ErrUnsupported] and
//     changes nothing. The socket still works; it just may be fragmented.
//
// [EnablePacketInfo], [PacketDestination] and [AppendPacketSource] are the
// second concern: a socket bound to the unspecified address learning which
// of the host's addresses a datagram reached, and answering from it.
// Linux only for now.
//
// Setting DF is the prerequisite for path MTU discovery (§14.2), which the
// transport does not do yet; until it does, datagrams stay at 1200 bytes
// and DF is defence rather than a measurement. The flow label is a SHOULD
// and is applied where the platform allows, never reported as an error
// where it does not.
package udp

import (
	"fmt"
	"net"
	"net/netip"
)

// Listen opens a UDP socket on network ("udp", "udp4" or "udp6") and
// address, as [net.ListenPacket] would, and applies [Configure] to it. A
// socket that cannot be configured is closed and the error returned: a
// caller asking for a QUIC socket gets one or nothing, never a silently
// fragmentable one.
func Listen(network, address string) (*net.UDPConn, error) {
	pc, err := net.ListenPacket(network, address)
	if err != nil {
		return nil, err
	}
	c, ok := pc.(*net.UDPConn)
	if !ok {
		_ = pc.Close() // nothing was configured; its close error adds nothing
		return nil, fmt.Errorf("udp: %q is not a UDP network", network)
	}
	if err := Configure(c); err != nil {
		_ = c.Close() // the configuration error is the one worth reporting
		return nil, err
	}
	return c, nil
}

// Configure sets the Don't Fragment option on c and, where the platform
// has one, asks for per-flow IPv6 labels. It returns
// [errors.ErrUnsupported] (wrapped) on a platform with no DF option, and
// the setsockopt error if the platform has one and it failed.
//
// Which family's option applies follows the socket's local address: an
// IPv4 address means an AF_INET socket, anything else an AF_INET6 one. A
// dual-stack IPv6 socket (the default for network "udp" bound to an
// unspecified address) carries IPv4 peers as mapped addresses, so the IPv4
// option is also attempted on it, best-effort: platforms disagree on
// whether that is allowed, and the IPv6 option already governs the
// datagrams a mapped peer receives on Linux.
func Configure(c *net.UDPConn) error {
	rc, err := c.SyscallConn()
	if err != nil {
		return fmt.Errorf("udp: %w", err)
	}
	v6 := isIPv6Socket(c)
	var opErr error
	if err := rc.Control(func(fd uintptr) {
		opErr = configure(fd, v6)
	}); err != nil {
		return fmt.Errorf("udp: %w", err)
	}
	if opErr != nil {
		return fmt.Errorf("udp: %w", opErr)
	}
	return nil
}

// isIPv6Socket reports whether c is an AF_INET6 socket. The standard
// library reports an AF_INET socket's address as a 4-byte IP and an
// AF_INET6 socket's as 16 bytes, including "::" for the dual-stack
// default; an IPv6 socket bound to an explicit IPv4-mapped address is the
// one case this misreads, and the best-effort IPv4 attempt in [Configure]
// still covers it.
func isIPv6Socket(c *net.UDPConn) bool {
	a, ok := c.LocalAddr().(*net.UDPAddr)
	if !ok || a == nil {
		return false
	}
	return len(a.IP) == net.IPv6len && a.IP.To4() == nil
}

// PacketInfoLen is the out-of-band buffer size [ReadMsgUDP] needs for the
// control message [EnablePacketInfo] turns on, with room to spare.
//
// [ReadMsgUDP]: net.UDPConn.ReadMsgUDP
const PacketInfoLen = 128

// EnablePacketInfo asks the kernel to report, with every datagram read
// through [net.UDPConn.ReadMsgUDP], the local address it was sent to —
// which a socket bound to the unspecified address (0.0.0.0 or [::]) does
// not otherwise learn. [PacketDestination] reads that address back out of
// the out-of-band bytes, and [AppendPacketSource] builds the control
// message that makes a reply leave from it.
//
// A server on a host with several addresses needs both halves: without
// them the kernel picks a reply's source by route, the client sees an
// answer from an address it never wrote to, and to QUIC that is a
// different path (RFC 9000 §9).
//
// Linux only for now (IP_PKTINFO / IPV6_RECVPKTINFO); elsewhere it returns
// [errors.ErrUnsupported] (wrapped) and changes nothing.
func EnablePacketInfo(c *net.UDPConn) error {
	rc, err := c.SyscallConn()
	if err != nil {
		return fmt.Errorf("udp: %w", err)
	}
	v6 := isIPv6Socket(c)
	var opErr error
	if err := rc.Control(func(fd uintptr) {
		opErr = enablePacketInfo(fd, v6)
	}); err != nil {
		return fmt.Errorf("udp: %w", err)
	}
	if opErr != nil {
		return fmt.Errorf("udp: %w", opErr)
	}
	return nil
}

// PacketDestination finds, in the out-of-band bytes of one datagram read
// from a socket [EnablePacketInfo] was applied to, the local address the
// datagram reached and the index of the interface it arrived on. An
// AF_INET socket reports an IPv4 address; an AF_INET6 one reports IPv4
// traffic as IPv4-mapped. ok is false when oob carries no such message.
// It does not allocate.
func PacketDestination(oob []byte) (dst netip.Addr, ifindex int, ok bool) {
	return packetDestination(oob)
}

// AppendPacketSource appends to oob the control message that makes a
// datagram sent with [net.UDPConn.WriteMsgUDP] leave from src. ifindex
// pins the outgoing interface and is only needed for a link-local src;
// zero leaves the choice to routing. src should be what
// [PacketDestination] reported on the same socket: an IPv4 address for an
// AF_INET socket, an IPv6 or IPv4-mapped one for an AF_INET6 socket. Where
// [EnablePacketInfo] is unsupported, oob is returned unchanged.
func AppendPacketSource(oob []byte, src netip.Addr, ifindex int) []byte {
	return appendPacketSource(oob, src, ifindex)
}
