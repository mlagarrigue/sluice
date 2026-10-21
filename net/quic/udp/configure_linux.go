package udp

import "syscall"

// Linux names (include/uapi/linux/in6.h); absent from package syscall's
// generated tables on some architectures, so spelled here.
const (
	ipv6PMTUDiscProbe = 0x3  // IPV6_PMTUDISC_PROBE
	ipv6AutoFlowLabel = 0x46 // IPV6_AUTOFLOWLABEL
	ipv6MTUDiscover   = 0x17 // IPV6_MTU_DISCOVER
)

// configure applies, on Linux, IP(V6)_MTU_DISCOVER = PROBE and, for IPv6
// sockets, IPV6_AUTOFLOWLABEL = 1.
//
// PROBE rather than DO: both set DF, and both refuse with EMSGSIZE, at send
// time, a datagram larger than the outgoing interface MTU. What DO adds is
// the kernel's path-MTU cache — the per-destination MTU an ICMP
// Fragmentation Needed installs — and that is the one thing RFC 9000
// §14.2.1 forbids acting on: "An endpoint MUST ignore an ICMP message that
// claims the PMTU has decreased below QUIC's smallest allowed maximum
// datagram size." The kernel cannot honour that. It floors the cached MTU
// at net.ipv4.route.min_pmtu, 552 by default, well under the 1200 of §14.1,
// and its validation of the quoted header reaches the addresses and ports
// but never the connection IDs §14.2.1 asks for "when possible"; nor is its
// reduction provisional on the quoted packet being lost, as §14.2.1 allows
// it to be. So under DO an ICMP that merely matches the UDP four-tuple
// makes every 1200-byte datagram to that peer fail with EMSGSIZE until the
// entry expires — net.ipv4.route.mtu_expires, 600 s by default — and since
// this transport's search never offers anything below 1200 (pmtu.go), the
// connection spends that time unable to send and closes on its idle
// timeout. PROBE tells the kernel to ignore the cache, which leaves the
// transport's own loss-based search the only judge of the path. RFC 8899
// §4.6 allows exactly that: "A simple implementation MAY ignore received
// PTB messages."
//
// What PROBE gives up is narrow: the kernel no longer refuses a datagram it
// believes too large for the far end of the path. The interface MTU is
// still enforced, and a path that drops full-size datagrams is what
// DPLPMTUD's probe loss already reads (pmtu.go, RFC 8899 §4.3).
//
// The IPv6 option changes with the IPv4 one, though IPv6 alone could not
// breach the MUST above: the kernel ignores a Packet Too Big below the IPv6
// minimum of 1280, which leaves room for 1200. It changes for one rule
// rather than two — the kernel's view of the remote path is not consulted,
// here either — and because a PTB at 1280 is still unauthenticated and
// still caps the search for 600 s.
//
// On an AF_INET6 socket the IPv4 option is set too, because the kernel
// routes a packet to an IPv4-mapped peer through the IPv4 stack and reads
// that option for it. It cannot fail on Linux for an IPv6 socket, but the
// best-effort filter stays, so the IPv6 option's outcome is the one that
// decides.
func configure(fd uintptr, v6 bool) error {
	if v6 {
		if err := syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IPV6, ipv6MTUDiscover, ipv6PMTUDiscProbe); err != nil {
			return err
		}
		if err := ignoreOptionAbsent(syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IP, syscall.IP_MTU_DISCOVER, syscall.IP_PMTUDISC_PROBE)); err != nil {
			return err
		}
		// A SHOULD (§9.7), and an option whose effect depends on the
		// net.ipv6.auto_flowlabels sysctl: mode 1 (the default) already
		// labels every socket, mode 2 labels only sockets that ask, mode
		// 0 labels nothing regardless. Asking is right in every mode.
		return ignoreOptionAbsent(syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IPV6, ipv6AutoFlowLabel, 1))
	}
	return syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IP, syscall.IP_MTU_DISCOVER, syscall.IP_PMTUDISC_PROBE)
}
