package udp

import "syscall"

// Darwin names (bsd/netinet/in.h, bsd/netinet6/in6.h). Package syscall's
// Darwin tables do not carry either, so they are spelled here.
//
// NOT YET VERIFIED ON A MAC. Written from the XNU headers; the values are
// the same ones quic-go and msquic use on this platform. The first run on
// macOS should confirm with the getsockopt check in configure_test.go
// adapted to these names.
const (
	ipDontFrag   = 0x1c // IP_DONTFRAG   (28)
	ipv6DontFrag = 0x3e // IPV6_DONTFRAG (62)
)

// configure sets IP_DONTFRAG or IPV6_DONTFRAG. Darwin has no socket-level
// flow-label control, so §9.7 is left to the kernel's default labelling.
//
// On an AF_INET6 socket the IPv4 option is attempted for mapped peers but
// not required: XNU may answer EINVAL for an IPv4 option on an IPv6
// socket, and the IPv6 option is the one that must take.
func configure(fd uintptr, v6 bool) error {
	if v6 {
		if err := syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IPV6, ipv6DontFrag, 1); err != nil {
			return err
		}
		return ignoreOptionAbsent(syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IP, ipDontFrag, 1))
	}
	return syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IP, ipDontFrag, 1)
}
