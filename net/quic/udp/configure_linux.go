package udp

import "syscall"

// Linux names (include/uapi/linux/in6.h); absent from package syscall's
// generated tables on some architectures, so spelled here.
const (
	ipv6PMTUDiscDO    = 0x2  // IPV6_PMTUDISC_DO
	ipv6AutoFlowLabel = 0x46 // IPV6_AUTOFLOWLABEL
	ipv6MTUDiscover   = 0x17 // IPV6_MTU_DISCOVER
)

// configure applies, on Linux, IP(V6)_MTU_DISCOVER = DO and, for IPv6
// sockets, IPV6_AUTOFLOWLABEL = 1.
//
// DO rather than PROBE: both set DF, but PROBE also tells the kernel to
// ignore its path-MTU cache, which is what an endpoint running its own
// PMTUD wants and this transport does not do yet. DO keeps the kernel's
// knowledge working for us: a datagram it knows will not fit comes back
// as EMSGSIZE from WriteTo instead of leaving and dying.
//
// On an AF_INET6 socket the IPv4 option is set too, because the kernel
// routes a packet to an IPv4-mapped peer through the IPv4 stack and reads
// that option for it. It cannot fail on Linux for an IPv6 socket, but the
// best-effort filter stays, so the IPv6 option's outcome is the one that
// decides.
func configure(fd uintptr, v6 bool) error {
	if v6 {
		if err := syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IPV6, ipv6MTUDiscover, ipv6PMTUDiscDO); err != nil {
			return err
		}
		if err := ignoreOptionAbsent(syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IP, syscall.IP_MTU_DISCOVER, syscall.IP_PMTUDISC_DO)); err != nil {
			return err
		}
		// A SHOULD (§9.7), and an option whose effect depends on the
		// net.ipv6.auto_flowlabels sysctl: mode 1 (the default) already
		// labels every socket, mode 2 labels only sockets that ask, mode
		// 0 labels nothing regardless. Asking is right in every mode.
		return ignoreOptionAbsent(syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IPV6, ipv6AutoFlowLabel, 1))
	}
	return syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IP, syscall.IP_MTU_DISCOVER, syscall.IP_PMTUDISC_DO)
}
