package udp_test

import (
	"net"
	"syscall"
	"testing"

	"github.com/mlagarrigue/sluice/net/quic/udp"
)

// Reads back an integer socket option on c.
func getsockopt(t *testing.T, c *net.UDPConn, level, opt int) int {
	t.Helper()
	rc, err := c.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var v int
	var opErr error
	if err := rc.Control(func(fd uintptr) {
		v, opErr = syscall.GetsockoptInt(int(fd), level, opt)
	}); err != nil {
		t.Fatal(err)
	}
	if opErr != nil {
		t.Fatalf("getsockopt(%d,%d): %v", level, opt, opErr)
	}
	return v
}

// Darwin names (bsd/netinet/in.h, bsd/netinet6/in6.h), spelled again here
// because the package's copies are unexported.
const (
	ipDontFrag   = 0x1c // IP_DONTFRAG
	ipv6DontFrag = 0x3e // IPV6_DONTFRAG
)

// What Configure claims to do on Darwin, read back from XNU: an AF_INET
// socket carries IP_DONTFRAG=1 and an AF_INET6 one IPV6_DONTFRAG=1. The
// IPv4 option on an AF_INET6 socket is best-effort and not read back:
// XNU routes IPPROTO_IP options on such a socket to ip6_ctloutput, which
// answers EINVAL.
func TestConfigureSetsDarwinOptions(t *testing.T) {
	v4, err := udp.Listen("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer v4.Close()
	if got := getsockopt(t, v4, syscall.IPPROTO_IP, ipDontFrag); got != 1 {
		t.Errorf("udp4 IP_DONTFRAG = %d, want 1", got)
	}

	// The dual-stack default: network "udp" on an unspecified address is an
	// AF_INET6 socket and gets the IPv6 treatment.
	dual, err := udp.Listen("udp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	defer dual.Close()
	if got := getsockopt(t, dual, syscall.IPPROTO_IPV6, ipv6DontFrag); got != 1 {
		t.Errorf("dual-stack IPV6_DONTFRAG = %d, want 1", got)
	}

	v6, err := udp.Listen("udp6", "[::1]:0")
	if err != nil {
		t.Skipf("no IPv6 loopback here: %v", err)
	}
	defer v6.Close()
	if got := getsockopt(t, v6, syscall.IPPROTO_IPV6, ipv6DontFrag); got != 1 {
		t.Errorf("udp6 IPV6_DONTFRAG = %d, want 1", got)
	}
}

// Before Configure the XNU default is 0 (fragment when needed), so the
// test above is not passing on a default it never changed.
func TestDarwinDefaultIsNotDontFrag(t *testing.T) {
	c, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if got := getsockopt(t, c, syscall.IPPROTO_IP, ipDontFrag); got != 0 {
		t.Fatalf("fresh socket already has IP_DONTFRAG=%d; this test cannot distinguish Configure from the default", got)
	}
}

// DF is observable: with IP_DONTFRAG set, ip_output refuses a datagram
// larger than the outgoing interface MTU with EMSGSIZE instead of
// fragmenting it. Loopback's MTU admits every UDP datagram, so the
// datagram goes towards TEST-NET-1 (RFC 5737), which leaves through the
// default route and reaches no one. A plain socket sending the same
// datagram is the control: if it fails too, there is no route to measure
// against.
func TestDontFragRefusesOversize(t *testing.T) {
	dst := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 4433}
	big := make([]byte, 3000)

	plain, err := net.ListenUDP("udp4", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Close()
	if _, err := plain.WriteTo(big, dst); err != nil {
		t.Skipf("no default IPv4 route to send through: %v", err)
	}

	df, err := udp.Listen("udp4", ":0")
	if err != nil {
		t.Fatal(err)
	}
	defer df.Close()
	if _, err := df.WriteTo(big, dst); err == nil {
		t.Fatal("oversize datagram accepted with DF set")
	}
	if _, err := df.WriteTo(big[:1200], dst); err != nil {
		t.Fatalf("1200-byte datagram refused: %v", err)
	}
}
