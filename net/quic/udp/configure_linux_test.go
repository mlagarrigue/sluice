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

const (
	ipv6MTUDiscover   = 0x17
	ipv6PMTUDiscProbe = 0x3
	ipv6AutoFlowLabel = 0x46
)

// What Configure claims to do on Linux, read back from the kernel: an
// AF_INET socket carries IP_MTU_DISCOVER=PROBE; an AF_INET6 one carries
// IPV6_MTU_DISCOVER=PROBE, IP_MTU_DISCOVER=PROBE for mapped peers, and
// IPV6_AUTOFLOWLABEL=1.
func TestConfigureSetsLinuxOptions(t *testing.T) {
	v4, err := udp.Listen("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer v4.Close()
	if got := getsockopt(t, v4, syscall.IPPROTO_IP, syscall.IP_MTU_DISCOVER); got != syscall.IP_PMTUDISC_PROBE {
		t.Errorf("udp4 IP_MTU_DISCOVER = %d, want %d (PROBE)", got, syscall.IP_PMTUDISC_PROBE)
	}

	v6, err := udp.Listen("udp6", "[::1]:0")
	if err != nil {
		t.Skipf("no IPv6 loopback here: %v", err)
	}
	defer v6.Close()
	if got := getsockopt(t, v6, syscall.IPPROTO_IPV6, ipv6MTUDiscover); got != ipv6PMTUDiscProbe {
		t.Errorf("udp6 IPV6_MTU_DISCOVER = %d, want %d (PROBE)", got, ipv6PMTUDiscProbe)
	}
	if got := getsockopt(t, v6, syscall.IPPROTO_IP, syscall.IP_MTU_DISCOVER); got != syscall.IP_PMTUDISC_PROBE {
		t.Errorf("udp6 IP_MTU_DISCOVER = %d, want %d (PROBE)", got, syscall.IP_PMTUDISC_PROBE)
	}
	if got := getsockopt(t, v6, syscall.IPPROTO_IPV6, ipv6AutoFlowLabel); got != 1 {
		t.Errorf("udp6 IPV6_AUTOFLOWLABEL = %d, want 1", got)
	}

	// The dual-stack default: network "udp" on an unspecified address is an
	// AF_INET6 socket and gets the IPv6 treatment.
	dual, err := udp.Listen("udp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	defer dual.Close()
	if got := getsockopt(t, dual, syscall.IPPROTO_IPV6, ipv6MTUDiscover); got != ipv6PMTUDiscProbe {
		t.Errorf("dual-stack IPV6_MTU_DISCOVER = %d, want %d (PROBE)", got, ipv6PMTUDiscProbe)
	}
}

// Before Configure the kernel default is WANT (fragment when needed), so the
// test above is not passing on a default it never changed.
func TestKernelDefaultIsNotProbe(t *testing.T) {
	c, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if got := getsockopt(t, c, syscall.IPPROTO_IP, syscall.IP_MTU_DISCOVER); got == syscall.IP_PMTUDISC_PROBE {
		t.Fatalf("fresh socket already has IP_MTU_DISCOVER=PROBE; this test cannot distinguish Configure from the default")
	}
}

// DF is observable: with PROBE the kernel refuses a datagram larger than the
// interface MTU with EMSGSIZE instead of fragmenting it. Loopback's MTU admits
// every UDP datagram, so the datagram goes towards TEST-NET-1 (RFC 5737),
// which leaves through the default route — MTU 1500 or thereabouts — and
// reaches no one. A plain socket sending the same datagram is the control:
// if it fails too, there is no route to measure against.
func TestDontFragmentRefusesOversize(t *testing.T) {
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
