package udp_test

import (
	"net"
	"os"
	"strconv"
	"strings"
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
// reaches no one. The route's MTU is read rather than assumed (IP_MTU on a
// connected socket): a runner whose interface carries jumbo frames admitted
// a 3000-byte datagram with DF set. A plain socket sending the same
// datagram is the control: if it fails too, there is no route to measure
// against.
func TestDontFragmentRefusesOversize(t *testing.T) {
	dst := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 4433}

	route, err := net.DialUDP("udp4", nil, dst)
	if err != nil {
		t.Skipf("no default IPv4 route to send through: %v", err)
	}
	mtu := getsockopt(t, route, syscall.IPPROTO_IP, syscall.IP_MTU)
	_ = route.Close() // only its route's MTU was wanted
	// 20 bytes of IPv4 header and 8 of UDP: one byte over the MTU, within
	// the largest UDP payload IPv4 can carry.
	size := mtu - 28 + 1
	if size > 65507 {
		t.Skipf("route MTU %d leaves no oversize UDP datagram", mtu)
	}
	big := make([]byte, size)

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

// Listen asks for ReceiveBuffer, which the kernel doubles for its own
// bookkeeping and caps at net.core.rmem_max (socket(7)); the default buffer
// lost datagrams in a burst where this one lost none
// (BenchmarkUDPReceiveBurst).
func TestListenAsksForTheReceiveBuffer(t *testing.T) {
	raw, err := os.ReadFile("/proc/sys/net/core/rmem_max")
	if err != nil {
		t.Skipf("rmem_max unreadable: %v", err)
	}
	limit, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil {
		t.Fatal(err)
	}
	c, err := udp.Listen("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	want := 2 * min(udp.ReceiveBuffer, limit)
	if got := getsockopt(t, c, syscall.SOL_SOCKET, syscall.SO_RCVBUF); got != want {
		t.Errorf("SO_RCVBUF %d after Listen, want %d: ReceiveBuffer %d under rmem_max %d, doubled", got, want, udp.ReceiveBuffer, limit)
	}
}
