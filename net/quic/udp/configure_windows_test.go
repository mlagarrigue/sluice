package udp_test

import (
	"errors"
	"net"
	"syscall"
	"testing"
	"time"
	"unsafe"

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
		v, opErr = syscall.GetsockoptInt(syscall.Handle(fd), level, opt)
	}); err != nil {
		t.Fatal(err)
	}
	if opErr != nil {
		t.Fatalf("getsockopt(%d,%d): %v", level, opt, opErr)
	}
	return v
}

// setConnReset sets SIO_UDP_CONNRESET on c: 1 reports an ICMP
// port-unreachable as WSAECONNRESET on the next read, 0 does not.
func setConnReset(t *testing.T, c *net.UDPConn, on uint32) {
	t.Helper()
	rc, err := c.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var opErr error
	if err := rc.Control(func(fd uintptr) {
		var returned uint32
		opErr = syscall.WSAIoctl(syscall.Handle(fd), syscall.SIO_UDP_CONNRESET,
			(*byte)(unsafe.Pointer(&on)), uint32(unsafe.Sizeof(on)), nil, 0, &returned, nil, 0)
	}); err != nil {
		t.Fatal(err)
	}
	if opErr != nil {
		t.Fatalf("SIO_UDP_CONNRESET %d: %v", on, opErr)
	}
}

// Winsock names (ws2ipdef.h), spelled again here because the package's
// copies are unexported.
const (
	ipDontFragment  = 14 // IP_DONTFRAGMENT
	ipv6DontFrag    = 14 // IPV6_DONTFRAG
	ipMTUDiscover   = 71 // IP_MTU_DISCOVER
	ipv6MTUDiscover = 71 // IPV6_MTU_DISCOVER
	pmtudiscProbe   = 3  // IP_PMTUDISC_PROBE
)

// RFC 9000 §14.2.1: a path MTU below 1200 bytes, which an unauthenticated
// ICMP writes into the stack's destination cache, must not stop the
// 1200-byte datagram. Winsock documents that IP_PMTUDISC_PROBE refuses a
// datagram only past the interface MTU, ignoring that cache; Configure
// asks for it, and Winsock reads it back. The cache itself cannot be
// filled here without forging an ICMP — Windows has no route MTU to stand
// in for it, as Linux and macOS do — so what is tested is the mode asked
// for, and the documented meaning of the mode is relied on.
func TestPathMTUCacheIsIgnoredByTheProbeMode(t *testing.T) {
	v4, err := udp.Listen("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer v4.Close()
	if got := getsockopt(t, v4, syscall.IPPROTO_IP, ipMTUDiscover); got != pmtudiscProbe {
		t.Errorf("udp4 IP_MTU_DISCOVER = %d, want IP_PMTUDISC_PROBE (%d)", got, pmtudiscProbe)
	}
	dual, err := udp.Listen("udp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	defer dual.Close()
	if got := getsockopt(t, dual, syscall.IPPROTO_IPV6, ipv6MTUDiscover); got != pmtudiscProbe {
		t.Errorf("dual-stack IPV6_MTU_DISCOVER = %d, want IP_PMTUDISC_PROBE (%d)", got, pmtudiscProbe)
	}
	if got := getsockopt(t, dual, syscall.IPPROTO_IP, ipMTUDiscover); got != pmtudiscProbe {
		t.Errorf("dual-stack IP_MTU_DISCOVER = %d, want IP_PMTUDISC_PROBE (%d) for mapped peers", got, pmtudiscProbe)
	}
	// A datagram of 1200 bytes still leaves: the mode refuses nothing the
	// interface carries.
	if _, err := v4.WriteTo(make([]byte, 1200), v4.LocalAddr()); err != nil {
		t.Errorf("a 1200-byte datagram on the loopback: %v", err)
	}
}

// What Configure claims to do on Windows, read back from Winsock: an
// AF_INET socket carries IP_DONTFRAGMENT=1; an AF_INET6 one, dual-stack
// included, carries IPV6_DONTFRAG=1 and IP_DONTFRAGMENT=1 for mapped peers.
func TestConfigureSetsWindowsOptions(t *testing.T) {
	v4, err := udp.Listen("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer v4.Close()
	if got := getsockopt(t, v4, syscall.IPPROTO_IP, ipDontFragment); got != 1 {
		t.Errorf("udp4 IP_DONTFRAGMENT = %d, want 1", got)
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
	if got := getsockopt(t, dual, syscall.IPPROTO_IP, ipDontFragment); got != 1 {
		t.Errorf("dual-stack IP_DONTFRAGMENT = %d, want 1", got)
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

// Before Configure the Winsock default is 0 (fragment when needed), so the
// test above is not passing on a default it never changed.
func TestWinsockDefaultIsNotDontFragment(t *testing.T) {
	c, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if got := getsockopt(t, c, syscall.IPPROTO_IP, ipDontFragment); got != 0 {
		t.Fatalf("fresh socket already has IP_DONTFRAGMENT=%d; this test cannot distinguish Configure from the default", got)
	}
}

// DF is observable: Winsock refuses a datagram larger than the outgoing
// interface MTU with WSAEMSGSIZE instead of fragmenting it. Loopback's MTU
// admits every UDP datagram, so the datagram goes towards TEST-NET-1
// (RFC 5737), which leaves through the default route and reaches no one.
// A plain socket sending the same datagram is the control: if it fails
// too, there is no route to measure against.
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

// With SIO_UDP_CONNRESET on (Winsock's default), a datagram sent to a
// closed loopback port draws an ICMP port-unreachable that the next read
// reports as WSAECONNRESET. Configure turns it off, so the same read just
// waits for a datagram and times out.
//
// Package net already turns it off on every UDP socket it opens
// (go.dev/issue/5834), so a socket from net.ListenUDP cannot show the
// difference: this one is turned back on first, its reset seen, and only
// then given to Configure. The control used to be a plain socket from
// net.ListenUDP, which never reported the reset, and the test skipped on
// every run.
func TestConfigureDisablesConnReset(t *testing.T) {
	// A port nobody listens on: bind one, note it, release it.
	hole, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	dst := hole.LocalAddr()
	_ = hole.Close()

	readAfterSend := func(c *net.UDPConn) error {
		if _, err := c.WriteTo([]byte("x"), dst); err != nil {
			t.Fatalf("WriteTo: %v", err)
		}
		_ = c.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
		_, _, err := c.ReadFrom(make([]byte, 64))
		return err
	}

	c, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	setConnReset(t, c, 1)
	if err := readAfterSend(c); !errors.Is(err, syscall.Errno(10054)) { // WSAECONNRESET
		t.Fatalf("with SIO_UDP_CONNRESET on, the read reported %v, want WSAECONNRESET", err)
	}

	if err := udp.Configure(c); err != nil {
		t.Fatal(err)
	}
	err = readAfterSend(c)
	var ne net.Error
	if !errors.As(err, &ne) || !ne.Timeout() {
		t.Fatalf("read after a send to a closed port: %v, want a timeout", err)
	}
}
