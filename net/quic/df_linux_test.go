//go:build linux

package quic

import (
	"net"
	"syscall"
	"testing"
)

// RFC 9000 §14: "In IPv4, the DF bit MUST be set if possible." A bare
// net.ListenPacket socket handed to the transport carries the kernel's
// default; building a connection on it must leave it with the option
// udp.Configure sets — PROBE, the choice udp/configure_linux.go explains —
// without the caller having to know the option exists.
func TestConnSetsDontFragmentOnABareSocket(t *testing.T) {
	pc, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	uc := pc.(*net.UDPConn)
	if got := readSockopt(t, uc, syscall.IPPROTO_IP, syscall.IP_MTU_DISCOVER); got == syscall.IP_PMTUDISC_PROBE {
		t.Fatalf("fresh socket already has IP_MTU_DISCOVER=PROBE; this test cannot tell the transport's doing from the default")
	}
	c := assembleConn(pc, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 4433}, []byte("dcid0000"), []byte("scid0000"), true, DefaultParameters())
	defer c.Close()
	if got := readSockopt(t, uc, syscall.IPPROTO_IP, syscall.IP_MTU_DISCOVER); got != syscall.IP_PMTUDISC_PROBE {
		t.Errorf("IP_MTU_DISCOVER = %d after building a connection on a bare socket, want %d (PROBE: Don't Fragment set, §14)", got, syscall.IP_PMTUDISC_PROBE)
	}
}

// A socket handed to Rebind is a bare one as much as the first was: it
// must leave with Don't Fragment set too, or the search on the new path
// probes sizes a router may fragment (RFC 8899 §3).
func TestRebindSetsDontFragmentOnTheNewSocket(t *testing.T) {
	client, _, _ := pmtuPair(t, 1500)
	second, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if got := readSockopt(t, second, syscall.IPPROTO_IP, syscall.IP_MTU_DISCOVER); got == syscall.IP_PMTUDISC_PROBE {
		t.Fatalf("fresh socket already has IP_MTU_DISCOVER=PROBE; this test cannot tell the transport's doing from the default")
	}
	var rebindErr error
	waitFor(t, "the client to confirm the handshake and rebind", func() bool {
		rebindErr = client.Rebind(second)
		return rebindErr == nil
	})
	if got := readSockopt(t, second, syscall.IPPROTO_IP, syscall.IP_MTU_DISCOVER); got != syscall.IP_PMTUDISC_PROBE {
		t.Errorf("IP_MTU_DISCOVER = %d on the rebound socket, want %d (PROBE: Don't Fragment set)", got, syscall.IP_PMTUDISC_PROBE)
	}
	client.mu.Lock()
	fragmentable := client.fragmentable
	client.mu.Unlock()
	if fragmentable {
		t.Error("the rebound socket took DF, yet the connection reads it as fragmentable")
	}
}

// dfSocket reports false only for a socket it configures and could not:
// here a closed one. A socket of another type is the caller's and reports
// true, configured as the caller chose.
func TestDFSocketReportsWhatItCouldNotSet(t *testing.T) {
	open, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer open.Close()
	closed, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	_ = closed.Close()
	if !dfSocket(open) {
		t.Error("dfSocket(open UDP socket) = false, want true")
	}
	if dfSocket(closed) {
		t.Error("dfSocket(closed UDP socket) = true, want false: the option was never set")
	}
	if !dfSocket(newMTUPC(t, 1500)) {
		t.Error("dfSocket(a caller's wrapper) = false, want true")
	}
}

// readSockopt reads an integer socket option back from c.
func readSockopt(t *testing.T, c *net.UDPConn, level, opt int) int {
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
