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
