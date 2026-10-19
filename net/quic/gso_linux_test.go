package quic

import (
	"net"
	"syscall"
	"testing"
)

// A kernel that refuses UDP_SEGMENT on the write itself loses nothing: the
// burst goes again one datagram per write, and the connection remembers the
// refusal rather than asking on every flush. SO_NO_CHECK makes the refusal
// real: a socket that sends without checksums cannot have the kernel
// checksum each segment, and udp_send_skb answers EINVAL. Were EINVAL taken
// for the write's own failure, the burst would be lost and the read below
// would time out.
func TestRefusedSegmentationFallsBackAndRemembers(t *testing.T) {
	pcA, pcB := newLoopbackPair(t)
	rc, err := pcA.(*net.UDPConn).SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var serr error
	if err := rc.Control(func(fd uintptr) {
		serr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_NO_CHECK, 1) //nolint:gosec // G115: a socket descriptor
	}); err != nil {
		t.Fatal(err)
	}
	if serr != nil {
		t.Skipf("SO_NO_CHECK: %v", serr)
	}
	c := writeBurst(t, pcA, pcB)
	c.mu.Lock()
	segmented, noGSO := c.segmented, c.noGSO
	c.mu.Unlock()
	if !noGSO || segmented != 0 {
		t.Fatalf("noGSO %v, %d segmented writes: want the refusal remembered and none segmented", noGSO, segmented)
	}
}
