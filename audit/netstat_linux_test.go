package audit

import (
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"
)

// TestARealOverflowIsPutOnTheListenQueue overflows a real accept queue: a
// socket listening with an accept queue of one that never accepts, and a burst of
// connections to it. The kernel counts the SYNs it drops in the network
// namespace the test runs in, and the report reads them off two copies of
// /proc/net/netstat taken around the burst.
func TestARealOverflowIsPutOnTheListenQueue(t *testing.T) {
	if _, err := os.Stat("/proc/net/netstat"); err != nil {
		t.Skip("no /proc/net/netstat to read")
	}
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(fd)
	if err := syscall.Bind(fd, &syscall.SockaddrInet4{Addr: [4]byte{127, 0, 0, 1}}); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Listen(fd, 1); err != nil {
		t.Fatal(err)
	}
	sa, err := syscall.Getsockname(fd)
	if err != nil {
		t.Fatal(err)
	}
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(sa.(*syscall.SockaddrInet4).Port))

	before := snapshot(t, "before")
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			c, err := net.DialTimeout("tcp", addr, 300*time.Millisecond)
			if err == nil {
				defer c.Close()
				time.Sleep(400 * time.Millisecond) // hold the queue's places
			}
		})
	}
	wg.Wait()
	after := snapshot(t, "after")

	target, err := Open(Spec{NetstatBefore: before, NetstatAfter: after})
	if err != nil {
		t.Fatal(err)
	}
	if target.Counters.Delta["TcpExtListenOverflows"] <= 0 {
		t.Skip("the kernel counted no overflow in this namespace; the seeded tests stand for it")
	}
	findings, err := Run(Selection{Components: {"linux"}, Sources: {"logs"}}, nil, target)
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 1 || findings[0].Rule != "linux/listen-overflows" {
		t.Fatalf("%+v", findings)
	}
}

func snapshot(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile("/proc/net/netstat")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, b, 0o600); err != nil { //nolint:gosec // G703: a file of the test's own directory
		t.Fatal(err)
	}
	return path
}
