//go:build linux || darwin

package udp_test

import (
	"net"
	"net/netip"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/net/quic/udp"
)

// A datagram sent to a second local address on a socket bound to the
// unspecified address is reported as reaching that address, and the reply
// built with AppendPacketSource leaves from it. Without the control
// message the kernel picks the source by route — 127.0.0.1 towards a
// 127.0.0.1 client — which is the path change this exists to prevent.
func TestPacketInfoAnswersFromDestination(t *testing.T) {
	for _, tc := range []struct{ network, bind, client, peer, second string }{
		{"udp4", "0.0.0.0:0", "udp4", "127.0.0.1", "127.0.0.2"},
		{"udp", ":0", "udp4", "127.0.0.1", "127.0.0.2"}, // dual-stack AF_INET6: IPv4 reported as mapped
		{"udp6", "[::]:0", "udp6", "::1", "fd00::2"},
	} {
		t.Run(tc.network, func(t *testing.T) {
			second := netip.MustParseAddr(tc.second)
			requireLocal(t, second)
			srv, err := udp.Listen(tc.network, tc.bind)
			if err != nil {
				t.Fatal(err)
			}
			defer srv.Close()
			if err := udp.EnablePacketInfo(srv); err != nil {
				t.Fatalf("EnablePacketInfo: %v", err)
			}
			cli, err := net.ListenUDP(tc.client, &net.UDPAddr{IP: net.ParseIP(tc.peer)})
			if err != nil {
				t.Fatal(err)
			}
			defer cli.Close()
			port := srv.LocalAddr().(*net.UDPAddr).Port
			to := net.UDPAddrFromAddrPort(netip.AddrPortFrom(second, uint16(port))) //nolint:gosec // G115: a port
			if _, err := cli.WriteTo([]byte("ping"), to); err != nil {
				t.Fatalf("send to %v: %v", second, err)
			}

			_ = srv.SetReadDeadline(time.Now().Add(2 * time.Second))
			buf := make([]byte, 64)
			oob := make([]byte, udp.PacketInfoLen)
			n, oobn, _, from, err := srv.ReadMsgUDP(buf, oob)
			if err != nil {
				t.Fatalf("ReadMsgUDP: %v", err)
			}
			dst, ifindex, ok := udp.PacketDestination(oob[:oobn])
			if !ok {
				t.Fatalf("no destination in %d control bytes", oobn)
			}
			if dst.Unmap() != second {
				t.Fatalf("destination %v, want %v", dst, second)
			}
			if ifindex <= 0 {
				t.Errorf("ifindex %d, want the loopback interface's", ifindex)
			}

			// Control: a plain reply leaves from whatever the kernel picks.
			if _, err := srv.WriteTo(buf[:n], from); err != nil {
				t.Fatal(err)
			}
			if src := readSource(t, cli); src == second {
				t.Skipf("kernel already answers from %v; nothing to distinguish", second)
			}

			src := udp.AppendPacketSource(nil, dst, 0)
			if _, _, err := srv.WriteMsgUDP(buf[:n], src, from); err != nil {
				t.Fatalf("WriteMsgUDP: %v", err)
			}
			if got := readSource(t, cli); got != second {
				t.Fatalf("reply came from %v, want %v", got, second)
			}
		})
	}
}

// requireLocal skips unless addr is one of this host's addresses. Linux
// has the whole of 127/8 on lo; macOS has only 127.0.0.1, and neither has
// fd00::2. The macOS job of the CI workflow adds both to lo0, so there
// their absence is a failure, not a skip.
func requireLocal(t *testing.T, addr netip.Addr) {
	t.Helper()
	c, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(netip.AddrPortFrom(addr, 0)))
	if err == nil {
		c.Close()
		return
	}
	if runtime.GOOS == "darwin" && os.Getenv("GITHUB_ACTIONS") == "true" {
		t.Fatalf("%v is not configured on this runner: %v", addr, err)
	}
	t.Skipf("%v is not one of this host's addresses: %v", addr, err)
}

func readSource(t *testing.T, c *net.UDPConn) netip.Addr {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, from, err := c.ReadFromUDPAddrPort(make([]byte, 64))
	if err != nil {
		t.Fatalf("client read: %v", err)
	}
	return from.Addr().Unmap()
}

// The receive-side parse runs once per datagram on the listener's read
// loop, so it must not allocate.
func TestPacketDestinationDoesNotAllocate(t *testing.T) {
	oob := udp.AppendPacketSource(nil, netip.MustParseAddr("192.0.2.7"), 3)
	oob = udp.AppendPacketSource(oob, netip.MustParseAddr("2001:db8::1"), 5)
	allocs := testing.AllocsPerRun(100, func() {
		if _, _, ok := udp.PacketDestination(oob); !ok {
			t.Fatal("no destination")
		}
	})
	if allocs != 0 {
		t.Fatalf("PacketDestination allocates %v times", allocs)
	}
	// The first message wins, and both layouts round-trip — the IPv4 one on
	// Linux only: XNU reports the destination in ipi_addr and takes the
	// source from ipi_spec_dst (pktinfo_darwin_test.go).
	if dst, ifindex, _ := udp.PacketDestination(oob); runtime.GOOS == "linux" && (dst != netip.MustParseAddr("192.0.2.7") || ifindex != 3) {
		t.Fatalf("got %v %d", dst, ifindex)
	}
	v6 := udp.AppendPacketSource(nil, netip.MustParseAddr("2001:db8::1"), 5)
	if dst, ifindex, _ := udp.PacketDestination(v6); dst != netip.MustParseAddr("2001:db8::1") || ifindex != 5 {
		t.Fatalf("got %v %d", dst, ifindex)
	}
	if _, _, ok := udp.PacketDestination(oob[:5]); ok {
		t.Fatal("truncated control bytes parsed")
	}
}

// Elsewhere than Linux and Darwin these are stubs; here they must be live.
func TestEnablePacketInfoIsLive(t *testing.T) {
	c, err := udp.Listen("udp6", "[::]:0")
	if err != nil {
		t.Skipf("no IPv6 here: %v", err)
	}
	defer c.Close()
	if err := udp.EnablePacketInfo(c); err != nil {
		t.Fatal(err)
	}
}
