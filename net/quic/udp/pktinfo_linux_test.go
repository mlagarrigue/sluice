package udp_test

import (
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/net/quic/udp"
)

// secondLoopback is 127.0.0.2: on Linux the whole of 127/8 answers on lo,
// so a socket bound to the unspecified address receives on two distinct
// local addresses without any configuration.
var secondLoopback = netip.MustParseAddr("127.0.0.2")

// A datagram sent to 127.0.0.2 on a socket bound to the unspecified
// address is reported as reaching 127.0.0.2, and the reply built with
// AppendPacketSource leaves from 127.0.0.2. Without the control message
// the kernel picks the source by route — 127.0.0.1 towards a 127.0.0.1
// client — which is the path change this exists to prevent.
func TestPacketInfoAnswersFromDestination(t *testing.T) {
	for _, tc := range []struct{ network, bind string }{
		{"udp4", "0.0.0.0:0"},
		{"udp", ":0"}, // dual-stack AF_INET6: IPv4 reported as mapped
	} {
		t.Run(tc.network, func(t *testing.T) {
			srv, err := udp.Listen(tc.network, tc.bind)
			if err != nil {
				t.Fatal(err)
			}
			defer srv.Close()
			if err := udp.EnablePacketInfo(srv); err != nil {
				t.Fatalf("EnablePacketInfo: %v", err)
			}
			cli, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			defer cli.Close()
			port := srv.LocalAddr().(*net.UDPAddr).Port
			to := net.UDPAddrFromAddrPort(netip.AddrPortFrom(secondLoopback, uint16(port)))
			if _, err := cli.WriteTo([]byte("ping"), to); err != nil {
				t.Skipf("127.0.0.2 not reachable here: %v", err)
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
			if dst.Unmap() != secondLoopback {
				t.Fatalf("destination %v, want %v", dst, secondLoopback)
			}
			if ifindex <= 0 {
				t.Errorf("ifindex %d, want the loopback interface's", ifindex)
			}

			// Control: a plain reply leaves from whatever the kernel picks.
			if _, err := srv.WriteTo(buf[:n], from); err != nil {
				t.Fatal(err)
			}
			if src := readSource(t, cli); src == secondLoopback {
				t.Skip("kernel already answers from 127.0.0.2; nothing to distinguish")
			}

			src := udp.AppendPacketSource(nil, dst, 0)
			if _, _, err := srv.WriteMsgUDP(buf[:n], src, from); err != nil {
				t.Fatalf("WriteMsgUDP: %v", err)
			}
			if got := readSource(t, cli); got != secondLoopback {
				t.Fatalf("reply came from %v, want %v", got, secondLoopback)
			}
		})
	}
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
	// The first message wins, and both layouts round-trip.
	if dst, ifindex, _ := udp.PacketDestination(oob); dst != netip.MustParseAddr("192.0.2.7") || ifindex != 3 {
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

// Elsewhere than Linux these are stubs; here they must be live.
func TestEnablePacketInfoOnLinux(t *testing.T) {
	c, err := udp.Listen("udp6", "[::]:0")
	if err != nil {
		t.Skipf("no IPv6 here: %v", err)
	}
	defer c.Close()
	if err := udp.EnablePacketInfo(c); err != nil {
		t.Fatal(err)
	}
}
