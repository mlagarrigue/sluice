package udp

import (
	"bytes"
	"encoding/binary"
	"net/netip"
	"syscall"
	"testing"
)

// ip_savecontrol fills in_pktinfo's ipi_addr and ipi_ifindex and leaves
// ipi_spec_dst zero; udp_check_pktinfo reads ipi_spec_dst and lets a
// non-zero ipi_ifindex override it with the unspecified address. The two
// directions therefore use different fields, which a round trip through
// AppendPacketSource cannot show.
func TestPacketInfoFollowsXNULayout(t *testing.T) {
	in := make([]byte, 12)
	binary.NativeEndian.PutUint32(in[0:4], 4) // ipi_ifindex
	copy(in[8:12], []byte{192, 0, 2, 7})      // ipi_addr; ipi_spec_dst stays zero
	oob := forgeCmsg(syscall.IPPROTO_IP, ipRecvTOS, []byte{2})
	oob = append(oob, forgeCmsg(syscall.IPPROTO_IP, ipPktinfo, in)...)
	if dst, ifindex, ok := packetDestination(oob); !ok || dst != netip.MustParseAddr("192.0.2.7") || ifindex != 4 {
		t.Fatalf("IPv4: %v %d %v, want 192.0.2.7 on 4", dst, ifindex, ok)
	}

	for _, src := range []string{"192.0.2.7", "::ffff:192.0.2.7"} {
		got := appendPacketSource(nil, netip.MustParseAddr(src), 4)
		want := forgeCmsg(syscall.IPPROTO_IP, ipPktinfo, []byte{0, 0, 0, 0, 192, 0, 2, 7, 0, 0, 0, 0})
		if !bytes.Equal(got, want) {
			t.Fatalf("source %s: % x, want IP_PKTINFO with ipi_spec_dst only: % x", src, got, want)
		}
	}

	got := appendPacketSource(nil, netip.MustParseAddr("fe80::1"), 4)
	in6 := make([]byte, 20)
	copy(in6, netip.MustParseAddr("fe80::1").AsSlice())
	binary.NativeEndian.PutUint32(in6[16:20], 4)
	if want := forgeCmsg(syscall.IPPROTO_IPV6, ipv6Pktinfo, in6); !bytes.Equal(got, want) {
		t.Fatalf("IPv6 source: % x, want % x", got, want)
	}
	if dst, ifindex, ok := packetDestination(got); !ok || dst != netip.MustParseAddr("fe80::1") || ifindex != 4 {
		t.Fatalf("IPv6: %v %d %v", dst, ifindex, ok)
	}
}
