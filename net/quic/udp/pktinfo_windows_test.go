package udp

import (
	"bytes"
	"encoding/binary"
	"net/netip"
	"syscall"
	"testing"
)

// IN_PKTINFO is { IN_ADDR ipi_addr; ULONG ipi_ifindex; }, eight bytes with
// the address first — not the twelve-byte BSD layout — and ipi_addr is
// the destination on receive and the source on send. A message of another
// kind ahead of it is walked past.
func TestPacketInfoFollowsWinsockLayout(t *testing.T) {
	in := []byte{192, 0, 2, 7, 0, 0, 0, 0}
	binary.NativeEndian.PutUint32(in[4:8], 4) // ipi_ifindex
	oob := forgeCmsg(syscall.IPPROTO_IP, ipECN, make([]byte, 4))
	oob = append(oob, forgeCmsg(syscall.IPPROTO_IP, ipPktinfo, in)...)
	if dst, ifindex, ok := packetDestination(oob); !ok || dst != netip.MustParseAddr("192.0.2.7") || ifindex != 4 {
		t.Fatalf("IPv4: %v %d %v, want 192.0.2.7 on 4", dst, ifindex, ok)
	}
	if _, _, ok := packetDestination(forgeCmsg(syscall.IPPROTO_IP, ipPktinfo, in[:7])); ok {
		t.Fatal("a seven-byte IN_PKTINFO parsed")
	}

	for _, src := range []string{"192.0.2.7", "::ffff:192.0.2.7"} {
		got := appendPacketSource(nil, netip.MustParseAddr(src), 4)
		if want := forgeCmsg(syscall.IPPROTO_IP, ipPktinfo, in); !bytes.Equal(got, want) {
			t.Fatalf("source %s: % x, want IP_PKTINFO % x", src, got, want)
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
