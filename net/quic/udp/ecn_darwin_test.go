package udp

import (
	"encoding/binary"
	"syscall"
	"testing"
)

// XNU reports the IPv4 TOS as one u_char under IP_RECVTOS and the IPv6
// traffic class as a C int under IPV6_TCLASS; the ECN field is the low
// two bits of either, and a message of another kind ahead of it is walked
// past. The header is 12 bytes and the data is aligned on 4.
func TestPacketECNReadsForgedControlMessages(t *testing.T) {
	if cmsgLen(0) != 12 {
		t.Fatalf("CMSG_LEN(0) = %d, want 12", cmsgLen(0))
	}
	v4 := forgeCmsg(syscall.IPPROTO_IP, 26, make([]byte, 12))                      // IP_PKTINFO
	v4 = append(v4, forgeCmsg(syscall.IPPROTO_IP, ipRecvTOS, []byte{0xb8 | 2})...) // AF41 DSCP, ECT(0)
	if ecn, ok := packetECN(v4); !ok || ecn != 2 {
		t.Fatalf("IPv4: ecn %d ok %v, want ECT(0)", ecn, ok)
	}
	tclass := make([]byte, 4)
	binary.NativeEndian.PutUint32(tclass, 0x03)                 // CE, no DSCP
	v6 := forgeCmsg(syscall.IPPROTO_IPV6, 46, make([]byte, 20)) // IPV6_PKTINFO
	v6 = append(v6, forgeCmsg(syscall.IPPROTO_IPV6, ipv6TClass, tclass)...)
	if ecn, ok := packetECN(v6); !ok || ecn != 3 {
		t.Fatalf("IPv6: ecn %d ok %v, want CE", ecn, ok)
	}
	if _, ok := packetECN(v4[:len(v4)-4]); ok { // cut into the data byte, not only the padding
		t.Fatal("a truncated message reported an ECN field")
	}
	if _, ok := packetECN(nil); ok {
		t.Fatal("no bytes reported an ECN field")
	}
}

// appendECN writes IP_TOS and IPV6_TCLASS as C ints, the one length
// ip_tos_from_control accepts; the received-side walk does not read the
// IP_TOS one (XNU reports under IP_RECVTOS) but reads the IPV6_TCLASS one.
func TestAppendECNRoundTrips(t *testing.T) {
	oob := appendECN(nil, 2)
	level, typ, data, rest, ok := nextCmsg(oob)
	if !ok || level != syscall.IPPROTO_IP || typ != ipTOS || len(data) != 4 || binary.NativeEndian.Uint32(data) != 2 {
		t.Fatalf("first message is %d/%d with %x", level, typ, data)
	}
	level, typ, data, rest, ok = nextCmsg(rest)
	if !ok || level != syscall.IPPROTO_IPV6 || typ != ipv6TClass || len(data) != 4 || binary.NativeEndian.Uint32(data) != 2 {
		t.Fatalf("second message is %d/%d with %x", level, typ, data)
	}
	if rest != nil {
		t.Fatalf("%d bytes after the two messages", len(rest))
	}
	if ecn, ok := packetECN(oob); !ok || ecn != 2 {
		t.Fatalf("IPV6_TCLASS read back: ecn %d ok %v", ecn, ok)
	}
}
