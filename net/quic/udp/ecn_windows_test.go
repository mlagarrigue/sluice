package udp

import (
	"encoding/binary"
	"syscall"
	"testing"
	"unsafe"
)

// Winsock reports the ECN bits as an INT under IP_ECN or IPV6_ECN; a
// message of another kind ahead of it is walked past. WSACMSGHDR is
// pointer-aligned: 16 bytes on a 64-bit build, 12 on a 32-bit one.
func TestPacketECNReadsForgedControlMessages(t *testing.T) {
	if want := int(unsafe.Sizeof(uintptr(0))) + 8; cmsgLen(0) != want {
		t.Fatalf("WSA_CMSG_LEN(0) = %d, want %d", cmsgLen(0), want)
	}
	if cmsgSpace(4) != cmsgLen(0)+cmsgLenSize {
		t.Fatalf("WSA_CMSG_SPACE(4) = %d, want %d", cmsgSpace(4), cmsgLen(0)+cmsgLenSize)
	}
	ecn := make([]byte, 4)
	binary.NativeEndian.PutUint32(ecn, 2)                     // ECT(0)
	v4 := forgeCmsg(syscall.IPPROTO_IP, 19, make([]byte, 12)) // IP_PKTINFO
	v4 = append(v4, forgeCmsg(syscall.IPPROTO_IP, ipECN, ecn)...)
	if got, ok := packetECN(v4); !ok || got != 2 {
		t.Fatalf("IPv4: ecn %d ok %v, want ECT(0)", got, ok)
	}
	binary.NativeEndian.PutUint32(ecn, 3)                       // CE
	v6 := forgeCmsg(syscall.IPPROTO_IPV6, 19, make([]byte, 20)) // IPV6_PKTINFO
	v6 = append(v6, forgeCmsg(syscall.IPPROTO_IPV6, ipv6ECN, ecn)...)
	if got, ok := packetECN(v6); !ok || got != 3 {
		t.Fatalf("IPv6: ecn %d ok %v, want CE", got, ok)
	}
	if _, ok := packetECN(v4[:len(v4)-2]); ok { // cut into the INT
		t.Fatal("a truncated message reported an ECN field")
	}
	if _, ok := packetECN(nil); ok {
		t.Fatal("no bytes reported an ECN field")
	}
}

// appendECN writes two messages the same walk reads back.
func TestAppendECNRoundTrips(t *testing.T) {
	oob := appendECN(nil, 2)
	if ecn, ok := packetECN(oob); !ok || ecn != 2 {
		t.Fatalf("first message: ecn %d ok %v", ecn, ok)
	}
	level, typ, _, rest, ok := nextCmsg(oob)
	if !ok || level != syscall.IPPROTO_IP || typ != ipECN {
		t.Fatalf("first message is %d/%d", level, typ)
	}
	level, typ, data, rest, ok := nextCmsg(rest)
	if !ok || level != syscall.IPPROTO_IPV6 || typ != ipv6ECN || binary.NativeEndian.Uint32(data) != 2 {
		t.Fatalf("second message is %d/%d with %x", level, typ, data)
	}
	if rest != nil {
		t.Fatalf("%d bytes after the two messages", len(rest))
	}
}
