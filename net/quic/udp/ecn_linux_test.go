package udp

import (
	"encoding/binary"
	"syscall"
	"testing"
)

// forgeCmsg lays out one control message as the kernel does: cmsghdr,
// then data, then padding to the next word.
func forgeCmsg(level, typ int, data []byte) []byte {
	b := make([]byte, syscall.CmsgSpace(len(data)))
	if cmsgLenSize == 8 {
		binary.NativeEndian.PutUint64(b, uint64(syscall.CmsgLen(len(data))))
	} else {
		binary.NativeEndian.PutUint32(b, uint32(syscall.CmsgLen(len(data))))
	}
	binary.NativeEndian.PutUint32(b[cmsgLenSize:], uint32(level))
	binary.NativeEndian.PutUint32(b[cmsgLenSize+4:], uint32(typ))
	copy(b[syscall.CmsgLen(0):], data)
	return b
}

// The kernel reports IP_TOS as one byte and IPV6_TCLASS as a C int; the
// ECN field is the low two bits of either, and a message of another kind
// ahead of it — the packet-information one — is walked past.
func TestPacketECNReadsForgedControlMessages(t *testing.T) {
	v4 := forgeCmsg(syscall.IPPROTO_IP, ipPktinfo, make([]byte, 12))
	v4 = append(v4, forgeCmsg(syscall.IPPROTO_IP, ipTOS, []byte{0xb8 | 2})...) // AF41 DSCP, ECT(0)
	if ecn, ok := packetECN(v4); !ok || ecn != 2 {
		t.Fatalf("IPv4: ecn %d ok %v, want ECT(0)", ecn, ok)
	}
	tclass := make([]byte, 4)
	binary.NativeEndian.PutUint32(tclass, 0x03) // CE, no DSCP
	v6 := forgeCmsg(syscall.IPPROTO_IPV6, ipv6Pktinfo, make([]byte, 20))
	v6 = append(v6, forgeCmsg(syscall.IPPROTO_IPV6, ipv6TClass, tclass)...)
	if ecn, ok := packetECN(v6); !ok || ecn != 3 {
		t.Fatalf("IPv6: ecn %d ok %v, want CE", ecn, ok)
	}
	if _, ok := packetECN(forgeCmsg(syscall.IPPROTO_IP, ipPktinfo, make([]byte, 12))); ok {
		t.Fatal("a packet-information message alone reported an ECN field")
	}
	if _, ok := packetECN(v4[:len(v4)-8]); ok { // cut into the data byte, not only the padding
		t.Fatal("a truncated message reported an ECN field")
	}
	if _, ok := packetECN(nil); ok {
		t.Fatal("no bytes reported an ECN field")
	}
}

// appendECN writes two messages the kernel can parse back: the same walk
// that reads a received TOS reads them, int-sized data included.
func TestAppendECNRoundTrips(t *testing.T) {
	oob := appendECN(nil, 2)
	if ecn, ok := packetECN(oob); !ok || ecn != 2 {
		t.Fatalf("first message: ecn %d ok %v", ecn, ok)
	}
	level, typ, _, rest, ok := nextCmsg(oob)
	if !ok || level != syscall.IPPROTO_IP || typ != ipTOS {
		t.Fatalf("first message is %d/%d", level, typ)
	}
	level, typ, data, rest, ok := nextCmsg(rest)
	if !ok || level != syscall.IPPROTO_IPV6 || typ != ipv6TClass || binary.NativeEndian.Uint32(data) != 2 {
		t.Fatalf("second message is %d/%d with %x", level, typ, data)
	}
	if rest != nil {
		t.Fatalf("%d bytes after the two messages", len(rest))
	}
}
