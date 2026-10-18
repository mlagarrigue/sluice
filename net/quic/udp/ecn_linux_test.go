package udp

import (
	"encoding/binary"
	"syscall"
	"testing"
)

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

// appendECN writes one message per family the kernel can parse back: the
// same walk that reads a received TOS reads it, int-sized data included.
func TestAppendECNRoundTrips(t *testing.T) {
	for _, tc := range []struct {
		v4         bool
		level, typ int
	}{{true, syscall.IPPROTO_IP, ipTOS}, {false, syscall.IPPROTO_IPV6, ipv6TClass}} {
		oob := appendECN(nil, 2, tc.v4)
		if ecn, ok := packetECN(oob); !ok || ecn != 2 {
			t.Fatalf("v4 %v: ecn %d ok %v", tc.v4, ecn, ok)
		}
		level, typ, data, rest, ok := nextCmsg(oob)
		if !ok || level != tc.level || typ != tc.typ || len(data) != 4 || binary.NativeEndian.Uint32(data) != 2 {
			t.Fatalf("v4 %v: message is %d/%d with %x", tc.v4, level, typ, data)
		}
		if rest != nil {
			t.Fatalf("v4 %v: %d bytes after the message", tc.v4, len(rest))
		}
	}
}
