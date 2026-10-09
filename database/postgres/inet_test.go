package postgres

import (
	"bytes"
	"net/netip"
	"strings"
	"testing"
)

// # inet and cidr

func TestInetAgainstServerVectors(t *testing.T) {
	tests := []struct {
		sql  string
		hex  string
		cidr bool
	}{
		{"192.168.1.1", "02200004c0a80101", false},
		{"192.168.1.1/24", "02180004c0a80101", false},
		{"10.0.0.0/8", "020800040a000000", false},
		{"2001:db8::1", "0380001020010db8000000000000000000000001", false},
		{"::1/128", "0380001000000000000000000000000000000001", false},
		{"10.0.0.0/8", "020801040a000000", true},
	}
	for _, tc := range tests {
		name := tc.sql
		if tc.cidr {
			name += " (cidr)"
		}
		t.Run(name, func(t *testing.T) {
			want := mustHex(t, tc.hex)

			p, err := DecodeInet(want)
			if err != nil {
				t.Fatalf("DecodeInet: %v", err)
			}

			var got []byte
			if tc.cidr {
				got = AppendCIDR(nil, p)
			} else {
				got = AppendInet(nil, p)
			}
			if !bytes.Equal(got, want) {
				t.Errorf("re-encoded %v as %x, want %x", p, got, want)
			}
		})
	}
}

// The host bits below the netmask are data, not noise: 192.168.1.1/24 is a
// host on a network and 192.168.1.0/24 is the network. Masking on the way
// through would turn one into the other with nothing to say it happened.
func TestInetKeepsTheHostBits(t *testing.T) {
	p, err := DecodeInet(mustHex(t, "02180004c0a80101"))
	if err != nil {
		t.Fatal(err)
	}
	if got := p.Addr().String(); got != "192.168.1.1" {
		t.Errorf("address = %s, want 192.168.1.1 with its host bits intact", got)
	}
	if p.Bits() != 24 {
		t.Errorf("netmask = %d bits, want 24", p.Bits())
	}
}

// An IPv4-mapped IPv6 address stays IPv6, and this test used to assert the
// opposite.
//
// Unmapping it looked like tidiness — the same four bytes, expressed twice —
// and it was a rewrite of the caller's value. The server settled it: PostgreSQL
// reports family 6 for '::ffff:1.2.3.4'::inet, and answers **false** to
// '::ffff:1.2.3.4'::inet = '1.2.3.4'::inet. Two values, not two spellings. A
// client that turned one into the other would write back something other than
// what it read, which is the most ordinary thing a client does with a column.
//
// Found by fuzzing, which produced exactly this shape and noticed the bytes
// did not survive the round trip. The test that preceded it asserted the
// corruption was correct, which is the worst kind of test: it does not merely
// fail to catch the bug, it defends it.
func TestInetKeepsFourInSixAsIPv6(t *testing.T) {
	mapped := netip.MustParseAddr("::ffff:192.168.1.1")
	if !mapped.Is4In6() {
		t.Fatal("the fixture is not a 4-in-6 address")
	}

	got := AppendInet(nil, netip.PrefixFrom(mapped, 128))
	if got[0] != 3 {
		t.Errorf("family byte = %d, want 3: a 4-in-6 address is IPv6 to PostgreSQL", got[0])
	}
	if got[3] != 16 {
		t.Errorf("address length = %d, want 16", got[3])
	}

	// And the plain IPv4 spelling is still IPv4, so the two remain distinct.
	plain := AppendInet(nil, netip.MustParsePrefix("192.168.1.1/32"))
	if plain[0] != 2 || plain[3] != 4 {
		t.Errorf("a plain IPv4 address encoded as family %d, %d bytes; want 2, 4", plain[0], plain[3])
	}
	if bytes.Equal(got, plain) {
		t.Error("the two spellings encoded identically; PostgreSQL considers them unequal")
	}
}

// The family byte is PostgreSQL's own numbering, not the operating system's.
// If this ever tracked syscall.AF_INET6 the bytes would be right on Linux and
// wrong everywhere else, and no test on one platform would show it.
func TestInetFamilyIsPostgresNumbering(t *testing.T) {
	v4 := AppendInet(nil, netip.MustParsePrefix("10.0.0.0/8"))
	v6 := AppendInet(nil, netip.MustParsePrefix("::1/128"))
	// The literals are asserted, not the constants: a test that compares the
	// output against the constant it was produced from agrees with any value.
	if v4[0] != 2 {
		t.Errorf("IPv4 family byte = %d, want 2", v4[0])
	}
	if v6[0] != 3 {
		t.Errorf("IPv6 family byte = %d, want 3, which is not any platform's AF_INET6 (10 on Linux, 30 on the BSDs, 23 on Windows)", v6[0])
	}
}

func TestDecodeInetRefusals(t *testing.T) {
	tests := []struct {
		name string
		hex  string
		want string
	}{
		{"too short to hold a header", "0220", "want at least 4"},
		{"an unknown address family", "0a200004c0a80101", "neither IPv4"},
		{"a family and a length that disagree", "02200010c0a80101", "want 4"},
		{"a length that does not match the value", "02200004c0a801", "want 8"},
		{"a netmask wider than the address", "02f00004c0a80101", "netmask is 240 bits"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeInet(mustHex(t, tc.hex))
			if err == nil {
				t.Fatal("DecodeInet accepted it")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}

// An invalid prefix has no encoding. Written naively, the zero Prefix claimed
// the IPv6 family with 255 netmask bits and no address; it goes out with
// family 0 instead, which both the server and DecodeInet refuse by name.
func TestInvalidPrefixEncodesAsAnUnknownFamily(t *testing.T) {
	outOfRange := netip.PrefixFrom(netip.MustParseAddr("10.0.0.1"), 33)
	for name, p := range map[string]netip.Prefix{"zero": {}, "bits past the address": outOfRange} {
		for kind, enc := range map[string][]byte{"inet": AppendInet(nil, p), "cidr": AppendCIDR(nil, p)} {
			if len(enc) != inetHeaderSize || enc[0] != 0 || enc[1] != 0 || enc[3] != 0 {
				t.Errorf("%s %s: encoded as % x, want family 0, no bits, no address", name, kind, enc)
			}
			if _, err := DecodeInet(enc); err == nil || !strings.Contains(err.Error(), "family 0") {
				t.Errorf("%s %s: DecodeInet = %v, want the family refused", name, kind, err)
			}
		}
	}
}
