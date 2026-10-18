package ipkey_test

import (
	"net/netip"
	"strings"
	"testing"

	"github.com/mlagarrigue/sluice/internal/ipkey"
)

// At the default lengths two addresses of one /64 share a key and the
// neighbouring /64 does not; an IPv4 address keys whole, and its
// IPv4-mapped IPv6 form keys with it rather than as IPv6.
func TestOfKeysByPrefix(t *testing.T) {
	key := func(s string) netip.Prefix {
		return ipkey.Of(netip.MustParseAddr(s), ipkey.DefaultIPv4Bits, ipkey.DefaultIPv6Bits)
	}
	for _, c := range []struct {
		a, b string
		same bool
	}{
		{"2001:db8:0:1::1", "2001:db8:0:1:ffff:ffff:ffff:ffff", true},
		{"2001:db8:0:1::1", "2001:db8:0:2::1", false},
		{"2001:db8:0:1::1", "2001:db8:0:0:ffff:ffff:ffff:ffff", false},
		{"192.0.2.1", "::ffff:192.0.2.1", true},
		{"192.0.2.1", "192.0.2.2", false},
		{"::ffff:192.0.2.1", "::ffff:192.0.2.2", false},
	} {
		if ka, kb := key(c.a), key(c.b); (ka == kb) != c.same {
			t.Errorf("%s keys %v, %s keys %v; same key wanted %v", c.a, ka, c.b, kb, c.same)
		}
	}
	if got, want := key("::ffff:192.0.2.1"), netip.MustParsePrefix("192.0.2.1/32"); got != want {
		t.Errorf("::ffff:192.0.2.1 keys %v, want %v", got, want)
	}
	if got, want := ipkey.Of(netip.MustParseAddr("192.0.2.200"), 24, 48), netip.MustParsePrefix("192.0.2.0/24"); got != want {
		t.Errorf("192.0.2.200 at /24 keys %v, want %v", got, want)
	}
}

// Zero resolves to the defaults; a length past its address's, or negative,
// is named in the error with its value.
func TestCheckResolvesAndRefuses(t *testing.T) {
	v4, v6 := 0, 0
	if err := ipkey.Check("C.", &v4, &v6); err != nil || v4 != 32 || v6 != 64 {
		t.Fatalf("zero lengths resolved to /%d and /%d, err %v; want /32 and /64", v4, v6, err)
	}
	for _, c := range []struct {
		v4, v6 int
		want   []string
	}{
		{33, 64, []string{"C.IPv4PrefixBits 33 is outside 1..32"}},
		{32, 129, []string{"C.IPv6PrefixBits 129 is outside 1..128"}},
		{-1, -8, []string{"C.IPv4PrefixBits -1", "C.IPv6PrefixBits -8"}},
	} {
		v4, v6 := c.v4, c.v6
		err := ipkey.Check("C.", &v4, &v6)
		if err == nil {
			t.Errorf("Check accepted /%d and /%d", c.v4, c.v6)
			continue
		}
		for _, w := range c.want {
			if !strings.Contains(err.Error(), w) {
				t.Errorf("Check(/%d, /%d) = %q, want it to say %q", c.v4, c.v6, err, w)
			}
		}
	}
}
