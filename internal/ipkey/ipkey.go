// Package ipkey is the key a per-address connection bound counts a peer
// under: net/quic's ListenerConfig.MaxConnsPerIP and net/httpstream's
// Config.MaxConnsPerIP share it, so that the two bounds cannot drift apart.
//
// The key is a prefix, not the whole address. An IPv6 host is handed a /64
// as a matter of course (RFC 4291 §2.5.4, RFC 6177), so a bound on the
// full 128 bits is a bound the peer steps around by changing address; a
// bound on the /64 is the one it cannot. An IPv4 address is counted whole
// by default: a host rarely holds more than one, and a shorter prefix would
// fold unrelated neighbours together.
package ipkey

import (
	"errors"
	"fmt"
	"net/netip"
)

// The prefix lengths zero stands for in the configurations that use Of.
const (
	DefaultIPv4Bits = 32
	DefaultIPv6Bits = 64
)

// Of returns the key ip is counted under: ip with an IPv4-mapped IPv6
// address reduced to its IPv4 form, masked to its first v4Bits or v6Bits
// bits as it is one or the other. The lengths are the ones Check resolved;
// the zero Prefix is the key of an invalid ip.
func Of(ip netip.Addr, v4Bits, v6Bits int) netip.Prefix {
	ip = ip.Unmap()
	bits := v6Bits
	if ip.Is4() {
		bits = v4Bits
	}
	p, _ := ip.Prefix(bits) // fails only on a length Check refuses
	return p
}

// Check resolves the two lengths a configuration names, zero standing for
// the default, and returns an error naming each one out of its address's
// range; config is the configuration's name, "quic: ListenerConfig." say.
func Check(config string, v4Bits, v6Bits *int) error {
	var err error
	if *v4Bits == 0 {
		*v4Bits = DefaultIPv4Bits
	}
	if *v6Bits == 0 {
		*v6Bits = DefaultIPv6Bits
	}
	if *v4Bits < 0 || *v4Bits > 32 {
		err = errors.Join(err, fmt.Errorf("%sIPv4PrefixBits %d is outside 1..32 — zero means %d", config, *v4Bits, DefaultIPv4Bits))
	}
	if *v6Bits < 0 || *v6Bits > 128 {
		err = errors.Join(err, fmt.Errorf("%sIPv6PrefixBits %d is outside 1..128 — zero means %d", config, *v6Bits, DefaultIPv6Bits))
	}
	return err
}
