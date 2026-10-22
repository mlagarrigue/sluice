package quic

import (
	"crypto/tls"
	"net"
	"testing"
)

// FuzzListenerDispatch: a datagram naming no connection this listener
// routes is answered statelessly or not at all — a stateless reset
// (RFC 9000 §10.3), a Version Negotiation packet (§5.2.2), or silence. The
// sender is unauthenticated and its source address may be someone else's,
// so the property is the one that keeps the listener from amplifying: at
// most one datagram goes back, and it is smaller than the one that came in
// (§10.3.3 requires it of the reset; the Version Negotiation answer is gated
// on a 1200-byte trigger, §14.1). An Initial would have to decrypt to earn
// more, which no mutation of noise does.
func FuzzListenerDispatch(f *testing.F) {
	short := func(n int) []byte {
		b := make([]byte, n)
		b[0] = 0x40
		copy(b[1:], "unknown-dcid")
		return b
	}
	long := func(version uint32, n int) []byte {
		b := make([]byte, n)
		b[0] = 0xc0
		b[1], b[2], b[3], b[4] = byte(version>>24), byte(version>>16), byte(version>>8), byte(version)
		b[5] = 8
		copy(b[6:], "dcid-abc")
		b[14] = 8
		copy(b[15:], "scid-xyz")
		return b
	}
	f.Add([]byte{})
	f.Add([]byte{0x40})
	f.Add(short(21))
	f.Add(short(22))
	f.Add(short(1200))
	f.Add(long(0x1a2a3a4a, 1200)) // a foreign version: Version Negotiation
	f.Add(long(0x1a2a3a4a, 1199)) // the same, under the §14.1 floor
	f.Add(long(0, 1200))          // a Version Negotiation packet: never answered
	f.Add(long(version1, 1200))   // an Initial that does not decrypt
	f.Add(long(version1, 64))

	pc := &capturePC{fuzzSink: fuzzSink{addr: "dispatch:self"}, stopped: make(chan struct{})}
	l, err := NewListener(pc, &tls.Config{MinVersion: tls.VersionTLS13, NextProtos: []string{"h3"}}, DefaultParameters(), ListenerConfig{})
	if err != nil {
		f.Fatal(err)
	}
	f.Cleanup(func() { _ = l.Close() })
	from := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 4433}

	f.Fuzz(func(t *testing.T, datagram []byte) {
		// The reset budget is per second; spent by earlier inputs, it would
		// silence this one and the property would hold vacuously.
		l.mu.Lock()
		l.resetsSent = 0
		clear(l.resetsTo)
		l.mu.Unlock()

		l.dispatch(append([]byte(nil), datagram...), from, pc, 0)

		writes := pc.taken()
		if len(writes) > 1 {
			t.Fatalf("%d datagrams answered one of %d bytes, want at most one", len(writes), len(datagram))
		}
		if len(writes) == 1 && len(writes[0]) >= len(datagram) {
			t.Fatalf("a %d-byte answer to a %d-byte datagram, want it smaller", len(writes[0]), len(datagram))
		}
	})
}
