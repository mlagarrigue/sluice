package quic

import (
	"bytes"
	"net"
	"testing"
	"time"
)

// FuzzParseShortHeader: the first parse every 1-RTT datagram goes through.
func FuzzParseShortHeader(f *testing.F) {
	f.Add([]byte{0x40, 1, 2, 3, 4, 5, 6, 7, 8, 0xaa, 0xbb}, 8)
	f.Add([]byte{0x7f}, 0)
	f.Add([]byte{}, 20)

	f.Fuzz(func(t *testing.T, data []byte, dcidLen int) {
		h, err := parseShortHeader(data, dcidLen)
		if err != nil {
			return
		}
		if len(h.DCID) != dcidLen {
			t.Fatalf("a %d-byte identifier came back %d bytes", dcidLen, len(h.DCID))
		}
		if h.PNOffset != 1+dcidLen {
			t.Fatalf("the packet number starts at %d", h.PNOffset)
		}
	})
}

// FuzzParseParameters: the peer's transport parameters arrive through the
// TLS handshake, but their encoding is this package's to parse.
func FuzzParseParameters(f *testing.F) {
	f.Add(appendParameters(nil, DefaultParameters()))
	f.Add([]byte{0x01, 0x01, 0x40})
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		p, err := parseParameters(data)
		if err != nil {
			return
		}
		// What parsed must re-encode and re-parse to the same values: the
		// identifiers are stable and the encoding has one shape. Every field,
		// numeric and identifier alike — a drift in any of them is the bug
		// this fuzzer exists to catch.
		again, err := parseParameters(appendParameters(nil, p))
		if err != nil {
			t.Fatalf("a parsed parameter set did not re-encode: %v", err)
		}
		if again.MaxIdleTimeout != p.MaxIdleTimeout ||
			again.MaxUDPPayloadSize != p.MaxUDPPayloadSize ||
			again.InitialMaxData != p.InitialMaxData ||
			again.InitialMaxStreamDataUni != p.InitialMaxStreamDataUni ||
			again.InitialMaxStreamDataBidiLocal != p.InitialMaxStreamDataBidiLocal ||
			again.InitialMaxStreamDataBidiRemote != p.InitialMaxStreamDataBidiRemote ||
			again.InitialMaxStreamsBidi != p.InitialMaxStreamsBidi ||
			again.InitialMaxStreamsUni != p.InitialMaxStreamsUni ||
			again.ActiveConnIDLimit != p.ActiveConnIDLimit ||
			// AckDelayExponent and MaxAckDelayMS are parse-only on purpose —
			// read from a peer, never announced — so AppendParameters drops
			// them and the round trip cannot include them.
			// bytes.Equal, not DeepEqual: an identifier absent from the wire
			// may parse as nil or empty, and the two are the same statement.
			!bytes.Equal(again.originalDCID, p.originalDCID) ||
			!bytes.Equal(again.initialSourceCID, p.initialSourceCID) ||
			!bytes.Equal(again.retrySourceCID, p.retrySourceCID) ||
			!bytes.Equal(again.statelessResetToken, p.statelessResetToken) ||
			!samePreferredAddress(again.preferredAddress, p.preferredAddress) {
			t.Fatalf("re-encoding changed the values: %+v vs %+v", again, p)
		}
	})
}

// FuzzOpen: packet unprotection against arbitrary bytes. It must refuse —
// never panic, never read past the packet — for anything that is not a
// packet its keys sealed.
func FuzzOpen(f *testing.F) {
	secrets, err := initialSecrets([]byte{1, 2, 3, 4, 5, 6, 7, 8})
	if err != nil {
		f.Fatal(err)
	}
	opener, err := newPacketOpener(secrets.Client)
	if err != nil {
		f.Fatal(err)
	}
	sealer, _ := newPacketSealer(secrets.Client)
	header := []byte{0xc0, 0, 0, 0, 1, 0, 0}
	header = AppendVarint(header, 0)
	header = AppendVarint(header, 64)
	pnOffset := len(header)
	header = append(header, 0x00)
	sealed, _ := sealer.Seal(nil, header, []byte("a payload long enough to sample from"), 0, pnOffset, 1)
	f.Add(sealed, pnOffset, uint64(0))
	f.Add([]byte{}, 0, uint64(0))
	f.Add(make([]byte, 64), 3, uint64(1<<40))

	f.Fuzz(func(t *testing.T, packet []byte, pnOff int, largest uint64) {
		if pnOff < 0 || pnOff > len(packet) {
			return
		}
		payload, _, err := opener.Open(packet, pnOff, largest)
		if err != nil {
			return
		}
		if len(payload) > len(packet) {
			t.Fatal("the payload is longer than the packet it came from")
		}
	})
}

// fuzzSink is a packet connection that swallows writes: the fuzzed
// connection may answer what it receives, and the answers go nowhere.
type fuzzSink struct{ addr fakeFuzzAddr }

type fakeFuzzAddr string

func (a fakeFuzzAddr) Network() string { return "fuzz" }
func (a fakeFuzzAddr) String() string  { return string(a) }

func (s *fuzzSink) ReadFrom(p []byte) (int, net.Addr, error) {
	select {} // the fuzz target never reads; receive is fed directly
}
func (s *fuzzSink) WriteTo(p []byte, addr net.Addr) (int, error) { return len(p), nil }
func (s *fuzzSink) Close() error                                 { return nil }
func (s *fuzzSink) LocalAddr() net.Addr                          { return s.addr }
func (s *fuzzSink) SetDeadline(t time.Time) error                { return nil }
func (s *fuzzSink) SetReadDeadline(t time.Time) error            { return nil }
func (s *fuzzSink) SetWriteDeadline(t time.Time) error           { return nil }

// FuzzReceive: hostile datagrams against an established connection — the
// property under test is the failure model itself. Nothing an
// unauthenticated sender can put in a datagram may end the connection, panic
// it, or make it read out of bounds; everything must land in a discard
// counter or a deferred buffer.
func FuzzReceive(f *testing.F) {
	f.Add([]byte{0x40, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11})
	f.Add(append([]byte{0xc0, 0, 0, 0, 1, 8, 1, 2, 3, 4, 5, 6, 7, 8, 0}, make([]byte, 32)...))
	f.Add(append([]byte{0xe0, 0, 0, 0, 1, 8, 1, 2, 3, 4, 5, 6, 7, 8, 0}, make([]byte, 32)...))
	f.Add(make([]byte, 1200))
	f.Add([]byte{0x80, 0, 0, 0, 0, 4, 1, 2, 3, 4, 4, 5, 6, 7, 8, 0, 0, 0, 1})
	f.Add([]byte{0xf0, 0, 0, 0, 1, 0, 0}) // a Retry shape

	peer := fakeFuzzAddr("fuzz:peer")
	newTarget := func(tb testing.TB) *Conn {
		tb.Helper()
		c := newConn(&fuzzSink{addr: "fuzz:self"}, peer, []byte{1, 2, 3, 4, 5, 6, 7, 8}, []byte{9, 10, 11, 12, 13, 14, 15, 16}, false, DefaultParameters())
		if err := c.installInitial([]byte{1, 2, 3, 4, 5, 6, 7, 8}); err != nil {
			tb.Fatal(err)
		}
		// Application keys, so short-header packets exercise the full path.
		secrets, err := initialSecrets([]byte("fuzz secrets"))
		if err != nil {
			tb.Fatal(err)
		}
		opener, err := newPacketOpener(secrets.Client)
		if err != nil {
			tb.Fatal(err)
		}
		sealer, err := newPacketSealer(secrets.Server)
		if err != nil {
			tb.Fatal(err)
		}
		c.mu.Lock()
		c.spaces[spaceApplication].opener = opener
		c.spaces[spaceApplication].sealer = sealer
		c.hasPeerParams = true
		c.peerParams = DefaultParameters()
		c.mu.Unlock()
		return c
	}

	f.Fuzz(func(t *testing.T, datagram []byte) {
		// A fresh connection per iteration: with one shared across inputs, a
		// crash would depend on the corpus order that shaped its state, and
		// would not reproduce from the failing input alone.
		target := newTarget(t)
		defer target.Close()
		if err := target.receive(datagram, peer); err != nil {
			t.Fatalf("an unauthenticated datagram was treated as a protocol violation: %v", err)
		}
		if err := target.Err(); err != nil {
			t.Fatalf("an unauthenticated datagram failed the connection: %v", err)
		}
	})
}

// samePreferredAddress compares two parsed preferred_address values field by
// field; both nil is equal, one nil is not.
func samePreferredAddress(a, b *preferredAddress) bool {
	if a == nil || b == nil {
		return a == b
	}
	same := func(x, y *net.UDPAddr) bool {
		if x == nil || y == nil {
			return x == y
		}
		return x.String() == y.String()
	}
	return same(a.ipv4, b.ipv4) && same(a.ipv6, b.ipv6) &&
		bytes.Equal(a.cid, b.cid) && a.statelessResetToken == b.statelessResetToken
}
