package quic

import (
	"errors"
	"testing"
	"time"
)

// RFC 9000 §6.2: a Version Negotiation packet that echoes this connection's
// identifiers and lists no version 1 ends the dial, and Err names the
// cause, ErrVersionNegotiation.
func TestVersionNegotiationIsReportedByItsSentinel(t *testing.T) {
	pcA, pcB := newLoopbackPair(t)
	c := newConn(pcA, pcB.LocalAddr(), []byte("dcid0000"), []byte("scid0000"), true, DefaultParameters())
	defer c.Close()
	c.mu.Lock()
	c.initialDCID = []byte("dcid0000") // what Dial records for the echo check
	c.mu.Unlock()

	// Long header, version 0, the client's identifiers swapped the way the
	// server echoes them, then one version this package does not speak.
	vn := []byte{0x80, 0, 0, 0, 0}
	vn = append(vn, byte(len("scid0000")))
	vn = append(vn, "scid0000"...)
	vn = append(vn, byte(len("dcid0000")))
	vn = append(vn, "dcid0000"...)
	vn = append(vn, 0, 0, 0, 2)
	_ = c.receive(vn, pcB.LocalAddr())

	select {
	case <-c.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("the connection did not close on a Version Negotiation packet")
	}
	if err := c.Err(); !errors.Is(err, ErrVersionNegotiation) {
		t.Fatalf("Err = %v, want ErrVersionNegotiation", err)
	}
}

// RFC 9000 §6.2: a Version Negotiation packet is discarded once any other
// packet has been processed — a Retry included. The identifiers a Version
// Negotiation echoes are the first flight's, which every observer of the
// path saw, so a replay of them after the Retry must not end the handshake.
func TestVersionNegotiationAfterARetryIsDiscarded(t *testing.T) {
	pcA, pcB := newLoopbackPair(t)
	c := newConn(pcA, pcB.LocalAddr(), []byte("dcid0000"), []byte("scid0000"), true, DefaultParameters())
	defer c.Close()
	c.mu.Lock()
	c.initialDCID = []byte("dcid0000")
	c.mu.Unlock()

	retry := appendRetryPacket(nil, []byte("scid0000"), []byte("server-scid-xyz"), []byte("dcid0000"), []byte("a-token"))
	_ = c.receive(retry, pcB.LocalAddr())
	c.mu.Lock()
	retried := c.retried
	c.mu.Unlock()
	if !retried {
		t.Fatal("the Retry was not honoured; the test needs one processed packet before the Version Negotiation")
	}

	vn := []byte{0x80, 0, 0, 0, 0}
	vn = append(vn, byte(len("scid0000")))
	vn = append(vn, "scid0000"...)
	vn = append(vn, byte(len("dcid0000")))
	vn = append(vn, "dcid0000"...)
	vn = append(vn, 0, 0, 0, 2)
	_ = c.receive(vn, pcB.LocalAddr())

	select {
	case <-c.Done():
		t.Fatalf("the connection closed on a Version Negotiation received after a Retry: %v", c.Err())
	case <-time.After(200 * time.Millisecond):
	}
}

// RFC 8999 §6: "An endpoint MUST ignore a packet that contains no Supported
// Version fields or contains a truncated Supported Version value." Both
// used to be read up to the truncation and judged — no version 1 found —
// and ended the handshake. The identifiers echo the first flight's, so the
// control shows the same packet with a well-formed list is still acted on.
func TestVersionNegotiationEmptyOrTruncatedIsIgnored(t *testing.T) {
	vnWith := func(tail ...byte) []byte {
		vn := []byte{0x80, 0, 0, 0, 0}
		vn = append(vn, byte(len("scid0000")))
		vn = append(vn, "scid0000"...)
		vn = append(vn, byte(len("dcid0000")))
		vn = append(vn, "dcid0000"...)
		return append(vn, tail...)
	}
	newClient := func() *Conn {
		pcA, pcB := newLoopbackPair(t)
		c := newConn(pcA, pcB.LocalAddr(), []byte("dcid0000"), []byte("scid0000"), true, DefaultParameters())
		t.Cleanup(func() { _ = c.Close() })
		c.mu.Lock()
		c.initialDCID = []byte("dcid0000")
		c.mu.Unlock()
		return c
	}
	for name, vn := range map[string][]byte{
		"no version":        vnWith(),
		"truncated version": vnWith(0, 0, 0, 2, 0, 0),
	} {
		c := newClient()
		_ = c.receive(vn, c.peer)
		if err := c.Err(); err != nil {
			t.Errorf("%s: the connection closed with %v; RFC 8999 §6 says ignore the packet", name, err)
		}
		if n := discardedCount(c); n != 1 {
			t.Errorf("%s: discarded = %d, want 1", name, n)
		}
	}
	// Control: the well-formed list without version 1 still ends the dial.
	c := newClient()
	_ = c.receive(vnWith(0, 0, 0, 2), c.peer)
	if err := c.Err(); !errors.Is(err, ErrVersionNegotiation) {
		t.Errorf("a well-formed Version Negotiation without version 1 left the connection with %v, want ErrVersionNegotiation", err)
	}
}

// RFC 9000 §17.2.5: a Retry answers the client's Initial, so its
// Destination Connection ID is the Source Connection ID that Initial
// carried. One addressed to another identifier answers some other
// connection, or none: it is discarded, and the Retry that does name the
// client is still honoured after it.
func TestRetryAddressedToAnotherSCIDIsDiscarded(t *testing.T) {
	pcA, pcB := newLoopbackPair(t)
	c := newConn(pcA, pcB.LocalAddr(), []byte("dcid0000"), []byte("scid0000"), true, DefaultParameters())
	defer c.Close()
	c.mu.Lock()
	c.initialDCID = []byte("dcid0000")
	c.mu.Unlock()
	retried := func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.retried
	}

	stray := appendRetryPacket(nil, []byte("other000"), []byte("server-scid-xyz"), []byte("dcid0000"), []byte("a-token"))
	_ = c.receive(stray, pcB.LocalAddr())
	if retried() {
		t.Fatal("a Retry addressed to another connection ID was honoured")
	}
	retry := appendRetryPacket(nil, []byte("scid0000"), []byte("server-scid-xyz"), []byte("dcid0000"), []byte("a-token"))
	_ = c.receive(retry, pcB.LocalAddr())
	if !retried() {
		t.Fatal("the Retry addressed to this client was not honoured after the stray one")
	}
}
