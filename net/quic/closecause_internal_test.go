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
