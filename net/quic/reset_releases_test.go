package quic

import "testing"

// A RESET_STREAM gives the connection back the credit of every byte the
// application never consumed (abandon's neverConsumed), out-of-order bytes
// parked past a gap included. Those bytes stayed parked in the stream's
// reassembly until the application closed the stream, so a peer that parked
// a stream's whole window behind a gap and reset it could have its credit
// back and the memory still held, on as many streams as it may open — past
// InitialMaxData, which the package's documentation gives as the bound.
func TestAResetReleasesTheBytesParkedOutOfOrder(t *testing.T) {
	c := newConn(&fuzzSink{addr: "rst:self"}, fakeFuzzAddr("rst:peer"),
		[]byte("dcid0000"), []byte("scid0000"), false, DefaultConfig())
	defer c.Close()
	const streams, parked = 100, 200 << 10
	data := make([]byte, parked)
	held := 0
	for i := range streams {
		s := newStream(c, StreamID(4*i))
		// One byte missing at offset 0: everything after it is parked.
		if err := s.rasm.add(1, data); err != nil {
			t.Fatal(err)
		}
		if _, never, err := s.abandon(0x10c, 1+parked); err != nil || never != 1+parked {
			t.Fatalf("stream %d: abandon = %d never consumed, %v; want %d", i, never, err, 1+parked)
		}
		held += s.rasm.held + len(s.rasm.chunks)
	}
	if held != 0 {
		t.Errorf("%d bytes and chunks still parked after the resets, want none: the credit went back, the memory did not", held)
	}
}
