package quic

import (
	"runtime"
	"testing"
	"time"
)

// A Stream.Write of several packets leaves in one segmented write where the
// kernel offers UDP_SEGMENT, and the peer receives what a write per packet
// gave it: full datagrams of one size, then a shorter last, each one packet
// that opens under the connection's keys. A segment size off by a byte
// would cut packets across datagrams, and none of them would open.
func TestStreamWriteLeavesInOneSegmentedWrite(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("UDP segmentation offload is driven on Linux only")
	}
	pcA, pcB := newLoopbackPair(t)
	c, _ := newKeyedConnOn(t, pcA, pcB)
	sec, err := initialSecrets([]byte("app-secrets"))
	if err != nil {
		t.Fatal(err)
	}
	opener, err := newOpenerSuite(sec.Server, aes128Suite)
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	c.reno().cwnd = 1 << 30
	c.connSend.max = 1 << 50
	s := newStream(c, 1) // the peer's bidi stream: this end may send on it
	s.sendQ.max = 1 << 50
	// Past 2^14 every offset takes a four-byte varint, so the full packets
	// are one size; offset 0's one-byte form would make the first a byte
	// shorter, and it would leave on its own.
	s.writeOff = 1 << 20
	c.streams[1] = s
	c.mu.Unlock()

	const body = 4096
	if _, err := s.Write(make([]byte, body)); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	segmented, noGSO, chunk := c.segmented, c.noGSO, c.maxStreamChunk()
	c.mu.Unlock()
	if noGSO {
		t.Skip("this kernel refused UDP_SEGMENT; the write per datagram ran instead")
	}
	if segmented != 1 {
		t.Fatalf("%d segmented writes for one Stream.Write, want 1", segmented)
	}

	want := (body + chunk - 1) / chunk
	buf := make([]byte, 65536)
	var sizes []int
	for i := range want {
		_ = pcB.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, _, err := pcB.ReadFrom(buf)
		if err != nil {
			t.Fatalf("datagram %d of %d: %v", i+1, want, err)
		}
		sizes = append(sizes, n)
		pnOffset := 1 + len(c.dcid)
		if _, _, err := opener.Open(append([]byte(nil), buf[:n]...), pnOffset, 0); err != nil {
			t.Fatalf("datagram %d of %d (%d bytes) does not open: %v", i+1, want, n, err)
		}
	}
	for i, n := range sizes[:len(sizes)-1] {
		if n != sizes[0] {
			t.Fatalf("datagram %d is %d bytes, the first %d: sizes %v", i+1, n, sizes[0], sizes)
		}
	}
	if last := sizes[len(sizes)-1]; last >= sizes[0] {
		t.Fatalf("the last datagram is %d bytes, not shorter than the others: sizes %v", last, sizes)
	}
}
