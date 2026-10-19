package quic

import (
	"net"
	"os"
	"runtime"
	"testing"
	"time"
)

// A Stream.Write of several packets leaves in one segmented write where the
// kernel offers segmentation (UDP_SEGMENT, UDP_SEND_MSG_SIZE), and the peer
// receives what a write per packet gave it: full datagrams of one size, then
// a shorter last, each one packet that opens under the connection's keys. A
// segment size off by a byte would cut packets across datagrams, and none of
// them would open. The CI runners offer it: there a refusal fails the test.
func TestStreamWriteLeavesInOneSegmentedWrite(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "windows" {
		t.Skip("UDP segmentation offload is driven on Linux and Windows only")
	}
	pcA, pcB := newLoopbackPair(t)
	c := writeBurst(t, pcA, pcB)
	c.mu.Lock()
	segmented, noGSO := c.segmented, c.noGSO
	c.mu.Unlock()
	if noGSO {
		if os.Getenv("GITHUB_ACTIONS") == "true" {
			t.Fatal("the runner's kernel refused segmentation")
		}
		t.Skip("this kernel refused segmentation; the write per datagram ran instead")
	}
	if segmented != 1 {
		t.Fatalf("%d segmented writes for one Stream.Write, want 1", segmented)
	}
}

// writeBurst writes one Stream.Write of several packets from a connection
// on pcA, and checks that pcB receives them as distinct datagrams, as a
// write per packet would have sent them. It returns the connection, for the
// caller to read how the burst left.
func writeBurst(t *testing.T, pcA, pcB net.PacketConn) *Conn {
	t.Helper()
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
	chunk := c.maxStreamChunk()
	c.mu.Unlock()

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
	return c
}

// BenchmarkStreamWriteBurst measures one Stream.Write of 16 KiB over a
// loopback socket: a flight of fourteen datagrams, which leave in one
// system call where the system offers segmentation and in one each where it
// does not. A goroutine drains the peer's socket, so the sender never meets
// a full receive buffer.
func BenchmarkStreamWriteBurst(b *testing.B) {
	pcA, pcB := newLoopbackPair(b)
	c, _ := newKeyedConnOn(b, pcA, pcB)
	go func() {
		buf := make([]byte, 65536)
		for {
			if _, _, err := pcB.ReadFrom(buf); err != nil {
				return
			}
		}
	}()
	c.mu.Lock()
	c.reno().cwnd = 1 << 30
	c.connSend.max = 1 << 50
	s := newStream(c, 1) // the peer's bidi stream: this end may send on it
	s.sendQ.max = 1 << 50
	s.writeOff = 1 << 20 // full packets of one size, as in writeBurst
	c.streams[1] = s
	c.mu.Unlock()

	payload := make([]byte, 16<<10)
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	for b.Loop() {
		if _, err := s.Write(payload); err != nil {
			b.Fatal(err)
		}
		// Keep the loop's state flat, as BenchmarkStreamWritePacket does.
		c.mu.Lock()
		c.spaces[spaceApplication].sent = sentTracker{}
		c.spaces[spaceApplication].sealer.sealed = 0
		c.reno().inFlight = 0
		c.mu.Unlock()
	}
	b.StopTimer()
	c.mu.Lock()
	segmented := c.segmented
	c.mu.Unlock()
	b.ReportMetric(float64(segmented)/float64(b.N), "segmented/op")
}
