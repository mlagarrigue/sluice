package quic

import (
	"net"
	"os"
	"runtime"
	"sync"
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

// gatedConn records every datagram written and swallows it. A gate, when
// set, holds the first write until the test opens it, after saying the
// write began; nothing then comes back to the connection, whose loops
// these tests do not run.
type gatedConn struct {
	net.PacketConn
	mu      sync.Mutex
	written [][]byte
	entered chan struct{}
	gate    chan struct{}
}

func (g *gatedConn) WriteTo(b []byte, _ net.Addr) (int, error) {
	g.mu.Lock()
	gate, entered := g.gate, g.entered
	g.gate = nil
	g.written = append(g.written, append([]byte(nil), b...))
	g.mu.Unlock()
	if gate != nil {
		close(entered)
		<-gate
	}
	return len(b), nil
}

// newGatedStreamConn is a keyed connection on a gatedConn, with credit for
// everything a test writes, and an opener for the packets it seals.
func newGatedStreamConn(t *testing.T) (*Conn, *gatedConn, *packetOpener) {
	t.Helper()
	pcA, pcB := newLoopbackPair(t)
	g := &gatedConn{PacketConn: pcA}
	c, _ := newKeyedConnOn(t, g, pcB)
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
	c.mu.Unlock()
	return c, g, opener
}

func addSendStream(c *Conn, id StreamID) *Stream {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := newStream(c, id)
	s.sendQ.max = 1 << 50
	c.streams[id] = s
	return s
}

// A Stream.Write does not hold the connection's lock across the system
// call that sends its packets: while the socket holds the write, the lock
// is free for the read loop and the other writers. Held, every
// acknowledgement waits behind the write — on a bulk transfer the call is
// most of what a write held the lock for.
func TestStreamWriteReleasesTheLockAcrossTheSend(t *testing.T) {
	c, g, _ := newGatedStreamConn(t)
	s := addSendStream(c, 1)
	g.mu.Lock()
	g.gate, g.entered = make(chan struct{}), make(chan struct{})
	gate, entered := g.gate, g.entered
	g.mu.Unlock()

	wrote := make(chan error, 1)
	go func() {
		_, err := s.Write(make([]byte, 4096))
		wrote <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the write never reached the socket")
	}
	locked := make(chan struct{})
	go func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		close(locked)
	}()
	select {
	case <-locked:
	case <-time.After(2 * time.Second):
		close(gate)
		<-wrote
		t.Fatal("the connection's lock is held while the socket writes")
	}
	close(gate)
	if err := <-wrote; err != nil {
		t.Fatal(err)
	}
}

// With the lock released across the send, the datagrams still leave in the
// order their packet numbers were taken, under two streams writing at once
// and packets sent outside any write (a PING, as the read loop's
// acknowledgements are): a number ahead of lower ones reads to the peer as
// a gap, which it acknowledges at once (RFC 9000 §13.2.1), and that
// acknowledgement as a loss here. Every packet sealed is written, once.
func TestConcurrentWritesLeaveInPacketNumberOrder(t *testing.T) {
	c, g, opener := newGatedStreamConn(t)
	streams := []*Stream{addSendStream(c, 1), addSendStream(c, 5)}
	const writes, size = 32, 16 << 10
	var wg sync.WaitGroup
	for _, s := range streams {
		wg.Go(func() {
			for range writes {
				if _, err := s.Write(make([]byte, size)); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	stop := make(chan struct{})
	pinged := make(chan struct{})
	go func() {
		defer close(pinged)
		for {
			select {
			case <-stop:
				return
			default:
			}
			c.mu.Lock()
			err := c.sendPacketLocked(spaceApplication, []byte{framePing}, sendOpts{})
			c.mu.Unlock()
			if err != nil {
				t.Error(err)
				return
			}
			runtime.Gosched()
		}
	}()
	wg.Wait()
	close(stop)
	<-pinged

	c.mu.Lock()
	queued, sending, sent := len(c.sendQ), c.sending, c.spaces[spaceApplication].nextPN
	c.mu.Unlock()
	if queued != 0 || sending {
		t.Fatalf("%d writes still queued (sending %v) once every writer returned", queued, sending)
	}
	g.mu.Lock()
	written := g.written
	g.mu.Unlock()
	pnOffset := 1 + len(c.dcid)
	var last uint64
	for i, d := range written {
		_, pn, err := opener.Open(d, pnOffset, last)
		if err != nil {
			t.Fatalf("datagram %d does not open: %v", i, err)
		}
		if i > 0 && pn != last+1 {
			t.Fatalf("datagram %d carries packet %d after packet %d", i, pn, last)
		}
		last = pn
	}
	if uint64(len(written)) != sent {
		t.Fatalf("%d datagrams written for %d packets sealed", len(written), sent)
	}
}
