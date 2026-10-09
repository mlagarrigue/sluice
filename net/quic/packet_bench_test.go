package quic

import (
	"fmt"
	"net"
	"sync"
	"testing"
)

// The packet-path harness the backlog's quic allocation inventory waits on:
// one benchmark per direction, each counting ns and allocations per packet
// with the socket stubbed out (fuzzSink), so the inventory's candidates —
// payload copies on send, ParseFrames and Open copies on receive, the lock
// takes of settleBatchConsumption — can be A/B-measured where they run.

// newBenchConn is a server-role connection with application keys installed
// and every limit opened wide, so what the loop measures is the packet path
// and not a quota refusing to play.
func newBenchConn(tb testing.TB) (*Conn, *packetSealer) {
	tb.Helper()
	peer := fakeFuzzAddr("bench:peer")
	params := DefaultParameters()
	params.InitialMaxData = 1 << 50
	params.InitialMaxStreamDataBidiRemote = 1 << 50
	params.InitialMaxStreamDataBidiLocal = 1 << 50
	params.InitialMaxStreamsBidi = 1 << 20
	c := newConn(&fuzzSink{addr: "bench:self"}, peer, []byte{1, 2, 3, 4, 5, 6, 7, 8}, []byte{9, 10, 11, 12, 13, 14, 15, 16}, false, params)
	secrets, err := initialSecrets([]byte("bench secrets"))
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
	peerSealer, err := newPacketSealer(secrets.Client)
	if err != nil {
		tb.Fatal(err)
	}
	c.mu.Lock()
	c.spaces[spaceApplication].opener = opener
	c.spaces[spaceApplication].sealer = sealer
	c.hasPeerParams = true
	c.peerParams = params
	c.handshakeConfirmed = true
	c.cc.cwnd = 1 << 30
	c.connSend.max = 1 << 50
	c.mu.Unlock()
	return c, peerSealer
}

// BenchmarkStreamWritePacket measures one Stream.Write of a 512-byte chunk:
// frame build, seal, the stubbed socket write, and the recovery bookkeeping
// — the send-side packet path of the allocation inventory (frame build copy,
// Seal(nil,…) copy, the retransmission copy).
func BenchmarkStreamWritePacket(b *testing.B) {
	c, _ := newBenchConn(b)
	defer c.Close()

	c.mu.Lock()
	s := newStream(c, 1) // the peer's bidi stream: this end may send on it
	s.sendQ.max = 1 << 50
	c.streams[1] = s
	c.mu.Unlock()

	payload := make([]byte, 512)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := s.Write(payload); err != nil {
			b.Fatal(err)
		}
		// Keep the loop's state flat: the recovery log and the in-flight
		// count grow per packet, and eight million sealed packets would
		// trip the AEAD limit mid-benchmark.
		c.mu.Lock()
		c.spaces[spaceApplication].sent = sentTracker{}
		c.spaces[spaceApplication].sealer.sealed = 0
		c.cc.inFlight = 0
		c.mu.Unlock()
	}
}

// benchSealStreamPacket seals one short-header packet carrying four STREAM
// frames for four streams at the given offset — the multiplexed shape one
// datagram of a busy connection carries.
func benchSealStreamPacket(sealer *packetSealer, dcid []byte, pn uint64, offset uint64, chunk []byte) []byte {
	var payload []byte
	for i := range uint64(4) {
		id := 4 * i // client-initiated bidi: 0, 4, 8, 12 — the peer’s streams, from a server’s seat
		payload = AppendVarint(payload, FrameStream|streamOFF|streamLEN)
		payload = AppendVarint(payload, id)
		payload = AppendVarint(payload, offset)
		payload = AppendVarint(payload, uint64(len(chunk)))
		payload = append(payload, chunk...)
	}
	const pnLen = 4
	header := []byte{0x40 | byte(pnLen-1)}
	header = append(header, dcid...)
	pnOffset := len(header)
	for i := pnLen - 1; i >= 0; i-- {
		header = append(header, byte(pn>>(8*uint(i))))
	}
	pkt, err := sealer.Seal(nil, header, payload, pn, pnOffset, pnLen)
	if err != nil {
		panic(err)
	}
	return pkt
}

// BenchmarkConnReceivePacket measures one datagram through the batch-mode
// receive path: header parse, Open, ParseFrames, delivery to the batch
// callback and settleBatchConsumption — the receive side of the allocation
// inventory, and nothing else. The peer's seal is not this connection's
// work: packets are sealed ahead in blocks with the timer stopped (a packet
// must be fresh — packet numbers do not repeat — so they cannot be sealed
// once for the whole run), and only receive is timed. Its allocations are
// therefore receive's own; BenchmarkConnReceivePacketWithSeal keeps the
// combined figure the earlier inventory was taken against.
func BenchmarkConnReceivePacket(b *testing.B) { benchConnReceive(b, false) }

// BenchmarkConnReceivePacketWithSeal is BenchmarkConnReceivePacket with the
// peer's seal inside the timed loop, as the benchmark measured before the
// seal was moved out: the difference between the two is the seal.
func BenchmarkConnReceivePacketWithSeal(b *testing.B) { benchConnReceive(b, true) }

func benchConnReceive(b *testing.B, timeSeal bool) {
	b.Helper()
	c, peerSealer := newBenchConn(b)
	defer c.Close()
	delivered := 0
	c.OnStreamFrames(func(frames []Frame) error {
		for i := range frames {
			if frames[i].IsStream() {
				delivered++
			}
		}
		return nil
	})

	from := fakeFuzzAddr("bench:peer")
	dcid := []byte{9, 10, 11, 12, 13, 14, 15, 16}
	chunk := make([]byte, 256)
	var pn, offset uint64
	seal := func() []byte {
		pkt := benchSealStreamPacket(peerSealer, dcid, pn, offset, chunk)
		pn++
		offset += uint64(len(chunk))
		return pkt
	}
	const block = 4096
	ready := make([][]byte, block)
	next := block // index of the next sealed packet; block means "seal more"
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		var pkt []byte
		if timeSeal {
			pkt = seal()
		} else {
			if next == block {
				b.StopTimer()
				for j := range ready {
					ready[j] = seal()
				}
				next = 0
				b.StartTimer()
			}
			pkt = ready[next]
			next++
		}
		if err := c.receive(pkt, from); err != nil {
			b.Fatal(err)
		}
		if i&0x3fff == 0x3fff {
			// Keep the seal counter clear of the AEAD limit on long runs.
			c.mu.Lock()
			if sp := c.spaces[spaceApplication].sealer; sp != nil {
				sp.sealed = 0
			}
			c.mu.Unlock()
		}
	}
	b.StopTimer()
	if delivered == 0 {
		b.Fatal("no stream frames reached the batch callback")
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(delivered), "ns/frame")
}

var _ net.Addr = fakeFuzzAddr("")

// BenchmarkCreditGrantHerd measures what one stream's MAX_STREAM_DATA costs
// as a function of how many OTHER writers are parked on the connection's
// one sync.Cond: every grant broadcasts, every parked writer wakes, re-takes
// mu, finds nothing for it and sleeps again. The backlog's thundering-herd
// suggestion stands or falls on how this number scales with the herd.
func BenchmarkCreditGrantHerd(b *testing.B) {
	for _, parked := range []int{0, 64, 512} {
		b.Run(fmt.Sprintf("parked=%d", parked), func(b *testing.B) {
			c, _ := newBenchConn(b)
			defer c.Close()

			// The herd: writers parked on streams whose quota is spent and
			// never granted. They wake per broadcast and re-sleep.
			var wg sync.WaitGroup
			for i := range parked {
				id := uint64(4*(i+1) + 1)
				c.mu.Lock()
				s := newStream(c, id)
				c.streams[id] = s
				c.mu.Unlock()
				wg.Go(func() {
					_, _ = s.Write([]byte("x")) // parks: no credit ever comes
				})
			}
			// The working stream: granted exactly one chunk per iteration.
			c.mu.Lock()
			w := newStream(c, 1)
			c.streams[1] = w
			c.mu.Unlock()

			payload := make([]byte, 64)
			var granted uint64
			b.ReportAllocs()
			for b.Loop() {
				granted += uint64(len(payload))
				c.mu.Lock()
				w.sendQ.grant(granted)
				c.cond.Broadcast()
				c.spaces[spaceApplication].sent = sentTracker{}
				c.spaces[spaceApplication].sealer.sealed = 0
				c.cc.inFlight = 0
				c.mu.Unlock()
				if _, err := w.Write(payload); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			_ = c.Close() // unparks the herd
			wg.Wait()
		})
	}
}
