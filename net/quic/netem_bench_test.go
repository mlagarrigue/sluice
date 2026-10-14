package quic_test

import (
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/internal/dgram"
	"github.com/mlagarrigue/sluice/net/quic"
)

// The loss/latency harness the congestion-control backlog items wait on:
// a middlebox between two dgram pairs that charges every datagram a one-way
// delay and, per profile, a random or bursty loss, a late delivery that
// reorders it behind its successors, and a drop-tail bottleneck queue that
// two flows may share. It is what lets a pacing, cwnd-growth or PTO change
// be measured as goodput on a path instead of asserted from the RFC's
// prose, and what CUBIC and BBR will have to prove themselves on.
//
// The emulation splits each direction in two stages so that reading never
// blocks behind the delay: the forwarder decides a packet's fate the moment
// it arrives (loss draw, queue-overflow drop, serialization time), and a
// deliverer sleeps out the computed arrival time before writing it on. The
// drop decision therefore sees the queue the bottleneck would actually have,
// not one inflated by whatever the delay goroutine has yet to sleep off.
type netemProfile struct {
	// owd is the one-way delay; the path's RTT is twice this.
	owd time.Duration
	// loss is the probability a datagram vanishes, drawn per datagram from
	// a fixed-seed generator so two runs of one benchmark walk the same
	// loss pattern (timing still moves, the draw sequence does not).
	loss float64
	// burstStart and burstEnd make the loss bursty instead of independent:
	// a two-state Gilbert model whose bad state loses every datagram.
	// burstStart is the per-datagram probability of entering the bad
	// state, burstEnd of leaving it, so a burst lasts 1/burstEnd datagrams
	// on average and the long-run loss rate is burstStart over the sum of
	// the two. Drawn from the same generator as loss. Zero burstStart
	// disables it.
	burstStart, burstEnd float64
	// reorder is the probability a datagram is delivered late — by an
	// extra delay drawn uniformly up to reorderDelay — so that it arrives
	// after datagrams sent after it. A delay, not a swap with the next
	// datagram: a swap stays under RFC 9002 §6.1.1's packet threshold of
	// three and measures nothing, while a millisecond inside a burst moves
	// a datagram past dozens of its successors. Zero disables it.
	reorder      float64
	reorderDelay time.Duration
	// rate, in bytes per second, serializes packets through a bottleneck;
	// zero means no bottleneck. queue is how long a packet may wait for
	// the bottleneck before it is dropped instead — depth expressed in
	// time, the way a router's buffer meets a flow.
	rate  int
	queue time.Duration
}

// timedPacket is one datagram and the instant the far end should see it.
type timedPacket struct {
	data     []byte
	arriveAt time.Time
}

// bottleneck is the serialization point of one direction: when its line
// frees up next. Flows that share a link share one of these, which is the
// whole of what makes them compete.
type bottleneck struct {
	mu        sync.Mutex
	lastTxEnd time.Time
}

// enqueue charges a packet of n bytes its serialization time behind
// whatever is already queued, returning when it leaves the line, or false
// when it would wait longer than the queue allows and is dropped instead.
func (b *bottleneck) enqueue(now time.Time, n int, prof netemProfile) (time.Time, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	txStart := now
	if b.lastTxEnd.After(txStart) {
		txStart = b.lastTxEnd
	}
	if txStart.Sub(now) > prof.queue {
		return time.Time{}, false // the bottleneck's buffer is full: drop-tail
	}
	txEnd := txStart.Add(time.Duration(n) * time.Second / time.Duration(prof.rate))
	b.lastTxEnd = txEnd
	return txEnd, true
}

// forward emulates one direction: src's peer traffic is read promptly,
// doomed packets are dropped at read time, survivors are handed to a
// deliverer that sleeps until each one's arrival instant. A reordered
// packet leaves that in-order queue for a timer of its own, which is what
// lets it overtake. Returns when src closes.
func forward(src, dst net.PacketConn, to net.Addr, prof netemProfile, rng *rand.Rand, bn *bottleneck) {
	ch := make(chan timedPacket, 4096)
	go func() {
		// On a closed pair the writes start failing; keep draining so the
		// forwarder never blocks on a full channel while shutting down.
		for pkt := range ch {
			if d := time.Until(pkt.arriveAt); d > 0 {
				time.Sleep(d)
			}
			_, _ = dst.WriteTo(pkt.data, to)
		}
	}()
	defer close(ch)

	bad := false
	buf := make([]byte, 2048)
	for {
		n, _, err := src.ReadFrom(buf)
		if err != nil {
			return
		}
		if prof.loss > 0 && rng.Float64() < prof.loss {
			continue
		}
		if prof.burstStart > 0 {
			if bad {
				bad = rng.Float64() >= prof.burstEnd
			} else {
				bad = rng.Float64() < prof.burstStart
			}
			if bad {
				continue
			}
		}
		now := time.Now()
		txEnd := now
		if prof.rate > 0 {
			var ok bool
			if txEnd, ok = bn.enqueue(now, n, prof); !ok {
				continue
			}
		}
		pkt := timedPacket{
			data:     append([]byte(nil), buf[:n]...),
			arriveAt: txEnd.Add(prof.owd),
		}
		if prof.reorder > 0 && rng.Float64() < prof.reorder {
			late := pkt.arriveAt.Add(time.Duration(rng.Float64() * float64(prof.reorderDelay)))
			time.AfterFunc(time.Until(late), func() { _, _ = dst.WriteTo(pkt.data, to) })
			continue
		}
		ch <- pkt
	}
}

// netemLink is one emulated link: a profile and the two bottlenecks, one a
// direction, that every path opened over it shares. Loss and reordering are
// drawn per path from its own seeds, so two flows on one link do not lose
// the same datagrams; the first path's seeds are the ones the single-flow
// benchmarks have always used, so their rows stay comparable.
type netemLink struct {
	prof     netemProfile
	up, down bottleneck
	paths    uint64
	// cc is the congestion controller both ends of a transfer on this
	// link are built with; the zero value is the package's default.
	cc quic.CongestionAlgorithm
}

// benchAlgorithm is the controller the path benchmarks run:
// SLUICE_QUIC_CC=cubic for CUBIC, bbr1 for BBR version 1, bbr for the IETF
// draft, anything else for the default, so one test binary measures both
// sides of a controller A/B when the rounds alternate the variable
// (docs/benchmarks.md).
var benchAlgorithm = func() quic.CongestionAlgorithm {
	switch os.Getenv("SLUICE_QUIC_CC") {
	case "cubic":
		return quic.CongestionCubic
	case "bbr1":
		return quic.CongestionBBRv1
	case "bbr":
		return quic.CongestionBBR
	}
	return quic.CongestionNewReno
}()

// path wires a client and a server packet connection through two
// forwarders on this link. dialTo is where the client addresses its
// datagrams — the middlebox end facing it. Closing either returned end
// tears the path down; the link outlives it.
func (l *netemLink) path() (client, server net.PacketConn, dialTo net.Addr, stop func()) {
	seed := 4*l.paths + 1
	l.paths++
	a1, a2 := dgram.Pair()                                                                     // client <-> middlebox
	b1, b2 := dgram.Pair()                                                                     // middlebox <-> server
	go forward(a2, b1, b2.LocalAddr(), l.prof, rand.New(rand.NewPCG(seed, seed+1)), &l.up)     //nolint:gosec // G404: a seeded loss pattern, reproducible on purpose
	go forward(b1, a2, a1.LocalAddr(), l.prof, rand.New(rand.NewPCG(seed+2, seed+3)), &l.down) //nolint:gosec // G404: same
	return a1, b2, a2.LocalAddr(), func() {
		_ = a1.Close()
		_ = b2.Close()
	}
}

// wideParameters opens flow control far beyond what any benchmark transfer
// uses, so the only regulator left on the path is congestion control — the
// thing these benchmarks exist to observe.
func wideParameters() quic.TransportParameters {
	p := quic.DefaultParameters()
	p.InitialMaxData = 1 << 30
	p.InitialMaxStreamDataBidiLocal = 1 << 28
	p.InitialMaxStreamDataBidiRemote = 1 << 28
	p.InitialMaxStreamDataUni = 1 << 28
	return p
}

// transfer is what one flow reports: how long it took and the sender's
// counters as they stood when the receiver had everything.
type transfer struct {
	elapsed time.Duration
	stats   quic.Stats
}

// transferOnce runs one connection over the link and moves total bytes
// client-to-server on a single stream, returning when the server has read
// them all. The handshake rides inside the measurement; at these transfer
// sizes it is two round trips against a second of data. It returns an
// error rather than failing the test because two flows run it from two
// goroutines, and only the test's own may call Fatal.
func transferOnce(tb testing.TB, link *netemLink, total int) (transfer, error) {
	tb.Helper()
	start := time.Now()
	clientPC, serverPC, dialTo, stop := link.path()
	defer stop()
	params := wideParameters()
	params.Congestion.Algorithm = link.cc

	type result struct {
		n   int64
		err error
	}
	done := make(chan result, 1)
	go func() {
		buf := make([]byte, 2048)
		if err := serverPC.SetReadDeadline(time.Now().Add(60 * time.Second)); err != nil {
			done <- result{err: err}
			return
		}
		n, peer, err := serverPC.ReadFrom(buf)
		if err != nil {
			done <- result{err: err}
			return
		}
		conn, err := quic.Accept(serverPC, peer, buf[:n], serverTLS(tb), params)
		if err != nil {
			done <- result{err: err}
			return
		}
		defer func() { _ = conn.Close() }()
		s, err := conn.AcceptStream(tb.Context())
		if err != nil {
			done <- result{err: err}
			return
		}
		got, err := io.Copy(io.Discard, s)
		if err != nil && !errors.Is(err, io.EOF) {
			done <- result{err: fmt.Errorf("server read: %w (conn: %v)", err, conn.Err())} //nolint:errorlint // conn.Err() is context and may be nil
			return
		}
		done <- result{n: got}
	}()

	conn, err := quic.Dial(clientPC, dialTo, clientTLS(), params)
	if err != nil {
		return transfer{}, fmt.Errorf("Dial: %w", err)
	}
	defer func() { _ = conn.Close() }()
	s, err := conn.OpenStream(tb.Context())
	if err != nil {
		return transfer{}, err
	}
	chunk := make([]byte, 64<<10)
	for sent := 0; sent < total; sent += len(chunk) {
		if _, err := s.Write(chunk); err != nil {
			return transfer{}, fmt.Errorf("after %d bytes: %w (conn: %v)", sent, err, conn.Err()) //nolint:errorlint // as above
		}
	}
	if err := s.CloseWrite(); err != nil {
		return transfer{}, err
	}

	select {
	case r := <-done:
		if r.err != nil {
			return transfer{}, fmt.Errorf("the server: %w", r.err)
		}
		if r.n != int64(total) {
			return transfer{}, fmt.Errorf("the server read %d of %d bytes", r.n, total)
		}
	case <-time.After(120 * time.Second):
		return transfer{}, errors.New("the transfer did not finish")
	}
	return transfer{elapsed: time.Since(start), stats: conn.Stats()}, nil
}

// benchTransfer moves total bytes on one flow and reports its goodput, and
// the sender's final smoothed RTT as a multiple of the smallest it sampled:
// on a deep buffer that ratio is the queue the controller let build, the
// figure BBR's "RTT within 1.5× min_rtt" criterion reads.
func benchTransfer(b *testing.B, prof netemProfile, total int) {
	b.Helper()
	b.SetBytes(int64(total))
	for b.Loop() {
		t, err := transferOnce(b, &netemLink{prof: prof, cc: benchAlgorithm}, total)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(float64(total)/1e6/t.elapsed.Seconds(), "MB/s")
		b.ReportMetric(float64(t.stats.SmoothedRTT)/float64(t.stats.MinRTT), "rtt_x")
	}
}

// benchTwoFlows moves total bytes on each of two flows that share the
// link's bottleneck, started together, and reports the aggregate goodput
// (what the link delivered) and the first flow's share of the bytes per
// second — 0.5 when the two controllers split the line, the figure a
// fairness criterion reads. One transfer's wall clock is the slower flow's.
func benchTwoFlows(b *testing.B, prof netemProfile, total int) {
	b.Helper()
	b.SetBytes(int64(2 * total))
	for b.Loop() {
		link := &netemLink{prof: prof, cc: benchAlgorithm}
		var (
			wg   sync.WaitGroup
			res  [2]transfer
			errs [2]error
		)
		for i := range res {
			wg.Go(func() { res[i], errs[i] = transferOnce(b, link, total) })
		}
		wg.Wait()
		if err := errors.Join(errs[:]...); err != nil {
			b.Fatal(err)
		}
		rate := func(i int) float64 { return float64(total) / 1e6 / res[i].elapsed.Seconds() }
		b.ReportMetric(float64(2*total)/1e6/max(res[0].elapsed, res[1].elapsed).Seconds(), "MB/s")
		b.ReportMetric(rate(0)/(rate(0)+rate(1)), "share")
	}
}

// BenchmarkTransferClean is the ramp benchmark: 20 ms RTT, no loss, no
// bottleneck. Time here is dominated by how fast the congestion window
// grows (slow start, then the per-ACK increase RFC 9002 B.5 aggregates),
// which is exactly what the per-packet cwnd-growth backlog item changes.
func BenchmarkTransferClean(b *testing.B) {
	benchTransfer(b, netemProfile{owd: 10 * time.Millisecond}, 8<<20)
}

// BenchmarkTransferLoss1pct is the recovery benchmark: the same path losing
// one datagram in a hundred, both directions. Loss detection, RTO/PTO
// behaviour and how growth resumes after recovery set the figure.
func BenchmarkTransferLoss1pct(b *testing.B) {
	benchTransfer(b, netemProfile{owd: 10 * time.Millisecond, loss: 0.01}, 4<<20)
}

// BenchmarkTransferBurstLoss is the same one percent of loss delivered in
// bursts of four datagrams on average (a bad state entered one datagram in
// 400 and left one in four) with clean stretches between: a wireless
// fade, not a random drop. A controller that halves once per loss event
// rather than once per lost packet (RFC 9002 §7.3.2) pays a quarter of
// what Loss1pct costs it; a burst that spans a whole flight is a probe
// timeout instead.
func BenchmarkTransferBurstLoss(b *testing.B) {
	benchTransfer(b, netemProfile{
		owd:        10 * time.Millisecond,
		burstStart: 0.0025,
		burstEnd:   0.25,
	}, 4<<20)
}

// BenchmarkTransferReorder is the spurious-loss benchmark: no loss at all,
// but two datagrams in a hundred arrive up to two milliseconds late, past
// the dozens sent after them inside the same burst. Every one of them
// trips RFC 9002 §6.1.1's packet threshold, is declared lost, retransmitted
// and charged as a congestion event the path never had. What separates
// controllers here is how they tolerate reordering, not how they recover.
func BenchmarkTransferReorder(b *testing.B) {
	benchTransfer(b, netemProfile{
		owd:          10 * time.Millisecond,
		reorder:      0.02,
		reorderDelay: 2 * time.Millisecond,
	}, 4<<20)
}

// BenchmarkTransferBottleneck is the pacing benchmark: a 50 Mbit/s
// bottleneck whose buffer holds five milliseconds of line rate, behind a
// 20 ms RTT. A sender whose bursts are bounded only by cwnd overruns a
// buffer this shallow and pays in retransmissions; RFC 9002 §7.7's burst
// bound (and pacing proper) exists for this path shape.
func BenchmarkTransferBottleneck(b *testing.B) {
	benchTransfer(b, netemProfile{
		owd:   10 * time.Millisecond,
		rate:  50_000_000 / 8,
		queue: 5 * time.Millisecond,
	}, 4<<20)
}

// BenchmarkTransferDeepBuffer is the same bottleneck behind ten round trips
// of buffer (200 ms at line rate): the bufferbloat shape. A loss-driven
// controller fills it before it sees a loss, so goodput is the line rate
// and the cost shows in rtt_x — the smoothed RTT ending at many times the
// path's own — where a model-based controller is meant to hold the queue
// short at the same goodput.
func BenchmarkTransferDeepBuffer(b *testing.B) {
	benchTransfer(b, netemProfile{
		owd:   10 * time.Millisecond,
		rate:  50_000_000 / 8,
		queue: 200 * time.Millisecond,
	}, 4<<20)
}

// BenchmarkTransferTwoFlows is the fairness benchmark: two flows started
// together on the 50 Mbit/s bottleneck behind one round trip of buffer
// (20 ms), each moving 4 MiB. share is the first flow's part of the
// delivered rate; two copies of one controller should split the line, and
// a new controller is measured by how far from a half it pushes the old
// one.
func BenchmarkTransferTwoFlows(b *testing.B) {
	benchTwoFlows(b, netemProfile{
		owd:   10 * time.Millisecond,
		rate:  50_000_000 / 8,
		queue: 20 * time.Millisecond,
	}, 4<<20)
}

// BenchmarkTransferRTT40 and RTT80 are the long-haul benchmarks: a
// 100 Mbit/s bottleneck behind 20 ms of buffer, at 40 then 80 ms of RTT.
// The bandwidth-delay product grows with the RTT (0.5 then 1 MB) while the
// buffer does not, so every overflow costs a window the controller has to
// grow back one segment per round trip under Reno — the regime CUBIC's
// window function (RFC 9438) was designed for, and where it must show its
// gain over Reno.
func BenchmarkTransferRTT40(b *testing.B) {
	benchTransfer(b, netemProfile{
		owd:   20 * time.Millisecond,
		rate:  100_000_000 / 8,
		queue: 20 * time.Millisecond,
	}, 8<<20)
}

func BenchmarkTransferRTT80(b *testing.B) {
	benchTransfer(b, netemProfile{
		owd:   40 * time.Millisecond,
		rate:  100_000_000 / 8,
		queue: 20 * time.Millisecond,
	}, 8<<20)
}

// BenchmarkTransferRTT80Shallow is the congestion-avoidance benchmark: the
// same 80 ms, 100 Mbit/s line behind 5 ms of buffer, moving 32 MiB. RTT80's
// 8 MiB is over before avoidance matters — the handshake and a slow start
// doubling from 12 kB floor it at about 1.2 s whatever the controller, and
// its 20 ms buffer leaves a halved window at 0.78 of the path (measured
// 2026-10-14: CUBIC 1.07× Reno on a 64 MiB transfer there). Here a loss
// leaves Reno at half of the path, a datagram per round trip from full, and
// the transfer is long enough to pay for it: this is where a controller's
// growth curve is read. Not gated: the figure moves with where the slow
// start's exit loss lands.
func BenchmarkTransferRTT80Shallow(b *testing.B) {
	benchTransfer(b, netemProfile{
		owd:   40 * time.Millisecond,
		rate:  100_000_000 / 8,
		queue: 5 * time.Millisecond,
	}, 32<<20)
}
