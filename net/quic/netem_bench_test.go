package quic_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/internal/dgram"
	"github.com/mlagarrigue/sluice/net/quic"
	"github.com/mlagarrigue/sluice/net/quic/udp"
)

// The loss/latency harness congestion control is measured on:
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
	// reorderDepth, when non-zero, makes a reordered datagram wait for the
	// next reorderDepth datagrams instead and arrive heldGap behind them —
	// overtaken by a count, not a delay, so the depth does not move with
	// the sending rate; reorderDelay then bounds the wait, for a datagram
	// that ends a flight or whose successors leave spaced out: only those
	// that arrive within the bound count. One datagram waits at a time. Below the packet
	// threshold this is the reordering §6.1.1 exists to tolerate: what it
	// measures is that it costs nothing.
	reorderDepth int
	// rate, in bytes per second, serializes packets through a bottleneck;
	// zero means no bottleneck. queue is how long a packet may wait for
	// the bottleneck before it is dropped instead — depth expressed in
	// time, the way a router's buffer meets a flow.
	rate  int
	queue time.Duration
	// mark is the queueing delay beyond which a datagram that arrived
	// ECT leaves marked CE instead of waiting unmarked — an AQM's step
	// marking at a threshold (RFC 3168 §5), before the drop at queue.
	// Zero never marks. A marking profile runs its path over UDP sockets
	// on the loopback interface rather than in-memory pairs, the only
	// transport here that carries the ECN field (netemLink.path).
	mark time.Duration
	// mtu, when non-zero, is the path's IP MTU: a datagram whose UDP
	// payload, with the 48 bytes of IPv6 and UDP headers, exceeds it
	// vanishes, the way a router without ICMP drops it — the path a
	// DPLPMTUD search (RFC 8899) has to find. Zero carries any size.
	mtu int
}

// timedPacket is one datagram, the ECN field it leaves with (zero on a
// transport without one) and the instant the far end should see it.
type timedPacket struct {
	data     []byte
	ecn      uint8
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
// whatever is already queued, returning when it leaves the line and how
// long it waited for it, or false when it would wait longer than the queue
// allows and is dropped instead.
func (b *bottleneck) enqueue(now time.Time, n int, prof netemProfile) (txEnd time.Time, wait time.Duration, ok bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	txStart := now
	if b.lastTxEnd.After(txStart) {
		txStart = b.lastTxEnd
	}
	wait = txStart.Sub(now)
	if wait > prof.queue {
		return time.Time{}, 0, false // the bottleneck's buffer is full: drop-tail
	}
	txEnd = txStart.Add(time.Duration(n) * time.Second / time.Duration(prof.rate))
	b.lastTxEnd = txEnd
	return txEnd, wait, true
}

// readMarked reads one datagram and, on a UDP socket, the ECN field it
// arrived with (zero elsewhere, and on a socket udp.EnableECN was not
// applied to).
func readMarked(src net.PacketConn, buf, oob []byte) (n int, ecn uint8, err error) {
	if uc, ok := src.(*net.UDPConn); ok {
		n, oobn, _, _, err := uc.ReadMsgUDP(buf, oob)
		if err != nil {
			return 0, 0, err
		}
		ecn, _ = udp.PacketECN(oob[:oobn])
		return n, ecn, nil
	}
	n, _, err = src.ReadFrom(buf)
	return n, 0, err
}

// writeMarked writes one datagram with its ECN field set, where the socket
// and the address can carry one; the mark is dropped elsewhere. oob is
// scratch, returned grown for reuse.
func writeMarked(dst net.PacketConn, data []byte, to net.Addr, ecn uint8, oob []byte) []byte {
	uc, isUDP := dst.(*net.UDPConn)
	ua, isUDPAddr := to.(*net.UDPAddr)
	if !isUDP || !isUDPAddr || ecn == 0 {
		_, _ = dst.WriteTo(data, to)
		return oob
	}
	ap := ua.AddrPort()
	oob = udp.AppendECN(oob[:0], ecn, ap.Addr())
	_, _, _ = uc.WriteMsgUDPAddrPort(data, oob, ap)
	return oob
}

// forward emulates one direction: src's peer traffic is read promptly,
// doomed packets are dropped at read time, survivors are handed to a
// deliverer that sleeps until each one's arrival instant. A reordered
// packet leaves that in-order queue for a timer of its own, which is what
// lets it overtake. Returns when src closes, once the deliverer has too:
// what is still in flight then is dropped rather than slept out, so a
// transfer inside a synctest bubble leaves no goroutine asleep behind it.
func forward(src, dst net.PacketConn, to net.Addr, prof netemProfile, rng *rand.Rand, bn *bottleneck) {
	ch := make(chan timedPacket, 4096)
	stopping, delivered := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(delivered)
		// On a closed pair the writes start failing; keep draining so the
		// forwarder never blocks on a full channel while shutting down.
		var oob []byte
		wait := time.NewTimer(time.Hour)
		defer wait.Stop()
		for pkt := range ch {
			if d := time.Until(pkt.arriveAt); d > 0 {
				wait.Reset(d)
				select {
				case <-wait.C:
				case <-stopping:
					continue
				}
			}
			oob = writeMarked(dst, pkt.data, to, pkt.ecn, oob)
		}
	}()
	defer func() {
		close(stopping)
		close(ch)
		<-delivered
	}()

	// held is the datagram reorderDepth makes wait, behind the count of
	// datagrams that passed it; released, by the count or by its timer,
	// once, which the flag decides.
	var held *heldPacket
	behind := 0
	bad := false
	buf := make([]byte, 2048)
	oob := make([]byte, udp.PacketInfoLen)
	for {
		n, ecn, err := readMarked(src, buf, oob)
		if err != nil {
			return
		}
		if prof.mtu > 0 && n+48 > prof.mtu {
			continue
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
			var wait time.Duration
			var ok bool
			if txEnd, wait, ok = bn.enqueue(now, n, prof); !ok {
				continue
			}
			// ECT(0) or ECT(1) becomes CE past the marking threshold; a
			// Not-ECT datagram waits unmarked, as RFC 3168 §5 requires.
			if prof.mark > 0 && wait > prof.mark && ecn != 0 {
				ecn = 3
			}
		}
		pkt := timedPacket{
			data:     append([]byte(nil), buf[:n]...),
			ecn:      ecn,
			arriveAt: txEnd.Add(prof.owd),
		}
		if prof.reorder > 0 && rng.Float64() < prof.reorder {
			if prof.reorderDepth == 0 {
				late := pkt.arriveAt.Add(time.Duration(rng.Float64() * float64(prof.reorderDelay)))
				time.AfterFunc(time.Until(late), func() { writeMarked(dst, pkt.data, to, pkt.ecn, nil) })
				continue
			}
			if held == nil {
				h := &heldPacket{pkt: pkt}
				h.timer = time.AfterFunc(time.Until(pkt.arriveAt.Add(prof.reorderDelay)), func() {
					if h.released.CompareAndSwap(false, true) {
						writeMarked(dst, h.pkt.data, to, h.pkt.ecn, nil)
					}
				})
				held, behind = h, 0
				continue
			}
		}
		ch <- pkt
		if held != nil && pkt.arriveAt.After(held.pkt.arriveAt.Add(prof.reorderDelay)) {
			// Past the bound: the timer delivers the held datagram ahead of
			// this one. Counted instead, it released the held datagram
			// heldGap behind a successor the sender's pacing had spaced
			// out — 2.7 ms late under BBR, past §6.1.2's time threshold.
			held = nil
		}
		if held != nil {
			behind++
			if behind == prof.reorderDepth {
				if held.released.CompareAndSwap(false, true) {
					held.timer.Stop()
					held.pkt.arriveAt = pkt.arriveAt.Add(heldGap)
					ch <- held.pkt
				}
				held = nil
			}
		}
	}
}

// heldGap is how far behind the datagrams that overtook it a held one
// arrives: long enough for the receiver to acknowledge them first, which is
// what makes the overtaking visible to the sender. Delivered together, the
// three are acknowledged together, and the gap never existed. The datagrams
// after it wait behind it in the deliverer's queue, so they still do not
// overtake it.
const heldGap = 100 * time.Microsecond

// heldPacket is a datagram reorderDepth holds back, and the timer that
// bounds its wait.
type heldPacket struct {
	pkt      timedPacket
	timer    *time.Timer
	released atomic.Bool
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
	// watch, when set, runs beside a transfer on the sending connection,
	// from the moment it is dialled, for a test that observes it while
	// the transfer runs rather than once it is over.
	watch func(*quic.Conn)
}

// benchAlgorithm is the controller the path benchmarks run, and
// benchSlowStart its slow start: SLUICE_QUIC_CC and SLUICE_QUIC_SS name
// them (see benchAlgorithmNamed, benchSlowStartNamed), so one test binary
// measures both sides of an A/B when the rounds alternate the variable
// (docs/benchmarks.md). A name neither knows stops the binary.
var (
	benchAlgorithm = mustBench(benchAlgorithmNamed(os.Getenv("SLUICE_QUIC_CC")))
	benchSlowStart = mustBench(benchSlowStartNamed(os.Getenv("SLUICE_QUIC_SS")))
)

// benchAlgorithmNamed reads SLUICE_QUIC_CC: unset or reno for the default,
// cubic for CUBIC, bbr for the IETF draft of BBR. Any
// other value is an error rather than the default, which would measure
// Reno under the misspelt name.
func benchAlgorithmNamed(name string) (quic.CongestionAlgorithm, error) {
	switch name {
	case "", "reno":
		return quic.CongestionNewReno, nil
	case "cubic":
		return quic.CongestionCubic, nil
	case "bbr":
		return quic.CongestionBBR, nil
	}
	return 0, fmt.Errorf("SLUICE_QUIC_CC=%s names no controller (reno, cubic, bbr)", name)
}

// benchSlowStartNamed reads SLUICE_QUIC_SS: unset for the algorithm's
// default, standard or hystart for the one named, an error otherwise.
func benchSlowStartNamed(name string) (quic.SlowStart, error) {
	switch name {
	case "":
		return quic.SlowStartDefault, nil
	case "standard":
		return quic.SlowStartStandard, nil
	case "hystart":
		return quic.SlowStartHyStart, nil
	}
	return 0, fmt.Errorf("SLUICE_QUIC_SS=%s names no slow start (standard, hystart)", name)
}

func TestBenchNamesRefuseTheUnknown(t *testing.T) {
	for name, want := range map[string]quic.CongestionAlgorithm{"": quic.CongestionNewReno, "reno": quic.CongestionNewReno, "cubic": quic.CongestionCubic, "bbr": quic.CongestionBBR} {
		if got, err := benchAlgorithmNamed(name); err != nil || got != want {
			t.Errorf("SLUICE_QUIC_CC=%q: %v, %v; want %v", name, got, err, want)
		}
	}
	for _, name := range []string{"newreno", "Cubic", "bbr2", "cubic "} {
		if _, err := benchAlgorithmNamed(name); err == nil {
			t.Errorf("SLUICE_QUIC_CC=%q accepted", name)
		}
	}
	if _, err := benchSlowStartNamed("hystart++"); err == nil {
		t.Error("SLUICE_QUIC_SS=hystart++ accepted")
	}
}

func mustBench[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// path wires a client and a server packet connection through two
// forwarders on this link. dialTo is where the client addresses its
// datagrams — the middlebox end facing it. Closing either returned end
// tears the path down; the link outlives it. The ends are in-memory pairs,
// or four UDP sockets on the loopback interface when the profile marks:
// only a socket carries the ECN field, and a marking profile over a
// transport that drops it would measure a path that never marks.
func (l *netemLink) path() (client, server net.PacketConn, dialTo net.Addr, stop func(), err error) {
	seed := 4*l.paths + 1
	l.paths++
	var a1, a2, b1, b2 net.PacketConn // client <-> middlebox, middlebox <-> server
	if l.prof.mark > 0 {
		socks, err := loopbackSockets(4)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		a1, a2, b1, b2 = socks[0], socks[1], socks[2], socks[3]
	} else {
		a1, a2 = dgram.Pair()
		b1, b2 = dgram.Pair()
	}
	var forwarders sync.WaitGroup
	forwarders.Go(func() { forward(a2, b1, b2.LocalAddr(), l.prof, rand.New(rand.NewPCG(seed, seed+1)), &l.up) })     //nolint:gosec // G404: a seeded loss pattern, reproducible on purpose
	forwarders.Go(func() { forward(b1, a2, a1.LocalAddr(), l.prof, rand.New(rand.NewPCG(seed+2, seed+3)), &l.down) }) //nolint:gosec // G404: same
	return a1, b2, a2.LocalAddr(), func() {
		_ = a1.Close()
		_ = b2.Close()
		_ = a2.Close()
		_ = b1.Close()
		forwarders.Wait()
	}, nil
}

// loopbackSockets opens n UDP sockets on 127.0.0.1, each reporting the ECN
// field of what it reads — the forwarders need it on their receiving ends,
// the connections ask for it again on theirs — or none, on the first error.
func loopbackSockets(n int) ([]*net.UDPConn, error) {
	socks := make([]*net.UDPConn, 0, n)
	for range n {
		uc, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
		if err == nil {
			err = udp.EnableECN(uc)
		}
		if err != nil {
			for _, s := range socks {
				_ = s.Close()
			}
			if uc != nil {
				_ = uc.Close()
			}
			return nil, err
		}
		socks = append(socks, uc)
	}
	return socks, nil
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
	marks   int // the sender's application-limited marks

	// discarded is what the receiving end refused to read: its
	// connection's DiscardedPackets, plus its listener's when one fronts it.
	discarded uint64

	// Set when small messages came first (transferAfterSmall): the bulk
	// phase's own wall clock, the sender's counters as it began, and the
	// largest flight the messages kept.
	bulk      time.Duration
	atBulk    quic.Stats
	maxFlight int
}

// transferOnce runs one connection over the link and moves total bytes
// client-to-server on a single stream, returning when the server has read
// them all. The handshake rides inside the measurement; at these transfer
// sizes it is two round trips against a second of data. It returns an
// error rather than failing the test because two flows run it from two
// goroutines, and only the test's own may call Fatal.
func transferOnce(tb testing.TB, link *netemLink, total int) (transfer, error) {
	tb.Helper()
	return transferWith(tb, link, total, link.cc)
}

// transferWith is transferOnce with the controller named rather than the
// link's: what lets two flows on one link run two controllers.
func transferWith(tb testing.TB, link *netemLink, total int, cc quic.CongestionAlgorithm) (transfer, error) {
	tb.Helper()
	return transferAfterSmall(tb, link, cc, 0, total)
}

// transferAfterSmall is transferWith preceded, on the same stream, by small
// bytes of messages of 1 to 4 kB written one a millisecond — 2.5 MB/s, half
// of what the Bottleneck line carries, so the window, not the path, is
// what they leave behind: RFC 9002 §7.8 has a sender that did not fill
// its window not grow it.
func transferAfterSmall(tb testing.TB, link *netemLink, cc quic.CongestionAlgorithm, small, total int) (transfer, error) {
	tb.Helper()
	start := time.Now()
	clientPC, serverPC, dialTo, stop, err := link.path()
	if err != nil {
		return transfer{}, err
	}
	defer stop()
	params := wideParameters()
	params.Congestion.Algorithm = cc
	params.Congestion.SlowStart = benchSlowStart

	type result struct {
		n         int64
		discarded uint64
		err       error
	}
	done := make(chan result, 1)
	go func() {
		conn, l, err := acceptOne(tb, serverPC, params, link.prof.mark > 0)
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
		discarded := conn.Stats().DiscardedPackets
		if l != nil {
			discarded += l.DiscardedPackets()
		}
		done <- result{n: got, discarded: discarded}
	}()

	conn, err := quic.Dial(context.Background(), clientPC, dialTo, clientTLS(), params)
	if err != nil {
		return transfer{}, fmt.Errorf("Dial: %w", err)
	}
	defer func() { _ = conn.Close() }()
	if link.watch != nil {
		go link.watch(conn)
	}
	s, err := conn.OpenStream(tb.Context())
	if err != nil {
		return transfer{}, err
	}
	chunk := make([]byte, 64<<10)
	var t transfer
	if small > 0 {
		sizes := rand.New(rand.NewPCG(1, 2)) //nolint:gosec // G404: message sizes, reproducible on purpose
		tick := time.NewTicker(time.Millisecond)
		for sent := 0; sent < small; {
			n := min(1<<10+sizes.IntN(3<<10+1), small-sent)
			if _, err := s.Write(chunk[:n]); err != nil {
				tick.Stop()
				return transfer{}, fmt.Errorf("message after %d bytes: %w", sent, err)
			}
			sent += n
			t.maxFlight = max(t.maxFlight, conn.Stats().BytesInFlight)
			<-tick.C
		}
		tick.Stop()
		t.atBulk = conn.Stats()
	}
	bulk := time.Now()
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
		if r.n != int64(small+total) {
			return transfer{}, fmt.Errorf("the server read %d of %d bytes", r.n, small+total)
		}
		t.discarded = r.discarded
	case <-time.After(120 * time.Second):
		return transfer{}, errors.New("the transfer did not finish")
	}
	t.elapsed, t.bulk = time.Since(start), time.Since(bulk)
	t.stats, t.marks = conn.Stats(), quic.AppLimitedMarks(conn)
	return t, nil
}

// acceptOne completes one server handshake on pc: by reading the first
// datagram and handing it to quic.Accept, or, when the path marks, behind a
// Listener, which reads every datagram's ECN field with its bytes. The
// marking figures in docs/benchmarks.md were measured behind a Listener;
// the shape stays so they remain comparable. The listener is returned for
// its counters, nil without one.
func acceptOne(tb testing.TB, pc net.PacketConn, params quic.TransportParameters, marks bool) (*quic.Conn, *quic.Listener, error) {
	tb.Helper()
	if marks {
		l, err := quic.NewListener(pc, serverTLS(tb), params, quic.ListenerConfig{})
		if err != nil {
			return nil, nil, err
		}
		conn, err := l.Accept()
		if err != nil {
			_ = l.Close()
			return nil, nil, err
		}
		// The listener stays open as long as the connection: it owns the
		// socket's read loop. Closing pc (the path's stop) ends both.
		return conn, l, nil
	}
	buf := make([]byte, 2048)
	if err := pc.SetReadDeadline(time.Now().Add(60 * time.Second)); err != nil {
		return nil, nil, err
	}
	n, peer, err := pc.ReadFrom(buf)
	if err != nil {
		return nil, nil, err
	}
	conn, err := quic.Accept(pc, quic.Datagram{Peer: peer, Payload: buf[:n]}, serverTLS(tb), params)
	return conn, nil, err
}

// benchTransfer moves total bytes on one flow and reports its goodput, and
// the sender's final smoothed RTT as a multiple of the smallest it sampled:
// on a deep buffer that ratio is the queue the controller let build, the
// figure BBR's "RTT within 1.5× min_rtt" criterion reads. retx_B is the
// bytes the sender's declared losses put back on the queue, and lost the
// packets behind them: on a path that reorders and loses nothing, both
// count spurious losses alone, a figure the scheduler moves far less than
// the transfer's wall clock. dgram_B is the datagram size the sender's
// DPLPMTUD search reached (1452 on a path with no MTU). discarded is what
// the receiver refused to read; nothing on these paths should be.
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
		b.ReportMetric(float64(t.stats.BytesRetransmitted), "retx_B")
		b.ReportMetric(float64(t.stats.LostPackets), "lost")
		b.ReportMetric(float64(t.stats.DatagramSize), "dgram_B")
		b.ReportMetric(float64(t.discarded), "discarded")
	}
}

// benchTwoFlows moves total bytes on each of two flows that share the
// link's bottleneck, started together, and reports the aggregate goodput
// (what the link delivered) and the first flow's share of the bytes per
// second — 0.5 when the two controllers split the line, the figure a
// fairness criterion reads. One transfer's wall clock is the slower flow's.
func benchTwoFlows(b *testing.B, prof netemProfile, total int) {
	b.Helper()
	benchTwoFlowsAgainst(b, prof, total, benchAlgorithm)
}

// benchTwoFlowsAgainst is benchTwoFlows with the second flow's controller
// named: the first runs benchAlgorithm, and share is still the first's.
func benchTwoFlowsAgainst(b *testing.B, prof netemProfile, total int, second quic.CongestionAlgorithm) {
	b.Helper()
	b.SetBytes(int64(2 * total))
	for b.Loop() {
		link := &netemLink{prof: prof, cc: benchAlgorithm}
		var (
			wg   sync.WaitGroup
			res  [2]transfer
			errs [2]error
		)
		ccs := [2]quic.CongestionAlgorithm{benchAlgorithm, second}
		for i := range res {
			wg.Go(func() { res[i], errs[i] = transferWith(b, link, total, ccs[i]) })
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
// which is exactly what a per-packet cwnd growth would change.
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

// BenchmarkTransferBottleneckMTU1420 is Bottleneck behind a tunnel: an IP
// MTU of 1420, so the first DPLPMTUD probe of 1452 bytes is lost and the
// search (RFC 8899 §5.3) has to find a size between 1200 and 1372. Not
// gated: what it reads is dgram_B, the size the search settled at — 1357,
// where a single probe of 1452 stayed at 1200: 13 % more payload per
// datagram, 12 % fewer packets. MB/s does not move with it (A/B of
// 2026-10-18, inside the noise): this line counts bytes, and a packet's
// header is a few percent of either size.
func BenchmarkTransferBottleneckMTU1420(b *testing.B) {
	benchTransfer(b, netemProfile{
		owd:   10 * time.Millisecond,
		rate:  50_000_000 / 8,
		queue: 5 * time.Millisecond,
		mtu:   1420,
	}, 4<<20)
}

// BenchmarkTransferBottleneck16MiB is Bottleneck moving 16 MiB. On 4 MiB
// the handshake and the ramp from 12 kB cost a controller 130 to 140 ms of
// 800 — a ceiling near 0.84 of the line whatever it does — so the share of
// the line a controller holds once it cruises is read here. Not gated: it
// says nothing Bottleneck does not, four times slower.
func BenchmarkTransferBottleneck16MiB(b *testing.B) {
	benchTransfer(b, netemProfile{
		owd:   10 * time.Millisecond,
		rate:  50_000_000 / 8,
		queue: 5 * time.Millisecond,
	}, 16<<20)
}

// BenchmarkTransferBottleneckAfterSmall is Bottleneck's 4 MiB written after
// 5 MiB of small messages on the same connection (transferAfterSmall): a
// connection that served requests, then a download. The messages never fill
// the window, so it must leave them no larger than they needed — cwnd_x is
// the window as the transfer starts over the largest flight they kept,
// at most two (one doubling of a slow start not yet ended) — and the
// transfer must then lose no more than Bottleneck's alone: lost counts its
// own losses. Not gated: the question is the slow start's, measured with
// SLUICE_QUIC_SS, and the figure sits two seconds of messages behind.
func BenchmarkTransferBottleneckAfterSmall(b *testing.B) {
	const small, total = 5 << 20, 4 << 20
	b.SetBytes(total)
	for b.Loop() {
		link := &netemLink{prof: netemProfile{
			owd:   10 * time.Millisecond,
			rate:  50_000_000 / 8,
			queue: 5 * time.Millisecond,
		}, cc: benchAlgorithm}
		t, err := transferAfterSmall(b, link, benchAlgorithm, small, total)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(float64(total)/1e6/t.bulk.Seconds(), "MB/s")
		b.ReportMetric(float64(t.stats.LostPackets-t.atBulk.LostPackets), "lost")
		b.ReportMetric(float64(t.atBulk.CongestionWindow)/float64(max(t.maxFlight, 1)), "cwnd_x")
	}
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

// BenchmarkTransferDeepBufferECN is DeepBuffer behind an AQM that marks CE
// once a datagram waits more than 5 ms for the line — the Bottleneck
// profile's buffer depth — and still drops at 200 ms: the signal §3.7 of the
// BBR draft names for holding the queue short without loss. Over UDP
// sockets on the loopback interface (the marks have to travel), so its
// absolute times are not DeepBuffer's; rtt_x is the figure to read. A
// controller that ignores marks fills the buffer as on DeepBuffer.
func BenchmarkTransferDeepBufferECN(b *testing.B) {
	benchTransfer(b, netemProfile{
		owd:   10 * time.Millisecond,
		rate:  50_000_000 / 8,
		queue: 200 * time.Millisecond,
		mark:  5 * time.Millisecond,
	}, 4<<20)
}

// BenchmarkTransferTwoFlows is the fairness benchmark: two flows started
// together on the 50 Mbit/s bottleneck behind one round trip of buffer
// (20 ms), each moving 4 MiB. share is the first flow's part of the
// delivered rate; two copies of one controller should split the line, and
// a new controller is measured by how far from a half it pushes the old
// one.
func BenchmarkTransferTwoFlows(b *testing.B) {
	benchTwoFlows(b, twoFlowsProfile, 4<<20)
}

// BenchmarkTransferTwoFlowsVsReno is the coexistence benchmark: the same
// link and sizes as TwoFlows, the first flow on the controller under
// measurement and the second on Reno, whatever SLUICE_QUIC_CC says. share
// is the first flow's: a half when both run Reno, and for a model-based
// controller the figure its bound on what it takes from a loss-based flow
// is read on. Not in the gate: its question is the controller's, not the
// tree's.
func BenchmarkTransferTwoFlowsVsReno(b *testing.B) {
	benchTwoFlowsAgainst(b, twoFlowsProfile, 4<<20, quic.CongestionNewReno)
}

var twoFlowsProfile = netemProfile{
	owd:   10 * time.Millisecond,
	rate:  50_000_000 / 8,
	queue: 20 * time.Millisecond,
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
