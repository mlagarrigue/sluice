package quic_test

import (
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/internal/dgram"
	"github.com/mlagarrigue/sluice/net/quic"
)

// The loss/latency harness the congestion-control backlog items wait on:
// a middlebox between two dgram pairs that charges every datagram a one-way
// delay and, per profile, a random loss probability and a drop-tail
// bottleneck queue. It is what lets a pacing, cwnd-growth or PTO change be
// measured as goodput on a path instead of asserted from the RFC's prose.
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

// forward emulates one direction: src's peer traffic is read promptly,
// doomed packets are dropped at read time, survivors are handed to a
// deliverer that sleeps until each one's arrival instant. Returns when src
// closes.
func forward(src, dst net.PacketConn, to net.Addr, prof netemProfile, rng *rand.Rand) {
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

	var lastTxEnd time.Time
	buf := make([]byte, 2048)
	for {
		n, _, err := src.ReadFrom(buf)
		if err != nil {
			return
		}
		if prof.loss > 0 && rng.Float64() < prof.loss {
			continue
		}
		now := time.Now()
		txEnd := now
		if prof.rate > 0 {
			txStart := now
			if lastTxEnd.After(txStart) {
				txStart = lastTxEnd
			}
			if txStart.Sub(now) > prof.queue {
				continue // the bottleneck's buffer is full: drop-tail
			}
			txEnd = txStart.Add(time.Duration(n) * time.Second / time.Duration(prof.rate))
			lastTxEnd = txEnd
		}
		ch <- timedPacket{
			data:     append([]byte(nil), buf[:n]...),
			arriveAt: txEnd.Add(prof.owd),
		}
	}
}

// netemPath wires client and server packet connections through two
// forwarders sharing one profile. dialTo is where the client addresses its
// datagrams — the middlebox end facing it. Closing either returned end tears
// the whole path down.
func netemPath(prof netemProfile) (client, server net.PacketConn, dialTo net.Addr, stop func()) {
	a1, a2 := dgram.Pair()                                                // client <-> middlebox
	b1, b2 := dgram.Pair()                                                // middlebox <-> server
	go forward(a2, b1, b2.LocalAddr(), prof, rand.New(rand.NewPCG(1, 2))) //nolint:gosec // G404: a seeded loss pattern, reproducible on purpose
	go forward(b1, a2, a1.LocalAddr(), prof, rand.New(rand.NewPCG(3, 4))) //nolint:gosec // G404: same
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

// transferOnce runs one connection over the path and moves total bytes
// client-to-server on a single stream, returning when the server has read
// them all. The handshake rides inside the measurement; at these transfer
// sizes it is two round trips against a second of data.
func transferOnce(b *testing.B, prof netemProfile, total int) {
	b.Helper()
	clientPC, serverPC, dialTo, stop := netemPath(prof)
	defer stop()

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
		conn, err := quic.Accept(serverPC, peer, buf[:n], serverTLS(b), wideParameters())
		if err != nil {
			done <- result{err: err}
			return
		}
		defer func() { _ = conn.Close() }()
		s, err := conn.AcceptStream()
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

	conn, err := quic.Dial(clientPC, dialTo, clientTLS(), wideParameters())
	if err != nil {
		b.Fatalf("Dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	s, err := conn.OpenStream()
	if err != nil {
		b.Fatal(err)
	}
	chunk := make([]byte, 64<<10)
	for sent := 0; sent < total; sent += len(chunk) {
		if _, err := s.Write(chunk); err != nil {
			b.Fatalf("after %d bytes: %v (conn: %v)", sent, err, conn.Err())
		}
	}
	if err := s.CloseWrite(); err != nil {
		b.Fatal(err)
	}

	select {
	case r := <-done:
		if r.err != nil {
			b.Fatalf("the server: %v", r.err)
		}
		if r.n != int64(total) {
			b.Fatalf("the server read %d of %d bytes", r.n, total)
		}
	case <-time.After(120 * time.Second):
		b.Fatal("the transfer did not finish")
	}
}

func benchTransfer(b *testing.B, prof netemProfile, total int) {
	b.Helper()
	b.SetBytes(int64(total))
	for b.Loop() {
		start := time.Now()
		transferOnce(b, prof, total)
		mbps := float64(total) / 1e6 / time.Since(start).Seconds()
		b.ReportMetric(mbps, "MB/s")
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
