package quic_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/mlagarrigue/sluice/net/quic"
)

// After a transfer, Stats reports what the congestion controller knows: a
// round-trip estimate the handshake sampled, a window that grew past its
// floor, and nothing left in flight once everything is acknowledged.
func TestStatsReportsRTTWindowAndFlight(t *testing.T) {
	client, server, _, _ := handshakePair(t, serverTLS(t), clientTLS())
	cs, ss := openAccepted(t, client, server)

	payload := make([]byte, 64<<10)
	if _, err := cs.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := cs.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	got := 0
	buf := make([]byte, 4096)
	for got < len(payload) {
		n, err := ss.Read(buf)
		got += n
		if err != nil {
			t.Fatalf("after %d bytes: %v", got, err)
		}
	}

	for _, end := range []struct {
		name string
		conn *quic.Conn
	}{{"client", client}, {"server", server}} {
		name, s := end.name, end.conn.Stats()
		if s.SmoothedRTT <= 0 || s.MinRTT <= 0 || s.LatestRTT <= 0 {
			t.Errorf("%s: no round trip sampled: smoothed %v, min %v, latest %v", name, s.SmoothedRTT, s.MinRTT, s.LatestRTT)
		}
		if s.BytesRetransmitted != 0 || s.PersistentCongestions != 0 {
			t.Errorf("%s: loopback requeued %d bytes, collapsed %d times", name, s.BytesRetransmitted, s.PersistentCongestions)
		}
		if s.MinRTT > s.SmoothedRTT+s.RTTVariance {
			t.Errorf("%s: min %v above smoothed %v + variance %v", name, s.MinRTT, s.SmoothedRTT, s.RTTVariance)
		}
		if s.CongestionWindow < 2*1200 {
			t.Errorf("%s: window %d below the 2 × 1200 floor", name, s.CongestionWindow)
		}
		// Loopback loses nothing. Probe timeouts are not asserted: the
		// handshake fires one on each side even here — the first sample
		// puts the PTO near its 1 ms floor, and a TLS step that takes
		// longer than that is a timeout, not a loss.
		if s.LostPackets != 0 {
			t.Errorf("%s: loopback lost %d packets", name, s.LostPackets)
		}
	}
	waitFor(t, "the sender's flight to empty", func() bool {
		return client.Stats().BytesInFlight == 0
	})
	// Growth happened: 64 KiB in one direction pushes past the initial
	// window (10 × 1200) on a path that acknowledges everything.
	if w := client.Stats().CongestionWindow; w <= 10*1200 {
		t.Errorf("the sender's window did not grow past its initial %d: %d", 10*1200, w)
	}
}

// A connection built with CongestionCubic runs CUBIC: Stats says so, and
// the transfer completes on a lossy path. The controller does not have to
// be fast here, only chosen; its figures are docs/benchmarks.md's.
func TestCubicIsChosenPerConnection(t *testing.T) {
	if testing.Short() {
		t.Skip("a 512 KiB transfer over an emulated 4 ms lossy path")
	}
	link := &netemLink{prof: netemProfile{owd: 2 * time.Millisecond, loss: 0.02}, cc: quic.CongestionCubic}
	tr, err := transferOnce(t, link, 512<<10)
	if err != nil {
		t.Fatal(err)
	}
	if tr.stats.Algorithm != quic.CongestionCubic {
		t.Errorf("Stats.Algorithm = %v, want cubic", tr.stats.Algorithm)
	}
	if tr.stats.LostPackets == 0 {
		t.Error("a 2% lossy path lost nothing: the transfer did not exercise the controller")
	}
}

// On a path that loses one datagram in twenty, the sender's LostPackets
// moves; the server, which only acknowledges, loses nothing it tracks.
func TestStatsCountsLostPackets(t *testing.T) {
	if testing.Short() {
		t.Skip("a 512 KiB transfer over an emulated 4 ms lossy path")
	}
	tr, err := transferOnce(t, &netemLink{prof: netemProfile{owd: 2 * time.Millisecond, loss: 0.05}}, 512<<10)
	if err != nil {
		t.Fatal(err)
	}
	stats := tr.stats
	if stats.LostPackets == 0 {
		t.Fatalf("5%% loss, nothing declared lost: %+v", stats)
	}
	// Every loss put a payload back on the queue, and a 512 KiB stream
	// loses more than one stream frame's worth; a count below the number
	// of packets is a counter that did not move on one of the two
	// detection paths.
	if stats.BytesRetransmitted < stats.LostPackets {
		t.Fatalf("%d packets lost, %d bytes requeued: %+v", stats.LostPackets, stats.BytesRetransmitted, stats)
	}
	// And no lost packet puts back more than it carried: 1452 bytes, the
	// largest datagram DPLPMTUD ever proves. A payload counted twice, on
	// both detection paths or once per detection pass, lands above.
	if limit := stats.LostPackets * 1452; stats.BytesRetransmitted > limit {
		t.Fatalf("%d packets lost, %d bytes requeued, above %d: %+v", stats.LostPackets, stats.BytesRetransmitted, limit, stats)
	}
	if stats.LatestRTT <= 0 || stats.LatestRTT < stats.MinRTT {
		t.Fatalf("latest round trip %v, min %v: %+v", stats.LatestRTT, stats.MinRTT, stats)
	}
	// Five percent of independent loss is never three probe intervals
	// without an acknowledgement: a collapse here is the detector firing
	// outside §7.6's conditions.
	if stats.PersistentCongestions != 0 {
		t.Fatalf("%d persistent-congestion collapses under 5%% random loss: %+v", stats.PersistentCongestions, stats)
	}
	if stats.BytesInFlight < 0 {
		t.Fatalf("negative flight: %+v", stats)
	}
}

// Over a 50 Mbit/s bottleneck behind a 20 ms round trip, the delivery
// estimator reads what the sender let the path carry: the line while the
// window holds at least the bandwidth-delay product and keeps the
// bottleneck's queue busy, the window per round trip once a late loss has
// halved a Reno window below it. The buffer holds 20 ms, the product
// itself, so that a window halved from what overflows it still covers the
// product and the path carries the line to the end: on a 5 ms buffer the
// last sample depended on where the last loss fell — 0.56 and 0.70 of the
// ceiling below once Reno paced its slow start at twice the window
// (2026-10-28), a trajectory, not the estimator. The floor is 0.8 of the
// lesser of the two, the share of a sample interval the queue can stand
// empty after a loss; the line bounds it above with only measurement
// noise, since nothing is delivered faster than the line
// (draft-ietf-ccwg-bbr-06 §4.1.1.2.4). An interval read twice too long
// reads 0.50.
//
// The transfer runs in a synctest bubble: the link is dgram.Pair and the
// emulator's timers, so its clock is the bubble's, which only moves when
// every goroutine waits. On the wall clock, a runner busy with the rest of
// the suite delayed the emulator's deliveries and the figure measured the
// runner (0.33 of the line, CI 2026-10-16); here it measures the estimator.
func TestDeliveryRateOnBottleneck(t *testing.T) {
	if testing.Short() {
		t.Skip("a 4 MiB transfer over an emulated 50 Mbit/s bottleneck")
	}
	const line = 50_000_000 / 8
	var tr transfer
	synctest.Test(t, func(t *testing.T) {
		link := &netemLink{prof: netemProfile{owd: 10 * time.Millisecond, rate: line, queue: 20 * time.Millisecond}}
		var err error
		if tr, err = transferOnce(t, link, 4<<20); err != nil {
			t.Fatal(err)
		}
	})
	rate := tr.stats.DeliveryRate
	// What the sender let the path carry. A Reno window halved by a loss in
	// the last round trips is below the bandwidth-delay product, and the
	// path then delivers that window per round trip, not the line: seen
	// twice under the whole suite at 0.71 and 0.795 of the line, the window
	// at 0.9 of the product and the rate at 0.98 of window/RTT. The estimator
	// is measured against that ceiling; the line stays the ceiling of the
	// ceiling.
	ceiling := min(line, float64(tr.stats.CongestionWindow)/tr.stats.SmoothedRTT.Seconds())
	t.Logf("Stats.DeliveryRate %.0f B/s, %.3f of the line, %.3f of window/RTT", rate, rate/line, rate/ceiling)
	if rate < 0.8*ceiling || rate > 1.05*line {
		t.Fatalf("Stats.DeliveryRate %.0f B/s on a %d B/s line (%.2f of it, %.2f of window/RTT): %+v", rate, line, rate/line, rate/ceiling, tr.stats)
	}
}
