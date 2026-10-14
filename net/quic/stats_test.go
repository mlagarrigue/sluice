package quic_test

import (
	"testing"
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
