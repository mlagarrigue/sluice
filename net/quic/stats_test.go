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
		if s.SmoothedRTT <= 0 || s.MinRTT <= 0 {
			t.Errorf("%s: no round trip sampled: smoothed %v, min %v", name, s.SmoothedRTT, s.MinRTT)
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

// On a path that loses one datagram in twenty, the sender's LostPackets
// moves; the server, which only acknowledges, loses nothing it tracks.
func TestStatsCountsLostPackets(t *testing.T) {
	if testing.Short() {
		t.Skip("a 512 KiB transfer over an emulated 4 ms lossy path")
	}
	stats := transferOnce(t, netemProfile{owd: 2 * time.Millisecond, loss: 0.05}, 512<<10)
	if stats.LostPackets == 0 {
		t.Fatalf("5%% loss, nothing declared lost: %+v", stats)
	}
	if stats.BytesInFlight < 0 {
		t.Fatalf("negative flight: %+v", stats)
	}
}
