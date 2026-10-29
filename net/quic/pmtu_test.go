package quic_test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/net/quic"
)

// RFC 9000 §14.4 counts a DPLPMTUD probe in flight and has it wait for room
// in the congestion window; under a transfer that keeps the window full it
// must still leave. It does because it is tried first after every processed
// packet (flushControlLocked), so it takes the first room an
// acknowledgement opens, ahead of the writer it woke. Over Bottleneck the
// sender reaches 1452 within pmtuStarveBound round trips of the dial, long
// before 4 MiB are through; a probe sent only when nothing is in flight
// waits for the end of the transfer instead.
func TestPMTUProbeNotStarvedBySaturatingTransfer(t *testing.T) {
	const (
		rtt   = 20 * time.Millisecond
		total = 4 << 20
		// Measured at 2.2 to 2.3 round trips on Linux on 2026-10-18 —
		// the confirmation, then the probe's own round trip behind the
		// queue — and at 6.1 on a loaded macOS CI runner (2026-10-28),
		// whose timers and scheduling stretch every step; the bound of 5
		// that doubled the Linux figure failed there by chance. What the
		// test guards against is a probe starved by the flight, which is
		// acknowledged at the end of the transfer, 40 to 60 round trips in
		// (0.8 s here, 1.2 s on that runner): 10 stays four times short.
		pmtuStarveBound = 10
	)
	var reached atomic.Int64
	link := &netemLink{prof: netemProfile{owd: rtt / 2, rate: 50_000_000 / 8, queue: 5 * time.Millisecond}}
	link.watch = func(c *quic.Conn) {
		start := time.Now()
		for c.Err() == nil && time.Since(start) < 10*time.Second {
			if c.Stats().DatagramSize == 1452 {
				reached.Store(int64(time.Since(start)))
				return
			}
			time.Sleep(time.Millisecond)
		}
	}
	tr, err := transferOnce(t, link, total)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Duration(reached.Load())
	switch {
	case at == 0 || at > tr.elapsed:
		t.Fatalf("the probe was not acknowledged before the transfer ended (%v): starved by the flight", tr.elapsed)
	case at > pmtuStarveBound*rtt:
		t.Errorf("1452 reached %v after the dial, %.1f round trips, over the bound of %d", at, float64(at)/float64(rtt), pmtuStarveBound)
	}
	t.Logf("1452 reached after %v (%.1f round trips) of a %v transfer", at, float64(at)/float64(rtt), tr.elapsed)
}
