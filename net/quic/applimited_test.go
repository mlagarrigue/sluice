package quic_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/mlagarrigue/sluice/net/quic"
)

// A bulk transfer runs out of data once, before its first write: every
// later write starts as the one before it ends, so the sender never leaves
// a send opportunity unused and its rate samples measure the path. A
// spurious mark discounts a round trip of them — BBR's bandwidth filter and
// the window growth of every controller read the mark — so the count is
// exactly one on every path and for every controller. In a bubble: a
// writer descheduled between two writes for longer than a slot leaves the
// window unused as surely as an application that paused, and on a loaded
// machine the wall clock sees it (1.7 ms, four race-enabled copies of this
// test at once); the bubble's clock does not move while the writer is
// runnable.
func TestBulkTransferIsApplicationLimitedOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("sixteen 4 MiB transfers over an emulated path")
	}
	profiles := []struct {
		name string
		prof netemProfile
	}{
		{"Clean", netemProfile{owd: 10 * time.Millisecond}},
		{"Loss1pct", netemProfile{owd: 10 * time.Millisecond, loss: 0.01}},
		{"Bottleneck", netemProfile{owd: 10 * time.Millisecond, rate: 50_000_000 / 8, queue: 5 * time.Millisecond}},
		{"DeepBuffer", netemProfile{owd: 10 * time.Millisecond, rate: 50_000_000 / 8, queue: 200 * time.Millisecond}},
	}
	controllers := []quic.CongestionAlgorithm{quic.CongestionNewReno, quic.CongestionCubic, quic.CongestionBBR}
	for _, p := range profiles {
		for _, cc := range controllers {
			t.Run(p.name+"/"+cc.String(), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					tr, err := transferOnce(t, &netemLink{prof: p.prof, cc: cc}, 4<<20)
					if err != nil {
						t.Fatal(err)
					}
					if tr.marks != 1 {
						t.Fatalf("%d application-limited marks over a bulk transfer, want 1", tr.marks)
					}
				})
			})
		}
	}
}
