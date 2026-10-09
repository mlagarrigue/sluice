package bench

import (
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/window"
)

// Window walks its input once and copies each closed window out. The figures
// separate the two terms: the per-element work — the order check and the
// window arithmetic — and the per-pane copy, which only shows when windows are
// small enough to be numerous.

// windowRows spreads elements one second apart, so a window of d seconds holds
// exactly d of them.
func windowRows(n int) []int64 {
	rows := make([]int64, n)
	for i := range rows {
		rows[i] = int64(i)
	}
	return rows
}

func benchWindow(b *testing.B, size time.Duration) {
	b.Helper()
	src := windowRows(N)
	ts := func(v int64) time.Time { return time.Unix(v, 0) }

	b.SetBytes(N * 8)
	b.ReportAllocs()
	for b.Loop() {
		var acc int64
		window.Tumbling(sluice.Of(src, 1024), ts, size, 1<<20, sluice.Fail)(
			func(bt sluice.Batch[window.Pane[int64]]) bool {
				for _, p := range bt.Items {
					acc += int64(p.Len())
				}
				return true
			})
		sink = acc
	}
	reportPerElem(b, N)
}

// BenchmarkWindowWide: 1024 elements per window, so the per-pane copy is
// amortized over a batch's worth of elements — the per-element term dominates.
func BenchmarkWindowWide(b *testing.B) {
	benchWindow(b, 1024*time.Second)
}

// BenchmarkWindowNarrow: 8 elements per window. Windows close constantly, so
// this is where the per-pane copy and allocation are visible.
func BenchmarkWindowNarrow(b *testing.B) {
	benchWindow(b, 8*time.Second)
}
