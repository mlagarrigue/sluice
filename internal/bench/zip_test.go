package bench

import (
	"testing"

	"github.com/mlagarrigue/sluice"
)

// ZipLongest pairs positionally: no key extraction, no comparison, no sort
// check. It is the same shape of work as MergeJoinBy minus everything the join
// needs, so the pair of figures says what those cost.

// BenchmarkZipBaseline walks both halves in lockstep by index, materializing
// nothing: the floor for positional pairing.
func BenchmarkZipBaseline(b *testing.B) {
	left, right := data(N/2), data(N/2)
	b.SetBytes(N * 8)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		var acc int64
		for i := range left {
			acc += left[i] + right[i]
		}
		sink = acc
	}
	reportPerElem(b, N)
}

func BenchmarkZipLongest(b *testing.B) {
	left, right := data(N/2), data(N/2)
	b.SetBytes(N * 8)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		var acc int64
		sluice.ZipLongest(
			sluice.Of(left, 1024), sluice.Of(right, 1024),
		)(func(bt sluice.Batch[sluice.EitherOrBoth[int64, int64]]) bool {
			for _, e := range bt.Items {
				acc += e.Left + e.Right
			}
			return true
		})
		sink = acc
	}
	reportPerElem(b, N)
}

// Uneven lengths: half the rows are pairs, half are left-only tail. Checks that
// the tail path costs no more than the paired one.
func BenchmarkZipLongestUneven(b *testing.B) {
	left, right := data(N/2), data(N/4)
	b.SetBytes(N * 8)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		var acc int64
		sluice.ZipLongest(
			sluice.Of(left, 1024), sluice.Of(right, 1024),
		)(func(bt sluice.Batch[sluice.EitherOrBoth[int64, int64]]) bool {
			for _, e := range bt.Items {
				acc += e.Left
				if e.HasRight {
					acc += e.Right
				}
			}
			return true
		})
		sink = acc
	}
	reportPerElem(b, N)
}
