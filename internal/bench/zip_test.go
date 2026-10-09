package bench

import (
	"testing"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/join"
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
	for b.Loop() {
		var acc int64
		join.ZipLongest(
			sluice.Of(left, 1024), sluice.Of(right, 1024),
		)(func(bt sluice.Batch[join.EitherOrBoth[int64, int64]]) bool {
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
//
// The figures are per element actually consumed — N/2 + N/4, not the N the
// even benchmark reads — so the comparison against BenchmarkZipLongest judges
// the tail path rather than flattering it by a third.
func BenchmarkZipLongestUneven(b *testing.B) {
	const consumed = N/2 + N/4
	left, right := data(N/2), data(N/4)
	b.SetBytes(consumed * 8)
	b.ReportAllocs()
	for b.Loop() {
		var acc int64
		join.ZipLongest(
			sluice.Of(left, 1024), sluice.Of(right, 1024),
		)(func(bt sluice.Batch[join.EitherOrBoth[int64, int64]]) bool {
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
	reportPerElem(b, consumed)
}
