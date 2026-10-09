package bench

import (
	"testing"

	"github.com/mlagarrigue/sluice"
)

// Does a single-pass Pipe actually beat N stages, once both sides call through
// opaque function values? Variant B looked 19% ahead, but it called inc
// directly where E went through a package-level var — not the same conditions.
// Here both sides go through the same opaque values.

var (
	o1 = inc
	o2 = inc
	o3 = inc
	o4 = inc
)

// pipe4Opaque: one traversal, four opaque calls per element.
func pipe4Opaque(s sluice.Stream[int64], f1, f2, f3, f4 func(int64) int64) sluice.Stream[int64] {
	return func(yield func(sluice.Batch[int64]) bool) {
		s(func(b sluice.Batch[int64]) bool {
			items := b.Items
			for i := range items {
				items[i] = f4(f3(f2(f1(items[i]))))
			}
			return yield(b)
		})
	}
}

func BenchmarkPipeSinglePass(b *testing.B) {
	src := data(N)
	b.SetBytes(N * 8)
	b.ReportAllocs()
	for b.Loop() {
		s := pipe4Opaque(sluice.Of(src, 1024), o1, o2, o3, o4)
		var acc int64
		s(func(bt sluice.Batch[int64]) bool {
			for _, v := range bt.Items {
				acc += v
			}
			return true
		})
		sink = acc
	}
	reportPerElem(b, N)
}

func BenchmarkPipeFourStages(b *testing.B) {
	src := data(N)
	b.SetBytes(N * 8)
	b.ReportAllocs()
	for b.Loop() {
		s := sluice.Of(src, 1024)
		for _, f := range []func(int64) int64{o1, o2, o3, o4} {
			s = sluice.Map(s, f)
		}
		var acc int64
		s(func(bt sluice.Batch[int64]) bool {
			for _, v := range bt.Items {
				acc += v
			}
			return true
		})
		sink = acc
	}
	reportPerElem(b, N)
}
