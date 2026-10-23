package bench

import (
	"fmt"
	"testing"

	"github.com/mlagarrigue/sluice"
)

// Each count of results per element gets its own figure: the guard on what f
// returns runs once per element whatever the count, and a cost that helps the
// common case and quietly taxes the others is not an improvement, it is a
// trade nobody measured.
func BenchmarkFlatMapShapes(b *testing.B) {
	for _, out := range []int{0, 1, 2, 4} {
		b.Run(fmt.Sprintf("per-element=%d", out), func(b *testing.B) {
			src := data(N)
			b.SetBytes(N * 8)
			b.ReportAllocs()
			for b.Loop() {
				var acc int64
				sluice.FlatMap(sluice.Of(src, 1024), func(dst []int64, v int64) []int64 {
					for range out {
						dst = append(dst, v)
					}
					return dst
				})(func(bt sluice.Batch[int64]) bool {
					for _, v := range bt.Items {
						acc += v
					}
					return true
				})
				sink = acc
			}
			reportPerElem(b, N)
		})
	}
}
