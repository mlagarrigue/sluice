package bench

import (
	"fmt"
	"testing"

	"github.com/mlagarrigue/sluice"
)

// FlatMap takes the single-element result off the variadic path, so the shapes
// that still use it need their own figure: a branch that helps the common case
// and quietly taxes the others is not an improvement, it is a trade nobody
// measured.
func BenchmarkFlatMapShapes(b *testing.B) {
	for _, out := range []int{0, 1, 2, 4} {
		b.Run(fmt.Sprintf("per-element=%d", out), func(b *testing.B) {
			src := data(N)
			b.SetBytes(N * 8)
			b.ReportAllocs()
			for b.Loop() {
				scratch := make([]int64, out)
				var acc int64
				sluice.FlatMap(sluice.Of(src, 1024), func(v int64) []int64 {
					for i := range scratch {
						scratch[i] = v
					}
					return scratch
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
