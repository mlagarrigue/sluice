package window

import (
	"slices"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/internal/streamtest"
)

// FuzzCoalesceWithinConserves checks that CoalesceWithin neither loses,
// duplicates nor reorders an element, and never emits an empty or oversized
// batch, over irregular upstream batches with empty ones mixed in.
//
// The deadline is either one that never fires during the test or one so short
// that it fires between most upstream batches. How the output is cut then
// depends on scheduling; what is checked does not, which is what keeps the
// target deterministic.
//
// An early stop after a random number of output batches must leave a prefix
// of the input: whatever was emitted before the stop is still exactly the
// input's beginning.
func FuzzCoalesceWithinConserves(f *testing.F) {
	f.Add(uint64(1), uint16(500), uint8(5), uint8(16), false, uint8(0))
	f.Add(uint64(2), uint16(500), uint8(5), uint8(16), true, uint8(0))
	f.Add(uint64(3), uint16(0), uint8(3), uint8(1), true, uint8(0))
	f.Add(uint64(4), uint16(2000), uint8(64), uint8(7), true, uint8(3))
	f.Add(uint64(5), uint16(300), uint8(1), uint8(1), false, uint8(2))

	f.Fuzz(func(t *testing.T, seed uint64, nn uint16, maxBatch, size uint8, short bool, stopAfter uint8) {
		rng := streamtest.NewRand(seed)
		n := int(nn % 4096)
		src := make([]int, n)
		for i := range src {
			src[i] = i
		}
		within := time.Hour
		if short {
			within = time.Microsecond
		}
		sz := int(size%32) + 1

		var got []int
		batches := 0
		CoalesceWithin(streamtest.Chunks(src, int(maxBatch), rng), sz, within)(func(b sluice.Batch[int]) bool {
			if len(b.Items) == 0 || len(b.Items) > sz {
				t.Errorf("batch of %d elements, want 1..%d", len(b.Items), sz)
			}
			got = append(got, b.Items...)
			batches++
			return stopAfter == 0 || batches < int(stopAfter)
		})

		if stopAfter == 0 {
			if !slices.Equal(got, src) {
				t.Fatalf("got %d elements %v, want the %d input elements in order", len(got), got, n)
			}
			return
		}
		if len(got) > n || !slices.Equal(got, src[:len(got)]) {
			t.Fatalf("after an early stop got %v, not a prefix of the input", got)
		}
	})
}
