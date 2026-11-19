package parallel_test

import (
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/parallel"
)

// An f that returns the batch it was handed — or a view of it — returns its
// input slot, which Unordered keeps until the result is emitted, as Ordered
// does. Unordered freed the slot before it shipped the result, and the
// producer refilled it with the next input before the consumer read it: one
// batch lost and another seen twice, with no panic. The overwrite depends on
// scheduling, so the test plays several rounds.
func TestUnorderedKeepsTheSlotOfAnInputItsFReturned(t *testing.T) {
	keepsTheSlot(t, parallel.Unordered[int, int])
}

// Ordered keeps each input until its result's turn to be emitted, so the
// same f is safe there too, as its documentation says.
func TestOrderedKeepsTheSlotOfAnInputItsFReturned(t *testing.T) {
	keepsTheSlot(t, parallel.Ordered[int, int])
}

func keepsTheSlot(t *testing.T, op func(sluice.Stream[int], int, func(sluice.Batch[int]) sluice.Batch[int]) sluice.Stream[int]) {
	t.Helper()
	const batches, size = 5000, 4
	src := make([]int, batches*size)
	for i := range src {
		src[i] = i
	}
	passThrough := func(b sluice.Batch[int]) sluice.Batch[int] {
		for t0 := time.Now(); time.Since(t0) < 20*time.Microsecond; {
		}
		return b
	}
	for round := range 20 {
		seen := make(map[int]int, batches)
		for b := range op(sluice.Of(src, size), 4, passThrough) {
			for i, v := range b.Items {
				if v != b.Items[0]+i {
					t.Fatalf("round %d: batch %v is not a run of the input", round, b.Items)
				}
			}
			seen[b.Items[0]]++
		}
		if len(seen) != batches {
			t.Fatalf("round %d: %d distinct batches out of %d: a batch was overwritten before it was read", round, len(seen), batches)
		}
	}
}
