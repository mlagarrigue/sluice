package probe

import (
	"slices"
	"sync"
	"testing"

	"github.com/mlagarrigue/sluice"
)

func TestProbeCounts(t *testing.T) {
	src := []int{1, 2, 3, 4, 5, 6, 7}
	var c Counter

	got := sluice.Collect(Probe(sluice.Of(src, 3), &c))

	if !slices.Equal(got, src) {
		t.Errorf("Probe altered the stream: %v, want %v", got, src)
	}
	if c.Batches() != 3 {
		t.Errorf("Batches() = %d, want 3", c.Batches())
	}
	if c.Elements() != 7 {
		t.Errorf("Elements() = %d, want 7", c.Elements())
	}
}

// A refused batch is not counted: the counter means "accepted downstream",
// which is what makes the lag between two probes exact.
func TestProbeCountsAcceptedOnly(t *testing.T) {
	var c Counter
	n := 0
	Probe(sluice.Of(make([]int, 10), 2), &c)(func(sluice.Batch[int]) bool {
		n++
		return n < 3 // accept 2 batches, refuse the 3rd
	})
	if c.Batches() != 2 {
		t.Errorf("Batches() = %d, want 2 — the refused batch must not count", c.Batches())
	}
	if c.Elements() != 4 {
		t.Errorf("Elements() = %d, want 4", c.Elements())
	}
}

// Empty batches keep the upstream cadence and are counted as batches carrying
// zero elements — the batch count and the element count answer different
// questions.
func TestProbeCountsEmptyBatches(t *testing.T) {
	var c Counter
	filtered := sluice.Filter(sluice.Of([]int{1, 2, 3, 4}, 2), func(int) bool { return false })
	sluice.Collect(Probe(filtered, &c))
	if c.Batches() != 2 || c.Elements() != 0 {
		t.Errorf("Batches, Elements = %d, %d, want 2, 0", c.Batches(), c.Elements())
	}
}

// The counter is read from another goroutine while the pipeline writes it —
// the ticker shape from the package documentation. -race is the assertion.
func TestProbeConcurrentReads(t *testing.T) {
	var c Counter
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			select {
			case <-done:
				return
			default:
				_ = c.Batches()
				_ = c.Elements()
			}
		}
	})

	sluice.Collect(Probe(sluice.Of(make([]int, 1<<12), 64), &c))
	close(done)
	wg.Wait()

	if c.Elements() != 1<<12 {
		t.Errorf("Elements() = %d, want %d", c.Elements(), 1<<12)
	}
}

func TestProbeNilCounterPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("nil Counter: no panic")
		}
	}()
	Probe(sluice.Empty[int](), nil)
}
