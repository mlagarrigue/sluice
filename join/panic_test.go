package join

import (
	"cmp"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
)

// S9 of the architecture's security properties: every internal iter.Pull
// calls stop() on every path, a panic in the consumer included. Each
// operator below pulls both its inputs; a consumer that panics mid-stream
// must leave both released — their deferred calls run on the way out — or
// the pull coroutines outlive the pipeline.

// counted yields 0, 1, 2… in batches of 64, five output batches' worth, and
// marks *closed when its traversal ends: an operator that panics at its
// first output batch is then mid-stream on both sides.
func counted(closed *bool) sluice.Stream[int] {
	return func(yield func(sluice.Batch[int]) bool) {
		defer func() { *closed = true }()
		for start := 0; start < 5*sluice.DefaultBatchSize; start += 64 {
			items := make([]int, 64)
			for i := range items {
				items[i] = start + i
			}
			if !yield(sluice.Batch[int]{Items: items}) {
				return
			}
		}
	}
}

// panicsAtFirst drains s and panics in its consumer at the first batch; it
// reports whether the panic came back out.
func panicsAtFirst[T any](s sluice.Stream[T]) (propagated bool) {
	defer func() { propagated = recover() != nil }()
	s(func(sluice.Batch[T]) bool { panic("consumer failure") })
	return false
}

func TestPullingOperatorsReleaseBothInputsOnPanic(t *testing.T) {
	id := func(n int) int { return n }
	at := func(n int) time.Time { return time.Unix(int64(n), 0) }
	for _, tc := range []struct {
		name string
		join func(l, r sluice.Stream[int]) func() bool
	}{
		{"Merge", func(l, r sluice.Stream[int]) func() bool {
			return func() bool { return panicsAtFirst(Merge(l, r, id, id, cmp.Compare[int])) }
		}},
		{"ZipLongest", func(l, r sluice.Stream[int]) func() bool {
			return func() bool { return panicsAtFirst(ZipLongest(l, r)) }
		}},
		{"Interval", func(l, r sluice.Stream[int]) func() bool {
			spec := IntervalSpec[int, int, int, int]{
				KeyL: func(int) int { return 0 }, KeyR: func(int) int { return 0 },
				TimeL: at, TimeR: at,
				Lower: -time.Second, Upper: time.Second,
				Merge: func(l, r int) int { return l + r },
			}
			return func() bool {
				return panicsAtFirst(Interval(l, r, spec, BuildLimit{MaxEntries: 1000, OnOverflow: sluice.Fail}))
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var closedL, closedR bool
			if !tc.join(counted(&closedL), counted(&closedR))() {
				t.Fatal("the consumer's panic did not propagate")
			}
			if !closedL || !closedR {
				t.Errorf("after the consumer panicked: left released %v, right released %v; want both", closedL, closedR)
			}
		})
	}
}
