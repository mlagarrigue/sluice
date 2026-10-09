package parallel

import (
	"fmt"
	"runtime"
	"slices"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/mlagarrigue/sluice"
)

func TestParallelUnordered(t *testing.T) {
	src := make([]int, 100)
	for i := range src {
		src[i] = i
	}
	got := sluice.Collect(Unordered(sluice.Of(src, 8), 4, func(b sluice.Batch[int]) sluice.Batch[int] {
		out := make([]int, len(b.Items))
		for i, v := range b.Items {
			out[i] = v * 3
		}
		return sluice.Batch[int]{Items: out}
	}))

	want := make([]int, 100)
	for i := range want {
		want[i] = i * 3
	}
	// The contract is a multiset, not a sequence: sort both before comparing.
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Errorf("got %v, want the same multiset as %v", got, want)
	}
}

// One worker means no reordering to avoid: the inline no-op preserves order
// and agrees with Ordered.
func TestParallelUnorderedOneIsInlineAndOrdered(t *testing.T) {
	src := []int{5, 3, 8, 1}
	before := runtime.NumGoroutine()
	// The count is sampled from inside f, while the operator is running —
	// the same probe as TestParallelOneIsInline: sampled after sluice.Collect
	// returns, a worker pool would already have been joined and the
	// assertion could never fail.
	double := func(b sluice.Batch[int]) sluice.Batch[int] {
		if now := runtime.NumGoroutine(); now > before {
			t.Errorf("the inline path spawned a goroutine: %d -> %d", before, now)
		}
		out := make([]int, len(b.Items))
		for i, v := range b.Items {
			out[i] = v * 2
		}
		return sluice.Batch[int]{Items: out}
	}
	got := sluice.Collect(Unordered(sluice.Of(src, 2), 1, double))
	if want := []int{10, 6, 16, 2}; !slices.Equal(got, want) {
		t.Errorf("got %v, want %v in order", got, want)
	}
}

// The S14 composition: unordered fan-out downstream of a buffer-reusing
// upstream must be race-free and sum correctly.
func TestParallelUnorderedCopiesInputBatch(t *testing.T) {
	const n = 1 << 14
	src := make([]int64, n)
	var want int64
	for i := range src {
		src[i] = int64(i)
		want += int64(i)
	}
	conv := sluice.Convert(sluice.Of(src, 512), func(v int64) int64 { return v })

	var got int64
	Unordered(conv, 4, func(b sluice.Batch[int64]) sluice.Batch[int64] {
		var s int64
		for _, v := range b.Items {
			s += v
		}
		return sluice.Batch[int64]{Items: []int64{s}}
	})(func(b sluice.Batch[int64]) bool {
		for _, v := range b.Items {
			got += v
		}
		return true
	})
	if got != want {
		t.Fatalf("totals differ: got %d want %d", got, want)
	}
}

// Early stop, panic in f, panic in the consumer: every path must release the
// workers and the source before the call returns (S6, S10).
func TestParallelUnorderedReleasesOnEveryPath(t *testing.T) {
	tests := []struct {
		name string
		run  func(s sluice.Stream[int])
	}{
		{"early stop", func(s sluice.Stream[int]) {
			n := 0
			Unordered(s, 4, func(b sluice.Batch[int]) sluice.Batch[int] { return sluice.Batch[int]{Items: []int{1}} })(
				func(sluice.Batch[int]) bool {
					n++
					return n < 2
				})
		}},
		{"panic in f", func(s sluice.Stream[int]) {
			defer func() {
				if recover() == nil {
					t.Error("the panic in f never surfaced")
				}
			}()
			sluice.Collect(Unordered(s, 4, func(b sluice.Batch[int]) sluice.Batch[int] { panic("f boom") }))
		}},
		{"panic in consumer", func(s sluice.Stream[int]) {
			defer func() { _ = recover() }()
			Unordered(s, 4, func(b sluice.Batch[int]) sluice.Batch[int] { return sluice.Batch[int]{Items: []int{1}} })(
				func(sluice.Batch[int]) bool { panic("consumer boom") })
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				before := runtime.NumGoroutine()
				finalized := false
				src := sluice.Stream[int](func(yield func(sluice.Batch[int]) bool) {
					defer func() { finalized = true }()
					for i := range 500 {
						if !yield(sluice.Batch[int]{Items: []int{i}}) {
							return
						}
					}
				})
				tt.run(src)
				if !finalized {
					t.Error("the source's deferred cleanup did not run")
				}
				if got := goroutinesSettle(t); got > before {
					t.Errorf("%d goroutines before, %d after", before, got)
				}
			})
		})
	}
}

func TestParallelUnorderedNilFunction(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("nil f: no panic")
		}
	}()
	Unordered[int, int](sluice.Empty[int](), 2, nil)
}

// The consumer refusing a batch during the tail drain — after the source ran
// out, with work still in flight — ends the emission there, and a panic in
// the work left behind is re-raised rather than swallowed with its results.
//
// The order is forced, not hoped for: batch 0's f waits until the source has
// fed both batches, so nothing is emitted before the tail drain, and batch 1's
// f waits until the consumer has refused batch 0.
func TestParallelUnorderedStopDuringTailDrain(t *testing.T) {
	for _, panics := range []bool{false, true} {
		t.Run(fmt.Sprintf("panic in flight %v", panics), func(t *testing.T) {
			fed := make(chan struct{})
			refused := make(chan struct{})
			src := sluice.Stream[int](func(yield func(sluice.Batch[int]) bool) {
				defer close(fed)
				for i := range 2 {
					if !yield(sluice.Batch[int]{Items: []int{i}}) {
						return
					}
				}
			})

			var live atomic.Int64
			var seen []int
			var recovered any
			func() {
				defer func() { recovered = recover() }()
				Unordered(src, 2, func(b sluice.Batch[int]) sluice.Batch[int] {
					live.Add(1)
					defer live.Add(-1)
					if b.Items[0] == 0 {
						<-fed
						return sluice.Batch[int]{Items: []int{0}}
					}
					<-refused
					if panics {
						panic("in flight")
					}
					return sluice.Batch[int]{Items: []int{1}}
				})(func(b sluice.Batch[int]) bool {
					seen = append(seen, b.Items...)
					close(refused)
					return false
				})
			}()

			if !slices.Equal(seen, []int{0}) {
				t.Errorf("consumer saw %v, want [0] and nothing after its refusal", seen)
			}
			want := any(nil)
			if panics {
				want = "in flight"
			}
			if recovered != want {
				t.Errorf("recovered %v, want %v", recovered, want)
			}
			if n := live.Load(); n != 0 {
				t.Errorf("%d workers still inside f when the call returned", n)
			}
		})
	}
}
