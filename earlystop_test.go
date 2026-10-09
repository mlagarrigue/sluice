package sluice_test

import (
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/distinct"
	"github.com/mlagarrigue/sluice/join"
	"github.com/mlagarrigue/sluice/parallel"
	"github.com/mlagarrigue/sluice/window"
)

// S10 — every operator propagates false and lets the generator unwind.
//
// The specification calls this streaming's problem number one, ahead of
// performance, and says it is a tested invariant rather than a convention. It
// was neither for half of these: they behaved correctly by accident of how they
// were written, which is not the same as being guaranteed to.
//
// The assertion is threefold. The consumer's refusal must reach the source, so
// it stops producing; the source must be allowed to return normally, so its
// deferred cleanup runs — a file handle left open for the rest of the pipeline
// is the failure the conduit author documented; and no operator may swallow the
// refusal and carry on.
func TestOperatorsPropagateEarlyStop(t *testing.T) {
	// One entry per operator taking a stream and returning one. Adding an
	// operator without adding it here is the omission this test exists to make
	// visible.
	ops := []struct {
		name string
		wrap func(sluice.Stream[int]) sluice.Stream[int]
	}{
		{"sluice.Map", func(s sluice.Stream[int]) sluice.Stream[int] { return sluice.Map(s, func(v int) int { return v }) }},
		{"sluice.Filter", func(s sluice.Stream[int]) sluice.Stream[int] { return sluice.Filter(s, func(int) bool { return true }) }},
		{"sluice.FlatMap", func(s sluice.Stream[int]) sluice.Stream[int] {
			return sluice.FlatMap(s, func(v int) []int { return []int{v} })
		}},
		{"sluice.Peek", func(s sluice.Stream[int]) sluice.Stream[int] { return sluice.Peek(s, func(int) {}) }},
		{"sluice.Scan", func(s sluice.Stream[int]) sluice.Stream[int] {
			return sluice.Scan(s, 0, func(a, v int) int { return a + v })
		}},
		{"sluice.Take", func(s sluice.Stream[int]) sluice.Stream[int] { return sluice.Take(s, 1000) }},
		{"sluice.Drop", func(s sluice.Stream[int]) sluice.Stream[int] { return sluice.Drop(s, 0) }},
		{"sluice.TakeWhile", func(s sluice.Stream[int]) sluice.Stream[int] {
			return sluice.TakeWhile(s, func(int) bool { return true })
		}},
		{"sluice.DropWhile", func(s sluice.Stream[int]) sluice.Stream[int] {
			return sluice.DropWhile(s, func(int) bool { return false })
		}},
		{"sluice.Coalesce", func(s sluice.Stream[int]) sluice.Stream[int] { return sluice.Coalesce(s, 2) }},
		{"sluice.Concat/only", func(s sluice.Stream[int]) sluice.Stream[int] { return sluice.Concat(s) }},
		{"sluice.Concat/first", func(s sluice.Stream[int]) sluice.Stream[int] {
			return sluice.Concat(s, sluice.Of([]int{97, 98, 99}, 1))
		}},
		{"sluice.Merge/all", func(s sluice.Stream[int]) sluice.Stream[int] { return sluice.Merge(sluice.WhenAll, s) }},
		{"sluice.Merge/any", func(s sluice.Stream[int]) sluice.Stream[int] { return sluice.Merge(sluice.WhenAny, s) }},
		// MergeJoinBy is absent on purpose: it fills an output batch before
		// yielding anything, so the first refusal comes late. Its own
		// propagation is covered by TestMergeJoinByEarlyStopEveryBranch.
		{"sluice.Split", func(s sluice.Stream[int]) sluice.Stream[int] {
			return sluice.Split(s, 1, func(sluice.Batch[int]) []int { return []int{0} })[0]
		}},
		// One window per element, one second apart: element i closes pane
		// i-1, so the first pane goes out on the second element and the
		// refusal reaches the source within the two-batch allowance.
		{"Window", func(s sluice.Stream[int]) sluice.Stream[int] {
			panes := window.Tumbling(s, func(v int) time.Time { return time.Unix(int64(v), 0) },
				time.Second, 16, sluice.Fail)
			return sluice.Convert(panes, func(p window.Pane[int]) int { return p.Start.Second() })
		}},
		{"Distinct", func(s sluice.Stream[int]) sluice.Stream[int] {
			return distinct.By(s, 1024, func(v int) int { return v }, sluice.DropOldest)
		}},
		// parallel.Ordered(1) is the inline path; the concurrent one has its own
		// early-stop test, where the assertion has to allow for read-ahead.
		{"parallel.Ordered(1)", func(s sluice.Stream[int]) sluice.Stream[int] {
			return parallel.Ordered(s, 1, func(b sluice.Batch[int]) sluice.Batch[int] { return b })
		}},
		// The stream drives the probe side; the build side is materialised
		// before the first yield and is not what the refusal has to reach.
		// Output is re-batched through the emitter, so the refusal reaches
		// the probe at batch boundaries: the build key carries more rows
		// than sluice.DefaultBatchSize so a single probe element fills a batch and
		// the refusal arrives on the source's very next yield.
		{"JoinStreamTable", func(s sluice.Stream[int]) sluice.Stream[int] {
			build := make([]int, sluice.DefaultBatchSize+1)
			return join.StreamTable(s, sluice.Of(build, sluice.DefaultBatchSize),
				func(v int) int { return 0 },
				func(v int) int { return 0 },
				func(a, b int) int { return a },
				join.BuildLimit{MaxEntries: len(build), OnOverflow: sluice.Fail})
		}},
		{"identity", func(s sluice.Stream[int]) sluice.Stream[int] { return s }},
	}

	for _, op := range ops {
		t.Run(op.name, func(t *testing.T) {
			released := false
			produced := 0
			src := sluice.Stream[int](func(yield func(sluice.Batch[int]) bool) {
				defer func() { released = true }()
				for i := range 1000 {
					produced++
					if !yield(sluice.Batch[int]{Items: []int{i}}) {
						return
					}
				}
			})

			batches := 0
			op.wrap(src)(func(sluice.Batch[int]) bool {
				batches++
				return false
			})

			if batches != 1 {
				t.Errorf("consumer saw %d batches after refusing the first, want 1", batches)
			}
			// sluice.Coalesce buffers, so it may pull one batch beyond what it emitted;
			// what must not happen is the source running to completion.
			if produced > 2 {
				t.Errorf("source produced %d batches, want at most 2: the refusal did not reach it", produced)
			}
			if !released {
				t.Error("the source never unwound: its deferred cleanup did not run")
			}
		})
	}
}
