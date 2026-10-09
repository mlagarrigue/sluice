package sluice

import (
	"iter"
	"slices"
	"testing"
)

// Split hands the same batch to every destination without copying, so a branch
// that mutates it changes what the others see. That is documented and cheap; it
// is pinned because a defensive copy would look like an improvement and would
// silently double the operator's cost.
func TestSplitSharesBatchWithoutCopying(t *testing.T) {
	src, _ := singlePass(t, []int{1, 2, 3}, 1)
	branches := Split(src, 2, func(Batch[int]) []int { return []int{0, 1} })

	next0, stop0 := pullBranch(branches[0])
	defer stop0()
	next1, stop1 := pullBranch(branches[1])
	defer stop1()

	b0, ok0 := next0()
	b1, ok1 := next1()
	if !ok0 || !ok1 {
		t.Fatal("both branches should have received the first batch")
	}
	if &b0.Items[0] != &b1.Items[0] {
		t.Fatal("branches no longer share the batch: the documented contract changed")
	}

	b0.Items[0] = 99
	if b1.Items[0] != 99 {
		t.Error("a mutation through one branch is not visible in the other")
	}
}

// pullBranch drives a branch one batch at a time, which is how concurrent
// branches must be consumed.
func pullBranch[T any](s Stream[T]) (next func() (Batch[T], bool), stop func()) {
	return iter.Pull(iter.Seq[Batch[T]](s))
}

// alternate consumes two branches in lock-step and returns what each received.
func alternate(a, b Stream[int]) (gotA, gotB []int) {
	nextA, stopA := pullBranch(a)
	defer stopA()
	nextB, stopB := pullBranch(b)
	defer stopB()

	for {
		ba, okA := nextA()
		if okA {
			gotA = append(gotA, ba.Items...)
		}
		bb, okB := nextB()
		if okB {
			gotB = append(gotB, bb.Items...)
		}
		if !okA && !okB {
			return gotA, gotB
		}
	}
}

func TestSplitPartition(t *testing.T) {
	// Each batch goes to a single branch, chosen from its first element.
	src, _ := singlePass(t, []int{1, 2, 3, 4, 5, 6}, 1)
	branches := Split(src, 2, func(b Batch[int]) []int {
		if b.Items[0]%2 == 0 {
			return []int{1}
		}
		return []int{0}
	})

	odd, even := alternate(branches[0], branches[1])
	if !slices.Equal(odd, []int{1, 3, 5}) {
		t.Errorf("odd branch = %v, want [1 3 5]", odd)
	}
	if !slices.Equal(even, []int{2, 4, 6}) {
		t.Errorf("even branch = %v, want [2 4 6]", even)
	}
}

// A partition may be consumed one branch at a time: batches routed to a branch
// nobody is reading are dropped rather than stalling the pipeline.
func TestSplitPartitionSingleBranch(t *testing.T) {
	src, _ := singlePass(t, []int{1, 2, 3, 4, 5, 6}, 1)
	branches := Split(src, 2, func(b Batch[int]) []int {
		if b.Items[0]%2 == 0 {
			return []int{1}
		}
		return []int{0}
	})

	if got := collect(branches[0]); !slices.Equal(got, []int{1, 3, 5}) {
		t.Errorf("odd branch = %v, want [1 3 5]", got)
	}
}

// The point of the rewrite: broadcast works on a source that cannot be
// replayed, and the source is traversed exactly once.
func TestSplitBroadcastSinglePass(t *testing.T) {
	src, consumed := singlePass(t, []int{1, 2, 3}, 1)
	branches := Split(src, 2, func(Batch[int]) []int { return []int{0, 1} })

	got0, got1 := alternate(branches[0], branches[1])
	want := []int{1, 2, 3}
	if !slices.Equal(got0, want) {
		t.Errorf("branch 0 = %v, want %v", got0, want)
	}
	if !slices.Equal(got1, want) {
		t.Errorf("branch 1 = %v, want %v", got1, want)
	}
	if consumed() != 3 {
		t.Errorf("source produced %d elements, want 3: it was traversed more than once", consumed())
	}
}

// Draining one branch while another is mid-consumption cannot work with a
// one-batch slot. It must fail loudly rather than yield a silent prefix.
func TestSplitStalls(t *testing.T) {
	defer func() {
		switch r := recover(); {
		case isErr(r, ErrSplitStalled):
		case r == nil:
			t.Error("draining branches sequentially should have panicked")
		default:
			t.Errorf("panicked with %v, want ErrSplitStalled", r)
		}
	}()

	src, _ := singlePass(t, []int{1, 2, 3, 4}, 1)
	branches := Split(src, 2, func(Batch[int]) []int { return []int{0, 1} })

	next1, stop1 := pullBranch(branches[1])
	defer stop1()
	next1() // branch 1 is now live, holding nothing

	collect(branches[0]) // drains branch 0 while branch 1 lags: stalls
}

// A branch is single-pass, like the stream it comes from.
func TestSplitBranchNotReplayable(t *testing.T) {
	src, _ := singlePass(t, []int{1, 2, 3}, 1)
	branches := Split(src, 1, func(Batch[int]) []int { return []int{0} })

	if got := collect(branches[0]); !slices.Equal(got, []int{1, 2, 3}) {
		t.Errorf("first pass = %v, want [1 2 3]", got)
	}
	if got := collect(branches[0]); len(got) != 0 {
		t.Errorf("second pass = %v, want nothing: a branch is single-pass", got)
	}
}

// The shared traversal must be stopped exactly once, however many branches
// finish, and only once the last live branch is gone.
func TestSplitReleasesSourceOnce(t *testing.T) {
	stops := 0
	src := Stream[int](func(yield func(Batch[int]) bool) {
		defer func() { stops++ }()
		for i := range 4 {
			if !yield(Batch[int]{Items: []int{i}}) {
				return
			}
		}
	})

	branches := Split(src, 2, func(Batch[int]) []int { return []int{0, 1} })
	got0, got1 := alternate(branches[0], branches[1])

	if len(got0) != 4 || len(got1) != 4 {
		t.Fatalf("branches got %d and %d batches, want 4 each", len(got0), len(got1))
	}
	if stops != 1 {
		t.Errorf("source finalized %d times, want exactly 1", stops)
	}
}

// A branch that stops early must not cut off a branch that has not started
// yet: the caller is allowed to finish with one branch before opening the next.
func TestSplitEarlyStopKeepsSiblingUsable(t *testing.T) {
	src, _ := singlePass(t, []int{1, 2, 3}, 1)
	branches := Split(src, 2, func(Batch[int]) []int { return []int{0, 1} })

	n := 0
	branches[0](func(Batch[int]) bool {
		n++
		return false // take one batch, then stop
	})

	if got := collect(branches[1]); !slices.Equal(got, []int{1, 2, 3}) {
		t.Errorf("sibling branch = %v, want [1 2 3]", got)
	}
}

// Abandoning a branch must still let the source be finalized, otherwise the
// pull coroutine outlives the pipeline.
func TestSplitAbandonedBranchReleasesSource(t *testing.T) {
	finalized := false
	src := Stream[int](func(yield func(Batch[int]) bool) {
		defer func() { finalized = true }()
		for i := range 100 {
			if !yield(Batch[int]{Items: []int{i}}) {
				return
			}
		}
	})

	branches := Split(src, 2, func(Batch[int]) []int { return []int{0, 1} })
	n := 0
	branches[0](func(Batch[int]) bool {
		n++
		return n < 2
	})
	// branches[1] is never consumed.

	if !finalized {
		t.Error("the source was not finalized after every live branch stopped")
	}
}

// route is the caller's function and may carry state — round-robin, the mode
// the documentation recommends, is exactly that. Calling it more than once per
// batch would advance that state twice and misroute the data.
func TestSplitRoutesOncePerBatch(t *testing.T) {
	src, _ := singlePass(t, []int{1, 2, 3, 4, 5, 6}, 1)

	calls := 0
	branches := Split(src, 2, func(Batch[int]) []int {
		calls++
		return []int{calls % 2} // round-robin: relies on being called once
	})

	got0, got1 := alternate(branches[0], branches[1])

	if calls != 6 {
		t.Errorf("route called %d times for 6 batches, want 6", calls)
	}
	if want := []int{2, 4, 6}; !slices.Equal(got0, want) {
		t.Errorf("branch 0 = %v, want %v", got0, want)
	}
	if want := []int{1, 3, 5}; !slices.Equal(got1, want) {
		t.Errorf("branch 1 = %v, want %v", got1, want)
	}
}

func TestSplitBounds(t *testing.T) {
	if got := Split(Empty[int](), 0, nil); got != nil {
		t.Error("Split with n<=0 must return nil")
	}
	// An out-of-range branch index is ignored, without panicking.
	branches := Split(Of([]int{1, 2}, 1), 1, func(Batch[int]) []int {
		return []int{0, 5, -1}
	})
	if got := collect(branches[0]); !slices.Equal(got, []int{1, 2}) {
		t.Errorf("out-of-range indices mishandled: %v", got)
	}
}

func TestSplitNilRoute(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("Split with a nil route must panic rather than dereference it")
		}
	}()
	Split(Of([]int{1}, 1), 1, nil)
}

// Split documents that every branch belongs to one goroutine, and that the
// legitimate way to drive two branches is iter.Pull. That is worth pinning
// because iter.Pull runs each branch body on its own coroutine: the branches
// genuinely execute on different goroutines, they simply never run at the same
// time. Under -race this test fails if that stops being true — which is also
// why the concurrent misuse cannot be detected from inside Split.
func TestSplitAlternationIsRaceFree(t *testing.T) {
	src, consumed := singlePass(t, []int{1, 2, 3, 4, 5, 6}, 1)
	branches := Split(src, 2, func(Batch[int]) []int { return []int{0, 1} })

	got0, got1 := alternate(branches[0], branches[1])

	want := []int{1, 2, 3, 4, 5, 6}
	if !slices.Equal(got0, want) || !slices.Equal(got1, want) {
		t.Errorf("branches = %v and %v, want %v each", got0, got1, want)
	}
	if consumed() != 6 {
		t.Errorf("source produced %d elements, want 6", consumed())
	}
}

// Consuming branches one after the other lets the first drain the source, and
// every later branch would yield nothing at all — the silent prefix this
// operator exists to remove, simply moved from broadcast to partition. It must
// be reported, not returned.
func TestSplitLateBranchPanics(t *testing.T) {
	tests := []struct {
		name string
		// order is the branches to drain to exhaustion, one after the other.
		// The first drains the source; the second must be refused.
		order []int
	}{
		{"two branches, reverse order", []int{1, 0}},
		{"three branches, one after another", []int{1, 2}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				switch r := recover(); {
				case isErr(r, ErrSplitDrained):
				case r == nil:
					t.Error("a late branch yielded nothing instead of panicking")
				default:
					t.Errorf("panicked with %v, want ErrSplitDrained", r)
				}
			}()

			src, _ := singlePass(t, []int{1, 2, 3, 4, 5, 6}, 1)
			branches := Split(src, 3, func(b Batch[int]) []int {
				return []int{b.Items[0] % 3}
			})
			for _, i := range tt.order {
				collect(branches[i])
			}
		})
	}
}

// The panic above must not fire on a branch that is empty for a legitimate
// reason. Each case here consumes branches in a supported way and must stay
// silent — this is what keeps ErrSplitDrained from becoming a nuisance.
func TestSplitLateBranchFalsePositives(t *testing.T) {
	tests := []struct {
		name string
		// values feeds the source; empty means a source that yields nothing.
		values []int
		// run consumes the branches and returns what each one received, so the
		// case asserts on the data as well as on the absence of a panic.
		run func(branches []Stream[int]) [][]int
		// route wires the split; nil means broadcast to both branches.
		route func(Batch[int]) []int
		// want is the expected content of each returned branch.
		want [][]int
	}{
		{
			name:   "empty source: nothing was ever routed",
			values: nil,
			run: func(b []Stream[int]) [][]int {
				return [][]int{collect(b[0]), collect(b[1])}
			},
			want: [][]int{nil, nil},
		},
		{
			name:   "a branch consumed twice is single-pass, not late",
			values: []int{1, 2, 3},
			run: func(b []Stream[int]) [][]int {
				return [][]int{collect(b[0]), collect(b[0])}
			},
			want: [][]int{{1, 2, 3}, nil},
		},
		{
			name:   "early stop leaves the sibling usable",
			values: []int{1, 2, 3},
			run: func(b []Stream[int]) [][]int {
				b[0](func(Batch[int]) bool { return false })
				return [][]int{collect(b[1])}
			},
			want: [][]int{{1, 2, 3}},
		},
		{
			// The branch route never chose is empty for the same reason every
			// branch over an empty source is: nothing was addressed to it. It
			// is not late, and reporting it would turn the documented partition
			// mode — where a branch matching nothing is routine — into a crash.
			name:   "a branch route never chose was never a destination",
			values: []int{1, 2, 3},
			run: func(b []Stream[int]) [][]int {
				return [][]int{collect(b[0]), collect(b[1])}
			},
			route: func(Batch[int]) []int { return []int{0} },
			want:  [][]int{{1, 2, 3}, nil},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("panicked with %v on a legitimate consumption", r)
				}
			}()

			route := tt.route
			if route == nil {
				route = func(Batch[int]) []int { return []int{0, 1} }
			}

			src, _ := singlePass(t, tt.values, 1)
			branches := Split(src, 2, route)

			got := tt.run(branches)
			if len(got) != len(tt.want) {
				t.Fatalf("got %d branches, want %d", len(got), len(tt.want))
			}
			for i := range got {
				if !slices.Equal(got[i], tt.want[i]) {
					t.Errorf("branch %d = %v, want %v", i, got[i], tt.want[i])
				}
			}
		})
	}
}

// A partition read one branch at a time is the documented single-branch use,
// and it must stay silent: only the branch actually consumed receives anything.
func TestSplitSingleBranchOfPartitionIsSilent(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("panicked with %v on the documented single-branch use", r)
		}
	}()

	src, _ := singlePass(t, []int{1, 2, 3, 4, 5, 6}, 1)
	branches := Split(src, 2, func(b Batch[int]) []int {
		if b.Items[0]%2 == 0 {
			return []int{1}
		}
		return []int{0}
	})

	if got := collect(branches[0]); !slices.Equal(got, []int{1, 3, 5}) {
		t.Errorf("got %v, want [1 3 5]", got)
	}
}

// A partition branch that matches nothing is empty because nothing was
// addressed to it, not because it arrived late — the same reason every branch
// over an empty source is empty. Reading it after its sibling must stay silent.
//
// This is the case that decides the grain of the check: whether the source
// produced anything says nothing about whether *this* branch was ever a
// destination, so lateness is tracked per branch.
func TestSplitUnmatchedPartitionBranchIsSilent(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("panicked with %v on a branch route never chose", r)
		}
	}()

	// Only odd values: branch 1 is never a destination.
	src, _ := singlePass(t, []int{1, 3, 5}, 1)
	branches := Split(src, 2, func(b Batch[int]) []int {
		if b.Items[0]%2 == 0 {
			return []int{1}
		}
		return []int{0}
	})

	if got := collect(branches[0]); !slices.Equal(got, []int{1, 3, 5}) {
		t.Errorf("matched branch = %v, want [1 3 5]", got)
	}
	if got := collect(branches[1]); len(got) != 0 {
		t.Errorf("unmatched branch = %v, want nothing", got)
	}
}

// Reading the unmatched branch first is a different story: it drives the source
// to exhaustion, and the batches addressed to its sibling are written off as
// they arrive. The sibling would then yield nothing despite having been a
// destination — real data loss, so it is still reported.
//
// The pair with the test above is the point: the same wiring is silent or loud
// depending on whether data was actually lost, which is what the check is for.
func TestSplitUnmatchedBranchFirstStillReportsLoss(t *testing.T) {
	defer func() {
		switch r := recover(); {
		case isErr(r, ErrSplitDrained):
		case r == nil:
			t.Error("the sibling yielded nothing instead of panicking: its batches were dropped")
		default:
			t.Errorf("panicked with %v, want ErrSplitDrained", r)
		}
	}()

	src, _ := singlePass(t, []int{1, 3, 5}, 1)
	branches := Split(src, 2, func(b Batch[int]) []int {
		if b.Items[0]%2 == 0 {
			return []int{1}
		}
		return []int{0}
	})

	if got := collect(branches[1]); len(got) != 0 {
		t.Errorf("unmatched branch = %v, want nothing", got)
	}
	collect(branches[0]) // had three batches addressed to it: must not be silent
}

// A batch routed nowhere stops nothing: the remaining batches still flow.
func TestSplitRouteToNothing(t *testing.T) {
	src, _ := singlePass(t, []int{1, 2, 3, 4}, 1)
	branches := Split(src, 1, func(b Batch[int]) []int {
		if b.Items[0]%2 == 0 {
			return nil // drop even batches
		}
		return []int{0}
	})

	if got := collect(branches[0]); !slices.Equal(got, []int{1, 3}) {
		t.Errorf("got %v, want [1 3]", got)
	}
}
