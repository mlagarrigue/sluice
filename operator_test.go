package sluice

import (
	"slices"
	"strconv"
	"testing"
)

func TestFlatMap(t *testing.T) {
	tests := []struct {
		name string
		src  []int
		f    func(int) []string
		want []string
	}{
		{
			"expands each element",
			[]int{1, 2},
			func(v int) []string { return []string{strconv.Itoa(v), strconv.Itoa(v * 10)} },
			[]string{"1", "10", "2", "20"},
		},
		{
			"an empty slice drops the element",
			[]int{1, 2, 3},
			func(v int) []string {
				if v%2 == 0 {
					return nil
				}
				return []string{strconv.Itoa(v)}
			},
			[]string{"1", "3"},
		},
		{
			"everything dropped",
			[]int{1, 2, 3},
			func(int) []string { return nil },
			nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := Collect(FlatMap(Of(tc.src, 2), tc.f))
			if !slices.Equal(got, tc.want) {
				t.Errorf("FlatMap = %v, want %v", got, tc.want)
			}
		})
	}
}

// The expansion of one input batch stays in one output batch: fragmenting it
// would undo the batching the operator is supposed to preserve.
func TestFlatMapDoesNotFragment(t *testing.T) {
	var sizes []int
	FlatMap(Of([]int{1, 2, 3}, 3), func(v int) []int {
		return []int{v, v, v}
	})(func(b Batch[int]) bool {
		sizes = append(sizes, b.Len())
		return true
	})
	if want := []int{9}; !slices.Equal(sizes, want) {
		t.Errorf("batch sizes = %v, want %v: one input batch must expand into one output batch", sizes, want)
	}
}

// f may return a scratch slice it reuses; FlatMap copies out of it, so that
// must not corrupt the output.
func TestFlatMapCopiesFromReusedSlice(t *testing.T) {
	scratch := make([]int, 2)
	got := Collect(FlatMap(Of([]int{1, 2, 3}, 3), func(v int) []int {
		scratch[0], scratch[1] = v, -v
		return scratch
	}))
	if want := []int{1, -1, 2, -2, 3, -3}; !slices.Equal(got, want) {
		t.Errorf("FlatMap over a reused slice = %v, want %v", got, want)
	}
}

func TestPeek(t *testing.T) {
	var seen []int
	src := []int{1, 2, 3, 4}
	got := Collect(Peek(Of(src, 3), func(v int) { seen = append(seen, v) }))

	if !slices.Equal(seen, src) {
		t.Errorf("Peek saw %v, want %v", seen, src)
	}
	if !slices.Equal(got, src) {
		t.Errorf("Peek passed through %v, want %v: it must not change the stream", got, src)
	}
}

func TestScan(t *testing.T) {
	tests := []struct {
		name string
		src  []int
		want []int
	}{
		{"running sum", []int{1, 2, 3, 4}, []int{1, 3, 6, 10}},
		{"empty stream", nil, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := Collect(Scan(Of(tc.src, 3), 0, func(a, v int) int { return a + v }))
			if !slices.Equal(got, tc.want) {
				t.Errorf("Scan = %v, want %v", got, tc.want)
			}
		})
	}
}

// The accumulator must survive the batch boundary — that is the only part of
// Scan a batched implementation can get wrong.
func TestScanCarriesAcrossBatches(t *testing.T) {
	got := Collect(Scan(Of([]int{1, 1, 1, 1, 1}, 2), 0, func(a, v int) int { return a + v }))
	if want := []int{1, 2, 3, 4, 5}; !slices.Equal(got, want) {
		t.Errorf("Scan across batches = %v, want %v", got, want)
	}
}

func TestTake(t *testing.T) {
	tests := []struct {
		name string
		n    int
		want []int
	}{
		{"cuts inside a batch", 3, []int{1, 2, 3}},
		{"on a batch boundary", 4, []int{1, 2, 3, 4}},
		{"more than the stream holds", 99, []int{1, 2, 3, 4, 5, 6}},
		{"zero", 0, nil},
		{"negative", -1, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := Collect(Take(Of([]int{1, 2, 3, 4, 5, 6}, 4), tc.n))
			if !slices.Equal(got, tc.want) {
				t.Errorf("Take(%d) = %v, want %v", tc.n, got, tc.want)
			}
		})
	}
}

// Take must stop the source once it has its elements, or it does not bound an
// infinite stream and Collect(Take(s, n)) never returns.
func TestTakeStopsInfiniteSource(t *testing.T) {
	produced := 0
	released := false
	infinite := Stream[int](func(yield func(Batch[int]) bool) {
		defer func() { released = true }()
		for i := 0; ; i++ {
			produced++
			if !yield(Batch[int]{Items: []int{i}}) {
				return
			}
		}
	})

	got := Collect(Take(infinite, 3))
	if want := []int{0, 1, 2}; !slices.Equal(got, want) {
		t.Errorf("Take over an infinite source = %v, want %v", got, want)
	}
	if produced != 3 {
		t.Errorf("source produced %d batches, want 3: Take must stop it, not filter it", produced)
	}
	if !released {
		t.Error("Take did not let the source unwind")
	}
}

// Take must not touch the source at all when n <= 0: the bound is zero, so the
// work is zero.
func TestTakeZeroTouchesNothing(t *testing.T) {
	touched := false
	src := Stream[int](func(yield func(Batch[int]) bool) {
		touched = true
		yield(Batch[int]{Items: []int{1}})
	})
	if got := Collect(Take(src, 0)); got != nil {
		t.Errorf("Take(0) = %v, want nothing", got)
	}
	if touched {
		t.Error("Take(0) ran the source")
	}
}

// A consumer stopping early inside a Take must stop the source too: Take's own
// bound must not mask the consumer's refusal (S10).
func TestTakeForwardsConsumerStop(t *testing.T) {
	released := false
	src := Stream[int](func(yield func(Batch[int]) bool) {
		defer func() { released = true }()
		for i := range 100 {
			if !yield(Batch[int]{Items: []int{i}}) {
				return
			}
		}
	})

	n := 0
	Take(src, 50)(func(Batch[int]) bool {
		n++
		return false
	})

	if n != 1 {
		t.Errorf("consumer saw %d batches, want 1", n)
	}
	if !released {
		t.Error("a consumer stop inside Take did not reach the source")
	}
}

func TestDrop(t *testing.T) {
	tests := []struct {
		name string
		n    int
		want []int
	}{
		{"starts inside a batch", 3, []int{4, 5, 6}},
		{"on a batch boundary", 4, []int{5, 6}},
		{"more than the stream holds", 99, nil},
		{"zero passes everything", 0, []int{1, 2, 3, 4, 5, 6}},
		{"negative passes everything", -1, []int{1, 2, 3, 4, 5, 6}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := Collect(Drop(Of([]int{1, 2, 3, 4, 5, 6}, 4), tc.n))
			if !slices.Equal(got, tc.want) {
				t.Errorf("Drop(%d) = %v, want %v", tc.n, got, tc.want)
			}
		})
	}
}

// Drop holds the upstream cadence: a fully skipped batch is emitted empty
// rather than swallowed, like Filter's.
func TestDropKeepsCadence(t *testing.T) {
	var sizes []int
	Drop(Of([]int{1, 2, 3, 4, 5, 6}, 2), 3)(func(b Batch[int]) bool {
		sizes = append(sizes, b.Len())
		return true
	})
	if want := []int{0, 1, 2}; !slices.Equal(sizes, want) {
		t.Errorf("batch sizes = %v, want %v", sizes, want)
	}
}

func TestTakeWhile(t *testing.T) {
	tests := []struct {
		name string
		keep func(int) bool
		want []int
	}{
		{"stops inside a batch", func(v int) bool { return v < 3 }, []int{1, 2}},
		{"stops at the first element", func(v int) bool { return false }, nil},
		{"keeps everything", func(int) bool { return true }, []int{1, 2, 3, 4, 5, 6}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := Collect(TakeWhile(Of([]int{1, 2, 3, 4, 5, 6}, 4), tc.keep))
			if !slices.Equal(got, tc.want) {
				t.Errorf("TakeWhile = %v, want %v", got, tc.want)
			}
		})
	}
}

// Once the predicate fails, the element is neither emitted nor re-tested, and
// nothing after it is tested either.
func TestTakeWhileStopsCallingKeep(t *testing.T) {
	calls := 0
	got := Collect(TakeWhile(Of([]int{1, 2, 3, 4, 5, 6}, 6), func(v int) bool {
		calls++
		return v < 3
	}))
	if want := []int{1, 2}; !slices.Equal(got, want) {
		t.Errorf("TakeWhile = %v, want %v", got, want)
	}
	if calls != 3 {
		t.Errorf("keep called %d times, want 3: it must not run past the first refusal", calls)
	}
}

func TestTakeWhileStopsInfiniteSource(t *testing.T) {
	released := false
	infinite := Stream[int](func(yield func(Batch[int]) bool) {
		defer func() { released = true }()
		for i := 0; ; i++ {
			if !yield(Batch[int]{Items: []int{i}}) {
				return
			}
		}
	})

	got := Collect(TakeWhile(infinite, func(v int) bool { return v < 3 }))
	if want := []int{0, 1, 2}; !slices.Equal(got, want) {
		t.Errorf("TakeWhile over an infinite source = %v, want %v", got, want)
	}
	if !released {
		t.Error("TakeWhile did not let the source unwind")
	}
}

func TestDropWhile(t *testing.T) {
	tests := []struct {
		name string
		drop func(int) bool
		want []int
	}{
		{"starts inside a batch", func(v int) bool { return v < 3 }, []int{3, 4, 5, 6}},
		{"drops nothing", func(int) bool { return false }, []int{1, 2, 3, 4, 5, 6}},
		{"drops everything", func(int) bool { return true }, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := Collect(DropWhile(Of([]int{1, 2, 3, 4, 5, 6}, 4), tc.drop))
			if !slices.Equal(got, tc.want) {
				t.Errorf("DropWhile = %v, want %v", got, tc.want)
			}
		})
	}
}

// The asymmetry with Filter is the operator's whole point: once dropping ends,
// elements that satisfy the predicate again still pass through, and the
// predicate is no longer called.
func TestDropWhileStopsTesting(t *testing.T) {
	calls := 0
	got := Collect(DropWhile(Of([]int{1, 1, 5, 1, 1}, 2), func(v int) bool {
		calls++
		return v < 3
	}))
	if want := []int{5, 1, 1}; !slices.Equal(got, want) {
		t.Errorf("DropWhile = %v, want %v: it must not filter after the switch", got, want)
	}
	if calls != 3 {
		t.Errorf("drop called %d times, want 3", calls)
	}
}

func TestMergeWhenAllAlternates(t *testing.T) {
	tests := []struct {
		name    string
		streams []Stream[int]
		want    []int
	}{
		{
			"strict alternation",
			[]Stream[int]{Of([]int{1, 2}, 1), Of([]int{10, 20}, 1)},
			[]int{1, 10, 2, 20},
		},
		{
			"an exhausted source drops out, the rest carry on",
			[]Stream[int]{Of([]int{1}, 1), Of([]int{10, 20, 30}, 1)},
			[]int{1, 10, 20, 30},
		},
		{
			"three sources",
			[]Stream[int]{Of([]int{1, 2}, 1), Of([]int{10, 20}, 1), Of([]int{100, 200}, 1)},
			[]int{1, 10, 100, 2, 20, 200},
		},
		{"no streams", nil, nil},
		{"one stream", []Stream[int]{Of([]int{1, 2, 3}, 2)}, []int{1, 2, 3}},
		{"every stream empty", []Stream[int]{Empty[int](), Empty[int]()}, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := Collect(Merge(WhenAll, tc.streams...))
			if !slices.Equal(got, tc.want) {
				t.Errorf("Merge = %v, want %v", got, tc.want)
			}
		})
	}
}

// Merge with WhenAll alternates by batch, not by element: that is what keeps it O(1),
// and it is the contract callers plan around.
func TestMergeWhenAllAlternatesByBatch(t *testing.T) {
	got := Collect(Merge(WhenAll,
		Of([]int{1, 2, 3, 4}, 2),
		Of([]int{10, 20, 30, 40}, 2),
	))
	if want := []int{1, 2, 10, 20, 3, 4, 30, 40}; !slices.Equal(got, want) {
		t.Errorf("Merge = %v, want %v", got, want)
	}
}

// Every started source must be released on an early stop, or its coroutine
// leaks for the life of the process (S9).
//
// The stop comes on the fourth batch, after the rotation has started all three
// sources: a source iter.Pull never advanced has no coroutine to leak, so
// stopping before that would assert nothing.
func TestMergeWhenAllEarlyStopReleasesAll(t *testing.T) {
	const sources = 3
	released := make([]bool, sources)
	streams := make([]Stream[int], sources)
	for i := range streams {
		streams[i] = func(yield func(Batch[int]) bool) {
			defer func() { released[i] = true }()
			for n := 0; ; n++ {
				if !yield(Batch[int]{Items: []int{n}}) {
					return
				}
			}
		}
	}

	n := 0
	Merge(WhenAll, streams...)(func(Batch[int]) bool {
		n++
		return n <= sources
	})

	for i, r := range released {
		if !r {
			t.Errorf("stream %d was not released after an early stop", i)
		}
	}
}

// Merge with WhenAll must work on single-pass sources: it pulls once, it does not
// restart anything.
func TestMergeWhenAllSinglePass(t *testing.T) {
	runs := 0
	once := Stream[int](func(yield func(Batch[int]) bool) {
		runs++
		yield(Batch[int]{Items: []int{1, 2}})
	})

	got := Collect(Merge(WhenAll, once, Of([]int{10}, 1)))
	if want := []int{1, 2, 10}; !slices.Equal(got, want) {
		t.Errorf("Merge = %v, want %v", got, want)
	}
	if runs != 1 {
		t.Errorf("source ran %d times, want 1", runs)
	}
}

// TakeWhile ends without a trailing empty batch, matching Take. The empty
// batches Filter and Drop emit hold the upstream cadence; here the stream stops
// on the same call, so an empty batch would carry no information and would make
// two operators of the same family disagree on their contract.
func TestTakeWhileNoTrailingEmptyBatch(t *testing.T) {
	tests := []struct {
		name string
		src  []int
		size int
		keep func(int) bool
		want []int // batch sizes, in order
	}{
		{
			"predicate fails on a batch head",
			[]int{1, 2, 3, 4},
			2,
			func(v int) bool { return v < 3 },
			[]int{2},
		},
		{
			"predicate fails mid-batch",
			[]int{1, 2, 3, 4},
			4,
			func(v int) bool { return v < 3 },
			[]int{2},
		},
		{
			"predicate rejects everything",
			[]int{5, 6},
			2,
			func(int) bool { return false },
			nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var sizes []int
			TakeWhile(Of(tc.src, tc.size), tc.keep)(func(b Batch[int]) bool {
				sizes = append(sizes, b.Len())
				return true
			})
			if !slices.Equal(sizes, tc.want) {
				t.Errorf("batch sizes = %v, want %v", sizes, tc.want)
			}
		})
	}
}

// A nil function must be rejected where the pipeline is built, not dereferenced
// when the first element arrives: the panic then points at the caller's
// construction site and names the parameter, instead of surfacing as a nil
// dereference somewhere inside the library.
//
// Every operator taking a function is listed, including the ones that predate
// this file. The value is in the surface being uniform — a guard on half the
// operators teaches callers nothing they can rely on.
func TestOperatorsRejectNilFunctions(t *testing.T) {
	tests := []struct {
		name string
		call func()
	}{
		{"Map", func() { Map(Empty[int](), nil) }},
		{"Convert", func() { Convert[int, int](Empty[int](), nil) }},
		{"Filter", func() { Filter(Empty[int](), nil) }},
		{"FlatMap", func() { FlatMap[int, int](Empty[int](), nil) }},
		{"Peek", func() { Peek(Empty[int](), nil) }},
		{"Scan", func() { Scan[int, int](Empty[int](), 0, nil) }},
		{"TakeWhile", func() { TakeWhile(Empty[int](), nil) }},
		{"DropWhile", func() { DropWhile(Empty[int](), nil) }},
		{"ForEach", func() { ForEach(Empty[int](), nil) }},
		{"Reduce", func() { Reduce[int, int](Empty[int](), 0, nil) }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Error("expected a panic at construction rather than a later nil dereference")
				}
			}()
			tc.call()
		})
	}
}
