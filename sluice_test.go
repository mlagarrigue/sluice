package sluice

import (
	"cmp"
	"fmt"
	"iter"
	"slices"
	"strconv"
	"testing"
)

// collect gathers every element of a Stream. Batches may reuse a buffer, so
// values are copied as they are consumed.
func collect[T any](s Stream[T]) []T {
	var out []T
	s(func(b Batch[T]) bool {
		out = append(out, b.Items...)
		return true
	})
	return out
}

func TestOf(t *testing.T) {
	src := []int{1, 2, 3, 4, 5}
	got := collect(Of(src, 2))
	if !slices.Equal(got, src) {
		t.Errorf("Of = %v, want %v", got, src)
	}

	var sizes []int
	Of(src, 2)(func(b Batch[int]) bool {
		sizes = append(sizes, b.Len())
		return true
	})
	if want := []int{2, 2, 1}; !slices.Equal(sizes, want) {
		t.Errorf("batch sizes = %v, want %v", sizes, want)
	}
}

func TestOfDefaultSize(t *testing.T) {
	src := make([]int, DefaultBatchSize+1)
	var n int
	Of(src, 0)(func(b Batch[int]) bool {
		n++
		return true
	})
	if n != 2 {
		t.Errorf("size<=0 must mean DefaultBatchSize: got %d batches, want 2", n)
	}
}

func TestMapFilterConvert(t *testing.T) {
	src := []int{1, 2, 3, 4, 5, 6}

	got := collect(Map(Of(slices.Clone(src), 4), func(v int) int { return v * 10 }))
	if want := []int{10, 20, 30, 40, 50, 60}; !slices.Equal(got, want) {
		t.Errorf("Map = %v, want %v", got, want)
	}

	got = collect(Filter(Of(src, 4), func(v int) bool { return v%2 == 0 }))
	if want := []int{2, 4, 6}; !slices.Equal(got, want) {
		t.Errorf("Filter = %v, want %v", got, want)
	}

	letters := []string{"", "a", "b", "c", "d", "e", "f"}
	strs := collect(Convert(Of(src, 4), func(v int) string {
		return letters[v]
	}))
	if want := []string{"a", "b", "c", "d", "e", "f"}; !slices.Equal(strs, want) {
		t.Errorf("Convert = %v, want %v", strs, want)
	}
}

func TestConcat(t *testing.T) {
	got := collect(Concat(Of([]int{1, 2}, 2), Empty[int](), Of([]int{3, 4}, 2)))
	if want := []int{1, 2, 3, 4}; !slices.Equal(got, want) {
		t.Errorf("Concat = %v, want %v", got, want)
	}
}

func TestCoalesce(t *testing.T) {
	// A heavily fragmented source: one batch per element.
	src := []int{1, 2, 3, 4, 5, 6, 7}
	var sizes []int
	Coalesce(Of(src, 1), 3)(func(b Batch[int]) bool {
		sizes = append(sizes, b.Len())
		return true
	})
	if want := []int{3, 3, 1}; !slices.Equal(sizes, want) {
		t.Errorf("sizes after Coalesce = %v, want %v", sizes, want)
	}

	if got := collect(Coalesce(Of(src, 1), 3)); !slices.Equal(got, src) {
		t.Errorf("Coalesce altered the data: %v, want %v", got, src)
	}
}

func TestCoalesceDefaultSize(t *testing.T) {
	src := make([]int, DefaultBatchSize+1)
	var sizes []int
	Coalesce(Of(src, 1), 0)(func(b Batch[int]) bool {
		sizes = append(sizes, b.Len())
		return true
	})
	if want := []int{DefaultBatchSize, 1}; !slices.Equal(sizes, want) {
		t.Errorf("size<=0 must mean DefaultBatchSize: got %v, want %v", sizes, want)
	}
}

// Every operator must propagate an early stop to its source, and let the
// source's deferred calls run at once. This is the invariant the whole model
// rests on: swallowing the false breaks upstream resource release silently.
//
// The table covers every operator rather than a representative one, because the
// bug this guards against is per-operator — one forgotten return is enough.
func TestEarlyStopPropagates(t *testing.T) {
	// wrap builds the operator under test on top of the instrumented source.
	tests := []struct {
		name string
		wrap func(Stream[int]) Stream[int]
	}{
		{"Map", func(s Stream[int]) Stream[int] {
			return Map(s, func(v int) int { return v })
		}},
		{"Filter", func(s Stream[int]) Stream[int] {
			return Filter(s, func(int) bool { return true })
		}},
		{"Convert", func(s Stream[int]) Stream[int] {
			return Convert(s, func(v int) int { return v })
		}},
		{"Coalesce", func(s Stream[int]) Stream[int] {
			return Coalesce(s, 1)
		}},
		{"Concat/first", func(s Stream[int]) Stream[int] {
			return Concat(s, Of([]int{97, 98, 99}, 1))
		}},
		{"Concat/only", func(s Stream[int]) Stream[int] {
			return Concat(s)
		}},
		{"Merge/all", func(s Stream[int]) Stream[int] {
			return Merge(WhenAll, s)
		}},
		{"Merge/any", func(s Stream[int]) Stream[int] {
			return Merge(WhenAny, s)
		}},
		// MergeJoinBy is absent on purpose: it fills an output batch before
		// yielding anything, so it cannot stop within three elements. Its own
		// propagation is covered by TestMergeJoinByEarlyStopEveryBranch, which
		// stops at the first full batch instead.
		{"Split", func(s Stream[int]) Stream[int] {
			return Split(s, 1, func(Batch[int]) []int { return []int{0} })[0]
		}},
		{"identity", func(s Stream[int]) Stream[int] { return s }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var (
				produced int
				closed   bool
			)
			src := Stream[int](func(yield func(Batch[int]) bool) {
				defer func() { closed = true }()
				for i := range 100 {
					produced++
					if !yield(Batch[int]{Items: []int{i}}) {
						return
					}
				}
			})

			consumed := 0
			tt.wrap(src)(func(b Batch[int]) bool {
				consumed += b.Len()
				return consumed < 3
			})

			if consumed != 3 {
				t.Errorf("consumed = %d, want 3", consumed)
			}
			// The source may run one batch ahead: what must not happen is it
			// running to completion.
			if produced > 4 {
				t.Errorf("produced = %d, want <= 4: the source was not stopped", produced)
			}
			if !closed {
				t.Error("the source's deferred call did not run on early stop")
			}
		})
	}
}

// Concat must not start the next stream once the consumer has stopped.
func TestMerge(t *testing.T) {
	tests := []struct {
		name string
		done Completion
		a, b []int
		want []int
	}{
		{"all/even lengths", WhenAll, []int{1, 2}, []int{10, 20}, []int{1, 10, 2, 20}},
		{"all/left shorter", WhenAll, []int{1}, []int{10, 20, 30}, []int{1, 10, 20, 30}},
		{"all/right shorter", WhenAll, []int{1, 2, 3}, []int{10}, []int{1, 10, 2, 3}},
		{"all/left empty", WhenAll, nil, []int{10, 20}, []int{10, 20}},
		{"all/right empty", WhenAll, []int{1, 2}, nil, []int{1, 2}},
		{"all/both empty", WhenAll, nil, nil, nil},
		// WhenAny stops at the first exhausted source, but batches already
		// pulled in the same round are still emitted.
		{"any/even lengths", WhenAny, []int{1, 2}, []int{10, 20}, []int{1, 10, 2, 20}},
		{"any/left shorter", WhenAny, []int{1}, []int{10, 20, 30}, []int{1, 10}},
		{"any/right shorter", WhenAny, []int{1, 2, 3}, []int{10}, []int{1, 10, 2}},
		{"any/left empty", WhenAny, nil, []int{10, 20}, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := collect(Merge(tt.done, Of(tt.a, 1), Of(tt.b, 1)))
			if !slices.Equal(got, tt.want) {
				t.Errorf("Merge = %v, want %v", got, tt.want)
			}
		})
	}
}

// Merge holds one iter.Pull per source and no buffer, so it must work on
// sources that cannot be replayed — the case the operator exists for.
func TestMergeSinglePass(t *testing.T) {
	a, consumedA := singlePass(t, []int{1, 2, 3}, 1)
	b, consumedB := singlePass(t, []int{10, 20, 30}, 1)

	got := collect(Merge(WhenAll, a, b))

	if want := []int{1, 10, 2, 20, 3, 30}; !slices.Equal(got, want) {
		t.Errorf("Merge = %v, want %v", got, want)
	}
	if consumedA() != 3 || consumedB() != 3 {
		t.Errorf("consumed %d and %d elements, want 3 each", consumedA(), consumedB())
	}
}

// An early stop must release every source that was started, not just the one
// being read when the consumer stopped. A source that was never pulled from has
// not run at all, so there is nothing to release — that is the Stream contract,
// not a leak.
func TestMergeEarlyStopReleasesAll(t *testing.T) {
	var started, closed [3]bool
	mk := func(i, base int) Stream[int] {
		return func(yield func(Batch[int]) bool) {
			started[i] = true
			defer func() { closed[i] = true }()
			for n := range 100 {
				if !yield(Batch[int]{Items: []int{base + n}}) {
					return
				}
			}
		}
	}

	n := 0
	Merge(WhenAll, mk(0, 0), mk(1, 100), mk(2, 200))(func(Batch[int]) bool {
		n++
		return n < 5 // far enough in that every source has been pulled from
	})

	for i := range started {
		if !started[i] {
			t.Errorf("source %d was never started: the test does not prove anything", i)
			continue
		}
		if !closed[i] {
			t.Errorf("source %d was not released on early stop", i)
		}
	}
}

// Merge is lazy per source: a stop before a source's turn means that source
// never runs at all. Nothing is read and thrown away.
func TestMergeStopsBeforeUntouchedSource(t *testing.T) {
	started := false
	late := Stream[int](func(yield func(Batch[int]) bool) {
		started = true
		yield(Batch[int]{Items: []int{99}})
	})

	Merge(WhenAll, Of([]int{1, 2, 3}, 1), late)(func(Batch[int]) bool {
		return false // stop on the very first batch, before late's turn
	})

	if started {
		t.Error("Merge started a source the consumer never reached")
	}
}

// A panic crossing the consumer must not strand the sources: the deferred
// release runs on the way out, whatever the reason for leaving.
func TestMergeReleasesOnPanic(t *testing.T) {
	var closed [2]bool
	mk := func(i, base int) Stream[int] {
		return func(yield func(Batch[int]) bool) {
			defer func() { closed[i] = true }()
			for n := range 100 {
				if !yield(Batch[int]{Items: []int{base + n}}) {
					return
				}
			}
		}
	}

	func() {
		defer func() {
			if recover() == nil {
				t.Error("the panic did not propagate")
			}
		}()
		n := 0
		Merge(WhenAll, mk(0, 0), mk(1, 100))(func(Batch[int]) bool {
			n++
			if n == 3 {
				panic("consumer failure")
			}
			return true
		})
	}()

	for i, c := range closed {
		if !c {
			t.Errorf("source %d was not released when the consumer panicked", i)
		}
	}
}

func TestMergeNoStreams(t *testing.T) {
	for _, done := range []Completion{WhenAll, WhenAny} {
		if got := collect(Merge[int](done)); len(got) != 0 {
			t.Errorf("Merge(%v) with no source = %v, want nothing", done, got)
		}
	}
}

func TestMergeOrdersRoundRobin(t *testing.T) {
	// Three sources, one batch each per round: the interleaving is visible.
	got := collect(Merge(WhenAll,
		Of([]int{1, 2}, 1),
		Of([]int{10, 20}, 1),
		Of([]int{100, 200}, 1),
	))
	if want := []int{1, 10, 100, 2, 20, 200}; !slices.Equal(got, want) {
		t.Errorf("Merge = %v, want %v", got, want)
	}
}

func TestCompletionString(t *testing.T) {
	tests := []struct {
		in   Completion
		want string
	}{
		{WhenAll, "WhenAll"},
		{WhenAny, "WhenAny"},
		{Completion(7), "Completion(7)"},
	}
	for _, tt := range tests {
		if got := tt.in.String(); got != tt.want {
			t.Errorf("Completion(%d).String() = %q, want %q", tt.in, got, tt.want)
		}
	}
}

// zipRows renders a ZipLongest result: B(l,r) a pair, L(l) or R(r) a tail row.
func zipRows(s Stream[EitherOrBoth[int, string]]) []string {
	var out []string
	s(func(b Batch[EitherOrBoth[int, string]]) bool {
		for _, e := range b.Items {
			switch {
			case e.Both():
				out = append(out, fmt.Sprintf("B(%d,%s)", e.Left, e.Right))
			case e.HasLeft:
				out = append(out, fmt.Sprintf("L(%d)", e.Left))
			default:
				out = append(out, fmt.Sprintf("R(%s)", e.Right))
			}
		}
		return true
	})
	return out
}

func TestZipLongest(t *testing.T) {
	tests := []struct {
		name  string
		left  []int
		right []string
		want  []string
	}{
		{
			"equal lengths",
			[]int{1, 2},
			[]string{"a", "b"},
			[]string{"B(1,a)", "B(2,b)"},
		},
		{
			"left longer",
			[]int{1, 2, 3},
			[]string{"a"},
			[]string{"B(1,a)", "L(2)", "L(3)"},
		},
		{
			"right longer",
			[]int{1},
			[]string{"a", "b", "c"},
			[]string{"B(1,a)", "R(b)", "R(c)"},
		},
		{
			"left empty", nil,
			[]string{"a", "b"},
			[]string{"R(a)", "R(b)"},
		},
		{
			"right empty",
			[]int{1, 2},
			nil,
			[]string{"L(1)", "L(2)"},
		},
		{"both empty", nil, nil, nil},
		{
			"single pair",
			[]int{1},
			[]string{"a"},
			[]string{"B(1,a)"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Different batch sizes on each side: the two inputs are not
			// aligned, which is the case §7.5 says nobody tries to fix.
			got := zipRows(ZipLongest(Of(tt.left, 2), Of(tt.right, 3)))
			if !slices.Equal(got, tt.want) {
				t.Errorf("batch 2/3: got %v, want %v", got, tt.want)
			}
			got = zipRows(ZipLongest(Of(tt.left, 1), Of(tt.right, 1)))
			if !slices.Equal(got, tt.want) {
				t.Errorf("batch 1/1: got %v, want %v", got, tt.want)
			}
		})
	}
}

// Zip — stopping at the shorter stream — is a filter over ZipLongest rather
// than an operator. That is the claim §7.4 rests on.
func TestZipLongestDerivesZip(t *testing.T) {
	zipped := ZipLongest(Of([]int{1, 2, 3}, 2), Of([]string{"a", "b"}, 2))
	got := zipRows(Filter(zipped, EitherOrBoth[int, string].Both))

	if want := []string{"B(1,a)", "B(2,b)"}; !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// Once a stream is exhausted it must not be restarted on later rounds.
func TestZipLongestDoesNotRestart(t *testing.T) {
	runs := 0
	short := Stream[int](func(yield func(Batch[int]) bool) {
		runs++
		yield(Batch[int]{Items: []int{1}})
	})

	got := zipRows(ZipLongest(short, Of([]string{"a", "b", "c"}, 1)))

	if want := []string{"B(1,a)", "R(b)", "R(c)"}; !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if runs != 1 {
		t.Errorf("exhausted source ran %d times, want 1", runs)
	}
}

func TestZipLongestSinglePass(t *testing.T) {
	left, consumedL := singlePass(t, []int{1, 2, 3}, 2)
	right, consumedR := singlePass(t, []int{10, 20}, 2)

	var got []string
	ZipLongest(left, right)(func(b Batch[EitherOrBoth[int, int]]) bool {
		for _, e := range b.Items {
			if e.Both() {
				got = append(got, fmt.Sprintf("%d+%d", e.Left, e.Right))
			} else {
				got = append(got, fmt.Sprintf("%d+_", e.Left))
			}
		}
		return true
	})

	if want := []string{"1+10", "2+20", "3+_"}; !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if consumedL() != 3 || consumedR() != 2 {
		t.Errorf("consumed %d and %d, want 3 and 2", consumedL(), consumedR())
	}
}

// Filter emits empty batches; a zip fed by one must not mistake them for the
// end of the stream.
func TestZipLongestEmptyBatches(t *testing.T) {
	left := Filter(Of([]int{1, 2, 3, 4}, 2), func(v int) bool { return v%2 == 0 })
	got := zipRows(ZipLongest(left, Of([]string{"a", "b"}, 1)))

	if want := []string{"B(2,a)", "B(4,b)"}; !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// Rows are buffered until a batch is full, so an early stop is observable past
// DefaultBatchSize. Both sources must be released whichever tail is running.
func TestZipLongestEarlyStopReleasesBoth(t *testing.T) {
	tests := []struct {
		name         string
		leftN, right int
	}{
		{"both running", DefaultBatchSize + 100, DefaultBatchSize + 100},
		{"left tail", DefaultBatchSize + 100, 1},
		{"right tail", 1, DefaultBatchSize + 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var closedL, closedR bool
			mkL := func(n int) Stream[int] {
				return func(yield func(Batch[int]) bool) {
					defer func() { closedL = true }()
					for i := range n {
						if !yield(Batch[int]{Items: []int{i}}) {
							return
						}
					}
				}
			}
			mkR := func(n int) Stream[string] {
				return func(yield func(Batch[string]) bool) {
					defer func() { closedR = true }()
					for i := range n {
						if !yield(Batch[string]{Items: []string{strconv.Itoa(i)}}) {
							return
						}
					}
				}
			}

			batches := 0
			ZipLongest(mkL(tt.leftN), mkR(tt.right))(func(Batch[EitherOrBoth[int, string]]) bool {
				batches++
				return false
			})

			if batches != 1 {
				t.Errorf("consumer saw %d batches, want 1", batches)
			}
			if !closedL || !closedR {
				t.Errorf("sources not released: left=%v right=%v", closedL, closedR)
			}
		})
	}
}

func TestZipLongestBatchesOutput(t *testing.T) {
	const n = 2500
	left := make([]int, n)
	for i := range left {
		left[i] = i
	}

	var sizes []int
	ZipLongest(Of(left, 100), Empty[string]())(func(b Batch[EitherOrBoth[int, string]]) bool {
		sizes = append(sizes, b.Len())
		return true
	})

	if want := []int{DefaultBatchSize, DefaultBatchSize, n - 2*DefaultBatchSize}; !slices.Equal(sizes, want) {
		t.Errorf("batch sizes = %v, want %v", sizes, want)
	}
}

// emitter is the shared way an N-to-1 operator batches its output, and flush is
// its last call. Today's callers return whatever it says, but a future operator
// with work left after the flush must be able to see a refusal — so the result
// is reported rather than dropped. Pinned here because nothing else observes it.
func TestEmitterFlushReportsRefusal(t *testing.T) {
	tests := []struct {
		name   string
		accept bool
		want   bool
	}{
		{"consumer accepts", true, true},
		{"consumer refuses", false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newEmitter(func(Batch[int]) bool { return tt.accept })
			e.push(1) // one row, far short of a full batch: only flush emits it

			if got := e.flush(); got != tt.want {
				t.Errorf("flush() = %v, want %v", got, tt.want)
			}
		})
	}

	// An empty buffer yields nothing and reports success: there was nothing to
	// refuse.
	called := false
	e := newEmitter(func(Batch[int]) bool { called = true; return false })
	if !e.flush() {
		t.Error("flush() on an empty buffer = false, want true")
	}
	if called {
		t.Error("flush() emitted a batch with nothing buffered")
	}
}

// joinRows renders a merge-join result compactly: L(x) unmatched left, R(x)
// unmatched right, B(x,y) a matched pair.
func joinRows(s Stream[EitherOrBoth[int, int]]) []string {
	var out []string
	s(func(b Batch[EitherOrBoth[int, int]]) bool {
		for _, e := range b.Items {
			switch {
			case e.Both():
				out = append(out, fmt.Sprintf("B(%d,%d)", e.Left, e.Right))
			case e.HasLeft:
				out = append(out, fmt.Sprintf("L(%d)", e.Left))
			default:
				out = append(out, fmt.Sprintf("R(%d)", e.Right))
			}
		}
		return true
	})
	return out
}

func identity(v int) int { return v }

func joinInts(left, right []int, batch int) []string {
	return joinRows(MergeJoinBy(
		Of(left, batch), Of(right, batch),
		identity, identity, cmp.Compare[int],
	))
}

func TestMergeJoinBy(t *testing.T) {
	tests := []struct {
		name        string
		left, right []int
		want        []string
	}{
		{
			"interleaved",
			[]int{1, 3, 5},
			[]int{3, 4, 5, 6},
			[]string{"L(1)", "B(3,3)", "R(4)", "B(5,5)", "R(6)"},
		},
		{
			"disjoint",
			[]int{1, 2},
			[]int{3, 4},
			[]string{"L(1)", "L(2)", "R(3)", "R(4)"},
		},
		{
			"identical",
			[]int{1, 2},
			[]int{1, 2},
			[]string{"B(1,1)", "B(2,2)"},
		},
		{
			"left empty", nil,
			[]int{1, 2},
			[]string{"R(1)", "R(2)"},
		},
		{
			"right empty",
			[]int{1, 2},
			nil,
			[]string{"L(1)", "L(2)"},
		},
		{"both empty", nil, nil, nil},
		{
			"left runs out first",
			[]int{1},
			[]int{1, 2, 3},
			[]string{"B(1,1)", "R(2)", "R(3)"},
		},
		{
			"right runs out first",
			[]int{1, 2, 3},
			[]int{1},
			[]string{"B(1,1)", "L(2)", "L(3)"},
		},
		// Duplicates on one side pair with the single row on the other.
		{
			"duplicate left",
			[]int{2, 2, 2},
			[]int{2},
			[]string{"B(2,2)", "B(2,2)", "B(2,2)"},
		},
		{
			"duplicate right",
			[]int{2},
			[]int{2, 2, 2},
			[]string{"B(2,2)", "B(2,2)", "B(2,2)"},
		},
		// Duplicates on both sides: the full cross product, as SQL requires.
		{
			"cross product 2x3",
			[]int{2, 2},
			[]int{2, 2, 2},
			[]string{"B(2,2)", "B(2,2)", "B(2,2)", "B(2,2)", "B(2,2)", "B(2,2)"},
		},
		{
			"consecutive groups",
			[]int{1, 1, 2},
			[]int{1, 2, 2},
			[]string{"B(1,1)", "B(1,1)", "B(2,2)", "B(2,2)"},
		},
		// A duplicated key with no partner stays unmatched, once per row.
		{
			"unmatched duplicates",
			[]int{2, 2},
			[]int{3},
			[]string{"L(2)", "L(2)", "R(3)"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Batch size 2 splits runs across batch boundaries, which is where
			// a cursor bug would show.
			if got := joinInts(tt.left, tt.right, 2); !slices.Equal(got, tt.want) {
				t.Errorf("batch=2: got %v, want %v", got, tt.want)
			}
			// Batch size 1 puts every element in its own batch.
			if got := joinInts(tt.left, tt.right, 1); !slices.Equal(got, tt.want) {
				t.Errorf("batch=1: got %v, want %v", got, tt.want)
			}
		})
	}
}

// Each join semantics is a filter over the same output — that is the claim the
// operator is built on, so it is worth pinning.
func TestMergeJoinBySemantics(t *testing.T) {
	left, right := []int{1, 2, 3}, []int{2, 3, 4}

	rows := func(keep func(EitherOrBoth[int, int]) bool) []string {
		s := MergeJoinBy(Of(left, 2), Of(right, 2), identity, identity, cmp.Compare[int])
		return joinRows(Filter(s, keep))
	}

	tests := []struct {
		name string
		keep func(EitherOrBoth[int, int]) bool
		want []string
	}{
		{
			"inner", func(e EitherOrBoth[int, int]) bool { return e.Both() },
			[]string{"B(2,2)", "B(3,3)"},
		},
		{
			"left join", func(e EitherOrBoth[int, int]) bool { return e.HasLeft },
			[]string{"L(1)", "B(2,2)", "B(3,3)"},
		},
		{
			"right join", func(e EitherOrBoth[int, int]) bool { return e.HasRight },
			[]string{"B(2,2)", "B(3,3)", "R(4)"},
		},
		{
			"full outer", func(EitherOrBoth[int, int]) bool { return true },
			[]string{"L(1)", "B(2,2)", "B(3,3)", "R(4)"},
		},
		{
			"except left", func(e EitherOrBoth[int, int]) bool { return e.HasLeft && !e.HasRight },
			[]string{"L(1)"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := rows(tt.keep); !slices.Equal(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

// Unsorted input is the operator's silent failure mode, so it must not be
// silent: it panics rather than returning a plausible wrong answer.
func TestMergeJoinByUnsorted(t *testing.T) {
	tests := []struct {
		name        string
		left, right []int
	}{
		{"left goes backwards", []int{1, 5, 2}, []int{1, 2, 5}},
		{"right goes backwards", []int{1, 2, 5}, []int{1, 5, 2}},
		{"left starts backwards", []int{9, 1}, []int{1, 9}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				switch r := recover(); r {
				case ErrUnsorted:
				case nil:
					t.Error("unsorted input produced a result instead of panicking")
				default:
					t.Errorf("panicked with %v, want ErrUnsorted", r)
				}
			}()
			joinInts(tt.left, tt.right, 2)
		})
	}
}

// Equal consecutive keys are sorted, not out of order: they must not trip the
// monotonicity check.
func TestMergeJoinByEqualKeysAreSorted(t *testing.T) {
	got := joinInts([]int{1, 1, 1}, []int{1}, 2)
	want := []string{"B(1,1)", "B(1,1)", "B(1,1)"}
	if !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestMergeJoinByNilFunctions(t *testing.T) {
	tests := []struct {
		name string
		call func()
	}{
		{"nil keyL", func() {
			MergeJoinBy(Empty[int](), Empty[int](), nil, identity, cmp.Compare[int])
		}},
		{"nil keyR", func() {
			MergeJoinBy(Empty[int](), Empty[int](), identity, nil, cmp.Compare[int])
		}},
		{"nil cmp", func() {
			MergeJoinBy[int, int, int](Empty[int](), Empty[int](), identity, identity, nil)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Error("expected a panic rather than a later nil dereference")
				}
			}()
			tt.call()
		})
	}
}

// The operator walks both inputs once, so it must work on sources that cannot
// be replayed.
func TestMergeJoinBySinglePass(t *testing.T) {
	left, consumedL := singlePass(t, []int{1, 3, 5}, 2)
	right, consumedR := singlePass(t, []int{3, 5, 7}, 2)

	got := joinRows(MergeJoinBy(left, right, identity, identity, cmp.Compare[int]))
	want := []string{"L(1)", "B(3,3)", "B(5,5)", "R(7)"}
	if !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if consumedL() != 3 || consumedR() != 3 {
		t.Errorf("consumed %d and %d, want 3 each", consumedL(), consumedR())
	}
}

func TestMergeJoinByEarlyStopReleasesBoth(t *testing.T) {
	var closedL, closedR bool
	mk := func(vals []int, closed *bool) Stream[int] {
		return func(yield func(Batch[int]) bool) {
			defer func() { *closed = true }()
			for _, v := range vals {
				if !yield(Batch[int]{Items: []int{v}}) {
					return
				}
			}
		}
	}

	n := 0
	s := MergeJoinBy(mk([]int{5, 5, 5}, &closedL), mk([]int{5, 5, 5}, &closedR),
		identity, identity, cmp.Compare[int])
	s(func(b Batch[EitherOrBoth[int, int]]) bool {
		n += b.Len()
		return false
	})

	if n == 0 {
		t.Error("no row was emitted: the test proves nothing")
	}
	if !closedL || !closedR {
		t.Errorf("sources not released on early stop: left=%v right=%v", closedL, closedR)
	}
}

// A merge join buffers rows until a batch is full, so an early stop is only
// observable past DefaultBatchSize. Each case below drives the stop through a
// different branch of the merge — every one of them must propagate it, which is
// the invariant the whole model rests on.
func TestMergeJoinByEarlyStopEveryBranch(t *testing.T) {
	const n = DefaultBatchSize + 100

	seq := func(start, count int) []int {
		s := make([]int, count)
		for i := range s {
			s[i] = start + i
		}
		return s
	}
	repeat := func(v, count int) []int {
		s := make([]int, count)
		for i := range s {
			s[i] = v
		}
		return s
	}

	tests := []struct {
		name        string
		left, right []int
	}{
		// Right is exhausted at once: every later row takes the !rok branch.
		{"left only", seq(0, n), nil},
		// Left is exhausted at once: the !lok branch.
		{"right only", nil, seq(0, n)},
		// Left keys all sort before the right ones: the c < 0 branch.
		{"left sorts first", seq(0, n), seq(n*2, 10)},
		// Right keys all sort before the left ones: the c > 0 branch.
		{"right sorts first", seq(n*2, 10), seq(0, n)},
		// One key on both sides: the cross-product branch.
		{"cross product", repeat(1, 40), repeat(1, 40)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var (
				closedL, closedR bool
				startedL         bool
			)
			mk := func(vals []int, started, closed *bool) Stream[int] {
				return func(yield func(Batch[int]) bool) {
					if started != nil {
						*started = true
					}
					defer func() { *closed = true }()
					for i := 0; i < len(vals); i += 16 {
						end := min(i+16, len(vals))
						if !yield(Batch[int]{Items: vals[i:end]}) {
							return
						}
					}
				}
			}

			batches := 0
			s := MergeJoinBy(
				mk(tt.left, &startedL, &closedL),
				mk(tt.right, nil, &closedR),
				identity, identity, cmp.Compare[int],
			)
			s(func(Batch[EitherOrBoth[int, int]]) bool {
				batches++
				return false // stop on the first full batch
			})

			if batches != 1 {
				t.Errorf("consumer saw %d batches, want 1: the stop did not propagate", batches)
			}
			if !closedL || !closedR {
				t.Errorf("sources not released: left=%v right=%v", closedL, closedR)
			}
		})
	}
}

// Output is batched rather than emitted one row at a time.
func TestMergeJoinByBatchesOutput(t *testing.T) {
	const n = 2500
	left := make([]int, n)
	for i := range left {
		left[i] = i
	}

	var sizes []int
	total := 0
	s := MergeJoinBy(Of(left, 100), Empty[int](), identity, identity, cmp.Compare[int])
	s(func(b Batch[EitherOrBoth[int, int]]) bool {
		sizes = append(sizes, b.Len())
		total += b.Len()
		return true
	})

	if total != n {
		t.Errorf("emitted %d rows, want %d", total, n)
	}
	if want := []int{DefaultBatchSize, DefaultBatchSize, n - 2*DefaultBatchSize}; !slices.Equal(sizes, want) {
		t.Errorf("batch sizes = %v, want %v", sizes, want)
	}
}

// Filter emits empty batches to keep the upstream cadence, so a join fed by one
// must skip them rather than mistake them for exhaustion.
func TestMergeJoinByEmptyBatches(t *testing.T) {
	left := Filter(Of([]int{1, 2, 3, 4, 5, 6}, 2), func(v int) bool { return v%3 == 0 })
	right := Of([]int{3, 6}, 1)

	got := joinRows(MergeJoinBy(left, right, identity, identity, cmp.Compare[int]))
	if want := []string{"B(3,3)", "B(6,6)"}; !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// Joining on a field rather than the element itself: the key functions exist so
// the two sides need not share a type.
func TestMergeJoinByDistinctTypes(t *testing.T) {
	type order struct {
		customerID int
		total      int
	}
	type customer struct {
		id   int
		name string
	}

	orders := []order{{1, 100}, {2, 250}, {4, 75}}
	customers := []customer{{1, "ana"}, {3, "bo"}, {4, "cy"}}

	var matched []string
	s := MergeJoinBy(Of(orders, 2), Of(customers, 2),
		func(o order) int { return o.customerID },
		func(c customer) int { return c.id },
		cmp.Compare[int],
	)
	s(func(b Batch[EitherOrBoth[order, customer]]) bool {
		for _, e := range b.Items {
			if e.Both() {
				matched = append(matched, fmt.Sprintf("%s:%d", e.Right.name, e.Left.total))
			}
		}
		return true
	})

	if want := []string{"ana:100", "cy:75"}; !slices.Equal(matched, want) {
		t.Errorf("matched = %v, want %v", matched, want)
	}
}

func TestConcatEarlyStopSkipsRest(t *testing.T) {
	started := false
	second := Stream[int](func(yield func(Batch[int]) bool) {
		started = true
		yield(Batch[int]{Items: []int{9}})
	})

	Concat(Of([]int{1, 2, 3}, 1), second)(func(Batch[int]) bool {
		return false // stop on the very first batch
	})

	if started {
		t.Error("Concat consumed the next stream after an early stop")
	}
}

// Of must stop cutting batches as soon as the consumer refuses one.
func TestOfEarlyStop(t *testing.T) {
	n := 0
	Of([]int{1, 2, 3, 4, 5, 6}, 2)(func(Batch[int]) bool {
		n++
		return false
	})
	if n != 1 {
		t.Errorf("Of yielded %d batches after a stop, want 1", n)
	}
}

// Coalesce drops what it has accumulated when the consumer stops: the contract
// says so, and this pins it down.
func TestCoalesceEarlyStopDiscards(t *testing.T) {
	var got [][]int
	Coalesce(Of([]int{1, 2, 3, 4, 5}, 1), 2)(func(b Batch[int]) bool {
		got = append(got, slices.Clone(b.Items))
		return false // stop on the first full batch; 3,4,5 never surface
	})
	if len(got) != 1 || !slices.Equal(got[0], []int{1, 2}) {
		t.Errorf("got %v, want [[1 2]]", got)
	}
}

// Filter and Convert reuse one buffer across batches and let it grow, so a
// source whose batches get bigger must not corrupt or truncate anything. The
// growth path is the one a fixed-size source never exercises.
func TestGrowingBatchesAreNotTruncated(t *testing.T) {
	// Batches of 1, 2, 3, ... elements: every batch is a new high-water mark.
	growing := Stream[int](func(yield func(Batch[int]) bool) {
		n := 1
		for v := 0; v < 60; {
			b := make([]int, 0, n)
			for range n {
				if v >= 60 {
					break
				}
				b = append(b, v)
				v++
			}
			if !yield(Batch[int]{Items: b}) {
				return
			}
			n++
		}
	})

	want := make([]int, 60)
	for i := range want {
		want[i] = i
	}

	if got := collect(Filter(growing, func(int) bool { return true })); !slices.Equal(got, want) {
		t.Errorf("Filter over growing batches = %v, want %v", got, want)
	}

	// Rebuild: the stream above is single-pass by construction.
	growing2 := Stream[int](func(yield func(Batch[int]) bool) {
		n := 1
		for v := 0; v < 60; {
			b := make([]int, 0, n)
			for range n {
				if v >= 60 {
					break
				}
				b = append(b, v)
				v++
			}
			if !yield(Batch[int]{Items: b}) {
				return
			}
			n++
		}
	})
	if got := collect(Convert(growing2, func(v int) int { return v })); !slices.Equal(got, want) {
		t.Errorf("Convert over growing batches = %v, want %v", got, want)
	}
}

// The operators that own a buffer reuse it between batches. That is the one
// core rule a caller can break silently — retaining Items without copying — so
// it is pinned here in both directions: a change either way is a contract
// change, and must not slip through unnoticed.
func TestBufferReuseContract(t *testing.T) {
	tests := []struct {
		name  string
		build func() Stream[int]
	}{
		{"Filter", func() Stream[int] {
			return Filter(Of([]int{1, 2, 3, 4}, 2), func(int) bool { return true })
		}},
		{"Convert", func() Stream[int] {
			return Convert(Of([]int{1, 2, 3, 4}, 2), func(v int) int { return v })
		}},
		{"Coalesce", func() Stream[int] {
			return Coalesce(Of([]int{1, 2, 3, 4}, 1), 2)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var kept [][]int
			tt.build()(func(b Batch[int]) bool {
				kept = append(kept, b.Items) // deliberately not copied
				return true
			})
			if len(kept) < 2 {
				t.Fatalf("need at least 2 batches to observe reuse, got %d", len(kept))
			}
			if &kept[0][0] != &kept[1][0] {
				t.Error("batches no longer share a buffer: the documented contract changed")
			}
		})
	}
}

// emitter is the buffer behind every N-to-1 operator, and it is reused between
// batches exactly like Filter's. Pinned separately because the operators above
// cannot reach it: it only fills past DefaultBatchSize.
func TestEmitterBufferReuseContract(t *testing.T) {
	const n = 2 * DefaultBatchSize
	left := make([]int, n)
	for i := range left {
		left[i] = i
	}

	build := map[string]func() Stream[EitherOrBoth[int, int]]{
		"MergeJoinBy": func() Stream[EitherOrBoth[int, int]] {
			return MergeJoinBy(Of(left, 100), Empty[int](), identity, identity, cmp.Compare[int])
		},
		"ZipLongest": func() Stream[EitherOrBoth[int, int]] {
			return ZipLongest(Of(left, 100), Empty[int]())
		},
	}

	for name, mk := range build {
		t.Run(name, func(t *testing.T) {
			var kept [][]EitherOrBoth[int, int]
			mk()(func(b Batch[EitherOrBoth[int, int]]) bool {
				kept = append(kept, b.Items) // deliberately not copied
				return true
			})
			if len(kept) < 2 {
				t.Fatalf("need at least 2 batches to observe reuse, got %d", len(kept))
			}
			if &kept[0][0] != &kept[1][0] {
				t.Error("emitter batches no longer share a buffer: the documented contract changed")
			}
		})
	}
}

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

// Of shares the caller's slice and Map writes in place, so composing them
// overwrites the input. Documented, and pinned here because it surprises.
func TestMapWritesThroughToCallerSlice(t *testing.T) {
	src := []int{1, 2, 3}
	collect(Map(Of(src, 3), func(v int) int { return v * 10 }))

	if !slices.Equal(src, []int{10, 20, 30}) {
		t.Errorf("caller slice = %v, want [10 20 30]", src)
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
		switch r := recover(); r {
		case ErrSplitStalled:
		case nil:
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
				switch r := recover(); r {
				case ErrSplitDrained:
				case nil:
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
		switch r := recover(); r {
		case ErrSplitDrained:
		case nil:
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
