package sluice

import (
	"errors"
	"slices"
	"testing"
)

var errBoom = errors.New("boom")

func TestSourceErr(t *testing.T) {
	tests := []struct {
		name string
		src  Source[int]
		want error
	}{
		{"zero value reports nothing", Source[int]{}, nil},
		{"nil error function", NewSource(Of([]int{1}, 1), nil), nil},
		{"reports its error", NewSource(Empty[int](), func() error { return errBoom }), errBoom},
		{"function returning nil", NewSource(Empty[int](), func() error { return nil }), nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.src.Err(); !errors.Is(got, tc.want) {
				t.Errorf("Err = %v, want %v", got, tc.want)
			}
		})
	}
}

// A setup failure means no element was ever produced. The stream is empty and
// the error is what tells the caller why — without it, an empty stream and a
// failed open are indistinguishable.
func TestSourceSetupFailure(t *testing.T) {
	src := NewSource(Empty[int](), func() error { return errBoom })

	got, err := src.Collect()
	if len(got) != 0 {
		t.Errorf("Collect = %v, want nothing", got)
	}
	if !errors.Is(err, errBoom) {
		t.Errorf("err = %v, want %v", err, errBoom)
	}
}

// A teardown failure arrives after every element. The data must be returned
// alongside the error rather than discarded: it was read successfully.
func TestSourceTeardownFailureKeepsData(t *testing.T) {
	var closeErr error
	stream := Stream[int](func(yield func(Batch[int]) bool) {
		defer func() { closeErr = errBoom }() // the Close that failed
		yield(Batch[int]{Items: []int{1, 2, 3}})
	})
	src := NewSource(stream, func() error { return closeErr })

	got, err := src.Collect()
	if want := []int{1, 2, 3}; !slices.Equal(got, want) {
		t.Errorf("Collect = %v, want %v: a teardown failure must not discard the data", got, want)
	}
	if !errors.Is(err, errBoom) {
		t.Errorf("err = %v, want %v", err, errBoom)
	}
}

// Err is consulted rather than cached, so a failure that only becomes known
// during consumption is still reported afterwards.
func TestSourceErrIsConsultedNotCached(t *testing.T) {
	var failed error
	stream := Stream[int](func(yield func(Batch[int]) bool) {
		yield(Batch[int]{Items: []int{1}})
		failed = errBoom
	})
	src := NewSource(stream, func() error { return failed })

	if err := src.Err(); err != nil {
		t.Errorf("before consumption: Err = %v, want nil", err)
	}
	if _, err := src.Collect(); !errors.Is(err, errBoom) {
		t.Errorf("after consumption: Err = %v, want %v", err, errBoom)
	}
}

func TestSourceConsume(t *testing.T) {
	src := NewSource(Of([]int{1, 2, 3, 4}, 2), nil)

	var got []int
	err := src.Consume(func(b Batch[int]) bool {
		got = append(got, b.Items...)
		return true
	})
	if err != nil {
		t.Errorf("Consume = %v, want nil", err)
	}
	if want := []int{1, 2, 3, 4}; !slices.Equal(got, want) {
		t.Errorf("Consume saw %v, want %v", got, want)
	}
}

// Stopping early must not silence the error: a source that failed to open is
// still a source that failed to open.
func TestSourceConsumeReportsErrorAfterEarlyStop(t *testing.T) {
	src := NewSource(Of([]int{1, 2, 3, 4}, 2), func() error { return errBoom })

	n := 0
	err := src.Consume(func(Batch[int]) bool {
		n++
		return false
	})
	if n != 1 {
		t.Errorf("consumer saw %d batches, want 1", n)
	}
	if !errors.Is(err, errBoom) {
		t.Errorf("err = %v, want %v: an early stop must not swallow the error", err, errBoom)
	}
}

// The zero value must be usable: consuming it yields nothing and reports
// nothing, rather than dereferencing a nil stream.
func TestSourceZeroValueIsUsable(t *testing.T) {
	var src Source[int]

	got, err := src.Collect()
	if got != nil {
		t.Errorf("Collect = %v, want nothing", got)
	}
	if err != nil {
		t.Errorf("Err = %v, want nil", err)
	}
}

func TestSourceConsumeNilFunction(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("expected a panic at the call rather than a later nil dereference")
		}
	}()
	// The return value is irrelevant: the call must not reach it.
	_ = Source[int]{}.Consume(nil)
}

// The stream stays composable with the ordinary operators: the accessor exists
// to keep that true while leaving no way to swap the stream out from under err.
func TestSourceStreamComposes(t *testing.T) {
	src := NewSource(Of([]int{1, 2, 3, 4, 5, 6}, 2), nil)

	got := Collect(Take(Filter(src.Stream(), func(v int) bool { return v%2 == 0 }), 2))
	if want := []int{2, 4}; !slices.Equal(got, want) {
		t.Errorf("composed = %v, want %v", got, want)
	}
}

// A zero Source must be rangeable, not a nil call: the accessor substitutes an
// empty stream so that every Source is usable without a construction step.
func TestSourceZeroStreamIsRangeable(t *testing.T) {
	var src Source[int]

	n := 0
	for range src.Stream() {
		n++
	}
	if n != 0 {
		t.Errorf("zero Source yielded %d batches, want 0", n)
	}
}

// The error belongs to the Source, not to the streams derived from it. A caller
// who bounds the stream must still be able to learn why it was short.
func TestSourceErrSurvivesDerivedStream(t *testing.T) {
	src := NewSource(Of([]int{1, 2, 3, 4}, 2), func() error { return errBoom })

	got := Collect(Take(src.Stream(), 2))
	if want := []int{1, 2}; !slices.Equal(got, want) {
		t.Errorf("bounded = %v, want %v", got, want)
	}
	if err := src.Err(); !errors.Is(err, errBoom) {
		t.Errorf("Err after consuming a derived stream = %v, want %v", err, errBoom)
	}
}
