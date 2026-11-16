package gateway

import (
	"errors"
	"runtime"
	"slices"
	"testing"

	"github.com/mlagarrigue/sluice"
)

func lookupCalls(keys ...int) []*Call[int, string] {
	calls := make([]*Call[int, string], len(keys))
	for i, k := range keys {
		calls[i] = &Call[int, string]{In: k, reply: make(chan result[string], 1)}
	}
	return calls
}

// streamOf hands the pipeline the given batches, one after the other.
func streamOf(batches ...[]*Call[int, string]) sluice.Stream[*Call[int, string]] {
	return func(yield func(sluice.Batch[*Call[int, string]]) bool) {
		for _, b := range batches {
			if !yield(sluice.Batch[*Call[int, string]]{Items: b}) {
				return
			}
		}
	}
}

// Each caller gets the value of its key, the error for a key not found, or
// the batch's error; fetch sees every key once, in the order first asked.
func TestLookup(t *testing.T) {
	errMissing := errors.New("no such key")
	errDown := errors.New("down")
	values := map[int]string{1: "one", 2: "two", 3: "three"}
	for _, tc := range []struct {
		name     string
		keys     []int
		fail     error
		missing  error
		wantKeys []int
		want     []string // "" with an error
		wantErr  []error
	}{
		{
			name: "found, absent, repeated", keys: []int{2, 9, 2, 1}, missing: errMissing,
			wantKeys: []int{2, 9, 1},
			want:     []string{"two", "", "two", "one"},
			wantErr:  []error{nil, errMissing, nil, nil},
		},
		{
			name: "the batch fails whole", keys: []int{1, 3}, fail: errDown, missing: errMissing,
			wantKeys: []int{1, 3},
			want:     []string{"", ""},
			wantErr:  []error{errDown, errDown},
		},
		{
			name: "a nil missing is ErrNoReply", keys: []int{7}, missing: nil,
			wantKeys: []int{7},
			want:     []string{""},
			wantErr:  []error{ErrNoReply},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var seen []int
			pipeline := Lookup(tc.missing, func(keys []int, found map[int]string) error {
				seen = slices.Clone(keys)
				for _, k := range keys {
					if v, ok := values[k]; ok {
						found[k] = v
					}
				}
				return tc.fail
			})
			calls := lookupCalls(tc.keys...)
			pipeline(streamOf(calls))
			if !slices.Equal(seen, tc.wantKeys) {
				t.Errorf("fetch saw %v, want %v", seen, tc.wantKeys)
			}
			for i, c := range calls {
				r := <-c.reply
				if r.out != tc.want[i] || !errors.Is(r.err, tc.wantErr[i]) || (r.err == nil) != (tc.wantErr[i] == nil) {
					t.Errorf("call %d (key %d) = %q, %v; want %q, %v", i, c.In, r.out, r.err, tc.want[i], tc.wantErr[i])
				}
			}
		})
	}
}

// What one batch found is not another's: the map is cleared between batches.
func TestLookupForgetsTheLastBatch(t *testing.T) {
	batch := 0
	pipeline := Lookup(ErrNoReply, func(keys []int, found map[int]string) error {
		batch++
		if batch == 1 {
			found[1] = "first batch only"
		}
		return nil
	})
	first, second := lookupCalls(1), lookupCalls(1)
	pipeline(streamOf(first, second))
	if r := <-first[0].reply; r.out != "first batch only" {
		t.Errorf("first batch = %q, %v", r.out, r.err)
	}
	if r := <-second[0].reply; r.err == nil {
		t.Errorf("the second batch answered %q from the first's map", r.out)
	}
}

// Once its buffers have grown, the pipeline allocates nothing per batch of
// its own: a thousand batches cost what the first few do.
func TestLookupAllocatesNothingPerBatch(t *testing.T) {
	const batches = 1000
	set := make([][]*Call[int, string], batches)
	for i := range set {
		set[i] = lookupCalls(1, 2, 3, 2, 4, 5, 6, 7)
	}
	pipeline := Lookup(ErrNoReply, func(keys []int, found map[int]string) error {
		for _, k := range keys {
			found[k] = "v"
		}
		return nil
	})
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	pipeline(streamOf(set...))
	runtime.ReadMemStats(&after)
	if n := after.Mallocs - before.Mallocs; n > 20 {
		t.Errorf("%d allocations for %d batches; want the setup's alone", n, batches)
	}
}

func TestLookupRequiresFetch(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("a nil fetch was accepted")
		}
	}()
	Lookup[int, string](nil, nil)
}
