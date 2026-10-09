package join

import (
	"cmp"
	"testing"

	"github.com/mlagarrigue/sluice"
)

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
			e := newEmitter(func(sluice.Batch[int]) bool { return tt.accept })
			e.push(1) // one row, far short of a full batch: only flush emits it

			if got := e.flush(); got != tt.want {
				t.Errorf("flush() = %v, want %v", got, tt.want)
			}
		})
	}

	// An empty buffer yields nothing and reports success: there was nothing to
	// refuse.
	called := false
	e := newEmitter(func(sluice.Batch[int]) bool { called = true; return false })
	if !e.flush() {
		t.Error("flush() on an empty buffer = false, want true")
	}
	if called {
		t.Error("flush() emitted a batch with nothing buffered")
	}
}

// emitter is the buffer behind every N-to-1 operator, and it is reused between
// batches exactly like sluice.Filter's. Pinned separately because the operators above
// cannot reach it: it only fills past sluice.DefaultBatchSize.
func TestEmitterBufferReuseContract(t *testing.T) {
	const n = 2 * sluice.DefaultBatchSize
	left := make([]int, n)
	for i := range left {
		left[i] = i
	}

	build := map[string]func() sluice.Stream[EitherOrBoth[int, int]]{
		"Merge": func() sluice.Stream[EitherOrBoth[int, int]] {
			return Merge(sluice.Of(left, 100), sluice.Empty[int](), identity, identity, cmp.Compare[int])
		},
		"ZipLongest": func() sluice.Stream[EitherOrBoth[int, int]] {
			return ZipLongest(sluice.Of(left, 100), sluice.Empty[int]())
		},
	}

	for name, mk := range build {
		t.Run(name, func(t *testing.T) {
			var kept [][]EitherOrBoth[int, int]
			mk()(func(b sluice.Batch[EitherOrBoth[int, int]]) bool {
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
