package parallel

import (
	"errors"
	"runtime"
	"strings"
	"testing"

	"github.com/mlagarrigue/sluice"
)

type cell struct{ x int }

// nilStage dereferences a nil pointer, the bug ADR 0006 has crash "loudly,
// with its stack": named, so its frame can be looked for.
func nilStage(b sluice.Batch[int]) sluice.Batch[int] {
	var c *cell
	return sluice.Batch[int]{Items: []int{c.x + b.Len()}}
}

// nilSource is the same bug in a source.
func nilSource(yield func(sluice.Batch[int]) bool) {
	var c *cell
	yield(sluice.Batch[int]{Items: []int{c.x}})
}

// A runtime error raised on a worker or a producer reaches the consumer as
// an error that still is the runtime error (errors.As) and whose message
// holds the stack it was raised on: without it, the crash shows the
// consumer's goroutine only, and the line that dereferenced nil is nowhere.
// Values the caller chose to panic with come back as raised: the tests of
// each operator compare them, unchanged.
func TestRuntimeErrorCrossesWithItsStack(t *testing.T) {
	for _, tc := range []struct {
		name  string
		frame string
		run   func()
	}{
		{"Ordered", "parallel.nilStage", func() {
			sluice.Collect(Ordered(sluice.Of(make([]int, 64), 4), 4, nilStage))
		}},
		{"Unordered", "parallel.nilStage", func() {
			sluice.Collect(Unordered(sluice.Of(make([]int, 64), 4), 4, nilStage))
		}},
		{"Async", "parallel.nilSource", func() {
			sluice.Collect(Async(sluice.Stream[int](nilSource), 2))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var recovered any
			func() {
				defer func() { recovered = recover() }()
				tc.run()
			}()
			err, ok := recovered.(error)
			if !ok {
				t.Fatalf("recovered %T %v, want an error", recovered, recovered)
			}
			if _, ok := errors.AsType[runtime.Error](err); !ok {
				t.Errorf("errors.As finds no runtime.Error in %v", err)
			}
			if !strings.Contains(err.Error(), tc.frame) {
				t.Errorf("the message does not show %s:\n%s", tc.frame, err)
			}
		})
	}
}
