package window

import (
	"errors"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
)

type cell struct{ x int }

// nilSource dereferences a nil pointer on CoalesceWithin's producer
// goroutine: named, so its frame can be looked for.
func nilSource(yield func(sluice.Batch[int]) bool) {
	var c *cell
	yield(sluice.Batch[int]{Items: []int{c.x}})
}

// A runtime error in the source reaches the consumer still a runtime error
// (errors.As), its message holding the producer's stack, as in parallel.
func TestCoalesceWithinRuntimeErrorCrossesWithItsStack(t *testing.T) {
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		sluice.Collect(CoalesceWithin(sluice.Stream[int](nilSource), 4, time.Second))
	}()
	err, ok := recovered.(error)
	if !ok {
		t.Fatalf("recovered %T %v, want an error", recovered, recovered)
	}
	if _, ok := errors.AsType[runtime.Error](err); !ok {
		t.Errorf("errors.As finds no runtime.Error in %v", err)
	}
	if !strings.Contains(err.Error(), "window.nilSource") {
		t.Errorf("the message does not show window.nilSource:\n%s", err)
	}
}
