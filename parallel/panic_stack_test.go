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

// stringStage panics with a value of the caller's own, not an error: named,
// so its frame can be looked for.
func stringStage(sluice.Batch[int]) sluice.Batch[int] {
	panic("stage boom")
}

// overflowStage raises a sentinel on the worker, as a bounded operator run
// inside the stage would.
func overflowStage(sluice.Batch[int]) sluice.Batch[int] {
	panic(sluice.ErrOverflow)
}

// A panic raised on a worker or a producer reaches the consumer as a
// *sluice.PanicError whose message holds the stack it was raised on: without
// it, the crash shows the consumer's goroutine only, and the line that
// failed is nowhere. A runtime error is still found by errors.As; any other
// value is the envelope's Value.
func TestPanicCrossesWithItsStack(t *testing.T) {
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
		{"Ordered/string", "parallel.stringStage", func() {
			sluice.Collect(Ordered(sluice.Of(make([]int, 64), 4), 4, stringStage))
		}},
		{"Unordered/string", "parallel.stringStage", func() {
			sluice.Collect(Unordered(sluice.Of(make([]int, 64), 4), 4, stringStage))
		}},
		// Relayed twice, by Ordered then by Async: one envelope, the
		// worker's stack.
		{"Async/Ordered/string", "parallel.stringStage", func() {
			sluice.Collect(Async(Ordered(sluice.Of(make([]int, 64), 4), 4, stringStage), 2))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var recovered any
			func() {
				defer func() { recovered = recover() }()
				tc.run()
			}()
			pe, ok := recovered.(*sluice.PanicError)
			if !ok {
				t.Fatalf("recovered %T %v, want a *sluice.PanicError", recovered, recovered)
			}
			if _, isErr := pe.Value.(error); isErr {
				if _, ok := errors.AsType[runtime.Error](pe); !ok {
					t.Errorf("errors.As finds no runtime.Error in %v", pe)
				}
			} else if pe.Value != "stage boom" {
				t.Errorf("Value %#v, want %q", pe.Value, "stage boom")
			}
			if _, twice := pe.Value.(*sluice.PanicError); twice {
				t.Errorf("relayed twice, wrapped twice: %v", pe)
			}
			if !strings.Contains(pe.Error(), tc.frame) {
				t.Errorf("the message does not show %s:\n%s", tc.frame, pe)
			}
		})
	}
}

// A sentinel raised on a worker crosses in the envelope like any panic, and
// Try still recognises it and returns it as the worker raised it.
func TestTryRecoversASentinelRaisedOnAWorker(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func()
	}{
		{"Ordered", func() { sluice.Collect(Ordered(sluice.Of(make([]int, 64), 4), 4, overflowStage)) }},
		{"Unordered", func() { sluice.Collect(Unordered(sluice.Of(make([]int, 64), 4), 4, overflowStage)) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := sluice.Try(tc.run)
			if err != sluice.ErrOverflow { //nolint:errorlint // returned as raised, not wrapped
				t.Errorf("Try returned %v, want sluice.ErrOverflow itself", err)
			}
		})
	}
}
