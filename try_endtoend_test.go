package sluice_test

import (
	"errors"
	"testing"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/distinct"
	"github.com/mlagarrigue/sluice/parallel"
)

// End to end: a real operator raising a sentinel through a real pipeline, not
// a hand-thrown panic — Distinct under sluice.Fail, and ParallelInit's wrapped error
// with the cause reachable through errors.Is.
func TestTryEndToEnd(t *testing.T) {
	t.Run("overflow", func(t *testing.T) {
		err := sluice.Try(func() {
			sluice.Collect(distinct.By(sluice.Of([]int{1, 2, 3}, 1), 1, func(v int) int { return v }, sluice.Fail))
		})
		if !errors.Is(err, sluice.ErrOverflow) {
			t.Errorf("got %v, want sluice.ErrOverflow", err)
		}
	})
	t.Run("parallel init", func(t *testing.T) {
		cause := errors.New("dictionary missing")
		err := sluice.Try(func() {
			sluice.Collect(parallel.WithState(sluice.Of([]int{1}, 1), 4,
				func() (int, error) { return 0, cause },
				func(int, sluice.Batch[int]) sluice.Batch[int] { return sluice.Batch[int]{} }))
		})
		if !errors.Is(err, parallel.ErrState) || !errors.Is(err, cause) {
			t.Errorf("got %v, want both parallel.ErrState and the cause reachable", err)
		}
	})
}
