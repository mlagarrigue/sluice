package bench

import (
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
)

// budgetNsPerStage is the ceiling recorded in docs/benchmarks.md: ~1.5 ns
// per stage per element for a batched pipeline.
//
// The assertion below allows twice that. The point is not to reproduce the
// measurement — a shared CI runner cannot — but to catch the regression that
// changes the order of magnitude: an allocation per batch, a lost inlining, an
// operator that copies where it used to reuse. Anything within 2x is noise
// between machines; anything beyond it is a design change.
const (
	budgetNsPerStage = 1.5
	tolerance        = 2.0
)

// TestStageBudget keeps the documented ceiling honest. Convention says every
// core operator is measured against it; without an assertion the figure lives
// only in a Markdown file, and a regression ships unnoticed.
func TestStageBudget(t *testing.T) {
	if testing.Short() {
		t.Skip("timing-sensitive: skipped under -short")
	}
	if raceEnabled {
		t.Skip("the race detector instruments every memory access: timings measure it, not the code")
	}

	const (
		elems  = 1 << 20
		stages = 4
		// One timed round covers one window of the source: short enough
		// (a fraction of a millisecond) that some rounds fit between two
		// preemptions even on an oversubscribed machine, while the windows
		// rotate through a source larger than the L2 cache, so the data
		// arrives from memory the way it does for a real pipeline.
		window = 1 << 16
	)
	src := make([]int64, elems)
	for i := range src {
		src[i] = int64(i)
	}

	runOver := func(src []int64) {
		s := sluice.Of(src, 1024)
		for range stages {
			s = sluice.Map(s, inc)
		}
		var acc int64
		s(func(b sluice.Batch[int64]) bool {
			for _, v := range b.Items {
				acc += v
			}
			return true
		})
		sink = acc
	}
	run := func() { runOver(src) }

	// A warm-up pass so the first run's page faults and branch predictor state
	// do not land in the measurement.
	run()

	// The fastest of many short rounds, not one long average. Interference on
	// a shared machine only ever adds time, so a single one-second average
	// reports the neighbours: it read 3.39 ns on a loaded run of the same code
	// that measures 1.3 ns idle. The minimum over short rounds is the code. A
	// real regression — an allocation per batch, a lost inlining — slows every
	// round, the fastest included.
	const rounds = 32 * elems / window
	best := time.Duration(1<<63 - 1)
	for r := range rounds {
		lo := r % (elems / window) * window
		start := time.Now()
		runOver(src[lo : lo+window])
		best = min(best, time.Since(start))
	}

	perStage := float64(best.Nanoseconds()) / float64(window) / float64(stages)
	ceiling := budgetNsPerStage * tolerance

	t.Logf("%.3f ns per stage per element, fastest of %d rounds (budget %.1f, ceiling %.1f)",
		perStage, rounds, budgetNsPerStage, ceiling)

	if perStage > ceiling {
		t.Errorf("pipeline costs %.3f ns per stage per element, over the %.1f ceiling: "+
			"re-run internal/bench and check docs/benchmarks.md before raising it",
			perStage, ceiling)
	}

	// Building the pipeline allocates a handful of escaping closures — a fixed
	// cost per construction, not per element. What must stay at zero is
	// allocation that scales with the data: a few dozen is construction, a few
	// thousand means an operator started allocating per batch.
	const maxSetupAllocs = 64
	if allocs := testing.AllocsPerRun(5, run); allocs > maxSetupAllocs {
		t.Errorf("pipeline allocated %.0f times per run over %d elements, want <= %d: "+
			"an operator is allocating per batch",
			allocs, elems, maxSetupAllocs)
	}
}
