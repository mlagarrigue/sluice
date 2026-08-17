package bench

import (
	"testing"

	"github.com/mlagarrigue/sluice"
)

// budgetNsPerStage is the ceiling recorded in docs/BENCHMARK-STEP-0.md: ~1.5 ns
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
	)
	src := make([]int64, elems)
	for i := range src {
		src[i] = int64(i)
	}

	run := func() {
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

	// A warm-up pass so the first run's page faults and branch predictor state
	// do not land in the measurement.
	run()

	// The closure is a benchmark body, not a test helper: b.Helper() would be
	// meaningless here.
	res := testing.Benchmark(func(b *testing.B) { //nolint:thelper // benchmark body
		for b.Loop() {
			run()
		}
	})

	perStage := float64(res.NsPerOp()) / float64(elems) / float64(stages)
	ceiling := budgetNsPerStage * tolerance

	t.Logf("%.3f ns per stage per element (budget %.1f, ceiling %.1f)",
		perStage, budgetNsPerStage, ceiling)

	if perStage > ceiling {
		t.Errorf("pipeline costs %.3f ns per stage per element, over the %.1f ceiling: "+
			"re-run internal/bench and check docs/BENCHMARK-STEP-0.md before raising it",
			perStage, ceiling)
	}

	// Building the pipeline allocates a handful of escaping closures — a fixed
	// cost per construction, not per element. What must stay at zero is
	// allocation that scales with the data: a few dozen is construction, a few
	// thousand means an operator started allocating per batch.
	const maxSetupAllocs = 64
	if res.AllocsPerOp() > maxSetupAllocs {
		t.Errorf("pipeline allocated %d times per run over %d elements, want <= %d: "+
			"an operator is allocating per batch",
			res.AllocsPerOp(), elems, maxSetupAllocs)
	}
}
