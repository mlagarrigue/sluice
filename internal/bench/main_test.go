package bench

import (
	"os"
	"testing"
	"time"
)

// TestMain spins the CPU before any measurement runs.
//
// Without it, the first benchmark of the process absorbs the frequency ramp: on
// this machine BenchmarkBaselineLoop reported 0.65 ns/element on its first
// iteration and 0.30 on its last, a monotonic decline of more than 2x. Since the
// baseline is the denominator every other figure is expressed against, that
// inflated it by ~24% and made the whole report drift between runs — the same
// iter.Pull measurement came out at x225 and x183 on two consecutive runs of an
// unchanged tree.
//
// Two causes, both handled here:
//
//   - the clock ramp, which needs sustained work to trigger;
//   - the first traversal of a freshly allocated dataset, which pays a page
//     fault per 4 KiB — 2048 of them for N int64s.
//
// Measured on an idle machine: 1s of spinning plus one faulted-in dataset
// brings the first iteration in line with the rest.
func TestMain(m *testing.M) {
	warmUp(time.Second)
	os.Exit(m.Run())
}

// warmUp keeps one core busy long enough for the governor to raise the clock.
// The work is deliberately dependent — each iteration needs the previous
// result — so the compiler cannot elide it and the pipeline cannot run it wide.
func warmUp(d time.Duration) {
	// Fault in a dataset of the size the benchmarks use, so the first one to
	// run does not pay for it inside a timed loop.
	warm := data(N)
	var acc int64
	for _, v := range warm {
		acc += v
	}

	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		for i := range 1 << 16 {
			acc = acc*31 + int64(i)
		}
	}
	sink = acc
}
