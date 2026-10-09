//go:build race

package bench

// raceEnabled reports whether the race detector is instrumenting this build.
// Its bookkeeping multiplies wall-clock time several fold, so timing assertions
// measure the instrumentation rather than the code.
const raceEnabled = true
