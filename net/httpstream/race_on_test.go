//go:build race

package httpstream

// raceEnabled reports whether the race detector is instrumenting this build.
// It keeps shadow state for every goroutine, so a heap measured under it
// counts the instrumentation of each one: an HTTP/2 connection runs more of
// them than an HTTP/1.1 one, and the ratio between the two moves.
const raceEnabled = true
