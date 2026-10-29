package sluice

import (
	"runtime"
	"strings"
	"testing"
)

// coroutines counts the runtime coroutines alive in the process, as the
// goroutine dump names them.
func coroutines() int {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return strings.Count(string(buf[:n]), "[coroutine]")
		}
		buf = make([]byte, 2*len(buf))
	}
}

// Like a Stream, a Split never read runs nothing. Split pulled its source
// at construction, and iter.Pull starts the source's coroutine there: a
// Split whose branches were never read left it parked for the life of the
// process, with no handle to stop it — one per request for a service that
// builds a Split per request and abandons it. The shared traversal now
// starts at the first read of a branch.
func TestASplitNeverReadParksNothing(t *testing.T) {
	const n = 200
	src := Of(make([]int, 64), 8)
	route := func(Batch[int]) []int { return []int{0} }
	before := coroutines()
	splits := make([][]Stream[int], 0, n)
	for range n {
		splits = append(splits, Split(src, 2, route))
	}
	runtime.GC()
	parked := coroutines() - before
	runtime.KeepAlive(splits)
	if parked > n/10 {
		t.Fatalf("%d Splits built and never read park %d coroutines", n, parked)
	}

	// Read, they still deliver, and the coroutine goes once every branch
	// is done.
	branches := Split(src, 2, route)
	got := 0
	for b := range branches[0] {
		got += b.Len()
	}
	for range branches[1] {
		t.Error("branch 1 received a batch route never chose for it")
	}
	if got != 64 {
		t.Errorf("branch 0 received %d items, want 64", got)
	}
}
