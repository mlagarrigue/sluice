package bench

import (
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/parallel"
)

// The macro benchmark the field report asks for: a reference ETL measured end
// to end, reporting an **overlap factor** — the busiest stage's own time
// divided by the wall clock. It tends to 1.0 when the pipeline is perfectly
// pipelined and to 1/n when n stages run strictly one after another.
//
// It exists because every other figure in this repository is per-element, and
// a pipeline can sit inside the ~1.5 ns budget at every stage and still lose
// half its wall clock to stages waiting on each other. "Hyper-performant" is
// claimed end to end or not at all, and this is the number that says whether
// Async earns its place outside a synthetic benchmark.
//
// The I/O is simulated rather than real. A benchmark that needed a PostgreSQL
// server would not run in CI, would not run on a laptop, and would measure
// that server's page cache more than this library — while the question here
// is the shape of the pipeline, which a sleep models exactly as well as a
// socket does.

// stageTimer accumulates how long one stage spent doing its own work, which is
// the numerator of the overlap factor, and how many batches it handled.
type stageTimer struct{ ns, calls atomic.Int64 }

func (s *stageTimer) spend(d time.Duration) {
	start := time.Now()
	for time.Since(start) < d {
	}
	s.ns.Add(int64(time.Since(start)))
	s.calls.Add(1)
}

func (s *stageTimer) elapsed() time.Duration { return time.Duration(s.ns.Load()) }

// etlShape is one run of the reference pipeline: read, transform, write, with
// the read and the write I/O-shaped and the transform CPU-shaped.
//
// wrap is where the two variants differ: the serial one returns its stream
// untouched, the overlapped one puts an Async between each pair of stages.
// calls counts the batches each stage handled, in read, transform, write order.
func etlShape(batches int, wrap func(sluice.Stream[int64]) sluice.Stream[int64]) (read, transform, write time.Duration, calls [3]int64) {
	const (
		perBatchRead  = 300 * time.Microsecond // a cursor fetch
		perBatchWrite = 400 * time.Microsecond // a COPY round trip
		perBatchCPU   = 200 * time.Microsecond // decode, enrich, encode
	)
	var rt, tt, wt stageTimer

	src := sluice.Stream[int64](func(yield func(sluice.Batch[int64]) bool) {
		buf := make([]int64, 1024)
		for i := range batches {
			rt.spend(perBatchRead)
			buf[0] = int64(i)
			if !yield(sluice.Batch[int64]{Items: buf}) {
				return
			}
		}
	})

	transformed := parallel.Ordered(wrap(src), 1, func(b sluice.Batch[int64]) sluice.Batch[int64] {
		tt.spend(perBatchCPU)
		return b
	})

	var acc int64
	wrap(transformed)(func(b sluice.Batch[int64]) bool {
		wt.spend(perBatchWrite)
		acc += b.Items[0]
		return true
	})
	sink = acc

	return rt.elapsed(), tt.elapsed(), wt.elapsed(),
		[3]int64{rt.calls.Load(), tt.calls.Load(), wt.calls.Load()}
}

// BenchmarkMacroETLSerial: the three stages strictly in sequence, which is
// what a synchronous pull pipeline does by default.
func BenchmarkMacroETLSerial(b *testing.B) {
	const batches = 40
	identity := func(s sluice.Stream[int64]) sluice.Stream[int64] { return s }

	var read, transform, write time.Duration
	for b.Loop() {
		r, tr, w, _ := etlShape(batches, identity)
		read, transform, write = read+r, transform+tr, write+w
	}
	reportOverlap(b, read, transform, write)
}

// BenchmarkMacroETLOverlapped: the same pipeline with Async between the
// stages, so reading, transforming and writing proceed at once.
func BenchmarkMacroETLOverlapped(b *testing.B) {
	const batches = 40
	async := func(s sluice.Stream[int64]) sluice.Stream[int64] { return parallel.Async(s, 4) }

	var read, transform, write time.Duration
	for b.Loop() {
		r, tr, w, _ := etlShape(batches, async)
		read, transform, write = read+r, transform+tr, write+w
	}
	reportOverlap(b, read, transform, write)
}

// reportOverlap publishes the ratio that makes the two runs comparable, plus
// the busiest stage's share so a reader can see which one the pipeline is
// waiting on.
// The stage durations are totals accumulated over every iteration, matched
// against the full elapsed time: a last-iteration numerator over an
// all-iterations denominator would let one preempted run drift the ratio past
// 1.0 with no code change.
func reportOverlap(b *testing.B, read, transform, write time.Duration) {
	b.Helper()
	busiest := max(read, transform, write)
	total := read + transform + write
	wall := b.Elapsed()

	// Overlap: 1.0 means the wall clock is the busiest stage alone — perfect
	// pipelining. The serial floor is busiest/total.
	b.ReportMetric(float64(busiest)/float64(wall), "overlap")
	b.ReportMetric(float64((wall / time.Duration(max(b.N, 1))).Microseconds()), "us/run")
	b.ReportMetric(float64(busiest)/float64(total), "busiest-share")
}

// The overlap factor must mean what it claims, so it is asserted rather than
// only reported: the serial shape cannot overlap, the Async shape must.
func TestMacroOverlapDistinguishesTheTwoShapes(t *testing.T) {
	if testing.Short() {
		t.Skip("timing-sensitive: skipped under -short")
	}
	const batches = 12
	// Three stages can only overlap on three cores: below that the claim is
	// false by construction, not by regression.
	if runtime.GOMAXPROCS(0) < 3 {
		t.Skipf("GOMAXPROCS=%d: three stages cannot overlap on fewer than three cores", runtime.GOMAXPROCS(0))
	}

	// Best of several runs per shape. One run each compared a single
	// preemption against another — 10.70 ms against 10.88 ms was observed on
	// a loaded machine — while the minimum of a few is what the code costs
	// when the scheduler leaves it alone, which is the claim under test.
	type run struct {
		r, t, w time.Duration
		calls   [3]int64
		wall    time.Duration
	}
	best := func(shape func(sluice.Stream[int64]) sluice.Stream[int64]) run {
		var b run
		for i := range 5 {
			start := time.Now()
			r, tr, w, calls := etlShape(batches, shape)
			got := run{r, tr, w, calls, time.Since(start)}
			if i == 0 || got.wall < b.wall {
				b = got
			}
		}
		return b
	}
	serial := best(func(s sluice.Stream[int64]) sluice.Stream[int64] { return s })
	async := best(func(s sluice.Stream[int64]) sluice.Stream[int64] { return parallel.Async(s, 4) })
	r1, t1, w1, calls1, serialWall := serial.r, serial.t, serial.w, serial.calls, serial.wall
	r2, t2, w2, calls2, asyncWall := async.r, async.t, async.w, async.calls, async.wall

	// The same work was done either way, which is what makes the wall clocks
	// comparable at all. Counted, not timed: a spin-wait overshoots its target
	// whenever the scheduler preempts it, so a ratio of measured work failed
	// on a loaded machine while both shapes had handled every batch.
	if want := [3]int64{batches, batches, batches}; calls1 != want || calls2 != want {
		t.Fatalf("batches per stage (read, transform, write): serial %v, async %v, want %v each — "+
			"they are not the same benchmark", calls1, calls2, want)
	}

	// The wall-clock assertions are skipped under the race detector — its
	// instrumentation makes three spin-waiting goroutines no faster
	// overlapped than serial on a loaded machine, and a gate that cries wolf
	// erodes the pre-commit command it lives in. The work-equivalence check
	// above still ran: the shapes themselves are exercised on every race run,
	// only the timing claim waits for an uninstrumented build.
	if raceEnabled {
		t.Skip("the race detector instruments every memory access: timings measure it, not the code")
	}
	if asyncWall >= serialWall {
		t.Errorf("overlapped run took %v against serial %v — Async bought nothing", asyncWall, serialWall)
	}
	overlapSerial := float64(max(r1, t1, w1)) / float64(serialWall)
	overlapAsync := float64(max(r2, t2, w2)) / float64(asyncWall)
	if overlapAsync <= overlapSerial {
		t.Errorf("overlap factor %.2f (async) is not above %.2f (serial)", overlapAsync, overlapSerial)
	}
	t.Logf("overlap: serial %.2f, async %.2f (wall %v vs %v)",
		overlapSerial, overlapAsync, serialWall, asyncWall)
}
