package bench

import (
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/parallel"
)

// The macro benchmark: a reference ETL measured end
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
// the numerator of the overlap factor, and how many batches it handled. The
// stages of one run share a working count, whose peak is how many of them
// were ever inside their work at once.
type stageTimer struct {
	ns, calls atomic.Int64
	working   *working
}

type working struct{ now, peak atomic.Int64 }

func (s *stageTimer) spend(d time.Duration) {
	n := s.working.now.Add(1)
	for p := s.working.peak.Load(); n > p && !s.working.peak.CompareAndSwap(p, n); p = s.working.peak.Load() {
	}
	start := time.Now()
	for time.Since(start) < d {
	}
	s.ns.Add(int64(time.Since(start)))
	s.calls.Add(1)
	s.working.now.Add(-1)
}

func (s *stageTimer) elapsed() time.Duration { return time.Duration(s.ns.Load()) }

// etlShape is one run of the reference pipeline: read, transform, write, with
// the read and the write I/O-shaped and the transform CPU-shaped.
//
// wrap is where the two variants differ: the serial one returns its stream
// untouched, the overlapped one puts an Async between each pair of stages.
// calls counts the batches each stage handled, in read, transform, write order;
// peak is the most stages that were inside their work at one instant.
func etlShape(batches int, wrap func(sluice.Stream[int64]) sluice.Stream[int64]) (read, transform, write time.Duration, calls [3]int64, peak int64) {
	const (
		perBatchRead  = 300 * time.Microsecond // a cursor fetch
		perBatchWrite = 400 * time.Microsecond // a COPY round trip
		perBatchCPU   = 200 * time.Microsecond // decode, enrich, encode
	)
	var w working
	rt, tt, wt := stageTimer{working: &w}, stageTimer{working: &w}, stageTimer{working: &w}

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
		[3]int64{rt.calls.Load(), tt.calls.Load(), wt.calls.Load()}, w.peak.Load()
}

// BenchmarkMacroETLSerial: the three stages strictly in sequence, which is
// what a synchronous pull pipeline does by default.
func BenchmarkMacroETLSerial(b *testing.B) {
	const batches = 40
	identity := func(s sluice.Stream[int64]) sluice.Stream[int64] { return s }

	var read, transform, write time.Duration
	for b.Loop() {
		r, tr, w, _, _ := etlShape(batches, identity)
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
		r, tr, w, _, _ := etlShape(batches, async)
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

// The overlap factor must mean what it claims, so the shapes behind it are
// asserted rather than only reported: the serial shape cannot overlap, the
// Async shape must.
//
// What is asserted is how many stages were ever inside their work at once,
// not the wall clock. The wall clock asked whether the runner had three
// cores free: under the whole suite with coverage it did not, and the
// overlapped run took 10.92 ms against the serial 10.84 ms (CI 2026-10-16)
// while its stages were working at once — a descheduled thread is still
// mid-stage. The overlap factor itself is the benchmarks' to report.
func TestMacroOverlapDistinguishesTheTwoShapes(t *testing.T) {
	const batches = 12
	// Two stages can only work at once on two Ps: below that the claim is
	// false by construction, not by regression.
	if runtime.GOMAXPROCS(0) < 2 {
		t.Skipf("GOMAXPROCS=%d: two stages cannot work at once on one P", runtime.GOMAXPROCS(0))
	}

	_, _, _, calls1, serial := etlShape(batches, func(s sluice.Stream[int64]) sluice.Stream[int64] { return s })
	_, _, _, calls2, async := etlShape(batches, func(s sluice.Stream[int64]) sluice.Stream[int64] { return parallel.Async(s, 4) })

	// The same work was done either way, which is what makes the two shapes
	// comparable at all.
	if want := [3]int64{batches, batches, batches}; calls1 != want || calls2 != want {
		t.Fatalf("batches per stage (read, transform, write): serial %v, async %v, want %v each — "+
			"they are not the same benchmark", calls1, calls2, want)
	}
	if serial != 1 {
		t.Errorf("serial shape: %d stages worked at once, want 1 — a synchronous pull pipeline overlapped", serial)
	}
	if async < 2 {
		t.Errorf("Async shape: %d stage worked at once, want at least 2 — Async bought nothing", async)
	}
	t.Logf("stages working at once: serial %d, async %d", serial, async)
}
