package diagnostics

// Collector collects diagnostics under a ceiling, counting what it had to
// drop.
//
// SARIF, FHIR and Problem Details are all finite documents: they assume the
// report ends. A stream does not. One pathological batch — a thousand entities
// each failing forty rules — produces forty thousand diagnostics, and an
// unbounded collector turns that into memory pressure at exactly the moment the
// system is already unhappy. Worse, the interesting diagnostic is buried under
// the repetitive ones.
//
//	diags := NewCollector(100)
//	diags.Add(d)
//	if diags.Truncated() > 0 {
//	    // 100 kept, Truncated() more were dropped
//	}
//
// The ceiling is a required parameter rather than a default, because a limit
// nobody chose is a limit nobody owns: guarantee S1 says every bound is stated
// by the caller.
//
// # What is kept
//
// The first n, in the order they arrived. Not the most severe, not the last:
// stable ordering is what makes a diagnostic report reproducible, and a
// collector that reshuffles under load produces a different answer for the same
// input depending on how much came before. Callers who want the worst
// diagnostics rather than the first ones filter by severity on the way in.
//
// # The zero value
//
// The zero Collector has a ceiling of zero: it keeps nothing and counts
// everything as truncated. That is deliberate — a collector nobody sized is a
// collector nobody meant to fill, and reporting the count still tells the
// caller diagnostics were produced.
//
// # Concurrency
//
// A Collector is used by one goroutine and is not itself safe to share. The
// shape that trips this up: a collector handed to an operator's limit (such
// as join.BuildLimit.Diagnostics) is written by whichever goroutine drives
// that operator — behind a parallel.Async, the producer goroutine — so calling
// All or Truncated while the pipeline still runs is a data race. Read it on
// the same goroutine, or after the stream has completed.
type Collector struct {
	items     []Diagnostic
	limit     int
	truncated int
}

// initialDiagnostics caps the storage claimed on the first Add, so that the
// ceiling bounds what is kept without dictating what is allocated up front.
// Reports beyond this size grow into their limit; see [Collector.Add] for the
// measurements either extreme costs.
const initialDiagnostics = 16

// NewCollector returns a collector that keeps at most limit diagnostics.
//
// A limit of zero or less keeps nothing and counts every diagnostic as
// truncated, which is a usable "count but do not store" mode rather than an
// error.
//
// The backing storage is claimed on the first Add rather than up front: limit
// is often a configuration value, and a large one must not allocate on the
// batches — the overwhelming majority — that produce no diagnostic at all.
func NewCollector(limit int) *Collector {
	return &Collector{limit: limit}
}

// Add records a diagnostic, or counts it as truncated if the ceiling is
// reached.
//
// It reports whether the diagnostic was kept, so a caller that stops producing
// once the report is full can do so without asking twice.
func (d *Collector) Add(diag Diagnostic) bool {
	if len(d.items) >= d.limit {
		d.truncated++
		return false
	}
	if d.items == nil {
		// Claimed on first use, and capped: nothing is allocated for the
		// batches that report nothing, and a large configured limit does not
		// turn the first diagnostic into a large allocation.
		//
		// Both extremes were measured on 112-byte diagnostics. Reserving the
		// full limit costs 125 MB and 8.6 ms to record four diagnostics under a
		// 1M ceiling. Reserving nothing and letting append grow costs 12
		// allocations, 3.3x the memory and 3x the time to fill a 1000-entry
		// report, because every doubling copies what is already there.
		//
		// Reserving min(limit, initialDiagnostics) takes neither: the common
		// report fits in the first allocation, and a ceiling set high "just in
		// case" is paid for only as it actually fills. Measured at 2 KB and one
		// allocation for the first Add under a 1M ceiling.
		//
		// Past the ceiling the cost is a counter increment — 1.9 ns, no
		// allocation — which is what makes a pathological batch cheap to
		// survive rather than merely bounded.
		d.items = make([]Diagnostic, 0, min(d.limit, initialDiagnostics))
	}
	d.items = append(d.items, diag)
	return true
}

// All returns the kept diagnostics, in arrival order.
//
// The slice aliases the collector's storage rather than copying it. A caller
// that retains it across further Add calls sees them appear, and one that
// retains it across a [Collector.Reset] finds it zeroed — Reset clears the
// entries, so a retained slice reads back as empty diagnostics rather than as
// the ones it held. Copy before either.
func (d *Collector) All() []Diagnostic { return d.items }

// Len reports how many diagnostics were kept.
func (d *Collector) Len() int { return len(d.items) }

// Truncated reports how many diagnostics were dropped because the ceiling was
// reached.
//
// A report with a non-zero count is incomplete, and saying so is the point: a
// truncation that is not counted is indistinguishable from a clean run, which
// is how a pathological batch hides.
func (d *Collector) Truncated() int { return d.truncated }

// Worst returns the highest severity recorded, and whether there was any
// diagnostic at all.
//
// It answers the question a caller actually asks — "did anything serious happen
// here" — without walking the slice at every call site. It reflects the kept
// diagnostics only: what was truncated is not inspected, so a report with a
// non-zero [Collector.Truncated] may hide something worse.
func (d *Collector) Worst() (Severity, bool) {
	if len(d.items) == 0 {
		return Info, false
	}
	worst := d.items[0].Severity
	for _, it := range d.items[1:] {
		if it.Severity > worst {
			worst = it.Severity
		}
	}
	return worst, true
}

// Reset clears the collector for reuse, keeping its ceiling and its storage.
//
// This is what makes a per-batch ceiling affordable: an operator reporting
// diagnostics batch by batch resets between batches instead of allocating a new
// collector each time. The truncation count is cleared too — it describes one
// batch, not the run.
//
// The kept diagnostics are zeroed rather than merely forgotten. Truncating the
// slice would leave them live in the backing array, and a Diagnostic holds an
// Args map and a wrapped cause — an error chain that can reach arbitrarily far.
// A collector reused across a long run would then pin the first batch's memory
// for the life of the process, which is the unbounded retention guarantee S1
// exists to prevent. Clearing costs one pass over at most limit entries, on a
// path that runs once per batch.
func (d *Collector) Reset() {
	clear(d.items)
	d.items = d.items[:0]
	d.truncated = 0
}
