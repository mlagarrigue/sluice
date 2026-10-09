package bench

import (
	"testing"

	"github.com/mlagarrigue/sluice/diagnostics"
)

// Diagnostics are not on the per-element hot path — a pipeline that reports one
// per element has bigger problems than the reporting cost — so these are not
// measured against the ~1.5 ns per-stage budget. What they pin is allocation
// behaviour, which is where a reporting type goes wrong: a path that allocates
// per segment, or a collector that claims its whole ceiling on first use.

var pathSink string

// BenchmarkPathBuild builds the §4.1 example path. Each extension copies, which
// is what makes a prefix safe to hold across branches; the count here is what
// that costs.
func BenchmarkPathBuild(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		pathSink = diagnostics.Field("lines").Index(7).Field("saleQty").String()
	}
}

// BenchmarkPathStringDeep renders a path deep enough that the builder has to
// grow, so the figure covers more than a single small write.
func BenchmarkPathStringDeep(b *testing.B) {
	p := diagnostics.Field("order")
	for i := range 16 {
		p = p.Field("nested").Index(i)
	}
	b.ReportAllocs()
	for b.Loop() {
		pathSink = p.String()
	}
}

// BenchmarkPathJSONPointerUnescaped is the common case: no reserved character,
// so escaping must not allocate at all.
func BenchmarkPathJSONPointerUnescaped(b *testing.B) {
	p := diagnostics.Field("lines").Index(7).Field("saleQty")
	b.ReportAllocs()
	for b.Loop() {
		pathSink = p.JSONPointer()
	}
}

// BenchmarkPathHasPrefix is the operation that justifies keeping segments
// rather than rendering to a string: grouping diagnostics by subtree.
func BenchmarkPathHasPrefix(b *testing.B) {
	prefix := diagnostics.Field("lines").Index(7)
	p := prefix.Field("saleQty")
	var ok bool
	b.ReportAllocs()
	for b.Loop() {
		ok = p.HasPrefix(prefix)
	}
	sink = 0
	if ok {
		sink = 1
	}
}

// BenchmarkDiagnosticsFirstAdd is the figure the initial-reservation cap
// exists for. Reserving the full ceiling made this 125 MB and 8.6 ms; it must
// now be one small allocation whatever the limit is.
func BenchmarkDiagnosticsFirstAdd(b *testing.B) {
	const huge = 1 << 20

	d := diagnostics.NewDiagnostic(diagnostics.Error, "Sales.Order.InvalidQty",
		diagnostics.Field("lines").Index(7).Field("saleQty"))

	b.ReportAllocs()
	for b.Loop() {
		diags := diagnostics.NewCollector(huge)
		diags.Add(d)
	}
}

// BenchmarkDiagnosticsFill records a realistic report in one collector. Growing
// past the initial reservation is allowed to reallocate; what must not happen
// is a reallocation per Add.
func BenchmarkDiagnosticsFill(b *testing.B) {
	const limit = 1000

	d := diagnostics.NewDiagnostic(diagnostics.Warning, "Sales.Order.OddPrice",
		diagnostics.Field("lines").Index(3).Field("unitPrice"))

	b.ReportAllocs()
	for b.Loop() {
		diags := diagnostics.NewCollector(limit)
		for range limit {
			diags.Add(d)
		}
	}
}

// BenchmarkDiagnosticsReuse is the per-batch shape: one collector, reset
// between batches. Reset clears rather than truncates, so this pins what that
// costs against the allocation it avoids.
func BenchmarkDiagnosticsReuse(b *testing.B) {
	const (
		limit    = 100
		perBatch = 20
	)

	d := diagnostics.NewDiagnostic(diagnostics.Warning, "Sales.Order.OddPrice", diagnostics.Field("price"))
	diags := diagnostics.NewCollector(limit)

	b.ReportAllocs()
	for b.Loop() {
		diags.Reset()
		for range perBatch {
			diags.Add(d)
		}
	}
}

// BenchmarkDiagnosticsOverCeiling is the pathological batch S12 exists for:
// far more diagnostics than the ceiling. Past the limit, recording must cost
// nothing but a counter increment — no allocation, no growth.
func BenchmarkDiagnosticsOverCeiling(b *testing.B) {
	const limit = 10

	d := diagnostics.NewDiagnostic(diagnostics.Error, "Sales.Order.InvalidQty", diagnostics.Field("qty"))
	diags := diagnostics.NewCollector(limit)
	for range limit {
		diags.Add(d) // fill it first: the loop below is the truncating path
	}

	b.ReportAllocs()
	for b.Loop() {
		diags.Add(d)
	}
}
