package pushdown_test

import (
	"encoding/binary"
	"testing"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/pushdown"
)

// Two things need measuring, and they pull in opposite directions.
//
// The **cost**: consulting the demand must be one atomic load per batch, which
// against a ~1.5 ns per-element budget over 1024-element batches should be
// unmeasurable. If it is not, the mechanism is not worth having on a pipeline
// that never tightens anything.
//
// The **gain**: a scan that can skip work it was about to do. This is the shape
// DataFusion measures at ×22 — the numbers below are far smaller because the
// simulated per-row work is small, and the ratio is what transposes, not the
// figure.

var sink int64

const (
	pages    = 512  // "pages" the scan walks
	perPage  = 1024 // rows in each
	rowCost  = 12   // arbitrary work per decoded row
	wantRows = 2048 // what the consumer actually needs
)

func rowKey(v int64) []byte { return binary.BigEndian.AppendUint64(nil, uint64(v)) }

// scan walks pages of sorted rows, decoding each one. When a demand is given
// it consults it once per page, which is the whole read side of the
// mechanism: skip a page whose highest key is already below what the consumer
// wants, and stop entirely once the consumer wants nothing.
func scan(d *pushdown.Demand) sluice.Stream[int64] {
	return func(yield func(sluice.Batch[int64]) bool) {
		r := pushdown.NewReader(d)
		buf := make([]int64, perPage)

		for p := range pages {
			base := int64(p * perPage)

			snap, _ := r.Current()
			if snap.Limit == 0 {
				return // the consumer wants nothing more
			}
			if snap.LowerBound != nil {
				highest := rowKey(base + perPage - 1)
				if string(highest) < string(snap.LowerBound) {
					continue // the whole page is behind the consumer: skip decoding it
				}
			}

			// The work a scan does per row, and the work skipping avoids.
			for i := range buf {
				v := base + int64(i)
				for range rowCost {
					v = v*2654435761 + 1
				}
				buf[i] = base + int64(i)
				sink += v & 1
			}
			if !yield(sluice.Batch[int64]{Items: buf}) {
				return
			}
		}
	}
}

// BenchmarkScanSkipAhead: a consumer walking in key order that will never look
// back — a merge join's probe side — publishing its position so the scan skips
// whole pages instead of decoding rows it is about to discard.
func BenchmarkScanSkipAhead(b *testing.B) {
	// The consumer only wants every eighth page's worth, and says so by
	// advancing its bound past the pages between.
	b.ReportAllocs()
	for b.Loop() {
		d := new(pushdown.Demand)
		var n int
		seen := 0
		scan(d)(func(bt sluice.Batch[int64]) bool {
			n += bt.Len()
			seen++
			// Skip forward eight pages' worth after each batch taken.
			last := bt.Items[bt.Len()-1]
			d.AdvanceTo(rowKey(last + 8*perPage))
			return seen < 16
		})
		sink += int64(n)
	}
}

// The cost side: a pipeline whose demand never changes must pay one atomic
// load per batch and nothing else. Compare against BenchmarkScanNoDemand.
func BenchmarkScanQuietDemand(b *testing.B) {
	d := new(pushdown.Demand)
	b.ReportAllocs()
	for b.Loop() {
		var n int
		scan(d)(func(bt sluice.Batch[int64]) bool {
			n += bt.Len()
			return true
		})
		sink += int64(n)
	}
}

// BenchmarkScanNoDemand is the control: the same scan with no demand at all.
func BenchmarkScanNoDemand(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		var n int
		scan(nil)(func(bt sluice.Batch[int64]) bool {
			n += bt.Len()
			return true
		})
		sink += int64(n)
	}
}

// BenchmarkScanSkipAheadControl is the same consumer taking the same batches
// without telling the scan where it has got to, so every page in between is
// decoded and thrown away. The gap between this and BenchmarkScanSkipAhead is
// what the mechanism is worth.
func BenchmarkScanSkipAheadControl(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		var n int
		seen, taken := 0, 0
		scan(nil)(func(bt sluice.Batch[int64]) bool {
			seen++
			if (seen-1)%8 != 0 {
				return true // discarded by the consumer, decoded by the scan
			}
			n += bt.Len()
			taken++
			return taken < 16
		})
		sink += int64(n)
	}
}
