package pushdown

import "github.com/mlagarrigue/sluice"

// Take is deliberately absent, and the measurement is why.
//
// The obvious operator here would be a [sluice.Take] that also publishes its
// remaining count, so a source could stop producing rather than be stopped
// after producing. It was written, benchmarked against the core operator over
// a scan, and removed: **8.63 µs against 8.35, a 3% cost and no gain**
// (BenchmarkScanWithoutPushdown / WithPushdown, before removal).
//
// The reason is structural rather than incidental. A source whose unit of work
// *is* the batch it yields cannot do less than one batch of work for the last
// batch — checking a limit inside its row loop would be a per-element cost,
// which is the thing this whole design refuses. The core Take already stops
// the stream on the batch after the one that satisfied it, and there is
// nothing left for a published limit to save.
//
// Where a limit still earns its keep is a source running *ahead* of its
// consumer — behind an [parallel.Async], where the producer would otherwise
// fill its slack with batches nobody will want. A caller in that shape
// publishes with [Demand.LimitTo] directly, and the source stops on
// [Reader.Exhausted]. That is a caller's decision about a pipeline it can see,
// not an operator this package can justify by measurement.

// AdvanceBy publishes a lower bound derived from each batch as it passes:
// key(last element of the batch) becomes the point below which nothing is
// wanted any more. A batch the consumer refuses publishes nothing.
//
// This is the shape a merge join's probe side has — it is walking in key order
// and will never look back — and it is what lets a scan skip whole pages
// rather than decode rows it is about to discard. The stream itself is
// untouched: [AdvanceBy] is a pass-through, like [sluice.Peek], so removing it
// changes speed and nothing else.
//
// key must encode its result so that bytewise order is the order the source
// means, which for integers is big-endian — the order the binary wire format
// already uses.
//
// Give each demand one advancing publisher. Two AdvanceBy stages on the same
// [Demand] — or AdvanceBy beside a manual [Demand.AdvanceTo] — publish bounds
// from two different walks, and every time one lands behind the other it
// counts as a violation: [Demand.Violations] stops meaning "a consumer tried
// to widen" and becomes noise.
//
// AdvanceBy panics if key is nil.
func AdvanceBy[T any](s sluice.Stream[T], d *Demand, key func(T) []byte) sluice.Stream[T] {
	if key == nil {
		panic("pushdown: AdvanceBy requires a non-nil key function")
	}
	if d == nil {
		return s
	}
	return func(yield func(sluice.Batch[T]) bool) {
		s(func(b sluice.Batch[T]) bool {
			// The bound is published from the last element and after the
			// batch was delivered: publishing earlier would tell the source
			// to skip past rows this batch had not finished carrying. A
			// refusal publishes nothing: the consumer may have stopped partway
			// through the batch, so its last key is not a point it reached.
			if !yield(b) {
				return false
			}
			if n := b.Len(); n > 0 {
				d.AdvanceTo(key(b.Items[n-1]))
			}
			return true
		})
	}
}
