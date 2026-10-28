package bench

import (
	"context"
	"testing"

	"github.com/mlagarrigue/sluice/diagnostics"
)

// The designs the architecture turned down, each measured beside the one it
// kept. A decision record that says "cost was not the reason" or "measured no
// slower" needs a row to point at, and the row needs a benchmark that can be
// run again: these mirror the rejected shape in a few lines rather than keep
// it in the library.

// ---------------------------------------------------------------------------
// A context in the batch: Batch[T] carries Items only. The rejected shape adds
// a context.Context, 24 bytes to 40, copied by every stage that builds a new
// batch; the third variant also checks it once per batch, the cancellation the
// field would exist for.
// ---------------------------------------------------------------------------

type ctxBatch[T any] struct {
	Items []T
	Ctx   context.Context
}

type ctxBatchSeq[T any] func(yield func(ctxBatch[T]) bool)

func ctxBatchesOf[T any](ctx context.Context, s []T, size int) ctxBatchSeq[T] {
	return func(yield func(ctxBatch[T]) bool) {
		for i := 0; i < len(s); i += size {
			end := min(i+size, len(s))
			if !yield(ctxBatch[T]{Items: s[i:end], Ctx: ctx}) {
				return
			}
		}
	}
}

// mapCtxBatch is mapBatch with the context carried into the batch it yields,
// as every stage would have to, and read for cancellation when check is set.
func mapCtxBatch[T any](s ctxBatchSeq[T], f func(T) T, check bool) ctxBatchSeq[T] {
	return func(yield func(ctxBatch[T]) bool) {
		s(func(b ctxBatch[T]) bool {
			if check && b.Ctx.Err() != nil {
				return false
			}
			items := b.Items
			for i := range items {
				items[i] = f(items[i])
			}
			return yield(ctxBatch[T]{Items: items, Ctx: b.Ctx})
		})
	}
}

// BenchmarkBatchContext runs four map stages over N elements in batches of
// 1024: the batch as it is, with a context carried, and with that context
// checked by each stage on each batch.
func BenchmarkBatchContext(b *testing.B) {
	src := data(N)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b.Run("items", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			s := batchesOf(src, 1024)
			for range 4 {
				s = mapBatch(s, inc)
			}
			var acc int64
			s(func(bt Batch[int64]) bool {
				for _, v := range bt.Items {
					acc += v
				}
				return true
			})
			sink = acc
		}
		reportPerElem(b, N)
	})
	for _, v := range []struct {
		name  string
		check bool
	}{{"ctx", false}, {"ctx-checked", true}} {
		b.Run(v.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				s := ctxBatchesOf(ctx, src, 1024)
				for range 4 {
					s = mapCtxBatch(s, inc, v.check)
				}
				var acc int64
				s(func(bt ctxBatch[int64]) bool {
					for _, v := range bt.Items {
						acc += v
					}
					return true
				})
				sink = acc
			}
			reportPerElem(b, N)
		})
	}
}

// ---------------------------------------------------------------------------
// The hash join's table: a flat value slice with an int32 index chain, the
// shape join keeps, against map[K][]B, which allocates a slice per distinct
// key. Both are the bare shape — build 65,536 distinct keys, probe each once —
// with the map left to the runtime's growth in both, so the gap is the slice
// per key and nothing else.
// ---------------------------------------------------------------------------

type chainTable struct {
	head map[int64]int32
	vals []int64
	prev []int32
}

func (t *chainTable) put(k, v int64) {
	previous, seen := t.head[k]
	if !seen {
		previous = -1
	}
	t.head[k] = int32(len(t.vals)) //nolint:gosec // G115: joinRows entries
	t.vals = append(t.vals, v)
	t.prev = append(t.prev, previous)
}

func (t *chainTable) sum(k int64) int64 {
	var acc int64
	for i, ok := t.head[k]; ok && i >= 0; i = t.prev[i] {
		acc += t.vals[i]
	}
	return acc
}

func BenchmarkJoinTableShape(b *testing.B) {
	keys := joinData(joinRows)

	b.Run("chain", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			t := &chainTable{head: map[int64]int32{}}
			for _, k := range keys {
				t.put(k, k)
			}
			var acc int64
			for _, k := range keys {
				acc += t.sum(k)
			}
			sink = acc
		}
		reportPerElem(b, joinRows)
	})
	b.Run("slice-per-key", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			t := map[int64][]int64{}
			for _, k := range keys {
				t[k] = append(t[k], k)
			}
			var acc int64
			for _, k := range keys {
				for _, v := range t[k] {
					acc += v
				}
			}
			sink = acc
		}
		reportPerElem(b, joinRows)
	})
}

// ---------------------------------------------------------------------------
// The input copy of parallel (S14): Ordered and Unordered copy each batch's
// Items into a recycled per-slot buffer before a worker reads it. The copy
// cannot be switched off inside the operator, so this is the copy alone, into
// one reused buffer, over the 65,536 elements BenchmarkParallelTrivialWork
// runs through the operator on a pass-through f.
// ---------------------------------------------------------------------------

func BenchmarkParallelInputCopy(b *testing.B) {
	src := data(N / 16)
	slot := make([]int64, 1024)
	b.ReportAllocs()
	for b.Loop() {
		var acc int64
		for i := 0; i < len(src); i += 1024 {
			n := copy(slot, src[i:min(i+1024, len(src))])
			acc += slot[n-1]
		}
		sink = acc
	}
	reportPerElem(b, N/16)
}

// ---------------------------------------------------------------------------
// The receiver of Diagnostic's With methods. A value receiver returns a copy,
// which is what lets a diagnostic be held as a template and specialised
// without touching it; the linter flags the struct's size. The pointer
// variant is the same struct with setters on a pointer, applied to a copy of
// the template the caller makes once. Both build the example of
// NewDiagnostic's documentation: a message, then an origin.
// ---------------------------------------------------------------------------

type ptrDiagnostic diagnostics.Diagnostic

func (d *ptrDiagnostic) setMessage(id string, args map[string]any) {
	d.MessageID, d.Args = id, args
}
func (d *ptrDiagnostic) setOrigin(o diagnostics.Origin) { d.Origin = o }

var diagSink diagnostics.Diagnostic

func BenchmarkDiagnosticReceiver(b *testing.B) {
	tmpl := diagnostics.NewDiagnostic(diagnostics.Error, "Sales.Order.InvalidQty",
		diagnostics.Field("lines").Index(7).Field("saleQty"))
	args := map[string]any{"min": 1}

	b.Run("value", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			diagSink = tmpl.WithMessage("QtyMustBeOverZero", args).
				WithOrigin(diagnostics.OriginBusinessRule)
		}
	})
	b.Run("pointer", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			d := ptrDiagnostic(tmpl)
			d.setMessage("QtyMustBeOverZero", args)
			d.setOrigin(diagnostics.OriginBusinessRule)
			diagSink = diagnostics.Diagnostic(d)
		}
	})
}
