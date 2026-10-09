package pushdown

import (
	"encoding/binary"
	"slices"
	"sync"
	"testing"

	"github.com/mlagarrigue/sluice"
)

func key(v int64) []byte { return binary.BigEndian.AppendUint64(nil, uint64(v)) }

func TestDemandTightens(t *testing.T) {
	d := new(Demand)
	if snap, changed := NewReader(d).Current(); snap.Limit != Unlimited || !changed {
		t.Fatalf("a fresh demand = %+v, changed %v; want unlimited and changed", snap, changed)
	}

	d.LimitTo(100)
	d.LimitTo(40)
	d.AdvanceTo(key(10))
	d.AdvanceTo(key(25))
	d.NeedColumns("id", "total", "label")
	d.NeedColumns("id", "total")

	snap := d.Snapshot()
	if snap.Limit != 40 {
		t.Errorf("Limit = %d, want 40", snap.Limit)
	}
	if want := key(25); !slices.Equal(snap.LowerBound, want) {
		t.Errorf("LowerBound = % x, want % x", snap.LowerBound, want)
	}
	if want := []string{"id", "total"}; !slices.Equal(snap.Columns, want) {
		t.Errorf("Columns = %v, want %v", snap.Columns, want)
	}
	if d.Violations() != 0 {
		t.Errorf("%d violations on a demand that only tightened", d.Violations())
	}
}

// A loosening is ignored and counted rather than applied: a source that has
// already skipped rows on the strength of a bound cannot un-skip them, so the
// alternative to ignoring is a result quietly missing rows.
func TestDemandRefusesToLoosen(t *testing.T) {
	d := new(Demand)
	d.LimitTo(10)
	d.AdvanceTo(key(50))
	d.NeedColumns("id")

	d.LimitTo(99)            // higher limit
	d.AdvanceTo(key(1))      // lower bound going backwards
	d.NeedColumns("id", "x") // asking for a column already dropped

	snap := d.Snapshot()
	if snap.Limit != 10 {
		t.Errorf("Limit = %d, want it to stay at 10", snap.Limit)
	}
	if want := key(50); !slices.Equal(snap.LowerBound, want) {
		t.Errorf("LowerBound = % x, want it to stay at % x", snap.LowerBound, want)
	}
	if want := []string{"id"}; !slices.Equal(snap.Columns, want) {
		t.Errorf("Columns = %v, want %v", snap.Columns, want)
	}
	if d.Violations() != 3 {
		t.Errorf("Violations = %d, want 3 — each loosening must be visible", d.Violations())
	}
}

// Re-stating what is already true is not a violation: an operator publishing
// the same bound on every batch is the normal case, not a bug.
func TestDemandIdempotentTightening(t *testing.T) {
	d := new(Demand)
	d.AdvanceTo(key(7))
	gen := d.generation()
	d.AdvanceTo(key(7))
	if d.generation() != gen {
		t.Error("re-stating the same bound bumped the generation")
	}
	if d.Violations() != 0 {
		t.Errorf("Violations = %d, want 0 — restating is not loosening", d.Violations())
	}
}

// The reader's contract: it reports a change once, then stops reporting it.
// A source that re-planned on every batch would undo the point of the
// mechanism.
func TestReaderReportsChangeOnce(t *testing.T) {
	d := new(Demand)
	r := NewReader(d)

	if _, changed := r.Current(); !changed {
		t.Error("the first read must report a change — the source has never planned")
	}
	if _, changed := r.Current(); changed {
		t.Error("an unchanged demand reported a change")
	}

	d.LimitTo(5)
	snap, changed := r.Current()
	if !changed || snap.Limit != 5 {
		t.Errorf("after LimitTo(5): %+v, changed %v", snap, changed)
	}
	if _, changed := r.Current(); changed {
		t.Error("the same change was reported twice")
	}
}

// A nil demand is the "nobody is pushing anything down" case, and a source
// written against pushdown must work unchanged.
func TestNilDemandIsUnlimited(t *testing.T) {
	r := NewReader(nil)
	snap, changed := r.Current()
	if snap.Limit != Unlimited || changed {
		t.Errorf("nil demand: %+v, changed %v; want unlimited and unchanged", snap, changed)
	}
	if r.Exhausted() {
		t.Error("a nil demand reported itself exhausted")
	}
}

// The snapshot must not alias the demand's own storage: a source holds it
// while the demand keeps tightening.
func TestSnapshotDoesNotAlias(t *testing.T) {
	d := new(Demand)
	d.AdvanceTo(key(1))
	d.NeedColumns("a", "b")
	snap := d.Snapshot()

	d.AdvanceTo(key(2))
	d.NeedColumns("a")

	if want := key(1); !slices.Equal(snap.LowerBound, want) {
		t.Errorf("the held snapshot's bound changed to % x", snap.LowerBound)
	}
	if want := []string{"a", "b"}; !slices.Equal(snap.Columns, want) {
		t.Errorf("the held snapshot's columns changed to %v", snap.Columns)
	}
}

// A source reads while operators write, which is the whole point: -race is
// the assertion.
func TestDemandUnderConcurrency(t *testing.T) {
	d := new(Demand)
	var wg sync.WaitGroup

	wg.Go(func() {
		r := NewReader(d)
		for range 20_000 {
			snap, _ := r.Current()
			_ = snap.Limit
			_ = len(snap.LowerBound)
		}
	})

	for w := range 4 {
		wg.Go(func() {
			for i := 1000; i > 0; i-- {
				d.LimitTo(int64(i))
				d.AdvanceTo(key(int64(w*1000 + (1000 - i))))
			}
		})
	}
	wg.Wait()

	// Every writer ends on LimitTo(1) and AdvanceTo of its own highest key;
	// tightening is monotone, so whatever the interleaving the result is the
	// minimum limit and the maximum bound.
	snap := d.Snapshot()
	if snap.Limit != 1 {
		t.Errorf("Limit = %d after tightening to 1", snap.Limit)
	}
	if want := key(3999); !slices.Equal(snap.LowerBound, want) {
		t.Errorf("LowerBound = % x, want % x (the highest key any writer advanced to)", snap.LowerBound, want)
	}
}

// AdvanceBy is a pass-through that publishes: removing it changes speed and
// nothing else.
func TestAdvanceByIsPassThrough(t *testing.T) {
	d := new(Demand)
	src := []int64{1, 2, 3, 4, 5}
	got := sluice.Collect(AdvanceBy(sluice.Of(src, 2), d, key))
	if !slices.Equal(got, src) {
		t.Errorf("got %v, want %v", got, src)
	}
	if want := key(5); !slices.Equal(d.Snapshot().LowerBound, want) {
		t.Errorf("LowerBound = % x, want the last element's key % x", d.Snapshot().LowerBound, want)
	}
}

func TestAdvanceByNilKeyPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("nil key: no panic")
		}
	}()
	AdvanceBy(sluice.Empty[int64](), new(Demand), nil)
}

// A consumer that stops on a batch may not have reached its last element, so
// that batch must not publish a bound past where the consumer actually was.
func TestAdvanceByRefusalPublishesNothing(t *testing.T) {
	d := new(Demand)
	AdvanceBy(sluice.Of([]int64{1, 2, 3, 4}, 2), d, key)(func(sluice.Batch[int64]) bool {
		return false
	})
	if lb := d.Snapshot().LowerBound; lb != nil {
		t.Errorf("LowerBound = % x after the first batch was refused, want none", lb)
	}
}

// The zero Demand is documented as usable and meaning everything, so it must
// not read as a consumer that wants nothing.
//
// It has no limit set, and the field holding one is zero — which is also the
// value that means "stop, I have all I need". Reporting the field as the limit
// made a source built on the zero value, which nobody has narrowed yet,
// exhausted before it emitted a batch.
func TestZeroDemandWantsEverything(t *testing.T) {
	d := new(Demand)
	snap := d.Snapshot()
	if snap.Limit != Unlimited {
		t.Errorf("Limit = %d, want Unlimited: a demand nobody narrowed wants everything", snap.Limit)
	}
	if r := NewReader(d); r.Exhausted() {
		t.Error("a demand nobody narrowed reported itself exhausted")
	}
}

// A zero Reader is the same as NewReader(nil): it reads no demand and
// reports the unlimited snapshot, so a source handed one by a caller who did
// not call the constructor does not stop before it starts.
func TestZeroReaderIsUnlimited(t *testing.T) {
	var r Reader
	if snap, changed := r.Current(); snap.Limit != Unlimited || changed {
		t.Errorf("zero Reader: Current = (Limit %d, changed %v), want (Unlimited, false)", snap.Limit, changed)
	}
	if r.Exhausted() {
		t.Error("zero Reader reported itself exhausted")
	}
}

// Nil columns mean "all of them", so a consumer that needs none — a count —
// must not come back looking like one that never narrowed at all. It asked for
// the tightest demand this type can express and used to receive the loosest.
func TestNarrowingToNoColumnsIsNotTheSameAsNeverNarrowing(t *testing.T) {
	never := new(Demand)
	if cols := never.Snapshot().Columns; cols != nil {
		t.Errorf("Columns = %v for a demand nobody narrowed, want nil", cols)
	}

	// Two consumers whose needs do not overlap: the intersection is empty, and
	// empty is a real answer.
	narrowed := new(Demand)
	narrowed.NeedColumns("a")
	narrowed.NeedColumns("b")

	cols := narrowed.Snapshot().Columns
	if cols == nil {
		t.Fatal("Columns = nil after narrowing to nothing, which reads as every column")
	}
	if len(cols) != 0 {
		t.Errorf("Columns = %v, want the empty set", cols)
	}
}

// An empty key advances nothing — it is the bytewise minimum, so there is
// nothing below it to give up on — and it must stay cheap to say so.
//
// A key function returning nil for some rows would otherwise bump the
// generation once per batch, sending every reader through the lock and the
// snapshot that the one-atomic-load fast path exists to avoid, and counting a
// violation per row once a real bound is in place.
func TestAdvanceToAnEmptyKeyIsANonEvent(t *testing.T) {
	d := new(Demand)

	before := d.generation()
	d.AdvanceTo(nil)
	d.AdvanceTo([]byte{})
	if d.generation() != before {
		t.Error("an empty key against no bound moved the generation")
	}

	d.AdvanceTo(key(7))
	withBound := d.generation()
	d.AdvanceTo(nil)
	if d.generation() != withBound {
		t.Error("an empty key against a bound moved the generation")
	}
	if v := d.Violations(); v != 0 {
		t.Errorf("Violations = %d: an empty key was counted as a loosening", v)
	}
	if got := d.Snapshot().LowerBound; !slices.Equal(got, key(7)) {
		t.Errorf("LowerBound = %v, want the bound that was actually set", got)
	}
}

// Exhausted refreshes the reader's view, and a source that checks it before
// re-planning must still be told the demand moved.
//
// Consuming the flag there left Current reporting "nothing changed" for an
// update Exhausted had already absorbed, so the source kept a stale plan —
// decoding columns nobody wants, scanning below a bound that has risen —
// while believing it was current.
func TestExhaustedDoesNotConsumeTheChange(t *testing.T) {
	d := new(Demand)
	r := NewReader(d)
	if _, changed := r.Current(); !changed {
		t.Fatal("the first Current reported no change")
	}

	d.LimitTo(10)
	if r.Exhausted() {
		t.Fatal("a limit of 10 reported exhausted")
	}

	snap, changed := r.Current()
	if !changed {
		t.Error("Current reported no change for an update Exhausted had seen")
	}
	if snap.Limit != 10 {
		t.Errorf("Limit = %d, want 10", snap.Limit)
	}
	// And the change is news exactly once.
	if _, changed := r.Current(); changed {
		t.Error("the same change was reported twice")
	}
}

// Violations counts an attempt to loosen the demand, which is a bug in the
// operators. Two consumers narrowing independently is not one: the
// documentation invites later calls to intersect, and counting that would fire
// the alarm on a healthy pipeline.
func TestViolationsCountWideningNotIntersection(t *testing.T) {
	d := new(Demand)
	d.NeedColumns("a", "b")
	d.NeedColumns("b", "c") // an ordinary intersection: keeps b, drops a
	if v := d.Violations(); v != 0 {
		t.Errorf("Violations = %d for an intersection, want 0", v)
	}
	if cols := d.Snapshot().Columns; !slices.Equal(cols, []string{"b"}) {
		t.Errorf("Columns = %v, want [b]", cols)
	}

	d.NeedColumns("b", "z") // asks for a column that is already gone, keeps b
	if v := d.Violations(); v != 1 {
		t.Errorf("Violations = %d after asking for more than remains, want 1", v)
	}
}

// LimitTo(Unlimited) asks for no limit: the loosest value there is, never a
// limit of zero. Other negatives still mean "nothing more".
func TestDemandLimitToTable(t *testing.T) {
	tests := []struct {
		name           string
		calls          []int64
		wantLimit      int64
		wantViolations int64
		wantGen        uint64 // generation bumps after the calls
	}{
		{"unlimited on a fresh demand", []int64{Unlimited}, Unlimited, 0, 0},
		{"unlimited after a limit", []int64{10, Unlimited}, 10, 1, 1},
		{"zero", []int64{0}, 0, 0, 1},
		{"negative other than unlimited", []int64{-5}, 0, 0, 1},
		{"tighten", []int64{10, 4}, 4, 0, 2},
		{"equal is no change", []int64{10, 10}, 10, 0, 1},
		{"loosen", []int64{10, 11}, 10, 1, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := new(Demand)
			for _, n := range tt.calls {
				d.LimitTo(n)
			}
			if got := d.Snapshot().Limit; got != tt.wantLimit {
				t.Errorf("Limit = %d, want %d", got, tt.wantLimit)
			}
			if got := d.Violations(); got != tt.wantViolations {
				t.Errorf("Violations = %d, want %d", got, tt.wantViolations)
			}
			if got := d.generation(); got != tt.wantGen {
				t.Errorf("generation = %d, want %d", got, tt.wantGen)
			}
		})
	}
}
