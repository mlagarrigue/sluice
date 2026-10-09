package pushdown_test

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/pushdown"
)

// keyOf encodes an integer so that bytewise order is numeric order, which is
// what the demand compares and what the binary wire format already does.
func keyOf(v int64) []byte { return binary.BigEndian.AppendUint64(nil, uint64(v)) }

// A scan that consults a demand can skip work it was about to do. This is the
// shape a merge join's probe side has — walking in key order, never looking
// back — and it is what turns "stop" into "skip ahead to here".
func ExampleDemand() {
	d := new(pushdown.Demand)

	// The source: four pages of two keys each. It asks the demand once per
	// page, which is the whole read side — one atomic load until something
	// changes.
	scanned := 0
	scan := sluice.Stream[int64](func(yield func(sluice.Batch[int64]) bool) {
		r := pushdown.NewReader(d)
		for page := range 4 {
			base := int64(page * 2)
			snap, _ := r.Current()
			if snap.LowerBound != nil && string(keyOf(base+1)) < string(snap.LowerBound) {
				continue // the whole page is behind the consumer: never read
			}
			scanned++
			if !yield(sluice.Batch[int64]{Items: []int64{base, base + 1}}) {
				return
			}
		}
	})

	// The consumer takes the first page, then says it has moved past key 5.
	first := true
	scan(func(b sluice.Batch[int64]) bool {
		fmt.Println("read", b.Items)
		if first {
			first = false
			d.AdvanceTo(keyOf(5))
		}
		return true
	})
	fmt.Println("pages actually scanned:", scanned, "of 4")
	// The page holding keys 2 and 3 is never read. The one holding 4 and 5
	// still is: the bound says nothing *below* key 5 is wanted, and key 5
	// itself is.
	// Output:
	// read [0 1]
	// read [4 5]
	// read [6 7]
	// pages actually scanned: 3 of 4
}

// The demand only tightens. A source that has already skipped rows on the
// strength of a bound cannot un-skip them, so a loosening is a programming
// error: it is ignored and counted where a test can see it, rather than
// quietly producing a result that is missing rows.
func ExampleDemand_onlyTightens() {
	d := new(pushdown.Demand)

	d.LimitTo(100)
	d.LimitTo(10) // tighter: applied
	d.LimitTo(50) // looser: ignored and counted

	fmt.Println("limit:", d.Snapshot().Limit)
	fmt.Println("violations:", d.Violations())
	// Output:
	// limit: 10
	// violations: 1
}

// AdvanceBy publishes the bound for you, from the last element of each batch.
// It is a pass-through, so removing it changes speed and nothing else.
func ExampleAdvanceBy() {
	d := new(pushdown.Demand)

	rows := pushdown.AdvanceBy(sluice.Of([]int64{1, 2, 3, 4}, 2), d, keyOf)
	fmt.Println(sluice.Collect(rows))

	// The source now knows nothing below 4 is wanted.
	fmt.Println("bound is key 4:", bytes.Equal(d.Snapshot().LowerBound, keyOf(4)))
	// Output:
	// [1 2 3 4]
	// bound is key 4: true
}
