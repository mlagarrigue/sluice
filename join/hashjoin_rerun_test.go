package join

import (
	"slices"
	"sync"
	"testing"

	"github.com/mlagarrigue/sluice"
)

// A Stream consumed twice runs twice. HashJoin made its table when the join
// was built, outside the stream function, so a second consumption filled
// the same table again: the build side counted twice ([11 22] then
// [11 11 22 22]), a MaxEntries the data fitted overflowed, and two
// consumptions at once raced on one table.
func TestAHashJoinConsumedTwiceRunsTwice(t *testing.T) {
	id := func(v int) int { return v }
	j := HashJoin(sluice.Of([]int{1, 2}, 0), sluice.Of([]int{1, 2}, 0), id, id,
		func(a, b int) int { return a*10 + b }, BuildLimit{MaxEntries: 3, OnOverflow: sluice.Fail})
	collect := func() []int {
		var out []int
		for b := range j {
			out = append(out, b.Items...)
		}
		return out
	}
	first, second := collect(), collect()
	if !slices.Equal(first, []int{11, 22}) || !slices.Equal(second, first) {
		t.Errorf("first consumption %v, second %v: want [11 22] twice", first, second)
	}

	// Two at once, each with its own table: the race detector reads it.
	var wg sync.WaitGroup
	got := make([][]int, 2)
	for i := range got {
		wg.Go(func() { got[i] = collect() })
	}
	wg.Wait()
	for i, g := range got {
		if !slices.Equal(g, []int{11, 22}) {
			t.Errorf("concurrent consumption %d: %v, want [11 22]", i, g)
		}
	}
}
