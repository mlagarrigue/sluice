package join

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/internal/streamtest"
)

// FuzzIntervalJoinMatchesBruteForce is TestIntervalJoinAgainstBruteForce with
// the inputs, window and batch size chosen by the fuzzer: the join, compared
// as a multiset, against a double loop over every pair.
func FuzzIntervalJoinMatchesBruteForce(f *testing.F) {
	f.Add(uint64(1), uint64(2), uint8(200), uint8(200), int8(-4), int8(3), uint8(64), uint8(17))
	f.Add(uint64(3), uint64(4), uint8(50), uint8(10), int8(0), int8(0), uint8(1), uint8(1))
	f.Add(uint64(5), uint64(6), uint8(0), uint8(30), int8(-2), int8(5), uint8(3), uint8(4))
	f.Add(uint64(7), uint64(8), uint8(120), uint8(120), int8(-10), int8(-1), uint8(2), uint8(3))

	f.Fuzz(func(t *testing.T, seedL, seedR uint64, nl, nr uint8, lo, hi int8, batch, keys uint8) {
		lower := time.Duration(lo%16) * time.Second
		upper := time.Duration(hi%16) * time.Second
		if lower > upper {
			lower, upper = upper, lower
		}
		k := uint64(keys%32) + 1
		gen := func(seed uint64, count int, tag string) []tRow {
			rng := streamtest.NewRand(seed)
			rows := make([]tRow, count)
			clock := int64(0)
			for i := range rows {
				clock += int64(rng.Intn(3)) // non-decreasing, frequent ties
				rows[i] = tRow{key: fmt.Sprintf("k%d", rng.Next()%k), sec: clock, name: fmt.Sprintf("%s%d", tag, i)}
			}
			return rows
		}
		left, right := gen(seedL, int(nl), "l"), gen(seedR, int(nr), "r")

		var want []string
		for _, l := range left {
			for _, r := range right {
				if l.key != r.key {
					continue
				}
				if d := time.Duration(r.sec-l.sec) * time.Second; d >= lower && d <= upper {
					want = append(want, l.name+"+"+r.name)
				}
			}
		}

		got := intervalRows(left, right, lower, upper, failLimit(1<<20), int(batch%16)+1)
		slices.Sort(got)
		slices.Sort(want)
		if !slices.Equal(got, want) {
			t.Fatalf("lower=%v upper=%v: got %d pairs, want %d\n got %v\nwant %v",
				lower, upper, len(got), len(want), got, want)
		}
	})
}
