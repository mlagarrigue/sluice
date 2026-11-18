package join_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/join"
)

// splitBranches is two branches of one Split, batches dealt to each in turn:
// left {0 1} {4 5}, right {2 3} {6 7}. reuse puts a Convert under the Split,
// which reuses its output buffer as the reuse contract allows: pulling one
// side then advances the source the other side's batch came from.
func splitBranches(reuse bool) []sluice.Stream[int] {
	up := sluice.Of([]int{0, 1, 2, 3, 4, 5, 6, 7}, 2)
	if reuse {
		up = sluice.Convert(up, func(v int) int { return v })
	}
	rr := 0
	return sluice.Split(up, 2, func(sluice.Batch[int]) []int { rr++; return []int{(rr - 1) % 2} })
}

// A join holds one side's batch while it pulls the other; over two branches
// of one Split that pull rewrites the held batch under a buffer-reusing
// upstream, unless the join copied it first (the architecture's S14). Zip
// paired (2, 2) for (0, 2).
func TestZipLongestOfTwoBranchesOfOneSplit(t *testing.T) {
	for _, reuse := range []bool{false, true} {
		br := splitBranches(reuse)
		var got [][2]int
		for b := range join.ZipLongest(br[0], br[1]) {
			for _, p := range b.Items {
				got = append(got, [2]int{p.Left, p.Right})
			}
		}
		if want := [][2]int{{0, 2}, {1, 3}, {4, 6}, {5, 7}}; !slices.Equal(got, want) {
			t.Errorf("reuse %v: %v, want %v", reuse, got, want)
		}
	}
}

// MergeJoin over the same branches: the keys are disjoint, so every row is
// one-sided. It matched 3 with 3 and 7 with 7, keys one side never had.
func TestMergeJoinOfTwoBranchesOfOneSplit(t *testing.T) {
	id := func(v int) int { return v }
	for _, reuse := range []bool{false, true} {
		br := splitBranches(reuse)
		var got []string
		for b := range join.MergeJoin(br[0], br[1], id, id, func(a, b int) int { return a - b }) {
			for _, p := range b.Items {
				got = append(got, fmt.Sprintf("%v/%v/%d/%d", p.HasLeft, p.HasRight, p.Left, p.Right))
			}
		}
		if len(got) != 8 || slices.ContainsFunc(got, func(s string) bool { return strings.HasPrefix(s, "true/true") }) {
			t.Errorf("reuse %v: %v, want eight one-sided rows", reuse, got)
		}
	}
}

// Interval over the same branches, a window of zero: the instants are
// disjoint, so no pair. It invented [3 3] and [7 7].
func TestIntervalOfTwoBranchesOfOneSplit(t *testing.T) {
	base := time.Unix(0, 0).UTC()
	ts := func(v int) time.Time { return base.Add(time.Duration(v) * time.Second) }
	for _, reuse := range []bool{false, true} {
		br := splitBranches(reuse)
		var got [][2]int
		s := join.Interval(br[0], br[1], join.IntervalSpec[int, int, int, [2]int]{
			KeyL: func(int) int { return 0 }, KeyR: func(int) int { return 0 },
			TimeL: ts, TimeR: ts,
			Merge: func(l, r int) [2]int { return [2]int{l, r} },
		}, join.BuildLimit{MaxEntries: 100, OnOverflow: sluice.Fail})
		for b := range s {
			got = append(got, b.Items...)
		}
		if len(got) != 0 {
			t.Errorf("reuse %v: pairs %v, want none", reuse, got)
		}
	}
}
