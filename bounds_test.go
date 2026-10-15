package sluice

import (
	"fmt"
	"strings"
	"testing"
)

// A negative size or count is a mistake, not a request for the default or for
// nothing: zero is the one way to ask for those. Each operator panics where
// the pipeline is built, naming itself and the value.
func TestNegativeSizeOrCountPanics(t *testing.T) {
	src := Of([]int{1, 2, 3}, 0)
	route := func(Batch[int]) []int { return nil }
	for _, tc := range []struct {
		name  string
		build func()
	}{
		{"Of", func() { Of([]int{1}, -1) }},
		{"Coalesce", func() { Coalesce(src, -1) }},
		{"Split", func() { Split(src, -1, route) }},
		{"Take", func() { Take(src, -1) }},
		{"Drop", func() { Drop(src, -1) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				msg := fmt.Sprint(recover())
				if !strings.Contains(msg, tc.name) || !strings.Contains(msg, "-1") {
					t.Errorf("panic = %q, want one naming %s and -1", msg, tc.name)
				}
			}()
			tc.build()
		})
	}
}

// Zero keeps its meaning: the default batch size, no branches, nothing taken,
// nothing dropped.
func TestZeroSizeOrCountKeepsItsMeaning(t *testing.T) {
	src := Of([]int{1, 2, 3}, 0)
	if got := Collect(Coalesce(src, 0)); len(got) != 3 {
		t.Errorf("Coalesce(0) = %v", got)
	}
	if got := Split(src, 0, func(Batch[int]) []int { return nil }); got != nil {
		t.Errorf("Split(0) = %v, want nil", got)
	}
	if got := Collect(Take(src, 0)); len(got) != 0 {
		t.Errorf("Take(0) = %v, want nothing", got)
	}
	if got := Collect(Drop(src, 0)); len(got) != 3 {
		t.Errorf("Drop(0) = %v, want everything", got)
	}
}
