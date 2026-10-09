package window

import (
	"slices"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/internal/streamtest"
)

// wpEvent is a timestamped element with an identity.
type wpEvent struct {
	at time.Time
	id int
}

func wpTime(e wpEvent) time.Time { return e.at }

// FuzzWindowPartitionsByTime checks Tumbling against its definition over random
// non-decreasing timestamps, many of them exactly on a window boundary:
//
//   - every pane's bounds are [Truncate(t, size), +size) for its elements,
//     an element at End belonging to the next pane, never this one;
//   - panes leave in strictly increasing Start order and none is empty;
//   - with sluice.Fail and a bound that is never reached, the panes concatenated are
//     the input, so Σ len = n;
//   - with sluice.DropNewest and a small bound, each pane is the first n elements of
//     its window, in order.
func FuzzWindowPartitionsByTime(f *testing.F) {
	f.Add(uint64(1), uint8(50), uint8(10), uint8(3), uint8(4))
	f.Add(uint64(2), uint8(200), uint8(1), uint8(0), uint8(1))
	f.Add(uint64(3), uint8(0), uint8(5), uint8(2), uint8(2))
	f.Add(uint64(4), uint8(100), uint8(60), uint8(7), uint8(3))

	f.Fuzz(func(t *testing.T, seed uint64, nn, sizeSec, maxStep, maxBatch uint8) {
		rng := streamtest.NewRand(seed)
		size := time.Duration(int(sizeSec%60)+1) * time.Second
		step := int(maxStep%8) + 1

		// Steps in seconds over a size in whole seconds land on boundaries
		// often, which is where an off-by-one in the window test would show.
		base := time.Unix(1_700_000_000, 0)
		events := make([]wpEvent, int(nn))
		clock := base
		for i := range events {
			if rng.Intn(3) != 0 {
				clock = clock.Add(time.Duration(rng.Intn(step)) * time.Second)
			}
			events[i] = wpEvent{at: clock, id: i}
		}

		// Reference grouping by the definition.
		var groups [][]wpEvent
		for i, e := range events {
			if i == 0 || !e.at.Truncate(size).Equal(events[i-1].at.Truncate(size)) {
				groups = append(groups, nil)
			}
			groups[len(groups)-1] = append(groups[len(groups)-1], e)
		}

		check := func(policy sluice.Overflow, bound int) {
			mb := int(maxBatch%8) + 1
			panes := sluice.Collect(Tumbling(streamtest.Chunks(events, mb, rng), wpTime, size, bound, policy))
			if len(panes) != len(groups) {
				t.Fatalf("%v/%d: %d panes, want %d", policy, bound, len(panes), len(groups))
			}
			total := 0
			for i, p := range panes {
				if len(p.Items) == 0 {
					t.Fatalf("pane %d is empty", i)
				}
				if i > 0 && !panes[i-1].Start.Before(p.Start) {
					t.Fatalf("pane %d starts at %v, not after %v", i, p.Start, panes[i-1].Start)
				}
				if !p.End.Equal(p.Start.Add(size)) {
					t.Fatalf("pane %d: End %v, want Start+size %v", i, p.End, p.Start.Add(size))
				}
				for _, e := range p.Items {
					if e.at.Before(p.Start) || !e.at.Before(p.End) {
						t.Fatalf("element %d at %v outside its pane [%v, %v)", e.id, e.at, p.Start, p.End)
					}
					if !e.at.Truncate(size).Equal(p.Start) {
						t.Fatalf("element %d at %v: pane starts %v, want %v", e.id, e.at, p.Start, e.at.Truncate(size))
					}
				}
				want := groups[i][:min(bound, len(groups[i]))]
				if !slices.Equal(p.Items, want) {
					t.Fatalf("%v/%d: pane %d = %v, want %v", policy, bound, i, p.Items, want)
				}
				total += len(p.Items)
			}
			if policy == sluice.Fail && total != len(events) {
				t.Fatalf("Σ len = %d, want %d", total, len(events))
			}
		}
		check(sluice.Fail, len(events)+1)
		check(sluice.DropNewest, int(maxStep%4)+1)
	})
}
