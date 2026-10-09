package join_test

import (
	"fmt"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/join"
	"github.com/mlagarrigue/sluice/window"
)

type impression struct {
	user string
	at   time.Time
}

// Interval pairs two time-ordered streams on a key and a temporal window:
// here, the impression that preceded each click by at most fifteen seconds.
// click and at mirror the fixture of the window example: both operators read
// an event time off the element, so both need the same shape and the same
// clock.
type click struct {
	user string
	at   time.Time
}

func at(sec int) time.Time { return time.Unix(int64(sec), 0).UTC() }

// Tumbling groups a time-ordered stream into consecutive windows and emits each
// one as a Pane, which carries the bounds a batch cannot hold.
func ExampleTumbling() {
	events := []click{
		{"alice", at(0)}, {"bob", at(7)}, // both in [0, 10)
		{"carol", at(12)}, // [10, 20)
		{"dave", at(95)},  // [90, 100) — the gap in between emits nothing
	}

	panes := window.Tumbling(sluice.Of(events, 2),
		func(c click) time.Time { return c.at },
		10*time.Second, 1000, sluice.Fail)

	for b := range panes {
		for _, p := range b.Items {
			users := make([]string, p.Len())
			for i, c := range p.Items {
				users[i] = c.user
			}
			fmt.Printf("[%d, %d) %v\n", p.Start.Unix(), p.End.Unix(), users)
		}
	}
	// Output:
	// [0, 10) [alice bob]
	// [10, 20) [carol]
	// [90, 100) [dave]
}

func ExampleInterval() {
	clicks := []click{
		{"alice", at(20)},
		{"bob", at(30)},
	}
	// Both sides must be ordered by event time — that requirement is what
	// replaces a watermark, and an input that goes backwards panics rather
	// than quietly producing unmatched rows.
	impressions := []impression{
		{"bob", at(5)},    // 25s before bob's click: outside the window
		{"alice", at(10)}, // 10s before alice's: inside it
	}

	// The right side may sit up to fifteen seconds before the left and no
	// later, which is what "the impression that led to this click" means.
	attributed := join.Interval(
		sluice.Of(clicks, 4), sluice.Of(impressions, 4),
		func(c click) string { return c.user },
		func(i impression) string { return i.user },
		func(c click) time.Time { return c.at },
		func(i impression) time.Time { return i.at },
		-15*time.Second, 0,
		func(c click, i impression) string {
			return fmt.Sprintf("%s clicked %ds after seeing the ad",
				c.user, int(c.at.Sub(i.at).Seconds()))
		},
		join.BuildLimit{MaxEntries: 1000, OnOverflow: sluice.Fail},
	)

	for _, s := range sluice.Collect(attributed) {
		fmt.Println(s)
	}
	// Output: alice clicked 10s after seeing the ad
}
