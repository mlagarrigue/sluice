package window_test

import (
	"fmt"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/window"
)

// click and at are the fixture every time-based example here reads: an event
// time off the element, against one clock.
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
