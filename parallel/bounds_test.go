package parallel

import (
	"fmt"
	"strings"
	"testing"

	"github.com/mlagarrigue/sluice"
)

// A negative depth or worker count is a mistake, not a request for the no-op:
// zero is the one way to ask for it. Each operator panics where the pipeline
// is built, naming itself and the value.
func TestNegativeDepthOrWorkersPanics(t *testing.T) {
	src := sluice.Of([]int{1, 2, 3}, 0)
	f := func(b sluice.Batch[int]) sluice.Batch[int] { return b }
	mk := func() (int, error) { return 0, nil }
	fw := func(_ int, b sluice.Batch[int]) sluice.Batch[int] { return b }
	for _, tc := range []struct {
		name  string
		build func()
	}{
		{"Async", func() { Async(src, -1) }},
		{"Ordered", func() { Ordered(src, -1, f) }},
		{"Unordered", func() { Unordered(src, -1, f) }},
		{"WithState", func() { WithState(src, -1, mk, fw) }},
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
