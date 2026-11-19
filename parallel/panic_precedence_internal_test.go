package parallel

import (
	"testing"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/internal/streamtest"
)

// A panic in f on OrderedWithState's pool path reaches the caller, with
// its value. The tests of that path dropped recover's value, so a recover
// that swallowed f's panic left them green.
func TestParallelInitPanicInFReachesCaller(t *testing.T) {
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		sluice.Collect(OrderedWithState(sluice.Of(make([]int, 64), 4), 4,
			func() (int, error) { return 0, nil },
			func(_ int, _ sluice.Batch[int]) sluice.Batch[int] { panic("from f") }))
	}()
	if got := streamtest.Relayed(recovered); got != "from f" {
		t.Errorf("recovered %v, want %q", got, "from f")
	}
}

// A panic already unwinding through the consumer takes precedence over the
// panic of an f still in flight, which the consumer never saw (Ordered's
// documentation). Without the guard in runParallel's deferred call, the f
// panic replaced the consumer's.
func TestParallelConsumerPanicWinsOverInFlightF(t *testing.T) {
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		Ordered(sluice.Of([]int{0, 1}, 1), 2, func(b sluice.Batch[int]) sluice.Batch[int] {
			if b.Items[0] == 1 {
				panic("f boom")
			}
			return b
		})(func(sluice.Batch[int]) bool { panic("consumer boom") })
	}()
	if recovered != "consumer boom" {
		t.Errorf("recovered %v, want %q: the unwinding consumer panic must win", recovered, "consumer boom")
	}
}
