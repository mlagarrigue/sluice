package benchmarks

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/gateway"
)

// What a lone call actually waits, against what Config.Within asked for.
//
// This exists because a gateway benchmark read wrong without it. Serving one
// request at a time through a gateway configured with Within=200µs measured
// 1.14 ms per request — 600× the direct handler — and the obvious reading was
// that the batching machinery costs a millisecond. It does not. The platform's
// timer granularity does: below roughly a millisecond, the requested delay is
// not honoured and every lone call waits for the floor instead.
//
// That matters twice over. It means a `Within` shorter than the floor is a
// setting with no effect, which a caller tuning latency needs to know. And it
// means the serial gateway figure published beside it is measuring the clock
// rather than the code — so the mechanism's real cost has to be read from the
// concurrent case and from the allocation counts.
//
// The floor is a property of the machine, not of this library. It was
// observed under WSL2, where Linux neither sees nor controls the host clock,
// and it may well differ elsewhere — which is exactly why this is a
// measurement that reruns rather than a number written into a document.
func TestGatewayTimerFloor(t *testing.T) {
	if os.Getenv("SLUICE_LATENCY") == "" {
		t.Skip("set SLUICE_LATENCY=1 to measure the platform's timer floor")
	}

	for _, within := range []time.Duration{
		100 * time.Microsecond, 200 * time.Microsecond, 500 * time.Microsecond,
		1 * time.Millisecond, 2 * time.Millisecond, 5 * time.Millisecond,
	} {
		gw := gateway.New(gateway.Config{Size: 64, Within: within},
			func(calls sluice.Stream[*gateway.Call[int, int]]) {
				calls(func(b sluice.Batch[*gateway.Call[int, int]]) bool {
					for _, c := range b.Items {
						c.Reply(c.In)
					}
					return true
				})
			})

		const n = 50
		start := time.Now()
		for i := range n {
			if _, err := gw.Do(context.Background(), i); err != nil {
				t.Fatal(err)
			}
		}
		per := time.Since(start) / n
		_ = gw.Close()

		ratio := float64(per) / float64(within)
		t.Logf("Within=%-8v  observed=%-12v  ratio=%.2f", within, per, ratio)
	}
}
