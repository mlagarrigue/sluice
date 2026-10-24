package gateway

import (
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/mlagarrigue/sluice"
)

// TestEagerAtLowTrafficWaitsForNothing: one call on an eager gateway is
// answered without the clock moving, where a windowed gateway holds it
// for Within first.
func TestEagerAtLowTrafficWaitsForNothing(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  Config
		want time.Duration
	}{
		{"eager", Config{Size: 64, Eager: true}, 0},
		{"within", Config{Size: 64, Within: 2 * time.Millisecond}, 2 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				gw := New(tc.cfg, echo)
				defer gw.Close()
				start := time.Now()
				out, err := gw.Do(t.Context(), 7)
				if err != nil || out != "v7" {
					t.Fatalf("%q %v", out, err)
				}
				if got := time.Since(start); got != tc.want {
					t.Fatalf("answered after %v, want %v", got, tc.want)
				}
			})
		})
	}
}

// TestEagerUnderLoadBatchesWhileThePipelineWorks: against a backend that
// takes 1 ms a batch, 200 callers arriving together are served in batches
// of up to Size, formed while the batch ahead is in the backend — the
// first leaves alone, the rest leave with whoever queued behind it. Every
// caller gets its own answer.
func TestEagerUnderLoadBatchesWhileThePipelineWorks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var mu sync.Mutex
		var sizes []int
		backend := func(calls sluice.Stream[*Call[int, string]]) {
			calls(func(b sluice.Batch[*Call[int, string]]) bool {
				mu.Lock()
				sizes = append(sizes, b.Len())
				mu.Unlock()
				time.Sleep(time.Millisecond) // the round trip
				for _, c := range b.Items {
					c.Reply("v" + string(rune('0'+c.In%10)))
				}
				return true
			})
		}
		const size, callers = 16, 200
		gw := New(Config{Size: size, Eager: true}, backend)
		defer gw.Close()
		var wg sync.WaitGroup
		for i := range callers {
			wg.Go(func() {
				out, err := gw.Do(t.Context(), i)
				if err != nil || out != "v"+string(rune('0'+i%10)) {
					t.Errorf("call %d: %q %v", i, out, err)
				}
			})
		}
		wg.Wait()
		total, most := 0, 0
		for _, n := range sizes {
			total += n
			most = max(most, n)
		}
		if total != callers || most != size || len(sizes) >= callers/2 {
			t.Fatalf("%d batches of %v: want %d calls in batches of at most %d, far fewer batches than calls", len(sizes), sizes, callers, size)
		}
		t.Logf("%d calls in %d batches", callers, len(sizes))
	})
}

// TestEagerConfig: under Eager, Within has no meaning and must be zero;
// without it, Within is still required.
func TestEagerConfig(t *testing.T) {
	for _, tc := range []struct {
		cfg Config
		ok  bool
	}{
		{Config{Size: 1, Eager: true}, true},
		{Config{Size: 1, Eager: true, Within: time.Millisecond}, false},
		{Config{Size: 1}, false},
		{Config{Size: 1, Within: time.Millisecond}, true},
	} {
		if err := tc.cfg.Validate(); (err == nil) != tc.ok {
			t.Errorf("%+v: %v, want ok %v", tc.cfg, err, tc.ok)
		}
	}
}
