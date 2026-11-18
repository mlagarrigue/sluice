package httpstream

import (
	"testing"
	"testing/synctest"
	"time"
)

// A call handed to the pool after another has returned runs on the
// goroutine that ran the first, which kept its stack; an idle goroutine
// leaves after warmIdle, and stop ends the ones still idle. Read from the
// pool's own list: runtime.NumGoroutine counts the whole process, which
// other tests' goroutines move (red once under Go 1.27.4 in CI).
func TestWarmGoroutinesReuseThenLeave(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var p warmGoroutines
		idle := func() int {
			p.mu.Lock()
			defer p.mu.Unlock()
			return len(p.idle)
		}
		done := make(chan struct{})
		call := func() { done <- struct{}{} }

		p.Go(call)
		<-done
		synctest.Wait() // the goroutine is back on the idle list
		if n := idle(); n != 1 {
			t.Fatalf("%d goroutines idle after one call, want 1", n)
		}
		p.Go(call)
		if n := idle(); n != 0 {
			t.Errorf("a call with a goroutine idle left %d idle: it did not take it", n)
		}
		<-done
		synctest.Wait()
		if n := idle(); n != 1 {
			t.Errorf("%d goroutines idle after the second call, want the one that ran both", n)
		}

		time.Sleep(warmIdle)
		synctest.Wait()
		if n := idle(); n != 0 {
			t.Errorf("%d goroutines idle %v after the last call, want none", n, warmIdle)
		}

		p.Go(call)
		<-done
		synctest.Wait()
		p.stop()
		synctest.Wait() // the stopped goroutine has returned, or the bubble would not end
		if n := idle(); n != 0 {
			t.Errorf("%d goroutines idle after stop", n)
		}
		ran := make(chan struct{})
		p.Go(func() { close(ran) }) // a stopped pool still runs what it is handed
		<-ran
	})
}
