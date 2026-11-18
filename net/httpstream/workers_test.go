package httpstream

import (
	"runtime"
	"testing"
	"testing/synctest"
	"time"
)

// A call handed to the pool after another has returned runs on the
// goroutine that ran the first, which kept its stack; an idle goroutine
// leaves after warmIdle, and stop ends the ones still idle.
func TestWarmGoroutinesReuseThenLeave(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var p warmGoroutines
		done := make(chan struct{})
		call := func() { done <- struct{}{} }

		p.Go(call)
		<-done
		synctest.Wait() // the goroutine is back on the idle list
		before := runtime.NumGoroutine()
		p.Go(call)
		if n := runtime.NumGoroutine(); n != before {
			t.Errorf("a call with a goroutine idle started another: %d goroutines, %d before", n, before)
		}
		<-done
		synctest.Wait()

		time.Sleep(warmIdle)
		synctest.Wait()
		if n := runtime.NumGoroutine(); n != before-1 {
			t.Errorf("%d goroutines %v after the last call, want %d: the idle one stayed", n, warmIdle, before-1)
		}

		p.Go(call)
		<-done
		synctest.Wait()
		p.stop()
		synctest.Wait()
		if n := runtime.NumGoroutine(); n != before-1 {
			t.Errorf("%d goroutines after stop, want %d", n, before-1)
		}
		ran := make(chan struct{})
		p.Go(func() { close(ran) }) // a stopped pool still runs what it is handed
		<-ran
	})
}
