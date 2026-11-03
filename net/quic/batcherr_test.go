package quic

import (
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

// The error of a batch flushed by a writer about to park for credit
// (waitForCreditLocked) is that writer's. It was left in the shared field,
// which the next writer resets when it starts and returns when it ends: the
// parked writer woke to find it erased, or another writer returned it.
func TestAParkedWritersFlushErrorIsItsOwn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newConn(&fuzzSink{addr: "batch:self"}, fakeFuzzAddr("batch:peer"),
			[]byte("dcid0000"), []byte("scid0000"), true, DefaultConfig())
		defer c.Close()
		mine := errors.New("the parked writer's flush")
		parked := make(chan error, 1)
		go func() {
			c.mu.Lock()
			c.batching, c.batchErr = true, nil // Stream.write's start
			c.keepBatchErr(mine)               // its batch failed as it was flushed
			_ = c.waitForCreditLocked(time.Time{})
			err := c.batchErr // Stream.write's end
			c.batchErr = nil
			c.mu.Unlock()
			parked <- err
		}()
		synctest.Wait()

		// Another writer runs a whole Stream.write while the first sleeps.
		c.mu.Lock()
		c.batching, c.batchErr = true, nil
		c.batching = false
		other := c.batchErr
		c.batchErr = nil
		c.cond.Broadcast()
		c.mu.Unlock()

		if other != nil {
			t.Errorf("the other writer returned %v, the parked writer's error", other)
		}
		if err := <-parked; !errors.Is(err, mine) {
			t.Errorf("the parked writer returned %v, want its own flush's error", err)
		}
	})
}
