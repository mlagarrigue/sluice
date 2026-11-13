package quic

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

// BenchmarkCreditTrickle prices one MAX_DATA frame that raises a spent
// connection window by a single byte while N writers are parked on it, as
// a peer that grants credit a byte at a time would have it. The writers
// park as Stream.write parks them (waitForConnCreditLocked) and take the
// byte as it does. The cost is per frame and must not grow with N: on one
// cond, every parked writer woke for each frame and all but one parked
// again, so a hostile peer bought N wake-ups with three bytes.
func BenchmarkCreditTrickle(b *testing.B) {
	for _, n := range []int{1, 64, 1024} {
		b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
			c := assembleConn(&fuzzSink{addr: "trickle:self"}, fakeFuzzAddr("trickle:peer"),
				[]byte("dcid0000"), []byte("scid0000"), false, DefaultConfig())
			b.Cleanup(func() { _ = c.Close() })
			c.mu.Lock()
			c.connSend.sent = c.connSend.max
			limit := c.connSend.max
			c.mu.Unlock()
			took := make(chan struct{}, 1)
			stop := false
			var parked, done sync.WaitGroup
			parked.Add(n)
			done.Add(n)
			for range n {
				go func() {
					defer done.Done()
					c.mu.Lock()
					defer c.mu.Unlock()
					first := true
					for !stop {
						if c.connSend.avail() > 0 {
							c.connSend.sent++ // the writer's frame takes the byte
							c.passCreditLocked()
							took <- struct{}{}
							continue
						}
						if first {
							parked.Done()
							first = false
						}
						if err := c.waitForConnCreditLocked(time.Time{}); err != nil {
							return
						}
					}
				}()
			}
			parked.Wait()
			b.ResetTimer()
			for range b.N {
				limit++
				c.handleLimitGrant(Frame{Type: frameMaxData, value: limit})
				<-took
			}
			b.StopTimer()
			c.mu.Lock()
			stop = true
			c.wakeAllLocked()
			c.mu.Unlock()
			done.Wait()
		})
	}
}

// BenchmarkStreamLimitTrickle prices one MAX_STREAMS frame that raises the
// peer's limit by one while N writers are parked on the connection's credit,
// which that frame does not give. The writers should sleep through it; the
// Gosched after each frame lets any that woke run and park again, so their
// cost lands in the frame's. Flat in N, or a peer buys N wake-ups with two
// bytes, as BenchmarkCreditTrickle guards for MAX_DATA.
func BenchmarkStreamLimitTrickle(b *testing.B) {
	for _, n := range []int{1, 64, 1024} {
		b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
			c := assembleConn(&fuzzSink{addr: "limit-trickle:self"}, fakeFuzzAddr("limit-trickle:peer"),
				[]byte("dcid0000"), []byte("scid0000"), false, DefaultConfig())
			b.Cleanup(func() { _ = c.Close() })
			c.mu.Lock()
			c.connSend.sent = c.connSend.max
			limit := c.peerMaxStreamsUni
			c.mu.Unlock()
			stop := false
			var parked, done sync.WaitGroup
			parked.Add(n)
			done.Add(n)
			for range n {
				go func() {
					defer done.Done()
					c.mu.Lock()
					defer c.mu.Unlock()
					parked.Done()
					for !stop {
						if err := c.waitForConnCreditLocked(time.Time{}); err != nil {
							return
						}
					}
				}()
			}
			parked.Wait()
			c.mu.Lock()
			c.mu.Unlock() //nolint:staticcheck,gocritic // SA2001, badLock: the lock is the barrier — every writer is in Wait
			b.ResetTimer()
			for range b.N {
				limit++
				c.handleLimitGrant(Frame{Type: frameMaxStreams | 1, value: limit})
				runtime.Gosched()
			}
			b.StopTimer()
			c.mu.Lock()
			stop = true
			c.wakeAllLocked()
			c.mu.Unlock()
			done.Wait()
		})
	}
}
