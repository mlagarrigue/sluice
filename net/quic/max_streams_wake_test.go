package quic

import (
	"testing"
	"testing/synctest"
)

// A MAX_STREAMS wakes the writers parked on the stream limit only when it
// raises the limit, as MAX_DATA wakes them only when grant raises the
// window. Woken on every one, a peer's two-byte frame bought a wake-up per
// parked writer, each of which found nothing changed and parked again.
func TestAMaxStreamsThatRaisesNothingWakesNoOne(t *testing.T) {
	for _, typ := range []byte{frameMaxStreams, frameMaxStreams + 1} {
		synctest.Test(t, func(t *testing.T) {
			c := newConn(&fuzzSink{addr: "wake:self"}, fakeFuzzAddr("wake:peer"),
				[]byte("dcid0000"), []byte("scid0000"), true, DefaultParameters())
			defer c.Close()
			c.mu.Lock()
			c.peerMaxStreamsBidi, c.peerMaxStreamsUni = 10, 10
			c.mu.Unlock()

			woken := make(chan struct{}, 4)
			c.mu.Lock()
			go func() {
				c.mu.Lock()
				c.cond.Wait()
				c.mu.Unlock()
				woken <- struct{}{}
			}()
			c.mu.Unlock()
			synctest.Wait()

			c.handleLimitGrant(Frame{Type: typ, value: 10}) // raises nothing
			synctest.Wait()
			select {
			case <-woken:
				t.Fatalf("frame %#x at the current limit woke a parked writer", typ)
			default:
			}

			c.handleLimitGrant(Frame{Type: typ, value: 11})
			synctest.Wait()
			select {
			case <-woken:
			default:
				t.Fatalf("frame %#x raising the limit left the parked writer asleep", typ)
			}
		})
	}
}
