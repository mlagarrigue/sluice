package quic

import (
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/internal/varint"
)

// A peer that acknowledges only its newest packet makes every earlier one
// lost, and a lost packet's payload goes back on the queue. Each stream it
// opens and finishes earns it a MAX_STREAMS, one per packet: unless a
// credit frame that a newer one superseded is let go (RFC 9000 §13.3: an
// updated value is sent when the packet carrying the most recent one is
// lost), the queue follows the streams retired, outside the bound of the
// sent record, for as long as the connection lives. The retransmission pass
// is given every chance between rounds — a full pacer bucket, repeated
// passes — and the MAX_STREAMS payloads held in the record and the queue
// are counted together.
func TestSupersededCreditLeavesTheResendQueue(t *testing.T) {
	const bound = 256
	const rounds, perRound = 40, 100

	countGrants := func(c *Conn) (tracked, queued, grants int) {
		sp := &c.spaces[spaceApplication]
		has := func(p []byte) bool {
			for n := 0; n < len(p); {
				f, tail, err := parseFrame(p[n:])
				if err != nil {
					return false
				}
				if f.Type == frameMaxStreams|1 {
					return true
				}
				n = len(p) - len(tail)
			}
			return false
		}
		for i := range sp.sent.packets {
			if has(sp.sent.packets[i].payload) {
				grants++
			}
		}
		for _, p := range sp.retrans {
			if has(p) {
				grants++
			}
		}
		return len(sp.sent.packets), len(sp.retrans), grants
	}

	t.Run("newest acknowledged only", func(t *testing.T) {
		c := unackedResetServer(t)
		c.OnStreamFrames(CreditOnDelivery, func([]Frame) error { return nil })
		now := time.Now()
		endPacket := func() {
			c.mu.Lock()
			c.flushAcksLocked(now)
			c.retransmitLocked()
			c.flushControlLocked()
			c.mu.Unlock()
		}
		drain := func() {
			c.mu.Lock()
			for range 8 {
				c.pacer.tokens = c.pacer.burst
				c.retransmitLocked()
			}
			c.mu.Unlock()
		}
		next := uint64(0)
		for r := range rounds {
			for range perRound {
				c.mu.Lock()
				granted := c.localMaxStreamsUni
				c.mu.Unlock()
				if next >= granted {
					t.Fatalf("round %d: stream %d past the granted limit %d", r, next, granted)
				}
				payload := varint.Append(nil, FrameStream|streamLEN|streamFIN)
				payload = varint.Append(payload, 2+4*next)
				payload = varint.Append(payload, 0)
				if err := c.frames(spaceApplication, payload, now); err != nil {
					t.Fatalf("stream %d: %v", next, err)
				}
				endPacket()
				next++
			}
			now = now.Add(50 * time.Millisecond)
			c.mu.Lock()
			pk := c.spaces[spaceApplication].sent.packets
			if len(pk) == 0 {
				c.mu.Unlock()
				t.Fatalf("round %d: nothing tracked", r)
			}
			last := pk[len(pk)-1].pn
			c.mu.Unlock()
			ack := appendAckFrame(nil, []pnRange{{lo: last, hi: last}}, 0, [3]uint64{})
			if err := c.frames(spaceApplication, ack, now); err != nil {
				t.Fatalf("round %d: ACK: %v", r, err)
			}
			endPacket()
			drain()
		}
		c.mu.Lock()
		tracked, queued, grants := countGrants(c)
		c.mu.Unlock()
		if tracked+queued > bound {
			t.Errorf("%d streams retired: %d tracked + %d queued (%d MAX_STREAMS payloads held), want at most %d",
				next, tracked, queued, grants, bound)
		}
	})

	t.Run("a superseded MAX_STREAMS is stale", func(t *testing.T) {
		c := unackedResetServer(t)
		c.mu.Lock()
		defer c.mu.Unlock()
		c.localMaxStreamsUni += 50
		if old := (Frame{Type: frameMaxStreams | 1, value: c.localMaxStreamsUni - 40}); !c.staleLocked(&old) {
			t.Errorf("MAX_STREAMS %d with %d announced since: not stale", old.value, c.localMaxStreamsUni)
		}
		if cur := (Frame{Type: frameMaxStreams | 1, value: c.localMaxStreamsUni}); c.staleLocked(&cur) {
			t.Errorf("the most recent MAX_STREAMS %d is stale", cur.value)
		}
	})

	t.Run("a superseded MAX_DATA is stale", func(t *testing.T) {
		c := unackedResetServer(t)
		c.mu.Lock()
		defer c.mu.Unlock()
		current := c.connRecv.max
		if old := (Frame{Type: frameMaxData, value: current - 1}); !c.staleLocked(&old) {
			t.Errorf("MAX_DATA %d with %d announced since: not stale", old.value, current)
		}
		if cur := (Frame{Type: frameMaxData, value: current}); c.staleLocked(&cur) {
			t.Errorf("the most recent MAX_DATA %d is stale", current)
		}
	})
}
