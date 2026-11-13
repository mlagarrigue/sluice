package quic

import (
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/internal/varint"
)

// The sent record of the application space has one written bound,
// whatever sends fill it. Sends the congestion window gates stop at half
// of it, so an honest flow levels off there and is never cut; sends the
// window does not gate — a MAX_STREAMS returned per stream retired, the
// PATH_CHALLENGEs of a path change, a RESET_STREAM in answer — reach the
// bound only from a peer that acknowledges none of them, and the
// connection then closes with PROTOCOL_VIOLATION (RFC 9000 §13.2.1: an
// endpoint "MUST acknowledge ... all ack-eliciting 0-RTT and 1-RTT
// packets within its advertised max_ack_delay"). Without the bound, a
// record only an ACK empties grew by one packet per peer packet, with
// every timer run.
//
// The peer's packets are spaced past the probe timeout and its backoff,
// and the connection's timer pass runs before each one, so no timer that
// would prune the record or end the connection is skipped; the idle clock
// restarts as receiving a packet restarts it.
func TestUnacknowledgedSendsCloseAtTheRecordBound(t *testing.T) {
	const bound, gap = 64, 2 * time.Second

	t.Run("returned stream slots", func(t *testing.T) {
		c := unackedResetServer(t)
		c.OnStreamFrames(CreditOnDelivery, func([]Frame) error { return nil })
		boundRecord(c, bound)
		now := time.Now()
		var err error
		for opened := uint64(0); opened < 4*bound && err == nil; opened++ {
			c.actOnDeadlines(now)
			restartIdleAt(c, now)
			payload := varint.Append(nil, FrameStream|streamLEN|streamFIN)
			payload = varint.Append(payload, 2+4*opened) // client-initiated unidirectional
			payload = varint.Append(payload, 0)
			if err = c.frames(spaceApplication, payload, now); err != nil {
				break
			}
			c.mu.Lock()
			c.flushAcksLocked(now)
			c.retransmitLocked()
			c.flushControlLocked()
			c.mu.Unlock()
			checkRecordWithin(t, c, bound)
			now = now.Add(gap)
		}
		wantCode(t, err, transportProtocolViolation, "a peer acknowledging none of the stream slots returned")
	})

	t.Run("path moves", func(t *testing.T) {
		c, _ := newMigratableServer(t, "bound-moves")
		boundRecord(c, bound)
		c.mu.Lock()
		c.amplActive = false
		c.mu.Unlock()
		a, b := fakeFuzzAddr("bound-moves:peerA"), fakeFuzzAddr("bound-moves:peerB")
		now := time.Now()
		var err error
		for i := 0; i < 4*bound && err == nil; i++ {
			c.actOnDeadlines(now)
			restartIdleAt(c, now)
			to := b
			if i%2 == 1 {
				to = a
			}
			rxFromNewAddress(c, to, 40, true)
			if err = c.frames(spaceApplication, []byte{framePing}, now); err != nil {
				break
			}
			c.maybeMigrate([]Frame{{Type: framePing}}, now)
			checkRecordWithin(t, c, bound)
			now = now.Add(gap)
		}
		wantCode(t, err, transportProtocolViolation, "a peer acknowledging none of its path changes")
	})
}

// PATH_RESPONSE on the path in use is a send the window gates: with no ACK
// it stops at half the bound — before the congestion window would stop it
// — and the connection stays open.
func TestGatedSendsLevelOffAtHalfTheRecordBound(t *testing.T) {
	const bound = 8
	c, _ := newMigratableServer(t, "bound-gated")
	boundRecord(c, bound)
	for i := range 200 {
		payload := varint.Append(nil, framePathChallenge)
		payload = append(payload, byte(i), byte(i>>8), 3, 4, 5, 6, 7, 8)
		if err := c.frames(spaceApplication, payload, time.Now()); err != nil {
			t.Fatalf("challenge %d closed the connection: %v", i, err)
		}
	}
	c.mu.Lock()
	tracked := len(c.spaces[spaceApplication].sent.packets)
	room := c.cc.canSend(1200)
	c.mu.Unlock()
	if tracked != bound/2 {
		t.Errorf("gated answers left %d packets in the record, want them to stop at half the bound, %d", tracked, bound/2)
	}
	if !room {
		t.Error("precondition lost: the congestion window, not the record bound, stopped the answers")
	}
}

func boundRecord(c *Conn, n int) {
	c.mu.Lock()
	c.spaces[spaceApplication].sent.limit = n
	c.mu.Unlock()
}

func restartIdleAt(c *Conn, now time.Time) {
	c.mu.Lock()
	if c.idleTimeout != 0 {
		if d := now.Add(c.idleTimeout); d.After(c.idleDeadline) {
			c.idleDeadline = d
		}
	}
	c.mu.Unlock()
}

func checkRecordWithin(t *testing.T, c *Conn, bound int) {
	t.Helper()
	c.mu.Lock()
	tracked := len(c.spaces[spaceApplication].sent.packets)
	c.mu.Unlock()
	if tracked > bound {
		t.Fatalf("the sent record holds %d packets, past its bound of %d", tracked, bound)
	}
}

// The packet that closes a connection whose record filled was judged before
// its frames were read: when that packet carried the acknowledgement that
// empties the record, the peer was closed for not acknowledging in the very
// packet that did. Its frames are read first now. The control is a packet
// that acknowledges nothing: it still closes.
func TestThePacketThatAcknowledgesAFullRecordIsRead(t *testing.T) {
	const bound = 8
	fill := func(t *testing.T) (*Conn, uint64) {
		t.Helper()
		c, _ := newMigratableServer(t, "bound-acked")
		boundRecord(c, bound)
		c.mu.Lock()
		c.amplActive = false
		for range bound + 1 {
			_ = c.sendPacketLocked(spaceApplication, []byte{framePing}, sendOpts{})
		}
		full := c.recordFull
		largest := c.spaces[spaceApplication].nextPN - 1
		c.mu.Unlock()
		if !full {
			t.Fatal("precondition lost: the record did not fill")
		}
		return c, largest
	}
	t.Run("an ACK of the last packet", func(t *testing.T) {
		c, largest := fill(t)
		// The last packet alone: one fewer in the record is under its bound.
		// (A range down to 0 would name the number skipped on purpose.)
		ack := appendFrame(nil, frameACK, largest, 0, 0, 0)
		if err := c.frames(spaceApplication, ack, time.Now()); err != nil {
			t.Errorf("the packet acknowledging the full record closed the connection: %v", err)
		}
	})
	t.Run("control, a PING", func(t *testing.T) {
		c, _ := fill(t)
		err := c.frames(spaceApplication, []byte{framePing}, time.Now())
		wantCode(t, err, transportProtocolViolation, "a peer acknowledging none of a full record")
	})
}
