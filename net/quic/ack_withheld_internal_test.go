package quic

import (
	"testing"
	"time"
)

// RFC 9000 §13.2.1: "An endpoint MUST NOT send a non-ack-eliciting packet in
// response to a non-ack-eliciting packet". A server Initial packet that
// carries the ACK only because Initial packets always carry one (resend),
// withheld by the §8.1 budget, owed nothing: rearmed, its ACK answered the
// client's next datagram, an ACK-only padded Initial, with an ACK-only
// Initial from flushAcksLocked. Only a debt is owed again (sendOpts.ackDue);
// the control, an ACK owed and withheld, leaves after the client's next
// datagram.
func TestAWithheldPacketOwesAgainOnlyTheAckThatWasDue(t *testing.T) {
	setup := func(t *testing.T) (*Conn, *tapPC, *space) {
		t.Helper()
		pcA, pcB := newLoopbackPair(t)
		tap := newTap(pcA)
		c := newConn(tap, pcB.LocalAddr(), []byte("dcid0000"), []byte("scid0000"), false, DefaultConfig())
		t.Cleanup(func() { _ = c.Close() })
		sec, err := initialSecrets([]byte("dcid0000"))
		if err != nil {
			t.Fatal(err)
		}
		sealer, err := newPacketSealer(sec.Server)
		if err != nil {
			t.Fatal(err)
		}
		c.mu.Lock()
		c.spaces[spaceInitial].sealer = sealer
		c.amplActive, c.amplRecv, c.amplSent = true, 1200, 3600 // budget spent
		c.mu.Unlock()
		return c, tap, &c.spaces[spaceInitial]
	}

	// Control: an ACK that was owed, withheld, leaves after the client's
	// next datagram. Proves the setup can emit an Initial ACK-only packet.
	t.Run("an ACK that was owed", func(t *testing.T) {
		c, tap, sp := setup(t)
		now := time.Now()
		c.mu.Lock()
		sp.ack.record(0, now)
		sp.ack.onAckEliciting(spaceInitial, now, false)
		c.flushAcksLocked(now) // withheld by the budget, rearmed
		c.amplRecv += 1200     // the client's next datagram
		c.flushAcksLocked(now)
		c.mu.Unlock()
		if n := len(tap.recorded()); n != 1 {
			t.Fatalf("control: %d datagrams, want the owed ACK", n)
		}
	})

	t.Run("an ACK that rode along", func(t *testing.T) {
		c, tap, sp := setup(t)
		now := time.Now()
		c.mu.Lock()
		sp.ack.record(0, now)
		sp.ack.onAckEliciting(spaceInitial, now, false)
		sp.ack.settle() // the client Initial was acknowledged already
		if sp.ack.owes(now) || sp.ack.pending() {
			c.mu.Unlock()
			t.Fatal("setup: an ACK is still owed")
		}
		// A probe (PING) in the Initial space carries the ACK by resend
		// and is withheld by the spent budget.
		if err := c.sendPacketLocked(spaceInitial, []byte{framePing}, sendOpts{}); err != nil {
			c.mu.Unlock()
			t.Fatal(err)
		}
		if n := len(tap.recorded()); n != 0 {
			c.mu.Unlock()
			t.Fatalf("setup: %d datagrams left past the budget", n)
		}
		owedAfterWithhold := sp.ack.owes(now)
		// The client's next datagram: an ACK-only Initial padded to 1200
		// bytes, non-ack-eliciting. Its processing ends with this flush
		// (receive.go, before retransmitLocked).
		c.amplRecv += 1200
		c.flushAcksLocked(now)
		c.mu.Unlock()
		if owedAfterWithhold {
			t.Error("withholding a packet whose ACK was not due made an ACK owed")
		}
		if n := len(tap.recorded()); n != 0 {
			t.Errorf("%d ACK-only datagram(s) sent in response to a non-ack-eliciting packet", n)
		}
	})
}
