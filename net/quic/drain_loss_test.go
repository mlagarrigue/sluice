package quic

import (
	"context"
	"testing"
	"time"
)

// Shutdown waits for the flight to empty. A packet with nothing to resend —
// here a PING sent noRetrans, as a DPLPMTUD probe or a PATH_CHALLENGE is —
// left alone in flight by an ACK of a newer one, and then declared lost by
// the loss timer, empties it without queueing or sending anything: nothing
// woke the wait, and Shutdown stayed parked until its context ended.
func TestShutdownEndsWhenTheLossTimerEmptiesTheFlight(t *testing.T) {
	c, _ := appConn(t)
	ackInFlight(t, c)
	c.mu.Lock()
	// A round trip of 100 ms, so that the loss delay (9/8 of it) is far
	// above a coarse clock's tick: under a near-zero sample it fell to its
	// 1 ms floor, and on Windows the probe was declared lost by the ACK
	// itself, before the timer this test fires was ever armed.
	c.rtt.sampled = true
	c.rtt.latest, c.rtt.min, c.rtt.smoothed = 100*time.Millisecond, 100*time.Millisecond, 100*time.Millisecond
	c.rtt.variance = 50 * time.Millisecond
	sp := &c.spaces[spaceApplication]
	if err := c.sendPacketLocked(spaceApplication, []byte{framePing}, sendOpts{noRetrans: true}); err != nil {
		c.mu.Unlock()
		t.Fatal(err)
	}
	newer := sp.nextPN
	if err := c.sendPacketLocked(spaceApplication, []byte{framePing}, sendOpts{}); err != nil {
		c.mu.Unlock()
		t.Fatal(err)
	}
	c.mu.Unlock()
	ack := appendAckFrame(nil, []pnRange{{lo: newer, hi: newer}}, 0, [3]uint64{})
	if err := c.frames(spaceApplication, ack, time.Now()); err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	n, armed := len(sp.sent.packets), !sp.sent.lossTime.IsZero()
	c.mu.Unlock()
	if n != 1 || !armed {
		t.Fatalf("setup: %d in flight, loss timer armed %v; want the probe alone, the timer armed", n, armed)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Shutdown(ctx) }()
	time.Sleep(200 * time.Millisecond) // Shutdown parked on the flight
	c.mu.Lock()
	at := sp.sent.lossTime.Add(time.Millisecond)
	c.mu.Unlock()
	c.actOnDeadlines(at)
	c.mu.Lock()
	empty, queued := c.flightEmptyLocked(), len(sp.retrans)
	c.mu.Unlock()
	if !empty || queued != 0 {
		t.Fatalf("after the loss timer: flight empty %v, %d queued; want empty and nothing to resend", empty, queued)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Shutdown = %v, want nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the flight is empty after the loss timer, and Shutdown is still parked")
	}
}
