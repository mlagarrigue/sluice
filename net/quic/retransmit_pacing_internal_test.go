package quic

import (
	"testing"
	"time"
)

// queueLostPayloads sets a connection up as the tail of a long transfer
// left it: the window grown to a hundred datagrams, the last ACK having
// released the flight and declared thirty payloads lost, each one packet's
// worth. It returns the initial window, the burst RFC 9002 §7.7 bounds a
// retransmission pass to. Callers hold mu.
func queueLostPayloads(t *testing.T, c *Conn) (sp *space, initial int) {
	t.Helper()
	sp = &c.spaces[spaceApplication]
	nr, ok := c.cc.(*newReno)
	if !ok {
		t.Fatalf("default controller is %T", c.cc)
	}
	initial = nr.cwnd
	nr.cwnd = 100 * 1200
	payload := make([]byte, 1100)
	payload[0] = framePing // PING then PADDING: a payload one packet carries
	for range 30 {
		sp.retrans = append(sp.retrans, append([]byte(nil), payload...))
	}
	return sp, initial
}

// RFC 9002 §7.7: "Senders MUST either use pacing or limit such bursts", and
// a burst SHOULD be limited to the initial window. One pass of
// retransmitLocked, with the window wide open, sends at most the pacer's
// bucket — one initial window, plus what the pacing rate refilled while the
// pass ran, a packet's worth under the race detector — and leaves the rest
// queued. Before the pass asked the pacer, it sent all thirty.
func TestRetransmitPassIsBoundedByTheInitialWindow(t *testing.T) {
	c, _ := newKeyedConn(t)
	c.mu.Lock()
	defer c.mu.Unlock()
	sp, initial := queueLostPayloads(t, c)
	rate := c.cc.pacingRate(c.rtt.current())
	start := time.Now()
	c.retransmitLocked()
	elapsed := time.Since(start)
	sent, bytes := 0, 0
	for i := range sp.sent.packets {
		sent++
		bytes += sp.sent.packets[i].size
	}
	if left := len(sp.retrans); sent+left != 30 {
		t.Fatalf("sent %d packets, %d payloads still queued: the harness did not drive the queue as intended", sent, left)
	}
	if allowance := initial + int(rate*elapsed.Seconds()); bytes > allowance {
		t.Fatalf("one pass of retransmitLocked sent %d packets, %d bytes, in %v; §7.7 bounds a burst to the initial window (%d bytes), the bucket to that plus the rate's refill (%d bytes)", sent, bytes, elapsed, initial, allowance)
	}
	if sent == 0 {
		t.Fatal("a pass with a full bucket sent nothing")
	}
}

// The twin: what the pacer held back leaves on its own, pass after pass at
// the pace deadline the timer loop answers, without an ACK to prompt it and
// well before the probe timeout — a retransmission delayed past its PTO
// would answer loss with more loss.
func TestRetransmitRestLeavesByTheTimerBeforeThePTO(t *testing.T) {
	c, _ := newKeyedConn(t)
	c.mu.Lock()
	sp, _ := queueLostPayloads(t, c)
	start := time.Now()
	c.retransmitLocked()
	left := len(sp.retrans)
	pto := c.rtt.pto(c.peerMaxAckDelayLocked())
	c.mu.Unlock()
	if left == 0 {
		t.Fatal("the first pass drained the queue: nothing left for the timer to send")
	}
	for {
		c.mu.Lock()
		left = len(sp.retrans)
		c.mu.Unlock()
		if left == 0 {
			break
		}
		if time.Since(start) > pto {
			t.Fatalf("%d payloads still queued after the probe timeout (%v): the pace deadline did not bring them out", left, pto)
		}
		time.Sleep(time.Millisecond)
	}
}
