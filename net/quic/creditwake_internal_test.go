package quic

import (
	"runtime"
	"strings"
	"testing"
	"time"
)

// creditWaiters counts the goroutines parked for their stream's credit.
func creditWaiters() int {
	buf := make([]byte, 1<<20)
	buf = buf[:runtime.Stack(buf, true)]
	n := 0
	for g := range strings.SplitSeq(string(buf), "\n\n") {
		if strings.Contains(g, "sync.(*Cond).Wait") && strings.Contains(g, "waitForStreamCreditLocked") {
			n++
		}
	}
	return n
}

func waitCreditWaiters(t *testing.T, want int) {
	t.Helper()
	got := 0
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		if got = creditWaiters(); got == want {
			return
		}
	}
	t.Fatalf("%d writers parked on their stream's credit, want %d", got, want)
}

// unwoken clears the blocked marks of ss, which a writer sets again each
// time it goes back to wait (sendBlockedFramesLocked), and returns a check
// that fails the test if any of them was set since: a writer woken for
// nothing parks again at once, and the count of parked writers alone
// cannot tell it from one that never woke.
func unwoken(t *testing.T, c *Conn, ss ...*Stream) func(event string) {
	t.Helper()
	c.mu.Lock()
	for _, s := range ss {
		s.sendQ.blockedSent = false
	}
	c.mu.Unlock()
	return func(event string) {
		t.Helper()
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, s := range ss {
			if s.sendQ.blockedSent {
				t.Fatalf("%s woke the writer of stream %d, which it gives no credit", event, s.id)
			}
		}
	}
}

// A writer parked for its stream's credit is woken by what can give it
// some — a MAX_STREAM_DATA for its stream — and by nothing meant for
// another: an ACK, or another stream's MAX_STREAM_DATA, leaves it asleep.
// The stack shows which: parked on the stream's own cond, or gone from it;
// the blocked mark shows a writer woken for nothing that parked again.
func TestStreamCreditWakesItsOwnWriter(t *testing.T) {
	c, ss := deadlineConn(t, 2)
	a, b := ss[0], ss[1]
	c.mu.Lock()
	c.connSend.grant(1 << 20)
	a.sendQ, b.sendQ = sendQuota{max: 0}, sendQuota{max: 0}
	c.mu.Unlock()
	for _, s := range ss {
		go func() { _, _ = s.Write([]byte{1}) }()
	}
	waitCreditWaiters(t, 2)

	// An ACK for something the connection sent: nothing for either.
	asleep := unwoken(t, c, a, b)
	now := time.Now()
	sp := &c.spaces[spaceInitial]
	c.mu.Lock()
	sp.sent.record(sentPacket{pn: 0, sentAt: now.Add(-time.Millisecond), size: 1200, ackEliciting: true})
	c.cc.onSent(now, 1200)
	sp.nextPN = 1
	c.mu.Unlock()
	var peer ackTracker
	peer.record(0, now)
	if err := c.handleAck(spaceInitial, ackFrameFor(t, &peer), now); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	waitCreditWaiters(t, 2)
	asleep("an ACK")

	// b's credit: b's writer leaves its wait, a's stays.
	asleep = unwoken(t, c, a)
	if err := c.handleMaxStreamData(Frame{Type: frameMaxStreamData, StreamID: b.id, value: 10}, spaceApplication); err != nil {
		t.Fatal(err)
	}
	waitCreditWaiters(t, 1)
	time.Sleep(20 * time.Millisecond)
	asleep("b's MAX_STREAM_DATA")
	c.mu.Lock()
	aWaits := a.credit != nil && a.sendQ.avail() == 0
	c.mu.Unlock()
	if !aWaits {
		t.Fatal("the writer left parked is not a's")
	}

	// a's reset by this end ends its wait too. With no keys the
	// RESET_STREAM cannot be sealed, which Reset reports; the stream is
	// reset and its writer woken before that.
	_ = a.Reset(0)
	waitCreditWaiters(t, 0)
}

// A writer whose stream has room and whose connection has no credit parks on
// connCredit. A STOP_SENDING for its stream ends its writing side; the
// writer must see that at once, not at the next MAX_DATA, deadline or ACK
// that happens to wake connCredit.
func TestStopSendingWakesAWriterParkedOnTheConnectionsCredit(t *testing.T) {
	c, ss := deadlineConn(t, 1)
	s := ss[0]
	c.mu.Lock()
	s.sendQ = sendQuota{max: 1 << 20}
	c.connSend = sendQuota{max: 0}
	c.mu.Unlock()
	done := make(chan error, 1)
	go func() { _, err := s.Write([]byte{1}); done <- err }()
	time.Sleep(50 * time.Millisecond) // the writer reaches connCredit
	select {
	case err := <-done:
		t.Fatalf("Write returned %v before the STOP_SENDING", err)
	default:
	}
	if _, err := c.handleStopSending(Frame{Type: FrameStopSending, StreamID: s.id, value: 7}, spaceApplication); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Write returned nil after STOP_SENDING")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a writer parked on the connection's credit slept through STOP_SENDING")
	}
}
