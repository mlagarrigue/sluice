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

// A writer parked for its stream's credit is woken by what can give it
// some — a MAX_STREAM_DATA for its stream — and by nothing meant for
// another: an ACK, or another stream's MAX_STREAM_DATA, leaves it asleep.
// The stack shows which: parked on the stream's own cond, or gone from it.
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

	// b's credit: b's writer leaves its wait, a's stays.
	if err := c.handleMaxStreamData(Frame{Type: frameMaxStreamData, StreamID: b.id, value: 10}, spaceApplication); err != nil {
		t.Fatal(err)
	}
	waitCreditWaiters(t, 1)
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
