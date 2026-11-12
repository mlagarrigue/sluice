package quic

import (
	"testing"
	"testing/synctest"
	"time"
)

// A peer may announce an idle timeout of a millisecond, and the
// connection then waits three probe timeouts before it calls the silence
// fatal (RFC 9000 §10.1). The keepalive was capped at half the
// announcement, so a peer that went silent after announcing a millisecond
// had this end send a PING a millisecond, each one ack-eliciting, for as
// long as the three probe timeouts lasted. The cap is half the timeout in
// force: a handful of PINGs per silence, whatever the peer announced.
func TestKeepAliveFollowsTheIdleTimeoutInForce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := assembleConn(&fuzzSink{addr: "keepalive:self"}, fakeFuzzAddr("keepalive:peer"), []byte{1, 2, 3, 4}, []byte{5, 6, 7, 8}, true, DefaultConfig())
		defer func() { _ = c.Close() }()
		sec, err := initialSecrets([]byte("appkeys0"))
		if err != nil {
			t.Fatal(err)
		}
		sealer, err := newPacketSealer(sec.Client)
		if err != nil {
			t.Fatal(err)
		}
		c.mu.Lock()
		sp := &c.spaces[spaceApplication]
		sp.sealer = sealer
		c.idleTimeout = time.Millisecond // the peer's announcement, the smaller
		c.rtt.sample(20*time.Millisecond, 0)
		pto := c.rtt.pto(c.peerMaxAckDelayLocked())
		c.idleDeadline = time.Now().Add(c.idleTimeout)
		c.mu.Unlock()
		if err := c.SetKeepAlive(time.Hour); err != nil {
			t.Fatal(err)
		}
		if got, want := c.KeepAlive(), 3*pto/2; got != want {
			t.Errorf("KeepAlive = %v under a 1 ms announcement and a %v probe timeout, want %v (half of three)", got, pto, want)
		}

		// The peer is silent from here: the deadlines are run a millisecond
		// apart until the connection gives up.
		c.mu.Lock()
		before := len(sp.sent.packets)
		c.mu.Unlock()
		for range 3 * pto / time.Millisecond {
			time.Sleep(time.Millisecond)
			c.actOnDeadlines(time.Now())
		}
		c.mu.Lock()
		sent := len(sp.sent.packets) - before
		c.mu.Unlock()
		t.Logf("%d packets sent in %v of silence (probe timeout %v)", sent, 3*pto, pto)
		if sent > 8 {
			t.Errorf("%d packets sent in %v of silence after a 1 ms idle announcement, want a handful", sent, 3*pto)
		}
	})
}
