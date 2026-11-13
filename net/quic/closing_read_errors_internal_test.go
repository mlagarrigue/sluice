package quic

import (
	"net"
	"syscall"
	"testing"
	"time"
)

// icmpRefusedPC is a socket every read of which fails as an ICMP
// port-unreachable surfaces on Linux.
type icmpRefusedPC struct{ fuzzSink }

func (*icmpRefusedPC) ReadFrom([]byte) (int, net.Addr, error) { return 0, nil, syscall.ECONNREFUSED }

// Stats.ReadErrors counts the failed reads of the connection's own socket,
// and the read loop's closing state reads that socket too: a read that
// failed there ended the state and went uncounted.
func TestAReadFailingInTheClosingStateIsCounted(t *testing.T) {
	c := assembleConn(&icmpRefusedPC{fuzzSink{addr: "closing:self"}}, fakeFuzzAddr("closing:peer"), []byte{1, 2, 3, 4}, []byte{5, 6, 7, 8}, true, DefaultConfig())
	c.mu.Lock()
	c.closing.active.Store(true)
	c.closing.end = time.Now().Add(time.Second)
	c.mu.Unlock()
	c.holdClosing(make([]byte, 1500))
	if got := c.Stats().ReadErrors; got != 1 {
		t.Errorf("ReadErrors = %d after one failed read in the closing state, want 1", got)
	}
}
