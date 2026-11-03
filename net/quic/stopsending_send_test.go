package quic

import (
	"net"
	"syscall"
	"testing"
	"time"
)

// refusingPC refuses every send, as a socket whose route went away does.
type refusingPC struct{ fuzzSink }

func (p *refusingPC) WriteTo([]byte, net.Addr) (int, error) {
	return 0, &net.OpError{Op: "write", Net: "udp", Err: syscall.ENETUNREACH}
}

// The read loop sends what it owes at its best effort: the packet is
// recorded before it is written, so a send the socket refuses is a loss that
// recovery repairs, and the connection goes on — the ACK it owes, the
// control frames, the retransmissions. The RESET_STREAM that answers a
// STOP_SENDING was the exception: its send error went up to processPacket,
// which aborted the connection and every stream on it, for a refusal
// (ENETUNREACH, a firewall's EPERM, an ICMP error on a connected socket) the
// same socket survives anywhere else.
func TestAFailedResetAnswerDoesNotEndTheConnection(t *testing.T) {
	c := newConn(&refusingPC{fuzzSink{addr: "stop:self"}}, fakeFuzzAddr("stop:peer"),
		[]byte("dcid0000"), []byte("scid0000"), true, DefaultParameters())
	defer c.Close()
	installAppSealer(t, c)
	c.mu.Lock()
	c.peerMaxStreamsBidi = 1
	c.mu.Unlock()
	s, err := c.Stream(0)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	before := c.Stats().WriteFailures
	err = c.processPacket(spaceApplication, false, 0, appendFrame(nil, FrameStopSending, 0, 0x10c), now)
	if c.Stats().WriteFailures == before {
		t.Fatal("the RESET_STREAM answer never reached the socket; the test proves nothing")
	}
	if code, reset := s.WasReset(); !reset || code != 0x10c {
		t.Fatalf("WasReset = (%#x, %v), want (0x10c, true)", code, reset)
	}
	if err != nil || c.Err() != nil {
		t.Fatalf("a refused send of the RESET_STREAM ended the connection: processPacket = %v, Err = %v", err, c.Err())
	}
}
