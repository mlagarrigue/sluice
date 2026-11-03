package quic

import (
	"crypto/tls"
	"testing"
	"time"
)

// The ticket a resuming client presents is reserved on the handshake
// goroutine and given back by a Close on any other, under c.mu. It was
// written without the lock — a race with a concurrent Close — and a Close
// that ran first had already given back what there was, so the reservation
// made after it was never returned: the ticket stayed spent for its window,
// and a legitimate client fell back to 1-RTT.
func TestAReservationAfterTheCloseIsGivenBack(t *testing.T) {
	g := newEarlyGate(10*time.Minute, 16)
	ticket := g.ticketExtra(time.Now())
	resume := func(c *Conn) *tls.SessionState {
		ss := &tls.SessionState{EarlyData: true, Extra: [][]byte{ticket}}
		if _, err := c.handleTLSEvent(tls.QUICEvent{Kind: tls.QUICResumeSession, SessionState: ss}); err != nil {
			t.Fatal(err)
		}
		return ss
	}
	newServer := func(name string) *Conn {
		c := newConn(&fuzzSink{addr: fakeFuzzAddr("early:" + name)}, fakeFuzzAddr("early:peer"),
			[]byte{1, 2, 3, 4}, []byte{5, 6, 7, 8}, false, DefaultConfig())
		c.early = g
		return c
	}

	// Concurrent: run under -race, the write and the read of earlyTicket
	// must not be a race.
	a := newServer("a")
	done := make(chan struct{})
	go func() { defer close(done); a.Close() }()
	resume(a)
	<-done

	// Closed first: the reservation is given back at once, the 0-RTT
	// refused, and the ticket is eligible for the next connection.
	b := newServer("b")
	b.Close()
	if ss := resume(b); ss.EarlyData {
		t.Error("a connection already closed accepted 0-RTT")
	}
	if ss := resume(newServer("c")); !ss.EarlyData {
		t.Fatal("the ticket stayed spent after a closed connection's reservation: a legitimate client is refused 0-RTT")
	}
}
