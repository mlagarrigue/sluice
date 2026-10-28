package quic

import (
	"testing"
	"time"
)

// RFC 9846 §4.7.1: "it is possible to continue issuing new tickets which
// indefinitely extend the lifetime of the keying material originally
// derived from an initial non-PSK handshake", and limits on that total
// lifetime are RECOMMENDED. crypto/tls dates every TLS 1.3 ticket from its
// issue, and the server issued one on every connection, resumed or not: a
// client returning within seven days resumed forever from one certificate
// check. The ticket now carries the time of the chain's full handshake,
// and no ticket is issued once the chain is maxTicketChain old.
func TestATicketChainStopsAfterItsTotalLifetime(t *testing.T) {
	c := &Conn{}
	day0 := time.Now()
	var extra [][]byte // the ticket the client presents; none on day 0
	for _, step := range []struct {
		day    int
		issued bool
	}{
		{0, true}, {3, true}, {6, true}, {9, false},
	} {
		now := day0.Add(time.Duration(step.day) * 24 * time.Hour)
		c.ticketChainOrigin = ticketChainOrigin(extra) // what QUICResumeSession reads
		opts, ok := c.ticketOptions(now)
		if ok != step.issued {
			t.Fatalf("day %d: ticket issued %v, want %v", step.day, ok, step.issued)
		}
		if !ok {
			continue
		}
		got := ticketChainOrigin(opts.Extra)
		if !got.Equal(time.Unix(0, day0.UnixNano())) {
			t.Fatalf("day %d: the ticket names its chain's origin as %v, want day 0 (%v)", step.day, got, day0)
		}
		extra = opts.Extra
	}

	// A ticket without the entry — issued before it existed, or by another
	// build — opens a fresh chain rather than ending one.
	c.ticketChainOrigin = ticketChainOrigin([][]byte{[]byte("something else")})
	if !c.ticketChainOrigin.IsZero() {
		t.Fatalf("an unrelated Extra entry read as a chain origin %v", c.ticketChainOrigin)
	}
	opts, ok := c.ticketOptions(day0)
	if !ok || !ticketChainOrigin(opts.Extra).Equal(time.Unix(0, day0.UnixNano())) {
		t.Errorf("a ticket without a chain entry: issued %v, origin %v; want a new chain from now", ok, ticketChainOrigin(opts.Extra))
	}
}
