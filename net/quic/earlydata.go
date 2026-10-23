package quic

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"sync"
	"time"
)

// defaultEarlyDataWindow is what a zero [ListenerConfig.EarlyDataWindow]
// means, and defaultMaxEarlyDataTickets what a zero
// [ListenerConfig.MaxEarlyDataTickets] means: as many entries as the record
// of spent Retry tokens holds by default, and full at 27 0-RTT connections
// a second sustained over the default window.
const (
	defaultEarlyDataWindow     = 10 * time.Minute
	defaultMaxEarlyDataTickets = 1 << 14
)

// earlyTicketMagic opens the one SessionState.Extra entry this package
// writes into a ticket that allows 0-RTT, so that it is told apart from
// anything else a ticket might carry.
var earlyTicketMagic = []byte("sluice-0rtt\x01")

// earlyTicketLen is an entry's length: the magic, the listener's instance
// identifier, the ticket's own identifier, and its time of issue in
// nanoseconds since the epoch.
var earlyTicketLen = len(earlyTicketMagic) + 16 + 16 + 8

// earlyGate is a [Listener]'s 0-RTT admission (ADR 0019): the tickets it
// issues name the listener instance, carry an identifier of their own and
// their time of issue, and a resumption has its 0-RTT accepted only for a
// ticket of this instance, younger than the window, never admitted before.
// The ticket is encrypted and authenticated by crypto/tls with the
// Config's ticket keys, so none of these fields can be forged by a client.
type earlyGate struct {
	instance [16]byte
	window   time.Duration

	mu sync.Mutex
	// spent is the ticket identifiers already admitted, each kept for a
	// window from its admission — at least as long as the ticket itself
	// stays young enough — and refusing rather than forgetting when full.
	spent spentTokens
}

func newEarlyGate(window time.Duration, maxTickets int) *earlyGate {
	g := &earlyGate{window: window, spent: spentTokens{ttl: window, max: maxTickets, failClosed: true}}
	_, _ = rand.Read(g.instance[:]) // crypto/rand.Read never fails (Go 1.24+)
	return g
}

// ticketExtra is the Extra entry of a ticket issued at now.
func (g *earlyGate) ticketExtra(now time.Time) []byte {
	b := make([]byte, 0, earlyTicketLen)
	b = append(b, earlyTicketMagic...)
	b = append(b, g.instance[:]...)
	var id [16]byte
	_, _ = rand.Read(id[:])
	b = append(b, id[:]...)
	return binary.BigEndian.AppendUint64(b, uint64(now.UnixNano())) //nolint:gosec // G115: a time after the epoch
}

// admit reports whether a resumption whose ticket carries extra may have its
// 0-RTT accepted at now, and records the ticket as spent when it may: RFC
// 8446 §8's "at most once" on the instance that issued it.
func (g *earlyGate) admit(extra [][]byte, now time.Time) bool {
	for _, e := range extra {
		if len(e) != earlyTicketLen || !bytes.HasPrefix(e, earlyTicketMagic) {
			continue
		}
		e = e[len(earlyTicketMagic):]
		if !bytes.Equal(e[:16], g.instance[:]) {
			return false
		}
		issued := time.Unix(0, int64(binary.BigEndian.Uint64(e[32:]))) //nolint:gosec // G115: written by ticketExtra from a positive time
		if age := now.Sub(issued); age < 0 || age >= g.window {
			return false
		}
		g.mu.Lock()
		ok := g.spent.consume(e[16:32], now)
		g.mu.Unlock()
		return ok
	}
	return false
}
