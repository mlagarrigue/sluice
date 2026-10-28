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
	// spent is the ticket identifiers already admitted, each kept until
	// the ticket itself is too old (its time of issue plus the window, on
	// the wall clock) and refusing rather than forgetting when full.
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

// eligible reports whether a resumption whose ticket carries extra may have
// its 0-RTT accepted at now — a ticket of this instance, young enough, not
// spent, and room in the register — and reserves it: the ticket is spent
// here, when crypto/tls asks (QUICResumeSession), so that its answer is the
// right one. A ticket another connection reserved first (a replay racing
// the original) and a full register (spentTokens.failClosed) both come back
// as not eligible, and crypto/tls answers without early_data: every refusal
// is a TLS refusal, after which no 0-RTT packet is read (RFC 9001 §4.6.2).
// Until 2026-10-27 the ticket was spent later, in installEarly, once the
// 0-RTT was accepted; a full register or a lost race was then found with
// early_data already announced, and the connection kept no key: its 0-RTT
// packets were dropped, which the client saw as loss and resent in 1-RTT
// after a probe timeout instead of the refusal ADR 0019 promises.
//
// crypto/tls asks before it checks the PSK binder and before it knows
// whether the client offered 0-RTT at all, so a reservation may go unused:
// release gives it back when the handshake ends without the early keys, or
// the connection does. The identifier returned is what release takes.
func (g *earlyGate) eligible(extra [][]byte, now time.Time) ([]byte, bool) {
	for _, e := range extra {
		if len(e) != earlyTicketLen || !bytes.HasPrefix(e, earlyTicketMagic) {
			continue
		}
		e = e[len(earlyTicketMagic):]
		if !bytes.Equal(e[:16], g.instance[:]) {
			return nil, false
		}
		issued := time.Unix(0, int64(binary.BigEndian.Uint64(e[32:]))) //nolint:gosec // G115: written by ticketExtra from a positive time
		// One clock for the age and for the spent entry: the wall clock,
		// the ticket's own, since its time of issue must survive a
		// restart. An entry timed on the monotonic reading time.Now
		// carries would expire a window after the admission, and a wall
		// clock stepped back meanwhile would leave the ticket young once
		// its entry was gone: admitted twice, against RFC 9846 §8. On the
		// ticket's clock the entry lives exactly as long as the ticket is
		// young.
		wall := now.Round(0)
		if age := wall.Sub(issued); age < 0 || age >= g.window {
			return nil, false
		}
		id := e[16:32]
		g.mu.Lock()
		ok := g.spent.consumeUntil(id, wall, issued.Add(g.window))
		g.mu.Unlock()
		return id, ok
	}
	return nil, false
}

// release gives back a reservation eligible made that the connection did
// not use — no 0-RTT offered, or a handshake that failed before the early
// keys — so that the ticket's 0-RTT, and the register's slot, are intact.
// RFC 9846 §8's "at most once" holds: a 0-RTT handshake with this ticket
// was never accepted here. Nothing happens for a ticket whose 0-RTT was
// accepted (installEarly), or for one released already.
func (g *earlyGate) release(id []byte) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.spent.forget(id)
}
