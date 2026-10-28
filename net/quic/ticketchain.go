package quic

import (
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"time"
)

// maxTicketChain bounds how long a chain of resumptions lives: no ticket is
// issued on a connection whose chain — the full handshake it resumes from,
// ticket after ticket — is this old. RFC 9846 §4.7.1 RECOMMENDS a limit on
// the total lifetime of keying material that tickets keep extending, with
// the peer's certificate, revocation and the age of its last
// CertificateVerify in mind; crypto/tls dates each TLS 1.3 ticket from its
// own issue, so without it a client returning within the week resumed
// forever from one certificate check. Seven days is the ticket's own
// longest life, and the last ticket of a chain still lives that long:
// keying material outlives its handshake by at most twice this.
const maxTicketChain = 7 * 24 * time.Hour

// ticketChainMagic opens the SessionState.Extra entry that carries a
// chain's origin, in nanoseconds since the epoch: the wall clock, because
// a ticket outlives the process that issued it.
var ticketChainMagic = []byte("sluice-chain\x01")

// ticketChainEntry is the Extra entry naming origin as the chain's full
// handshake.
func ticketChainEntry(origin time.Time) []byte {
	b := append(make([]byte, 0, len(ticketChainMagic)+8), ticketChainMagic...)
	return binary.BigEndian.AppendUint64(b, uint64(origin.UnixNano())) //nolint:gosec // G115: a time after the epoch
}

// ticketChainOrigin reads the chain's origin from a resumed ticket's Extra,
// or the zero time when it carries none — a ticket issued before the
// entry existed, which starts a new chain rather than ending one. The
// ticket is sealed by crypto/tls with the Config's ticket keys, so the
// entry is this server's own writing.
func ticketChainOrigin(extra [][]byte) time.Time {
	for _, e := range extra {
		if len(e) == len(ticketChainMagic)+8 && bytes.HasPrefix(e, ticketChainMagic) {
			return time.Unix(0, int64(binary.BigEndian.Uint64(e[len(ticketChainMagic):]))) //nolint:gosec // G115: written by ticketChainEntry from a positive time
		}
	}
	return time.Time{}
}

// ticketOptions is what the ticket issued at now carries — the chain's
// origin, now for a full handshake, and the 0-RTT admission entry when the
// listener has EarlyData — or false when the chain is maxTicketChain old
// and no ticket is issued.
func (c *Conn) ticketOptions(now time.Time) (tls.QUICSessionTicketOptions, bool) {
	origin := c.ticketChainOrigin
	if origin.IsZero() {
		origin = now
	}
	if now.Sub(origin) >= maxTicketChain {
		return tls.QUICSessionTicketOptions{}, false
	}
	opts := tls.QUICSessionTicketOptions{Extra: [][]byte{ticketChainEntry(origin)}}
	if c.early != nil {
		opts.EarlyData = true
		opts.Extra = append(opts.Extra, c.early.ticketExtra(now))
	}
	return opts, true
}
