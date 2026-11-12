package quic

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"net"
	"slices"
	"time"
)

// ErrEarlyDataRejected is what a stream opened in 0-RTT reports once the
// server refused 0-RTT ([DialEarly]): nothing written on it reached the
// application at the other end, and the stream is gone. The connection
// continues in 1-RTT; whoever opened the stream decides whether to send the
// same again on a new one (RFC 9001 §4.6.2, ADR 0019). It also opens the
// error of a connection that a HelloRetryRequest ended after its 0-RTT
// offer (see DialEarly).
var ErrEarlyDataRejected = errors.New("quic: the server refused 0-RTT; the stream's data was not delivered")

// helloRetryRequestRandom is the Random of a ServerHello that is a
// HelloRetryRequest: the SHA-256 of "HelloRetryRequest" (RFC 9846 §4.2.3).
var helloRetryRequestRandom = []byte{
	0xCF, 0x21, 0xAD, 0x74, 0xE5, 0x9A, 0x61, 0x11,
	0xBE, 0x1D, 0x8C, 0x02, 0x1E, 0x65, 0xB8, 0x91,
	0xC2, 0xA2, 0x11, 0x16, 0x7A, 0xBB, 0x8C, 0x5E,
	0x07, 0x9E, 0x09, 0xE2, 0xC8, 0xA8, 0x33, 0x9C,
}

// isHelloRetryRequest reports whether b, the start of the server's Initial
// CRYPTO stream, is a HelloRetryRequest: a server_hello (type 2) whose
// Random, after the four-byte message header and the two-byte
// legacy_version, is helloRetryRequestRandom. A first delivery shorter
// than that reads as no HelloRetryRequest; a server's first Initial
// carries the whole message in practice.
func isHelloRetryRequest(b []byte) bool {
	return len(b) >= 6+len(helloRetryRequestRandom) && b[0] == 2 &&
		bytes.Equal(b[6:6+len(helloRetryRequestRandom)], helloRetryRequestRandom)
}

// errEarlyHelloRetry is what a DialEarly connection ends with when the
// server answered its 0-RTT offer with a HelloRetryRequest. crypto/tls
// (Go 1.26 and 1.27) computes the PSK binders of its second ClientHello
// before it drops early_data from it, so the server cannot verify them and
// closes with decrypt_error: the connection is lost, not only its 0-RTT.
// Nothing outside crypto/tls can mend the binders, and DialEarly has
// returned by then, so the error says what happened and what to do — dial
// again with Dial, which offers no 0-RTT and resumes through a
// HelloRetryRequest unharmed. earlyTLSConfig keeps the retry from
// happening whenever the ticket names the group the server chose; what is
// left is a server that changed its group, or a ticket that names none.
var errEarlyHelloRetry = errors.New("the server sent a HelloRetryRequest, which crypto/tls cannot complete after a 0-RTT offer; dial again with Dial")

// rememberedMagic opens the SessionState.Extra entry a client keeps with a
// ticket: the server's transport parameters as they arrived on the
// connection that issued it (RFC 9000 §7.4.1).
var rememberedMagic = []byte("sluice-tp\x01")

// rememberedEntry is the Extra entry for the parameters raw.
func rememberedEntry(raw []byte) []byte {
	return append(append([]byte(nil), rememberedMagic...), raw...)
}

// rememberedParams reads back the server's parameters from a ticket's Extra
// and keeps the ones RFC 9000 §7.4.1 lets 0-RTT use: not ack_delay_exponent,
// max_ack_delay, the connection identifiers, preferred_address or the
// stateless reset token, which the client "MUST NOT use" from memory — left
// at zero here, so the defaults apply until the handshake brings new ones.
func rememberedParams(extra [][]byte) (TransportParameters, bool) {
	for _, e := range extra {
		raw, ok := bytes.CutPrefix(e, rememberedMagic)
		if !ok {
			continue
		}
		p, err := parseParameters(raw)
		if err != nil {
			return TransportParameters{}, false
		}
		return TransportParameters{
			MaxIdleTimeout:                 p.MaxIdleTimeout,
			MaxUDPPayloadSize:              p.MaxUDPPayloadSize,
			InitialMaxData:                 p.InitialMaxData,
			InitialMaxStreamDataUni:        p.InitialMaxStreamDataUni,
			InitialMaxStreamDataBidiLocal:  p.InitialMaxStreamDataBidiLocal,
			InitialMaxStreamDataBidiRemote: p.InitialMaxStreamDataBidiRemote,
			InitialMaxStreamsBidi:          p.InitialMaxStreamsBidi,
			InitialMaxStreamsUni:           p.InitialMaxStreamsUni,
			ActiveConnIDLimit:              p.ActiveConnIDLimit,
			DisableActiveMigration:         p.DisableActiveMigration,
		}, true
	}
	return TransportParameters{}, false
}

// groupMagic opens the SessionState.Extra entry naming the key exchange
// group the server chose on the connection that issued the ticket.
var groupMagic = []byte("sluice-group\x01")

// groupEntry is the Extra entry for group id.
func groupEntry(id tls.CurveID) []byte {
	return binary.BigEndian.AppendUint16(append([]byte(nil), groupMagic...), uint16(id))
}

// rememberedGroup reads back the group a ticket's Extra names.
func rememberedGroup(extra [][]byte) (tls.CurveID, bool) {
	for _, e := range extra {
		if raw, ok := bytes.CutPrefix(e, groupMagic); ok && len(raw) == 2 {
			return tls.CurveID(binary.BigEndian.Uint16(raw)), true
		}
	}
	return 0, false
}

// earlyTLSConfig is cfg as DialEarly offers it: when the ticket crypto/tls
// will resume carries 0-RTT and names the group the server chose last
// time, a copy whose ClientHello offers that group alone, so that its key
// share is the one the server wants. A server that answers 0-RTT with a
// HelloRetryRequest loses the connection (errEarlyHelloRetry), and a
// server's choice of group is the one thing that sends one back to a
// resuming client: crypto/tls shares a key for its first group only and,
// since Go 1.24, takes the order of CurvePreferences from its own list, so
// a server that keeps P-256 alone retried every resumption by a client
// with the default groups. Offering the group the server took is no
// downgrade — it is the one that connection used — and a group cfg no
// longer lists is not offered. The ticket is read from the cache under
// the key crypto/tls files it under, cfg.ServerName, and put back for the
// handshake to take. Anything else leaves cfg as it is.
func earlyTLSConfig(cfg *tls.Config) *tls.Config {
	if cfg == nil || cfg.ClientSessionCache == nil {
		return cfg // newClientConn refuses a nil cfg
	}
	cache := cfg.ClientSessionCache
	sessionCacheMu.Lock() // the look and the putting back are one step for other Dials
	cs, ok := cache.Get(cfg.ServerName)
	if ok && cs != nil {
		cache.Put(cfg.ServerName, cs)
	}
	sessionCacheMu.Unlock()
	if !ok || cs == nil {
		return cfg
	}
	_, state, err := cs.ResumptionState()
	if err != nil || state == nil || !state.EarlyData {
		return cfg
	}
	group, ok := rememberedGroup(state.Extra)
	if !ok || (len(cfg.CurvePreferences) > 0 && !slices.Contains(cfg.CurvePreferences, group)) {
		return cfg
	}
	own := cfg.Clone()
	own.CurvePreferences = []tls.CurveID{group}
	return own
}

// DialEarly is [Dial] for a client that wants to send before the handshake
// ends. When cfg's session cache holds a ticket the server marked for early
// data, DialEarly returns as soon as the ClientHello has left with it, and
// what the caller writes on streams it opens then travels in 0-RTT packets
// (RFC 9001 §4.6), under the transport parameters remembered with the
// ticket — a round trip earlier than after Dial. Otherwise it returns once
// the handshake completes, as Dial does. [Conn.HandshakeComplete] tells the
// two apart.
//
// Early data can be replayed by anyone who copies the flight that carried
// it, and the server cannot tell until the handshake ends: send in 0-RTT
// only what a duplicate does not hurt — RFC 9846 §8 leaves that judgement
// to the client.
//
// Early data has no forward secrecy either: it is sealed under keys derived
// from the ticket's secret alone, and whether the server can still decrypt
// it after a leak of that secret depends on how the server keeps tickets,
// which the protocol does not tell the client. Absent such knowledge, RFC
// 9846 §2.3 has the client assume 0-RTT data is not forward secret: send
// nothing in it that must stay confidential should the ticket keys leak.
//
// The server may refuse 0-RTT. Every stream opened before the refusal then
// fails with [ErrEarlyDataRejected] — on Read, on Write, and on the next
// call after either — and the connection continues in 1-RTT, where new
// streams start over from the first identifier (RFC 9001 §4.6.2: the
// client resets the state of every stream). Sending the same again is the
// caller's decision, because what the transport cannot know is what else in
// the application depended on those streams.
//
// One refusal the connection does not survive: a server that answers the
// 0-RTT offer with a HelloRetryRequest — one that wants a key share the
// ClientHello did not carry. crypto/tls (Go 1.26 and 1.27) then sends a
// second ClientHello whose PSK binders the server cannot verify, and the
// server closes with decrypt_error. [Conn.Err] wraps [ErrEarlyDataRejected]
// and names the HelloRetryRequest, over the server's [ConnectionError];
// dial again with Dial, which offers no 0-RTT and resumes through a
// HelloRetryRequest unharmed. To keep that from happening, the ticket
// records the key exchange group the server chose, and DialEarly offers
// that group alone when it resumes with 0-RTT — on a copy of cfg, whose
// CurvePreferences it narrows only when they list the group or are empty.
// The retry is left to a server that changed its group since, or to a
// ticket stored without one.
//
// ctx bounds DialEarly's own wait, as it bounds Dial's handshake. Once
// DialEarly has returned early, the rest of the handshake runs under
// [DefaultHandshakeTimeout], or under ctx's deadline when that is later; a
// handshake that fails then closes the connection, which [Conn.Done]
// reports. cfg follows Dial's rules.
func DialEarly(ctx context.Context, pc net.PacketConn, addr net.Addr, cfg *tls.Config, params Config) (*Conn, error) {
	c, err := newClientConn(ctx, pc, addr, earlyTLSConfig(cfg), params)
	if err != nil {
		return nil, err
	}
	c.wantsEarly = true
	done := make(chan error, 1)
	hsCtx, cancel := earlyHandshakeContext(ctx, time.Now())
	go func() {
		defer cancel()
		done <- c.handshake(hsCtx)
	}()
	select {
	case err := <-done:
		if err != nil {
			return nil, err
		}
		return c, nil
	case <-c.earlyAccepted:
		return c, nil
	case <-ctx.Done():
		_ = c.Close()
		<-done
		return nil, abandonedErr(ctx)
	}
}

// earlyHandshakeContext is the context DialEarly's handshake runs under: not
// canceled with ctx, since the handshake outlives an early return, but held
// to ctx's deadline when that is later than DefaultHandshakeTimeout from now.
// A shorter one is DialEarly's own wait's to keep (its select), and once
// DialEarly has returned the default applies, as its doc says; without the
// longer one a slow path got the default where Dial, under the same ctx,
// waits for ctx.
func earlyHandshakeContext(ctx context.Context, now time.Time) (context.Context, context.CancelFunc) {
	hs := context.WithoutCancel(ctx)
	if d, ok := ctx.Deadline(); ok && d.After(now.Add(DefaultHandshakeTimeout)) {
		return context.WithDeadline(hs, d)
	}
	return hs, func() {}
}

// rejectEarlyLocked is a client's answer to the server's refusal of 0-RTT,
// which crypto/tls reports before the 1-RTT keys, so nothing has left in
// 1-RTT yet. RFC 9001 §4.6.2: "all connection characteristics that the
// client assumed might be incorrect" and the client "MUST reset the state
// of all streams". The packets sent in 0-RTT are forgotten — none is
// retransmitted, and none counts in flight any longer — every stream fails
// with ErrEarlyDataRejected, and the stream counters and the connection's
// flow-control account start over from the server's own parameters —
// crypto/tls reports them before the refusal, from the same
// EncryptedExtensions — or from nothing when a HelloRetryRequest refused
// 0-RTT before any arrived. Two things built before the refusal outlive
// it, and both are cut where they surface: a 0-RTT packet still gathered
// is dropped when it is sealed (sealLocked), and a writer parked on the
// congestion window does not refund its charge to the new account
// (Stream.writeLocked).
func (c *Conn) rejectEarlyLocked() {
	c.earlySealer = nil
	c.earlyRejected = true
	sp := &c.spaces[spaceApplication]
	_, inFlight := sp.sent.takeAll()
	sp.sent.refusedBelow = sp.nextPN
	sp.retrans = nil
	c.cc.onLifted(inFlight)
	for id, s := range c.streams {
		s.rejected.Store(true)
		s.signal()
		delete(c.streams, id)
	}
	c.openedBidi, c.openedUni = 0, 0
	c.connSend = sendQuota{}
	c.peerMaxStreamsBidi, c.peerMaxStreamsUni = 0, 0
	// The server's limits apply from here, below or not at all: nothing
	// waits for the 1-RTT key to apply them again.
	c.limitsHeld = false
	if c.paramsRemembered {
		c.peerParams, c.hasPeerParams, c.paramsRemembered = TransportParameters{}, false, false
	} else if c.hasPeerParams {
		c.connSend.grant(c.peerParams.InitialMaxData)
		c.peerMaxStreamsBidi = c.peerParams.InitialMaxStreamsBidi
		c.peerMaxStreamsUni = c.peerParams.InitialMaxStreamsUni
	}
	c.grants = c.grants[:0]
	c.wakeAllLocked()
}

// applyRememberedLocked takes a ticket's remembered parameters as the
// server's until the handshake brings its own (RFC 9000 §7.4.1): they are
// what lets a stream open, and what it may send, in 0-RTT.
func (c *Conn) applyRememberedLocked(p TransportParameters) {
	c.peerParams, c.hasPeerParams, c.paramsRemembered = p, true, true
	c.connSend.grant(p.InitialMaxData)
	c.peerMaxStreamsBidi = p.InitialMaxStreamsBidi
	c.peerMaxStreamsUni = p.InitialMaxStreamsUni
}

// storeSession keeps a ticket from the server, with what a later DialEarly
// needs to send 0-RTT under it: the parameters this connection's handshake
// brought (rememberedParams), and the group the server chose, which
// DialEarly offers alone so that no HelloRetryRequest meets its 0-RTT
// (earlyTLSConfig). Session events being on, crypto/tls stores nothing
// itself. A ticket that fails to store is one resumption less, not a
// reason to end this connection.
func (c *Conn) storeSession(s *tls.SessionState) {
	if raw := c.peerParamsRaw; raw != nil {
		s.Extra = append(s.Extra, rememberedEntry(raw))
	}
	if id := c.tls.ConnectionState().CurveID; id != 0 {
		s.Extra = append(s.Extra, groupEntry(id))
	}
	_ = c.tls.StoreSession(s)
}
