package quic

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"net"
)

// ErrEarlyDataRejected is what a stream opened in 0-RTT reports once the
// server refused 0-RTT ([DialEarly]): nothing written on it reached the
// application at the other end, and the stream is gone. The connection
// continues in 1-RTT; whoever opened the stream decides whether to send the
// same again on a new one (RFC 9001 §4.6.2, ADR 0019).
var ErrEarlyDataRejected = errors.New("quic: the server refused 0-RTT; the stream's data was not delivered")

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
// The server may refuse 0-RTT. Every stream opened before the refusal then
// fails with [ErrEarlyDataRejected] — on Read, on Write, and on the next
// call after either — and the connection continues in 1-RTT, where new
// streams start over from the first identifier (RFC 9001 §4.6.2: the
// client resets the state of every stream). Sending the same again is the
// caller's decision, because what the transport cannot know is what else in
// the application depended on those streams.
//
// ctx bounds DialEarly's own wait, as it bounds Dial's handshake. Once
// DialEarly has returned early, the rest of the handshake runs under
// [DefaultHandshakeTimeout]; a handshake that fails then closes the
// connection, which [Conn.Done] reports. cfg follows Dial's rules.
func DialEarly(ctx context.Context, pc net.PacketConn, addr net.Addr, cfg *tls.Config, params TransportParameters) (*Conn, error) {
	c, err := newClientConn(ctx, pc, addr, cfg, params)
	if err != nil {
		return nil, err
	}
	c.wantsEarly = true
	done := make(chan error, 1)
	go func() { done <- c.handshake(context.WithoutCancel(ctx)) }()
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
// 0-RTT before any arrived.
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
	if c.paramsRemembered {
		c.peerParams, c.hasPeerParams, c.paramsRemembered = TransportParameters{}, false, false
	} else if c.hasPeerParams {
		c.connSend.grant(c.peerParams.InitialMaxData)
		c.peerMaxStreamsBidi = c.peerParams.InitialMaxStreamsBidi
		c.peerMaxStreamsUni = c.peerParams.InitialMaxStreamsUni
	}
	c.grants = c.grants[:0]
	c.cond.Broadcast()
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
