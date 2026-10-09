package quic

import (
	"crypto/tls"
	"errors"
	"time"
)

// Connection lifecycle: how it ends, and what the peer is told.

// Transport error codes (RFC 9000 §20.1), the ones this package sends.
const (
	transportNoError              = 0x0
	transportInternalError        = 0x1
	transportConnectionRefused    = 0x2
	transportFlowControl          = 0x3
	transportStreamLimit          = 0x4
	transportStreamState          = 0x5
	transportFinalSize            = 0x6
	transportFrameEncoding        = 0x7
	transportParameterError       = 0x8
	transportConnectionIDLimit    = 0x9
	transportInvalidToken         = 0xb
	transportApplicationError     = 0xc
	transportProtocolViolation    = 0xa
	transportCryptoBufferExceeded = 0xd
	transportAEADLimitReached     = 0xf
	// transportCryptoErrorBase is the start of the range RFC 9001 §4.8
	// reserves for TLS alerts: 0x0100 plus the alert's own code, which is
	// how ALPN failure (0x0178), a missing extension (0x016d) and the rest
	// travel without QUIC inventing names for them.
	transportCryptoErrorBase = 0x0100
)

// transportError carries the CONNECTION_CLOSE code a violation deserves
// alongside the error that describes it.
type transportError struct {
	code uint64
	err  error
}

func (e *transportError) Error() string { return e.err.Error() }
func (e *transportError) Unwrap() error { return e.err }

// closeCodeFor picks the CONNECTION_CLOSE code an error deserves. A
// transportError carries its own; a TLS alert anywhere in the chain maps to
// RFC 9001 §4.8's crypto range (0x0100 + alert), which is how the codes that
// RFC mandates — 0x0178 for ALPN failure (§8.1), 0x016d for missing
// transport parameters (§8.2), 0x010a for a KeyUpdate message (§6) — all
// reach the wire without this package naming each alert; everything else is
// INTERNAL_ERROR.
func closeCodeFor(err error) uint64 {
	if te, ok := err.(*transportError); ok { //nolint:errorlint // the code rides the outermost error by construction
		return te.code
	}
	if alert, ok := errors.AsType[tls.AlertError](err); ok {
		return transportCryptoErrorBase + uint64(uint8(alert))
	}
	return transportInternalError
}

// abort ends the connection over a protocol violation: record why, tell the
// peer with a CONNECTION_CLOSE, and close. The close carries the code, not
// the text — error strings are for this end's operator, and what they reveal
// about internals is nobody else's business (the GOAWAY on the HTTP/2 side
// made the same decision).
func (c *Conn) abort(err error) {
	code := closeCodeFor(err)
	c.fail(err)
	c.mu.Lock()
	c.sendCloseLocked(code, false)
	c.mu.Unlock()
	_ = c.Close()
}

// Close ends the connection, telling the peer with a CONNECTION_CLOSE frame
// (NO_ERROR) so it can drop its state now rather than wait out an idle
// period (§10.2). Close never blocks on the peer: the frame is sent once,
// best effort — if it is lost, the peer's idle timer is the fallback the
// protocol designed for.
//
// The error return always comes back nil, kept for [io.Closer] rather than
// for anything it could report: a failed write here is already "best
// effort" and swallowed upstream (sendCloseLocked), not a reason for the
// caller to think closing failed.
func (c *Conn) Close() error {
	c.closeOnce(transportNoError, false)
	return nil
}

// CloseWithError ends the connection with an application-level code, which
// is how HTTP/3 says H3_NO_ERROR (§10.2's application close). The reason
// phrase is deliberately not sent; codes travel, prose stays home.
//
// Like [Conn.Close], the error return always comes back nil.
func (c *Conn) CloseWithError(code uint64) error {
	c.closeOnce(code, true)
	return nil
}

// closeOnce is the body Close and CloseWithError share: send the
// CONNECTION_CLOSE (transport or application flavour), then release
// everyone waiting on the connection. Only the first call does anything.
func (c *Conn) closeOnce(code uint64, app bool) {
	c.once.Do(func() {
		c.mu.Lock()
		c.sendCloseLocked(code, app)
		c.mu.Unlock()
		close(c.closed)
		c.wakeOwnedReadLoop()
		c.mu.Lock()
		c.cond.Broadcast() // writers parked on credit learn it is over
		c.mu.Unlock()
	})
}

// wakeOwnedReadLoop jolts the exclusive-socket readLoop out of its blocking
// ReadFrom with an already-expired read deadline — without it the goroutine
// and its receive buffer would linger until the next idle wakeup, as long as
// the idle period away. Called after close(c.closed): the loop re-checks
// c.closed after arming each deadline, so whichever SetReadDeadline lands
// last, it still exits promptly. A listener-fed connection selects on
// c.closed directly and needs no jolt.
func (c *Conn) wakeOwnedReadLoop() {
	if c.incoming == nil {
		c.mu.Lock()
		pc := c.pc
		c.mu.Unlock()
		_ = pc.SetReadDeadline(time.Now())
	}
}

// sendCloseLocked writes the CONNECTION_CLOSE, once, unless the peer closed
// first — an endpoint in the draining state sends nothing at all (§10.2.2).
//
// Before the handshake is confirmed the close goes out at *every* level that
// can still seal, not only the newest (§10.2.3): the newest keys are the ones
// the peer is least certain to have — a server closing during the handshake
// may hold Handshake keys the client has not derived yet — and a close the
// peer cannot read is a client that eats its whole handshake timeout instead.
func (c *Conn) sendCloseLocked(code uint64, app bool) {
	if c.closeSent || c.draining {
		return
	}
	c.closeSent = true

	// The most recent level with a sealer is the one the peer most likely
	// reads; everything below it is sealable insurance pre-confirmation.
	newest := -1
	for s := spaceApplication; s >= spaceInitial; s-- {
		if !c.spaces[s].discarded && c.spaces[s].sealer != nil {
			newest = s
			break
		}
	}
	if newest < 0 {
		return
	}
	lowest := newest
	if !c.handshakeConfirmed {
		lowest = spaceInitial
	}
	now := time.Now()
	for space := newest; space >= lowest; space-- {
		sp := &c.spaces[space]
		if sp.discarded || sp.sealer == nil {
			continue
		}
		typ, spaceCode := uint64(frameConnectionClose), code
		if app {
			if space == spaceApplication {
				typ = frameConnectionClose | 1
			} else {
				// §10.2.3: an application close leaving in an Initial or
				// Handshake packet would leak application state under keys
				// anyone (Initial) or any observer of the handshake may hold;
				// it travels as the transport form with APPLICATION_ERROR.
				spaceCode = transportApplicationError
			}
		}
		frame := AppendVarint(nil, typ)
		frame = AppendVarint(frame, spaceCode)
		if typ == frameConnectionClose {
			frame = AppendVarint(frame, 0) // the frame type being answered; none
		}
		frame = AppendVarint(frame, 0) // no reason phrase, deliberately

		// closeSent is already set, so sendPacketLocked would refuse; write
		// the packet directly. Best effort: the peer's idle timer covers a
		// loss.
		_ = c.writePacketLocked(sp, space, frame, nil, sendOpts{ackOnly: true}, now)
	}
}

// idleExpired closes silently: an idle timeout is the one end of a
// connection that sends nothing, because the whole premise is that the peer
// is gone (§10.1). It is not an error — it is how the protocol expects a
// connection nobody is using to end — so Err stays nil.
func (c *Conn) idleExpired() {
	c.mu.Lock()
	c.closeSent = true // suppress the CONNECTION_CLOSE a Close would send
	c.mu.Unlock()
	_ = c.Close()
}
