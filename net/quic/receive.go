package quic

import (
	"bytes"
	"crypto/subtle"
	"errors"
	"fmt"
	"net"
	"os"
	"time"
)

// The receive path. The rule that shapes all of it: unauthenticated bytes
// never end the connection. Anyone can address a datagram to this port, so a
// packet that does not parse, does not authenticate, or comes from the wrong
// peer is counted and dropped (RFC 9000 §5.2, §12.2) — one stray or hostile
// datagram must cost nothing but the cycles it took to refuse. Only what an
// authenticated packet proves the peer actually did can be fatal.

// minRecvBuf is the read buffer floor, comfortably over the 1200-byte
// protocol minimum and sized for a typical Ethernet/loopback MTU.
const minRecvBuf = 2048

// recvBufSize is how large a datagram this endpoint promised the peer it can
// receive (TransportParameters.MaxUDPPayloadSize, advertised in the
// handshake), never smaller than minRecvBuf. A caller configuring a payload
// size above the floor must actually be able to read one that big: without
// this, net.PacketConn.ReadFrom silently truncates an oversized datagram, the
// truncated bytes fail parsing downstream, and the packet is dropped like any
// other malformed one — a config promise the transport quietly could not
// keep.
func (c *Conn) recvBufSize() int {
	if n := int(c.params.MaxUDPPayloadSize); n > minRecvBuf { //nolint:gosec // G115: the endpoint's own configuration, not peer input
		return n
	}
	return minRecvBuf
}

// readLoop serves the connection once the handshake is done.
func (c *Conn) readLoop() {
	if c.incoming != nil {
		c.readLoopFed()
		return
	}
	buf := make([]byte, c.recvBufSize())
	readFails := 0 // consecutive transient ReadFrom errors, for the backoff
	for {
		// The socket is read under mu because [Conn.Rebind] replaces it:
		// one iteration reads one socket, and learns at the top of the
		// next that it has moved.
		c.mu.Lock()
		deadline := c.idleDeadline
		pc := c.pc
		c.mu.Unlock()
		if deadline.IsZero() {
			deadline = time.Now().Add(30 * time.Second)
		}
		if err := pc.SetReadDeadline(deadline); err != nil {
			if c.rebound(pc) {
				continue // the error belongs to a socket this end has left
			}
			c.fail(err)
			_ = c.Close()
			return
		}
		// Checked after arming the deadline, not before: Close wakes this
		// loop by expiring the read deadline (wakeOwnedReadLoop), and a check
		// on the other side of SetReadDeadline could overwrite that expired
		// deadline and park in ReadFrom until the idle wakeup anyway. Rebind
		// jolts the loop the same way, so the same ordering covers it.
		select {
		case <-c.closed:
			return
		default:
		}
		if c.rebound(pc) {
			continue
		}
		n, addr, err := pc.ReadFrom(buf)
		if err != nil {
			if errors.Is(err, os.ErrDeadlineExceeded) || c.rebound(pc) {
				// The idle deadline is the timer loop's to act on; the read
				// deadline only exists so this loop wakes up periodically.
				// And a socket Rebind just replaced may answer with its
				// deadline or, if the caller already closed it, ErrClosed:
				// either way it is not this connection's error any more.
				continue
			}
			if errors.Is(err, errClosed) || errors.Is(err, net.ErrClosed) {
				_ = c.Close()
				return
			}
			// Anything else is the socket's, not the connection's: on
			// Windows an ICMP port-unreachable surfaces here as
			// WSAECONNRESET, and an ICMP is unauthenticated — anyone can
			// forge one, and one must not end an established connection
			// (the Listener reads past the same errors). Counted, and read
			// past; a socket that keeps failing is backed off rather than
			// spun on, and the idle timeout ends the connection if nothing
			// gets through.
			c.noteDiscard()
			readFails++
			if readFails > 1 && !c.readBackoff(readFails) {
				return
			}
			continue
		}
		readFails = 0
		if err := c.processDatagram(rawDatagram{data: buf[:n], from: addr}); err != nil {
			return
		}
	}
}

// readBackoff pauses a read loop whose socket failed fails times in a row
// — 1 ms doubling to a 1 s ceiling — and reports false if the connection
// closed meanwhile.
func (c *Conn) readBackoff(fails int) bool { return readBackoff(fails, c.closed) }

// readBackoff is the pause itself, shared with the [Listener]'s read loop:
// false if stop closed meanwhile.
func readBackoff(fails int, stop <-chan struct{}) bool {
	t := time.NewTimer(min(time.Millisecond<<min(fails-2, 10), time.Second))
	defer t.Stop()
	select {
	case <-stop:
		return false
	case <-t.C:
		return true
	}
}

// processDatagram is receive followed by pump — act on what arrived, then
// send what TLS answers it with — under datagram gathering, so the ACK the
// datagram earns and the CRYPTO flight it provokes leave coalesced (§12.2)
// instead of as two 1200-byte datagrams. A non-nil return means the
// connection is already being torn down: an authenticated violation
// (abort already sent the CONNECTION_CLOSE) or a TLS failure.
func (c *Conn) processDatagram(d rawDatagram) error {
	c.mu.Lock()
	c.rxVia = d.via
	c.gatherLocked(true)
	c.mu.Unlock()
	err := c.receive(d.data, d.from)
	var perr error
	if err == nil {
		_, perr = c.pump()
	}
	c.mu.Lock()
	c.gatherLocked(false)
	c.mu.Unlock()
	if err != nil {
		return err
	}
	if perr != nil {
		c.abort(perr)
		return perr
	}
	return nil
}

// rebound reports whether pc is no longer the connection's socket — a
// [Conn.Rebind] happened since the read loop picked it up.
func (c *Conn) rebound(pc net.PacketConn) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pc != pc
}

// readLoopFed is readLoop for a [Listener]-managed connection: the listener
// owns the shared socket and pushes this connection's datagrams onto
// c.incoming instead, since every connection reading the same net.PacketConn
// directly would race the others for its bytes. Everything past "where did
// these bytes come from" — receive, pump, the idle deadline's wake-up
// cadence — is identical to the socket-owning readLoop above.
func (c *Conn) readLoopFed() {
	// One timer for the loop's life, re-armed per round: a fresh one per
	// datagram was three allocations on the receive path. Go ≥1.23 timers
	// need no drain before Reset.
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		select {
		case <-c.closed:
			return
		default:
		}
		c.mu.Lock()
		deadline := c.idleDeadline
		c.mu.Unlock()
		if deadline.IsZero() {
			deadline = time.Now().Add(30 * time.Second)
		}
		wait := max(time.Until(deadline), 0)
		timer.Reset(wait)
		select {
		case <-c.closed:
			return
		case d, ok := <-c.incoming:
			if !ok {
				_ = c.Close()
				return
			}
			if err := c.processDatagram(d); err != nil {
				return
			}
		case <-timer.C:
			// The idle deadline is the timer loop's to act on; this only
			// exists so the loop wakes up periodically to recheck it.
		}
	}
}

// receive parses every packet in a datagram, opens what it has keys for, and
// acts on the frames. from is the datagram's sender.
//
// The return is nil for everything an unauthenticated sender could have
// caused. A non-nil return means an authenticated protocol violation, and
// the connection is already being torn down.
func (c *Conn) receive(datagram []byte, from net.Addr) error {
	return c.receiveAt(datagram, from, time.Now())
}

// receiveAt is receive with the arrival time explicit, so a packet replayed
// from the deferred queue keeps the moment it actually reached this endpoint:
// the ACK delay reported back and the round-trip samples taken from its ACKs
// must not include time it spent parked waiting for keys (RFC 9000 §13.2.5,
// RFC 9002 §5.3).
func (c *Conn) receiveAt(datagram []byte, from net.Addr, now time.Time) error {
	c.mu.Lock()
	fromPeer := from == nil || sameAddr(from, c.peer)
	if !fromPeer && c.isClient && c.pathProbe.pending && c.pathProbe.target != nil && sameAddr(from, c.pathProbe.target) {
		// The one other address a client listens to: the server's
		// preferred address while this end is probing it (§9.6.2). What
		// arrives from it is authenticated like anything else; the
		// PATH_RESPONSE it should carry is what completes the move.
		fromPeer = true
	}
	if !fromPeer {
		// A new source address is only ever meaningful as the opening of a
		// server-side migration (RFC 9000 §9), and migration is 1-RTT
		// business on a confirmed handshake: a client never follows anyone
		// (servers do not migrate, and preferred_address is refused), a
		// pre-confirmation change is a spoof signal, and a long-header
		// packet from elsewhere is a stray. All of those are counted and
		// dropped, as any unauthenticated bytes are.
		if c.isClient || !c.handshakeConfirmed || len(datagram) == 0 || datagram[0]&0x80 != 0 {
			c.discarded++
			c.mu.Unlock()
			return nil
		}
	}
	if c.amplActive && fromPeer {
		// RFC 9000 §8.1 counts raw bytes received from the peer's address,
		// authenticated or not — a datagram that fails to parse still
		// proves nothing about whether *this* end was tricked into
		// answering it, which is the only thing the limit defends against.
		// Bytes from any other address count only if their packet migrates
		// the path, where they seed the new path's own accounting.
		c.amplRecv += int64(len(datagram))
	}
	c.rxFrom = from
	c.rxDatagramLen = len(datagram)
	c.rxLocalCIDSeq = 0
	c.rxHighest = false
	c.mu.Unlock()
	for len(datagram) > 0 {
		if datagram[0] == 0 {
			// Datagram padding: PADDING frames fill the *packet*, but a peer
			// may also pad the datagram itself with zeros, and zeros never
			// begin a QUIC packet.
			if !allZero(datagram) {
				c.noteDiscard()
			}
			return nil
		}

		var (
			space    int
			pnOffset int
			peerSCID []byte
			raw      []byte
			rest     []byte
		)
		if datagram[0]&0x80 != 0 {
			if len(datagram) >= 5 && datagram[1]|datagram[2]|datagram[3]|datagram[4] == 0 {
				c.handleVersionNegotiation(datagram)
				return nil
			}
			h, after, err := parseLongHeader(datagram)
			if err != nil {
				c.noteDiscard()
				return nil //nolint:nilerr // RFC 9000 §12.2: an unparseable packet is dropped, not fatal
			}
			switch h.Type {
			case packetInitial:
				// §17.2.2: only clients send tokens; a server's Initial must
				// carry a zero-length token field, and one that does not is
				// discarded (the MUST offers discard or PROTOCOL_VIOLATION —
				// discarding keeps unauthenticated bytes unable to end the
				// connection, same as every other refusal here).
				if c.isClient && len(h.Token) > 0 {
					c.noteDiscard()
					datagram = after
					continue
				}
				space = spaceInitial
			case packetHandshake:
				space = spaceHandshake
			case packetRetry:
				c.handleRetry(h)
				return nil
			default:
				// 0-RTT is not served; its declared length says where the
				// next packet starts.
				datagram = after
				continue
			}
			if !c.expectedDCID(h.DCID, space) {
				c.noteDiscard()
				datagram = after
				continue
			}
			peerSCID, raw, pnOffset, rest = h.SCID, h.Raw, h.PNOffset, after
		} else {
			// Every identifier this end issues is listenerCIDLen bytes —
			// the invariant that lets the length be known before knowing
			// which identifier it is.
			h, err := parseShortHeader(datagram, len(c.scid))
			if err != nil {
				c.noteDiscard()
				return nil //nolint:nilerr // RFC 9000 §12.2: an unparseable packet is dropped, not fatal
			}
			seq, ok := c.localCIDSeqFor(h.DCID)
			if !ok {
				// A short datagram naming no identifier of this end's is
				// either noise or the peer's stateless reset (§10.3.1): a
				// reset is built to look exactly like this, found only by
				// its trailing token.
				if c.isStatelessReset(datagram) {
					return c.handleStatelessReset()
				}
				c.noteDiscard()
				return nil
			}
			c.mu.Lock()
			c.rxLocalCIDSeq = seq
			c.mu.Unlock()
			space, raw, pnOffset, rest = spaceApplication, h.Raw, h.PNOffset, nil
		}

		c.mu.Lock()
		sp := &c.spaces[space]
		if space == spaceApplication && !c.ku.dropPrevAt.IsZero() && !now.Before(c.ku.dropPrevAt) {
			// RFC 9001 §6.5: the previous phase's read keys outlived their
			// three probe timeouts; a straggler under them is lost now.
			sp.opener = sp.opener.withoutPrevious()
			c.ku.dropPrevAt = time.Time{}
		}
		opener, largest, discarded := sp.opener, sp.largest, sp.discarded
		c.mu.Unlock()
		if discarded {
			datagram = rest
			continue
		}
		if opener == nil {
			// The keys for this level have not arrived yet. Held rather than
			// dropped: a peer that finishes its handshake and writes at once
			// hits this every time rather than rarely (§5.7).
			c.mu.Lock()
			if len(c.deferred[space]) < 16 {
				c.deferred[space] = append(c.deferred[space], deferredPacket{
					raw: append([]byte(nil), raw...),
					at:  now,
				})
			}
			c.mu.Unlock()
			datagram = rest
			continue
		}
		payload, pn, used, err := opener.openPhased(raw, pnOffset, largest)
		if err != nil {
			// A datagram that names this connection but does not
			// authenticate can still be the peer's stateless reset: a
			// resetting peer may reuse the identifier it was sent (§10.3).
			if space == spaceApplication && c.isStatelessReset(raw) {
				return c.handleStatelessReset()
			}
			// Failed authentication: anyone can send these bytes.
			c.mu.Lock()
			c.discarded++
			opener.failed++
			overIntegrity := opener.failed >= opener.k.intLimit
			c.mu.Unlock()
			if overIntegrity {
				// RFC 9001 §6.6: past 2^52 failed opens the key itself is
				// worn out — the one case where unauthenticated bytes end
				// the connection, because tolerating more forgeries would
				// erode the AEAD's integrity bound.
				err := &transportError{
					code: transportAEADLimitReached,
					err:  fmt.Errorf("%w: this key refused its 2^52 forged packets", ErrQUIC),
				}
				c.abort(err)
				return err
			}
			datagram = rest
			continue
		}

		if peerSCID != nil {
			c.adoptPeerCID(peerSCID)
		}
		if used == usedNext {
			// The peer moved to the next key phase (RFC 9001 §6.2): the
			// read keys follow, and the send keys with them unless this end
			// is the one that moved first.
			c.mu.Lock()
			err := c.onKeyPhaseOpenedLocked(opener, pn, now)
			c.mu.Unlock()
			if err != nil {
				c.abort(err)
				return err
			}
		}
		if err := c.processPacket(space, pn, payload, now); err != nil {
			return err
		}
		datagram = rest
	}
	return nil
}

// processPacket acts on one authenticated packet. now is when the packet
// arrived — for a packet replayed off the deferred queue, that is when it was
// first received, not when the keys finally let it be read.
func (c *Conn) processPacket(space int, pn uint64, payload []byte, now time.Time) error {
	c.mu.Lock()
	sp := &c.spaces[space]
	if !sp.ack.record(pn, now) {
		// A duplicate: already processed, or forgotten. Processing it again
		// would double-deliver.
		c.mu.Unlock()
		return nil
	}
	if pn > sp.largest {
		sp.largest = pn
	}
	// Whether this packet is the newest yet received gates the §9.3
	// migration decision: only the highest-numbered non-probing packet may
	// move the path, so a straggler from an abandoned address cannot drag
	// it backwards.
	c.rxHighest = pn == sp.largest
	if c.idleTimeout != 0 {
		// Never backwards: a deferred packet's arrival time can predate a
		// deadline a later packet already set.
		if d := now.Add(c.idleTimeout); d.After(c.idleDeadline) {
			c.idleDeadline = d
		}
	}
	// A packet was received and processed, so the next ack-eliciting send may
	// restart the idle timer again (§10.1).
	c.elicitingSinceRecv = false
	// A server that authenticates a Handshake packet is done with Initial
	// keys (RFC 9001 §4.9.1).
	if space == spaceHandshake && !c.isClient {
		c.discardSpaceLocked(spaceInitial)
		// RFC 9000 §8.1: decrypting a Handshake-level packet from the peer
		// is itself address validation — only the real recipient of this
		// end's Initial response could have the keys to produce one. The
		// anti-amplification limit stops mattering from here.
		c.amplActive = false
	}
	c.mu.Unlock()

	eliciting, err := c.frames(space, payload, now)
	if err != nil {
		c.abort(err)
		return err
	}

	c.mu.Lock()
	if eliciting {
		c.spaces[space].ack.onAckEliciting(space, now)
	}
	c.flushAcksLocked(now)
	c.retransmitLocked()
	// Control frames queued while processing — grants, mostly — usually ride
	// the next data packet; when there is none coming they must not wait for
	// one, because the peer may be blocked on exactly this credit.
	c.flushControlLocked()
	c.kickTimer()
	c.mu.Unlock()
	return nil
}

// expectedDCID says whether an incoming long-header packet is addressed to
// this connection. A server keeps accepting the client's original choice on
// Initial packets — the client uses it until the server's first answer
// arrives (§7.2) — and, when its [Listener] redirected the client through a
// Retry, the identifier that Retry handed out: a post-Retry client
// addresses its Initials there, not to the original choice and not yet to
// this connection's own scid.
func (c *Conn) expectedDCID(dcid []byte, space int) bool {
	if bytes.Equal(dcid, c.scid) {
		return true
	}
	if c.isClient || space != spaceInitial {
		return false
	}
	if bytes.Equal(dcid, c.odcid) {
		return true
	}
	return len(c.params.retrySourceCID) > 0 && bytes.Equal(dcid, c.params.retrySourceCID)
}

// isStatelessReset reports whether a datagram that failed to route or to
// authenticate ends in a stateless reset token the peer announced for one
// of its identifiers (RFC 9000 §10.3.1) — via the handshake's
// stateless_reset_token parameter or a NEW_CONNECTION_ID frame. Token
// comparison is constant-time; an unannounced (all-zero) slot never
// matches, so noise cannot probe for it.
func (c *Conn) isStatelessReset(datagram []byte) bool {
	if len(datagram) < 21 { // §10.3: a reset is at least 21 bytes
		return false
	}
	tail := datagram[len(datagram)-16:]
	var zero [16]byte
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range c.peerCIDs {
		t := &c.peerCIDs[i]
		if t.token == zero {
			continue
		}
		if subtle.ConstantTimeCompare(tail, t.token[:]) == 1 {
			return true
		}
	}
	return false
}

// handleStatelessReset ends the connection the way §10.3.1 asks: silently.
// The peer provably once owned this connection — only it was told the token
// — and provably has no state left for it, so there are no keys on the
// other side to read a CONNECTION_CLOSE with, and nothing to wait for.
func (c *Conn) handleStatelessReset() error {
	err := fmt.Errorf("%w: stateless reset — the peer no longer holds state for this connection", ErrQUIC)
	c.mu.Lock()
	if c.closeErr == nil {
		c.closeErr = err
	}
	c.closeSent = true // suppress the CONNECTION_CLOSE a Close would send
	c.mu.Unlock()
	_ = c.Close()
	return err
}

// adoptPeerCID pins the connection identifier the peer chose, from the first
// authenticated long-header packet it sent (§7.2). Everything this end sends
// from here on names it.
func (c *Conn) adoptPeerCID(scid []byte) {
	c.mu.Lock()
	if c.isClient && !c.peerCIDSet {
		c.peerCIDSet = true
		c.dcid = append([]byte(nil), scid...)
		// Sequence 0 of the peer's identifier pool is the one its first
		// packet carried (§5.1.1); NEW_CONNECTION_ID adds the rest.
		c.peerCIDs = []peerCID{{seq: 0, cid: c.dcid, used: true}}
		c.currentPeerSeq = 0
	}
	c.mu.Unlock()
}

// handleVersionNegotiation acts on a version-negotiation packet: a client
// that has not yet heard from the server learns the server does not speak
// version 1. One that lists version 1 anyway is lying — a real server only
// lists what it would have answered — and one whose identifiers do not echo
// this connection's is not answering this connection. Both are dropped
// (§6.2).
func (c *Conn) handleVersionNegotiation(datagram []byte) {
	if !c.isClient {
		c.noteDiscard()
		return
	}
	c.mu.Lock()
	heardPeer := c.peerCIDSet
	initialDCID := c.initialDCID
	c.mu.Unlock()
	if heardPeer {
		c.noteDiscard()
		return
	}
	b := datagram[5:]
	dcid, b, err := connectionID(b)
	if err != nil {
		c.noteDiscard()
		return
	}
	scid, b, err := connectionID(b)
	if err != nil || !bytes.Equal(dcid, c.scid) || !bytes.Equal(scid, initialDCID) {
		c.noteDiscard()
		return
	}
	for len(b) >= 4 {
		v := uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
		if v == version1 {
			c.noteDiscard()
			return
		}
		b = b[4:]
	}
	c.fail(fmt.Errorf("%w: the server does not speak QUIC version 1", ErrQUIC))
	_ = c.Close()
}

// handleRetry acts on a Retry packet (§17.2.5): re-derive the Initial keys
// from the server's new identifier, remember the token, and let loss
// recovery resend the ClientHello under them. At most one Retry is honoured,
// and only before any packet from the server has authenticated — afterwards
// a Retry can only be an attacker, because a real server that answered has
// chosen not to retry (RFC 9001 §5.8 is what makes the tag check meaningful).
func (c *Conn) handleRetry(h longHeader) {
	c.mu.Lock()
	if !c.isClient || c.retried || c.peerCIDSet || len(h.Token) == 0 {
		c.mu.Unlock()
		c.noteDiscard()
		return
	}
	odcid := c.initialDCID
	c.mu.Unlock()

	if !verifyRetryTag(odcid, h.Raw) {
		c.noteDiscard()
		return
	}

	c.mu.Lock()
	c.retried = true
	c.retryToken = append([]byte(nil), h.Token...)
	c.dcid = append([]byte(nil), h.SCID...)
	c.retrySCID = c.dcid // adoptPeerCID will move dcid on; this must not move with it
	sp := &c.spaces[spaceInitial]
	payloads, inFlight := sp.sent.takeAll()
	sp.retrans = append(sp.retrans, payloads...)
	c.cc.inFlight -= inFlight
	if c.cc.inFlight < 0 {
		c.cc.inFlight = 0
	}
	c.mu.Unlock()

	// New identifier, new Initial secrets (RFC 9001 §5.2).
	if err := c.installInitial(h.SCID); err != nil {
		c.abort(err)
		return
	}
	c.mu.Lock()
	c.retransmitLocked()
	c.mu.Unlock()
}

// frames acts on one authenticated packet's frames. It reports whether any
// of them elicit an acknowledgement. A non-nil error is a protocol violation
// for the caller to abort with. now is the packet's arrival time.
func (c *Conn) frames(space int, payload []byte, now time.Time) (eliciting bool, err error) {
	// RFC 9000 §12.4: a packet with no frames at all is PROTOCOL_VIOLATION.
	// A payload of nothing but PADDING contains frames and stays legal; only
	// the empty payload is refused here.
	if len(payload) == 0 {
		return true, &transportError{
			code: transportProtocolViolation,
			err:  fmt.Errorf("%w: a packet containing no frames", ErrQUIC),
		}
	}
	// Both frame slices are per-connection scratch: one receive goroutine
	// processes one packet at a time, and nothing keeps either slice past
	// this call — the batch callback's contract is copy-what-you-keep,
	// read-mode copies into stream buffers, and a parked batch goes through
	// cloneFrames. Two slice allocations per packet, measured away.
	//
	// Taken and restored rather than borrowed in place, because this
	// function re-enters itself on the same goroutine: a CRYPTO frame can
	// hand TLS the keys a deferred packet was waiting on, and that packet
	// replays from inside the handler. The nested call finds nil, allocates
	// its own, and the unwind puts each level's slice back.
	parseScratch := c.parseScratch
	c.parseScratch = nil
	frames, err := parseFrames(parseScratch[:0], payload)
	defer func() { c.parseScratch = frames[:0] }()
	if err != nil {
		return true, &transportError{code: transportFrameEncoding, err: err}
	}
	if space == spaceApplication {
		// The §9.3 decision point: a confirmed server whose client's
		// non-probing frames arrive from a new address follows it there
		// before acting on them, so everything they provoke — ACKs, grants
		// — already leaves on the new path.
		c.maybeMigrate(frames, now)
	}

	// The stream frames of one packet are delivered together: a datagram
	// carries frames for several streams at once, and handing them over as a
	// group is the whole reason this transport suits an all-batch model.
	// What is delivered is *ordered deltas* — the transport owns reordering,
	// and the layer above never needs to know an offset existed. Whether the
	// group goes to a batch callback or to per-stream read buffers is
	// decided once, after the packet, under the delivery lock.
	batchScratch := c.batchScratch
	c.batchScratch = nil
	batch := batchScratch[:0]
	defer func() { c.batchScratch = batch[:0] }()
	var connGrowth uint64
	// Streams whose lifecycle ended in this packet are retired only after
	// the deltas are dispatched: retiring first removes the stream from the
	// map, and the read-mode delivery that looks it up there would drop the
	// final signal — a Read blocked on it would then wait out the idle
	// timeout for an EOF that already happened.
	var ended []*Stream
	scratch := c.takeScratch()

	for _, f := range frames {
		if f.Type != frameACK && f.Type != frameACK|1 {
			eliciting = true
		}
		// RFC 9000 §12.4: Initial and Handshake packets may carry only
		// PADDING, PING, ACK, CRYPTO and the transport CONNECTION_CLOSE.
		// The table is not pedantry — Initial keys are public, so every
		// frame admitted at that level is a frame anyone who saw the first
		// datagram can forge: a HANDSHAKE_DONE there would make a client
		// discard its Handshake keys mid-handshake, for good.
		if space != spaceApplication {
			switch f.Type {
			case framePing, frameCrypto, frameACK, frameACK | 1, frameConnectionClose:
			default:
				return eliciting, &transportError{
					code: transportProtocolViolation,
					err:  fmt.Errorf("%w: frame type %#x at encryption level %d", ErrQUIC, f.Type, space),
				}
			}
		}
		switch {
		case f.Type == frameCrypto:
			if err := c.handleCrypto(space, f); err != nil {
				return eliciting, err
			}

		case f.Type == frameACK || f.Type == frameACK|1:
			if err := c.handleAck(space, f, now); err != nil {
				return eliciting, err
			}

		case f.IsStream():
			s, tolerated, err := c.streamFor(f.StreamID, space, false)
			if err != nil {
				return eliciting, err
			}
			if tolerated {
				continue
			}
			deltaStart := s.deliveredSoFar()
			grown, finished, growth, err := s.deliver(f, scratch)
			if err != nil {
				return eliciting, err
			}
			delta := grown[len(scratch):]
			scratch = grown
			connGrowth += growth
			if len(delta) > 0 || finished {
				batch = append(batch, Frame{
					Type: FrameStream, StreamID: f.StreamID,
					offset: deltaStart, Data: delta, Fin: finished,
				})
			}
			if finished {
				ended = append(ended, s)
			}

		case f.Type == FrameResetStream:
			s, tolerated, err := c.streamFor(f.StreamID, space, false)
			if err != nil {
				return eliciting, err
			}
			if tolerated {
				continue
			}
			growth, neverConsumed, err := s.abandon(f.value, f.offset)
			if err != nil {
				return eliciting, err
			}
			connGrowth += growth
			if neverConsumed > 0 {
				c.mu.Lock()
				if c.connRecv.consume(neverConsumed) {
					c.grants = appendFrame(c.grants, frameMaxData, c.connRecv.nextMax())
				}
				c.mu.Unlock()
			}
			batch = append(batch, Frame{Type: FrameResetStream, StreamID: f.StreamID, value: f.value})
			ended = append(ended, s)

		case f.Type == frameStopSending:
			s, tolerated, err := c.streamFor(f.StreamID, space, true)
			if err != nil {
				return eliciting, err
			}
			if tolerated {
				continue
			}
			c.mu.Lock()
			sendErr := s.onStopSendingLocked(f.value)
			c.mu.Unlock()
			if sendErr != nil && !errors.Is(sendErr, errClosed) {
				return eliciting, sendErr
			}

		case f.Type == frameMaxData:
			c.mu.Lock()
			if c.connSend.grant(f.value) {
				c.cond.Broadcast()
			}
			c.mu.Unlock()

		case f.Type == frameMaxStreamData:
			// Through streamFor rather than a bare map lookup: §3.2 says a
			// MAX_STREAM_DATA for a peer-initiated stream this end has not
			// seen yet opens it, and §19.10 makes one for a stream the peer
			// could never grant on — a receive-only stream, or a
			// locally-initiated one never opened — a STREAM_STATE_ERROR,
			// where both used to be silently ignored.
			s, tolerated, err := c.streamFor(f.StreamID, space, true)
			if err != nil {
				return eliciting, err
			}
			if !tolerated {
				c.mu.Lock()
				if s.sendQ.grant(f.value) {
					c.cond.Broadcast()
				}
				c.mu.Unlock()
			}

		case f.Type == frameMaxStreams, f.Type == frameMaxStreams|1:
			c.mu.Lock()
			if f.Type == frameMaxStreams {
				c.peerMaxStreamsBidi = max(c.peerMaxStreamsBidi, f.value)
			} else {
				c.peerMaxStreamsUni = max(c.peerMaxStreamsUni, f.value)
			}
			c.cond.Broadcast()
			c.mu.Unlock()

		case f.Type == framePathChallenge:
			// §8.2: must be answered, on pain of the peer concluding the
			// path is dead. The answer has its own rules (§8.2.2): expanded
			// to a 1200-byte datagram, because the probe is also an MTU
			// probe, and sent exactly once — never via loss recovery, since
			// the peer retries with a fresh challenge and a stale response
			// proves nothing. Hence its own packet, noRetrans, with PADDING
			// up to the short header's 1200 (1 type byte + dcid + 4 pn +
			// 16 tag around the payload) — and the control queue flushed
			// first, so no grant hitches a ride out of loss recovery. §9.3
			// sends the response back to the address the challenge came
			// from, which during a migration is not the path in use.
			c.mu.Lock()
			c.flushControlLocked()
			resp := AppendVarint(nil, framePathResponse)
			resp = append(resp, f.Data...)
			// §8.2.2: the response MUST be expanded to 1200 — except past
			// the anti-amplification limit, which the section itself says
			// happens only when the challenge did not arrive expanded. On
			// the path in use the books cover the send; toward any other
			// address there are no books, so the triggering datagram's own
			// size is the budget: a ≥1200-byte challenge earns a 1200-byte
			// answer (factor ≤ 1), a small one gets a small one — never a
			// 1200-byte reflection of a few spoofed-source bytes. (Replay
			// is already closed: duplicates drop before frames are read.)
			// On the path in use the expansion stops at what the §8.1
			// budget allows, if the path is still unvalidated: a response
			// padded past it would not leave at all.
			onPath := c.rxFrom == nil || sameAddr(c.rxFrom, c.peer)
			padTo := 0
			if onPath || c.rxDatagramLen >= 1200 {
				padTo = 1200
			}
			_ = c.sendPacketLocked(spaceApplication, resp, sendOpts{noRetrans: true, to: c.rxFrom, via: c.rxVia, padTo: padTo})
			c.mu.Unlock()

		case f.Type == framePathResponse:
			c.handlePathResponse(f)

		case f.Type == frameHandshakeDone:
			if !c.isClient {
				return eliciting, &transportError{
					code: transportProtocolViolation,
					err:  fmt.Errorf("%w: a client sent HANDSHAKE_DONE", ErrQUIC),
				}
			}
			c.mu.Lock()
			c.confirmLocked()
			c.mu.Unlock()

		case f.Type == frameConnectionClose || f.Type == frameConnectionClose|1:
			c.handlePeerClose(f)
			return eliciting, nil

		case f.Type == frameRetireConnectionID:
			if err := c.handleRetireConnectionID(f.value); err != nil {
				return eliciting, err
			}

		case f.Type == frameNewConnectionID:
			if err := c.handleNewConnectionID(f); err != nil {
				return eliciting, err
			}

		// NEW_TOKEN stores tokens this package will never spend; the
		// *_BLOCKED frames are the peer saying our grants are the
		// bottleneck, which consumption already answers with fresh grants.
		// All legal, all deliberately inert.
		case f.Type == frameNewToken, f.Type == frameDataBlocked,
			f.Type == frameStreamDataBlocked,
			f.Type == frameStreamsBlocked, f.Type == frameStreamsBlocked|1,
			f.Type == framePing:
		}
	}
	c.storeScratch(scratch)

	if connGrowth > 0 {
		c.mu.Lock()
		if err := c.connRecv.receive(c.connRecv.highest + connGrowth); err != nil {
			c.mu.Unlock()
			return eliciting, &transportError{code: transportFlowControl, err: err}
		}
		c.mu.Unlock()
	}

	if len(batch) > 0 {
		if err := c.dispatchDeltas(batch); err != nil {
			return eliciting, err
		}
	}
	if len(ended) > 0 {
		c.mu.Lock()
		for _, s := range ended {
			c.maybeRemoveLocked(s)
		}
		c.mu.Unlock()
	}
	return eliciting, nil
}

// dispatchDeltas hands one packet's ordered deltas to whichever consumer the
// connection has, decided under the delivery lock so a callback registering
// mid-packet cannot double-deliver: the batch callback if one is registered
// (its return blocks the read loop, which is the back-pressure), per-stream
// read buffers otherwise — held for a future callback as well, until a Read
// makes that copy pointless (see [Conn.pending]).
func (c *Conn) dispatchDeltas(batch []Frame) error {
	c.deliverMu.Lock()
	defer c.deliverMu.Unlock()
	if c.onFrames != nil {
		if err := c.onFrames(batch); err != nil {
			return err
		}
		c.settleBatchConsumption(batch)
		// The callback has seen the end; a Read on the same stream — which
		// got no bytes, the callback did — may now report it too.
		for _, f := range batch {
			if (f.Type == FrameStream && f.Fin) || f.Type == FrameResetStream {
				if s := c.streamIfOpen(f.StreamID); s != nil {
					s.endForRead()
				}
			}
		}
		return nil
	}
	for _, f := range batch {
		s := c.streamIfOpen(f.StreamID)
		if s == nil {
			continue
		}
		switch f.Type {
		case FrameStream:
			s.keepForRead(f.Data, f.Fin)
		case FrameResetStream:
			// abandon already marked the reset under the stream's lock; the
			// reader learns of it only now, behind every delta that preceded
			// it in this batch, so the bytes the peer did send are read first.
			s.endForRead()
		}
	}
	c.holdPending(batch)
	return nil
}

// streamIfOpen looks a stream up by identifier, nil if it is gone.
func (c *Conn) streamIfOpen(id uint64) *Stream {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.streams[id]
}

// pendingFrameCost is what each held frame is charged beyond its bytes, so
// frames carrying none (FIN-only, RESET_STREAM) still count.
const pendingFrameCost = 64

// holdPending keeps a copy of batch for a callback that has not registered
// yet. Every byte held is also uncredited, so the connection receive window
// bounds it; the guard at twice that window only trips on a flood of
// byteless frames, and falls back to the read buffers rather than dropping
// anything. Runs under deliverMu.
func (c *Conn) holdPending(batch []Frame) {
	if c.pendingLost {
		return
	}
	if c.readTaken.Load() {
		c.abandonPending()
		return
	}
	for i := range batch {
		c.pendingBytes += uint64(len(batch[i].Data)) + pendingFrameCost
	}
	// window is fixed at construction; reading it needs no lock.
	if c.pendingBytes > 2*c.connRecv.window {
		c.abandonPending()
		return
	}
	c.pending = append(c.pending, cloneFrames(batch))
}

func (c *Conn) abandonPending() {
	c.pending, c.pendingBytes, c.pendingLost = nil, 0, true
}

// maxCryptoBuffer bounds the bytes CRYPTO reassembly may park per space.
// Stream reassembly is bounded by flow control; CRYPTO has no flow control
// (RFC 9000 §7.5 says bound it anyway, and this is the customary bound), so
// without this a peer parks ~1.4 KB per fragment × 256 fragments per
// connection it never finishes the handshake of.
const maxCryptoBuffer = 64 << 10

// handleCrypto reassembles handshake bytes — retransmissions overlap here
// exactly as they do on streams — and feeds what is newly in order to TLS.
func (c *Conn) handleCrypto(space int, f Frame) error {
	c.mu.Lock()
	sp := &c.spaces[space]
	delta, err := sp.crypto.addThenTake(f.offset, f.Data, nil)
	held := sp.crypto.held
	c.mu.Unlock()
	if err != nil {
		return &transportError{code: transportProtocolViolation, err: err}
	}
	if held > maxCryptoBuffer {
		return &transportError{
			code: transportCryptoBufferExceeded,
			err:  fmt.Errorf("%w: over %d bytes of handshake data parked out of order", ErrQUIC, maxCryptoBuffer),
		}
	}
	if len(delta) == 0 {
		return nil
	}
	if err := c.tls.HandleData(levelFor(space), delta); err != nil {
		return fmt.Errorf("quic: the handshake refused data: %w", err)
	}
	return nil
}

// handleAck feeds one ACK to loss recovery: release what it covers, take a
// round-trip sample, declare losses, reopen the congestion window. now is
// when the packet carrying the ACK arrived — a deferred packet's ACK must
// not read its time parked waiting for keys as path latency.
func (c *Conn) handleAck(space int, f Frame, now time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	sp := &c.spaces[space]
	if f.largest >= sp.nextPN {
		return &transportError{
			code: transportProtocolViolation,
			err:  fmt.Errorf("%w: the peer acknowledged packet %d, which was never sent", ErrQUIC, f.largest),
		}
	}
	out, err := sp.sent.processAck(now, f.largest, f.Data, &c.rtt, c.cc.recoveryStart)
	if err != nil {
		return &transportError{code: transportFrameEncoding, err: err}
	}
	if out.largestWasNew && out.largestEliciting {
		var ackDelay time.Duration
		if space == spaceApplication {
			exp := c.peerAckDelayExponent()
			micros := f.delay << exp
			if exp > 0 && micros>>exp != f.delay {
				micros = 1 << 62 // overflowed; any huge value behaves the same below
			}
			// Bounded so the conversion to a Duration cannot overflow: a
			// hostile 62-bit delay does not poison the estimator anyway,
			// because sample refuses any subtraction that would push the
			// round trip below the smallest one seen.
			if lim := uint64((1 << 62) / time.Microsecond); micros > lim {
				micros = lim
			}
			ackDelay = time.Duration(micros) * time.Microsecond //nolint:gosec // G115: bounded above
			// RFC 9002 §5.3: the declared delay is capped at the peer's own
			// max_ack_delay only once the handshake is confirmed — before
			// that the peer is allowed to hold ACKs longer (its own timers
			// are not fully set up), and clamping early would fold genuine
			// hold time into the round-trip estimate.
			if c.handshakeConfirmed {
				if maxDelay := c.peerMaxAckDelayLocked(); ackDelay > maxDelay {
					ackDelay = maxDelay
				}
			}
		}
		c.rtt.sample(now.Sub(out.largestSentAt), ackDelay)
	}
	if out.newlyAcked > 0 {
		c.cc.onAcked(out.newlyAcked, out.growableBytes)
		c.cond.Broadcast()
	}
	if space == spaceApplication && !c.ku.acked && sp.sent.hasLargestAcked && sp.sent.largestAcked >= c.ku.firstPN {
		// A packet under the current send keys reached the peer: both
		// sides hold this phase, and the next rotation is permitted
		// (RFC 9001 §6.1).
		c.ku.acked = true
	}
	if out.newlyAckedCount > 0 && space == spaceHandshake {
		// An acknowledged Handshake packet is the client's proof the server
		// finished validating its address (RFC 9002 §6.2.1) — the gate for
		// the backoff reset below and for the anti-deadlock probe.
		c.peerAddrValidated = true
	}
	if out.sawAckEliciting {
		// §6.2.1's exception: a client must not read an Initial ACK as the
		// end of a stall — the server may still be amplification-blocked,
		// and probing it at the unbacked-off rate is what the backoff was
		// protecting it from.
		if !c.isClient || space != spaceInitial || c.peerAddrValidated {
			c.ptoCount = 0
		}
	}
	if len(out.lost) > 0 || out.lostBytes > 0 {
		sp.retrans = append(sp.retrans, out.lost...)
		c.cc.onLost(out.lostBytes, out.latestLossSent, now)
	}
	return nil
}

// peerAckDelayExponent is the scale the peer applies to its ACK delay
// fields. Callers hold mu.
func (c *Conn) peerAckDelayExponent() uint64 {
	if c.hasPeerParams && c.peerParams.AckDelayExponent != 0 {
		return c.peerParams.AckDelayExponent
	}
	return 3
}

// peerMaxAckDelayLocked is how long the peer said it may sit on an
// acknowledgement — the protocol default of 25 ms unless announced. It caps
// the delay field of incoming ACKs and pads the probe timeout. Callers hold
// mu.
func (c *Conn) peerMaxAckDelayLocked() time.Duration {
	if c.hasPeerParams && c.peerParams.MaxAckDelay != 0 {
		return c.peerParams.MaxAckDelay
	}
	return 25 * time.Millisecond
}

// streamFor resolves a stream-addressed frame: the stream, or tolerated=true
// for one that already closed — frames legitimately still in flight behind a
// FIN or a reset (§3.2) — or an error for one the peer had no right to name.
//
// sendSide says which of this end's directions the frame addresses: true for
// frames about this end's sending half (STOP_SENDING), false for frames that
// carry or end the peer's (STREAM, RESET_STREAM). On a unidirectional stream
// only one half exists, and §3.2/§3.5 make naming the missing one a
// STREAM_STATE_ERROR — honoured here before anything else, because acting on
// such a frame compounds it: a STOP_SENDING on the peer's own uni stream used
// to make this end answer with a RESET_STREAM on a stream it never sends on,
// which a conformant peer must kill the connection over.
func (c *Conn) streamFor(id uint64, space int, sendSide bool) (*Stream, bool, error) {
	if space != spaceApplication {
		return nil, false, &transportError{
			code: transportProtocolViolation,
			err:  fmt.Errorf("%w: a stream frame at encryption level %d", ErrQUIC, space),
		}
	}
	if id&0x02 != 0 {
		peerInitiated := (id&1 == 0) != c.isClient
		if peerInitiated == sendSide {
			return nil, false, &transportError{
				code: transportStreamState,
				err:  fmt.Errorf("%w: the peer addressed the direction stream %d does not have", ErrQUIC, id),
			}
		}
	}
	c.mu.Lock()
	if s := c.streams[id]; s != nil {
		c.mu.Unlock()
		return s, false, nil
	}
	if _, closed := c.closedIDs[id]; closed {
		c.mu.Unlock()
		return nil, true, nil
	}
	peerInitiated := (id&1 == 0) != c.isClient
	if !peerInitiated {
		c.mu.Unlock()
		return nil, false, &transportError{
			code: transportStreamState,
			err:  fmt.Errorf("%w: the peer sent on stream %d, which this end never opened", ErrQUIC, id),
		}
	}
	// Opening stream N implicitly opens every lower-numbered stream of its
	// kind (§3.2), so the count in use is the highest identifier's ordinal.
	count := id/4 + 1
	uni := id&0x02 != 0
	limit := c.localMaxStreamsBidi
	seen := &c.seenPeerBidi
	if uni {
		limit = c.localMaxStreamsUni
		seen = &c.seenPeerUni
	}
	if count > limit {
		c.mu.Unlock()
		return nil, false, &transportError{
			code: transportStreamLimit,
			err:  fmt.Errorf("%w: the peer opened stream %d, past the %d allowed", ErrQUIC, id, limit),
		}
	}
	if count > *seen {
		*seen = count
	}
	s := newStream(c, id) // newStream closes the direction a unidirectional stream lacks
	c.streams[id] = s
	batchMode := c.batchMode
	c.mu.Unlock()

	if !batchMode {
		// The accept queue applies back-pressure by blocking the read loop:
		// a server that stops calling AcceptStream stops reading, and the
		// peer feels it through this connection's flow control — nothing is
		// dropped and nothing torn down (the 65th stream used to be fatal
		// here).
		select {
		case c.accepted <- s:
		case <-c.closed:
			return nil, false, errClosed
		}
	}
	return s, false, nil
}

// settleBatchConsumption credits flow control — per stream and per
// connection — for the bytes the batch callback consumed, queueing the
// grants earned. In batch mode the callback *is* the consumer: no Read ever
// runs, so this is the only place the consumption that earns MAX_DATA can
// be accounted. Without the connection level here, a batch-mode server
// serves InitialMaxData cumulative bytes over the connection's whole life
// and then watches a conformant peer stall forever.
func (c *Conn) settleBatchConsumption(batch []Frame) {
	// Under manual credit (SetManualCredit) nothing settles here at all:
	// the batch consumer owns the moment bytes stop being retained, and
	// re-crediting them on delivery is exactly the gap that lets a peer
	// park more than the consumer's own memory bounds admit.
	c.mu.Lock()
	manual := c.manualCredit
	c.mu.Unlock()
	if manual {
		return
	}

	// Three passes so the connection lock is taken twice per batch instead
	// of twice per stream frame: resolve every stream under one take, walk
	// the per-stream quotas with no connection lock held (the repo-wide
	// ordering is never to hold s.mu and c.mu together), then queue what
	// the walk earned under one more. The resolved pointers live in
	// per-connection scratch — this runs under deliverMu, one batch at a
	// time.
	var connBytes uint64
	settled := c.settleScratch[:0]
	defer func() { c.settleScratch = settled[:0] }()
	c.mu.Lock()
	for i := range batch {
		f := &batch[i]
		if f.Type != FrameStream || len(f.Data) == 0 {
			continue
		}
		connBytes += uint64(len(f.Data))
		if s := c.streams[f.StreamID]; s != nil {
			settled = append(settled, settledStream{s: s, n: uint64(len(f.Data))})
		}
	}
	c.mu.Unlock()

	for i := range settled {
		e := &settled[i]
		e.s.mu.Lock()
		if e.s.recvQ.consume(e.n) {
			e.grant = e.s.recvQ.nextMax()
		}
		e.s.mu.Unlock()
	}

	c.mu.Lock()
	for i := range settled {
		if e := &settled[i]; e.grant > 0 {
			c.grants = appendFrame(c.grants, frameMaxStreamData, e.s.id, e.grant)
		}
	}
	if connBytes > 0 && c.connRecv.consume(connBytes) {
		c.grants = appendFrame(c.grants, frameMaxData, c.connRecv.nextMax())
	}
	c.flushControlLocked()
	c.mu.Unlock()
}

// SetManualCredit moves receive-side flow-control crediting from the
// transport to the batch consumer. With it on, bytes delivered to the
// [Conn.OnStreamFrames] callback earn the peer no new credit until the
// consumer calls [Conn.ReleaseStreamBytes] for them — which it must, for
// every delivered byte, or the peer's sending starves at the windows'
// initial values. What it buys: the peer can never hold more unacknowledged
// memory than the consumer admits to retaining, where automatic crediting
// re-opens the window for bytes the consumer still holds. Meaningful only
// in batch mode; read mode credits on [Stream.Read] as before.
func (c *Conn) SetManualCredit(on bool) {
	c.mu.Lock()
	c.manualCredit = on
	c.mu.Unlock()
}

// ReleaseStreamBytes reports n bytes of stream id's data released by a
// manual-credit consumer (see [Conn.SetManualCredit]): no longer retained,
// so the peer may be granted that much again. The stream-level window
// advances when the stream is still live; the connection-level window
// advances regardless, because its debt survives the stream. Safe from any
// goroutine.
func (c *Conn) ReleaseStreamBytes(id, n uint64) {
	if n == 0 {
		return
	}
	c.mu.Lock()
	s := c.streams[id]
	c.mu.Unlock()
	var nextMax uint64
	grant := false
	if s != nil {
		s.mu.Lock()
		if grant = s.recvQ.consume(n); grant {
			nextMax = s.recvQ.nextMax()
		}
		s.mu.Unlock()
	}
	c.mu.Lock()
	if grant {
		c.grants = appendFrame(c.grants, frameMaxStreamData, id, nextMax)
	}
	if c.connRecv.consume(n) {
		c.grants = appendFrame(c.grants, frameMaxData, c.connRecv.nextMax())
	}
	c.flushControlLocked()
	c.mu.Unlock()
}

// settledStream is one stream frame's flow-control settlement in flight
// between settleBatchConsumption's passes: the stream, the bytes the batch
// delivered on it, and the MAX_STREAM_DATA value its consumption earned
// (zero for none — a real grant is never zero, offsets being cumulative).
type settledStream struct {
	s     *Stream
	n     uint64
	grant uint64
}

// handlePeerClose acts on the peer's CONNECTION_CLOSE: record why, stop
// sending anything — including a close of this end's own (§10.2.2) — and
// end.
func (c *Conn) handlePeerClose(f Frame) {
	c.mu.Lock()
	c.draining = true
	c.peerClose = peerClose{code: f.value, app: f.Type == frameConnectionClose|1, reason: string(f.Data), seen: true}
	if f.Type == frameConnectionClose && f.value != 0 && c.closeErr == nil {
		c.closeErr = fmt.Errorf("%w: the peer closed the connection: code %#x %q", ErrQUIC, f.value, f.Data)
	}
	c.mu.Unlock()
	_ = c.Close()
}

func (c *Conn) noteDiscard() {
	c.mu.Lock()
	c.discarded++
	c.mu.Unlock()
}

func allZero(b []byte) bool {
	for _, x := range b {
		if x != 0 {
			return false
		}
	}
	return true
}

// sameAddr compares two addresses for "the same peer". It runs on every
// received datagram, so the UDP case compares netip values — no
// allocation — where rendering both to strings cost six. Unmap keeps the
// string comparison's equivalence of an IPv4 address and its IPv4-mapped
// IPv6 form, which a dual-stack socket reports interchangeably.
func sameAddr(a, b net.Addr) bool {
	if a == nil || b == nil {
		return a == b
	}
	if ua, ok := a.(*net.UDPAddr); ok {
		if ub, ok := b.(*net.UDPAddr); ok {
			pa, pb := ua.AddrPort(), ub.AddrPort()
			return pa.Port() == pb.Port() && pa.Addr().Unmap() == pb.Addr().Unmap()
		}
	}
	return a.Network() == b.Network() && a.String() == b.String()
}

// readOne reads one datagram and folds it in — the handshake's synchronous
// read path. got reports that a datagram was read: an error with got false
// is the socket's (or the deadline's), one with got true is what the
// datagram provoked. For a [Listener]-managed connection (c.incoming set),
// it waits on the listener's feed instead of the socket, which it does not
// own exclusively; see readLoopFed's longer explanation of why.
func (c *Conn) readOne(deadline time.Time) (got bool, err error) {
	if c.incoming != nil {
		return c.readOneFed(deadline)
	}
	buf := make([]byte, c.recvBufSize())
	if err := c.pc.SetReadDeadline(deadline); err != nil {
		return false, err
	}
	// After arming, as in readLoop: a Close that expired the deadline
	// before this call re-armed it would otherwise be waited out.
	select {
	case <-c.closed:
		return false, net.ErrClosed
	default:
	}
	n, addr, err := c.pc.ReadFrom(buf)
	if err != nil {
		return false, err
	}
	return true, c.receiveGathered(rawDatagram{data: buf[:n], from: addr})
}

// receiveGathered is receive under datagram gathering, for the handshake's
// synchronous read path: what the datagram provokes (its ACK, mostly) is
// held until handshake()'s pump has added the CRYPTO flight, and the two
// leave together.
func (c *Conn) receiveGathered(d rawDatagram) error {
	c.mu.Lock()
	c.rxVia = d.via
	c.gatherLocked(true)
	c.mu.Unlock()
	return c.receive(d.data, d.from)
}

func (c *Conn) readOneFed(deadline time.Time) (got bool, err error) {
	wait := max(time.Until(deadline), 0)
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-c.closed:
		return false, net.ErrClosed
	case d, ok := <-c.incoming:
		if !ok {
			return false, net.ErrClosed
		}
		return true, c.receiveGathered(d)
	case <-timer.C:
		return false, os.ErrDeadlineExceeded
	}
}

// takeScratch and storeScratch reuse one delta buffer across packets. Stream
// deliveries append into it and the batch callback borrows sub-slices of it
// for the duration of the call — the batch contract everywhere in this
// project. If an append outgrows it mid-packet, the earlier sub-slices keep
// the old backing array alive and stay valid; the grown one is what is kept.
func (c *Conn) takeScratch() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.deltaScratch
	if s == nil {
		s = make([]byte, 0, 4096)
	}
	c.deltaScratch = nil
	return s[:0]
}

func (c *Conn) storeScratch(s []byte) {
	c.mu.Lock()
	c.deltaScratch = s
	c.mu.Unlock()
}
