package quic

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"time"
)

// Connection-identifier rotation and server-side passive migration, RFC 9000
// §5.1 and §9.
//
// The shape in one paragraph: a [Listener]-managed server connection issues
// alternative identifiers for itself (NEW_CONNECTION_ID) once its handshake
// confirms, registered in the listener's demux table so a packet naming any
// of them still routes; every connection pools the identifiers its *peer*
// issues and honours Retire Prior To by switching what it sends under; and a
// server that sees a 1-RTT packet arrive from a new address follows the
// client there — new path unvalidated and amplification-limited until a
// PATH_CHALLENGE round trip proves it, congestion and round-trip state reset
// because the old path's numbers say nothing about the new one, the old path
// challenged too (§9.3.3), and a new path that never validates abandoned
// for the last validated one (§9.3.2).

// maxLocalActiveCIDs caps how many identifiers this end keeps active for
// itself, whatever the peer's active_connection_id_limit invites: each one is
// a demux-table entry, and four — the same limit this end advertises for the
// peer's — is enough for a NAT rebind and a deliberate rotation to overlap.
const maxLocalActiveCIDs = 4

// pathProbeMaxRetransmits is how many times an unanswered PATH_CHALLENGE is
// resent before this end gives up on a path. A server whose migrated path
// never validated reverts to the last validated one (RFC 9000 §9.3.2) —
// it does not kill the connection: if the peer is gone from both, the idle
// timer is the one deadline that already exists for exactly that silence.
const pathProbeMaxRetransmits = 3

// cidRegistrar is what a [Listener] gives each connection it manages: the
// ability to add and remove routing entries for additional local connection
// identifiers in the shared demux table. A connection without one — [Dial],
// standalone [Accept] — never issues identifiers, which RFC 9000 §5.1.1
// permits (issuing is a SHOULD): its peer simply keeps using sequence 0.
//
// Lock order: these are called with the connection's mu held and take the
// listener's own lock inside — Conn.mu before Listener.mu, never the
// reverse (nothing under Listener.mu ever takes a connection's).
type cidRegistrar interface {
	// addLocalCID mints a fresh [listenerCIDLen]-byte identifier, collision
	// -checked against the demux table, registers it as routing to c, and
	// returns it.
	addLocalCID(c *Conn) ([]byte, error)
	// removeLocalCID drops one identifier's routing entry.
	removeLocalCID(cid []byte)
	// resetTokenFor derives the stateless reset token for one identifier
	// this registrar issued (RFC 9000 §10.3). Derived, not stored: the
	// whole point of a stateless reset is that it can be sent after every
	// trace of the connection is gone, which only a key the registrar
	// keeps for its own lifetime can honour.
	resetTokenFor(cid []byte) [16]byte
}

// localCID is one identifier this end answers to. Sequence 0 is the
// connection's original scid; higher sequences were issued via
// NEW_CONNECTION_ID.
type localCID struct {
	seq uint64
	cid []byte
}

// peerCID is one identifier the peer issued for itself, pooled until this
// end switches its sends to it or the peer retires it. The stateless reset
// token is what isStatelessReset compares an unroutable datagram's tail
// against (§10.3.1); all-zero means the peer never announced one for this
// identifier.
type peerCID struct {
	seq   uint64
	cid   []byte
	token [16]byte
	used  bool // ever been this end's destination identifier
}

// pathProbe is one in-flight PATH_CHALLENGE on a migrated, not-yet-validated
// path.
type pathProbe struct {
	pending     bool
	data        [8]byte
	sentAt      time.Time
	retransmits int
	// fatal marks a probe whose failure ends the connection: the client
	// side of [Conn.Rebind], where the old path is gone and there is
	// nothing to fall back to. A server's probe of a migrated client is
	// never fatal — the idle timer owns that silence.
	fatal bool
	// target and targetCID are set while a client probes the server's
	// preferred address (§9.6.2): the challenge goes there, under that
	// identifier, while everything else stays on the path in use. A
	// matching response moves the connection; silence leaves it where it
	// is, without error — the path in use still works.
	target    net.Addr
	targetCID []byte
	// sent and full record whether the current data has left and whether
	// every datagram carrying it was expanded to 1200 bytes. A response to
	// a smaller probe validates the address but not the path MTU, and
	// §8.2.1 then requires a second, full-size validation.
	sent, full bool
	// oldPending and oldData are a server's §9.3.3 challenge on the path
	// it migrated away from, which induces the legitimate peer to send
	// there — what defeats a migration forged from copied packets.
	oldPending bool
	oldData    [8]byte
}

// preferredAddress reports the address the server asked this client to move
// to (RFC 9000 §9.6), for this connection's address family, or nil: none
// was offered, or this is a server. The move itself is automatic once the
// handshake confirms — a validated preferred path becomes the connection's
// path, counted in [Conn.Migrations]; one that never validates leaves the
// connection on the handshake's path. Internal, like the listener's
// preferredAddress configuration: only this package's tests read it.
func (c *Conn) preferredAddress() net.Addr {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.preferred
}

// Migrations reports how many times this connection changed path: a server
// following its peer to a new address, or a client moving itself with
// [Conn.Rebind]. Whether the newest one has validated shows up as traffic
// flowing again; a server-side one that never validates ends by idle
// timeout, like any dead peer, a Rebind that never validates ends the
// connection with an error.
func (c *Conn) Migrations() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.migrations
}

// localCIDSeqFor says whether an incoming short-header packet's destination
// identifier is one this connection currently answers to, and which sequence
// number it carries — the RETIRE_CONNECTION_ID check in §19.16 needs the
// arrival identifier by number.
func (c *Conn) localCIDSeqFor(dcid []byte) (uint64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, e := range c.localCIDs {
		if bytes.Equal(e.cid, dcid) {
			return e.seq, true
		}
	}
	return 0, false
}

// issueLocalCIDsLocked tops the set of identifiers this end answers to up to
// min(the peer's active_connection_id_limit, [maxLocalActiveCIDs]), issuing
// each new one as a NEW_CONNECTION_ID on the control queue — which rides the
// next packet's retransmittable payload, so a lost one is resent like any
// other control frame. No-op without a registrar (nowhere to route the new
// identifier) or before the handshake confirms (§5.1.1 wants them in 1-RTT
// packets the peer is known able to read). Callers hold mu.
func (c *Conn) issueLocalCIDsLocked() {
	if c.registrar == nil || !c.handshakeConfirmed {
		return
	}
	limit := uint64(2) // the RFC 9000 §18.2 default when the peer announced nothing
	if c.hasPeerParams && c.peerParams.ActiveConnIDLimit != 0 {
		limit = c.peerParams.ActiveConnIDLimit
	}
	if limit > maxLocalActiveCIDs {
		limit = maxLocalActiveCIDs
	}
	for uint64(len(c.localCIDs)) < limit {
		cid, err := c.registrar.addLocalCID(c)
		if err != nil {
			return // a full table or a dying listener; the connection works without
		}
		// Derived from the registrar's key, so the listener can honour the
		// token in a stateless reset long after this connection is gone.
		token := c.registrar.resetTokenFor(cid)
		seq := c.nextLocalSeq
		c.nextLocalSeq++
		c.localCIDs = append(c.localCIDs, localCID{seq: seq, cid: cid})
		c.control = AppendVarint(c.control, frameNewConnectionID)
		c.control = AppendVarint(c.control, seq)
		c.control = AppendVarint(c.control, 0)        // Retire Prior To: never forced here
		c.control = append(c.control, byte(len(cid))) //nolint:gosec // G115: a connection ID is at most 20 bytes
		c.control = append(c.control, cid...)
		c.control = append(c.control, token[:]...)
	}
}

// handleRetireConnectionID acts on the peer retiring one of this end's
// identifiers (§19.16): the entry is dropped from the demux table and a
// replacement issued, keeping the active count. The two violations the
// section names are checked first — a sequence this end never issued, and
// the identifier the frame's own packet arrived under (retiring the thing
// you are using to say so proves the sender confused, and §5.1.2 forbids
// honouring it).
func (c *Conn) handleRetireConnectionID(seq uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if seq >= c.nextLocalSeq {
		return &transportError{
			code: transportProtocolViolation,
			err:  fmt.Errorf("%w: the peer retired connection identifier %d, which was never issued", ErrQUIC, seq),
		}
	}
	if seq == c.rxLocalCIDSeq {
		return &transportError{
			code: transportProtocolViolation,
			err:  fmt.Errorf("%w: the peer retired the connection identifier its own packet arrived on", ErrQUIC),
		}
	}
	for i, e := range c.localCIDs {
		if e.seq != seq {
			continue
		}
		c.localCIDs = append(c.localCIDs[:i], c.localCIDs[i+1:]...)
		if c.registrar != nil {
			c.registrar.removeLocalCID(e.cid)
		}
		c.issueLocalCIDsLocked()
		return nil
	}
	// Already retired: a retransmitted frame, tolerated like any duplicate.
	return nil
}

// handleNewConnectionID pools an identifier the peer issued for itself
// (§19.15) and honours its Retire Prior To field — including switching this
// end's sends to a fresh identifier when the one in use is among the
// retired, which is the rotation a load balancer forces.
func (c *Conn) handleNewConnectionID(f Frame) error {
	cid := f.Data[:len(f.Data)-16]
	var token [16]byte
	copy(token[:], f.Data[len(f.Data)-16:])
	seq, retire := f.value, f.offset

	c.mu.Lock()
	defer c.mu.Unlock()
	// §19.15: an endpoint whose peer took a zero-length identifier has
	// nothing to rotate and MUST treat the frame as PROTOCOL_VIOLATION.
	if len(c.dcid) == 0 {
		return &transportError{
			code: transportProtocolViolation,
			err:  fmt.Errorf("%w: NEW_CONNECTION_ID on a connection with a zero-length peer identifier", ErrQUIC),
		}
	}
	for i := range c.peerCIDs {
		e := &c.peerCIDs[i]
		if e.seq != seq {
			continue
		}
		if !bytes.Equal(e.cid, cid) || e.token != token {
			return &transportError{
				code: transportProtocolViolation,
				err:  fmt.Errorf("%w: NEW_CONNECTION_ID repeated sequence %d with different content", ErrQUIC, seq),
			}
		}
		// A retransmission; honouring its Retire Prior To again below is
		// idempotent.
		return c.retirePeerCIDsBeforeLocked(retire)
	}
	if seq < c.peerRetirePrior {
		// §5.1.2: an identifier that arrives already ordered retired is
		// retired at once, never pooled.
		return c.queueRetireLocked(seq)
	}
	c.peerCIDs = append(c.peerCIDs, peerCID{seq: seq, cid: append([]byte(nil), cid...), token: token})
	if err := c.retirePeerCIDsBeforeLocked(retire); err != nil {
		return err
	}
	// The limit this end advertised (§5.1.1, counted after retirement): more
	// simultaneously-active peer identifiers than that is the dedicated
	// error, not a generic violation.
	if uint64(len(c.peerCIDs)) > c.params.ActiveConnIDLimit {
		return &transportError{
			code: transportConnectionIDLimit,
			err: fmt.Errorf("%w: the peer keeps %d identifiers active, over the %d this end advertised",
				ErrQUIC, len(c.peerCIDs), c.params.ActiveConnIDLimit),
		}
	}
	return nil
}

// retirePeerCIDsBeforeLocked honours a Retire Prior To threshold: every
// pooled sequence below it is answered with RETIRE_CONNECTION_ID (reliable —
// the control queue rides retransmittable payloads) and dropped, and if the
// identifier currently in use is among them, sending switches to a fresh one
// immediately. Callers hold mu.
func (c *Conn) retirePeerCIDsBeforeLocked(retire uint64) error {
	if retire <= c.peerRetirePrior {
		return nil
	}
	c.peerRetirePrior = retire
	kept := c.peerCIDs[:0]
	for _, e := range c.peerCIDs {
		if e.seq < retire {
			if err := c.queueRetireLocked(e.seq); err != nil {
				return err
			}
			continue
		}
		kept = append(kept, e)
	}
	c.peerCIDs = kept
	if c.currentPeerSeq < retire {
		// The parse refused Retire Prior To above the frame's own sequence,
		// so the frame that forced this always left at least its own
		// identifier in the pool.
		c.switchToFreshPeerCIDLocked()
	}
	return nil
}

// queueRetireLocked queues one RETIRE_CONNECTION_ID, bounded. Every
// NEW_CONNECTION_ID whose Retire Prior To equals its own sequence costs the
// peer a dozen bytes and makes this end queue a retirement; a peer that
// then withholds its ACKs keeps the congestion window shut, the control
// queue never drains, and it grows by one frame per frame received.
// §5.1.2 lets an endpoint bound the retirements it tracks — it should
// allow at least twice active_connection_id_limit — and treat exceeding
// the bound as CONNECTION_ID_LIMIT_ERROR. Callers hold mu.
func (c *Conn) queueRetireLocked(seq uint64) error {
	limit := max(2*min(c.params.ActiveConnIDLimit, 1<<10), 8)
	if uint64(c.controlRetires) >= limit { //nolint:gosec // G115: a count, never negative
		return &transportError{
			code: transportConnectionIDLimit,
			err: fmt.Errorf("%w: the peer forced %d connection identifier retirements this end has not yet sent",
				ErrQUIC, c.controlRetires),
		}
	}
	c.control = appendFrame(c.control, frameRetireConnectionID, seq)
	c.controlRetires++
	return nil
}

// switchToFreshPeerCIDLocked moves this end's sends to a pooled peer
// identifier, preferring one never used on any path (§9.5) and the lowest
// sequence among those. Callers hold mu.
func (c *Conn) switchToFreshPeerCIDLocked() bool {
	pick := -1
	for i := range c.peerCIDs {
		e := &c.peerCIDs[i]
		if e.seq == c.currentPeerSeq {
			continue
		}
		if pick == -1 ||
			(!e.used && c.peerCIDs[pick].used) ||
			(e.used == c.peerCIDs[pick].used && e.seq < c.peerCIDs[pick].seq) {
			pick = i
		}
	}
	if pick == -1 {
		return false
	}
	e := &c.peerCIDs[pick]
	e.used = true
	c.dcid = e.cid
	c.currentPeerSeq = e.seq
	// A fresh identifier starts a fresh linkability epoch; a spin value
	// carried across the rotation would tie the two (§17.4).
	c.rollSpinLocked()
	return true
}

// probingOnly reports whether a packet carried nothing but the frames §9.1
// calls probing: PATH_CHALLENGE, PATH_RESPONSE, NEW_CONNECTION_ID and
// PADDING (already stripped by the parser, so an all-padding packet arrives
// here empty). Everything else — an ACK included — is use of the path.
func probingOnly(frames []Frame) bool {
	for _, f := range frames {
		switch f.Type {
		case framePathChallenge, framePathResponse, frameNewConnectionID:
		default:
			return false
		}
	}
	return true
}

// maybeMigrate is the §9.3 decision, taken once per authenticated 1-RTT
// packet: a confirmed server whose peer's non-probing frames arrive from a
// new address follows it there — but only on the highest-numbered packet
// yet received, so a late straggler from an address the client already left
// cannot drag the path backwards.
//
// The same gate serves the server's own preferred address (§9.6): a packet
// that arrives on the preferred-address socket rather than the main one is
// the client having validated that path and moved, and the server moves its
// sending there with it — no probe of its own, since the client's address
// is unchanged and was validated long ago; §9.4's reset still applies,
// because the path is new even if the peer is not.
func (c *Conn) maybeMigrate(frames []Frame, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.isClient || !c.handshakeConfirmed {
		return
	}
	addrChanged := c.rxFrom != nil && !sameAddr(c.rxFrom, c.peer)
	viaChanged := c.rxVia != nil && c.rxVia != c.pc
	if !addrChanged && !viaChanged {
		return
	}
	if !c.rxHighest || probingOnly(frames) {
		return
	}
	if addrChanged {
		c.migrateLocked(c.rxFrom, c.rxVia, now)
		return
	}
	c.pc = c.rxVia
	c.migrations++
	c.resetPathStateLocked()
}

// resetPathStateLocked is §9.4's bill for a new path: congestion window and
// round-trip estimate restart from their initial state, because the old
// path's measurements are not evidence about the new one. Callers hold mu.
func (c *Conn) resetPathStateLocked() {
	c.rtt = rttEstimator{}
	c.cc = newNewReno(c.maxDatagram)
	c.ptoCount = 0
}

// migrateLocked switches the send path to the peer's new address. Everything
// the old path proved is treated as unproven again: the address pays the
// §8.1 amplification limit until a PATH_CHALLENGE round trip validates it,
// and the congestion window and round-trip estimate restart from their
// initial state because the old path's measurements are not evidence about
// the new one (§9.4). The destination identifier moves to a never-used one
// if the peer issued any (§9.5) — retiring nothing: the old identifier may
// still be answering stragglers on the old path. via, when non-nil, is the
// local socket the move also lands on (the listener's preferred-address
// one, §9.6). Callers hold mu.
func (c *Conn) migrateLocked(to net.Addr, via net.PacketConn, now time.Time) {
	newPC := c.pc
	if via != nil {
		newPC = via
	}
	unvalidated := c.amplActive || c.pathProbe.pending
	if unvalidated && c.lastValidatedPeer != nil &&
		sameAddr(to, c.lastValidatedPeer) && newPC == c.lastValidatedPC {
		// The peer is back on the path this end last validated while the
		// new one is still unproven — the §9.3.3 outcome of a migration
		// forged from copied packets, or a peer that never really left.
		c.revertToValidatedLocked()
		return
	}
	if !unvalidated {
		// The path being left is proven: it is what §9.3.2 falls back to
		// if the new one never validates, and §9.3.3 has it challenged
		// now, under the identifier it has always used, before anything
		// moves.
		c.lastValidatedPeer, c.lastValidatedPC, c.lastValidatedSeq = c.peer, c.pc, c.currentPeerSeq
		if data, err := randomID(8); err == nil {
			copy(c.pathProbe.oldData[:], data)
			c.pathProbe.oldPending = true
			c.sendOldPathChallengeLocked()
		}
	}
	c.peer = to
	c.pc = newPC
	c.migrations++
	c.amplActive = true
	c.amplRecv = int64(c.rxDatagramLen)
	c.amplSent = 0
	c.resetPathStateLocked()
	c.switchToFreshPeerCIDLocked()
	c.startPathChallengeLocked(now)
	c.kickTimer()
}

// probePreferredAddressLocked is the client half of §9.6.2: a PATH_CHALLENGE
// to the server's preferred address under the identifier it offered for it,
// while data keeps flowing on the handshake's path. handlePathResponse
// completes the move; pathProbeDeadlineLocked abandons it. Callers hold mu.
func (c *Conn) probePreferredAddressLocked(now time.Time) {
	var cid []byte
	for i := range c.peerCIDs {
		if c.peerCIDs[i].seq == 1 {
			cid = c.peerCIDs[i].cid
		}
	}
	if cid == nil {
		return // the pool lost sequence 1 to a Retire Prior To; nothing to probe under
	}
	c.startPathChallengeLocked(now)
	if !c.pathProbe.pending {
		return
	}
	c.pathProbe.target = c.preferred
	c.pathProbe.targetCID = cid
	// startPathChallengeLocked already sent one challenge on the path in
	// use, which is harmless (the peer answers it there) but is not the
	// probe this is about; the one that matters goes to the target now.
	c.sendPathChallengeLocked()
	c.kickTimer()
}

// adoptPreferredPathLocked completes a client's move to the server's
// preferred address once its probe is answered: sends go there, under the
// offered identifier, with the path state reset (§9.4). Callers hold mu.
func (c *Conn) adoptPreferredPathLocked() {
	c.peer = c.pathProbe.target
	for i := range c.peerCIDs {
		e := &c.peerCIDs[i]
		if bytes.Equal(e.cid, c.pathProbe.targetCID) {
			e.used = true
			c.dcid = e.cid
			c.currentPeerSeq = e.seq
			c.rollSpinLocked()
		}
	}
	c.pathProbe.target, c.pathProbe.targetCID = nil, nil
	c.migrations++
	c.resetPathStateLocked()
	c.kickTimer()
}

// Rebind moves the connection onto pc: client-initiated migration (RFC
// 9000 §9), for a client whose network changed under it or that chooses to
// change it. The connection keeps every stream, its keys and its state; what
// changes is the socket its datagrams leave from and arrive on, and so the
// address the server sees them from.
//
// Rebind returns as soon as the first PATH_CHALLENGE has left on the new
// path. Validation is asynchronous, as on the server side: the peer's
// PATH_RESPONSE validates it, data written meanwhile already travels the new
// path, and a path the peer never answers on ends the connection with an
// error — there is no old path to fall back to, since the caller is about
// to close that socket. Use [Conn.Done] and [Conn.Err] to observe either
// outcome. Congestion and round-trip state restart from their initial
// values (§9.4), and sending moves to a connection identifier never used on
// the old path when the server issued one (§9.5).
//
// The old socket stays the caller's: Rebind stops reading it before it
// returns, and the caller closes it afterwards. Passing the socket already
// in use is allowed and means "the address behind this socket changed":
// the path is re-validated under a fresh identifier without replacing
// anything.
//
// Refused before the handshake is confirmed — §9 forbids migrating
// earlier — on a server, which never migrates, and on a closed connection.
func (c *Conn) Rebind(pc net.PacketConn) error {
	if pc == nil {
		return errors.New("quic: Rebind to a nil socket")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case !c.isClient:
		return errors.New("quic: a server does not migrate (RFC 9000 §9)")
	case c.incoming != nil:
		return errors.New("quic: this connection does not own its socket")
	case c.closeSent || c.draining:
		return errClosed
	case !c.handshakeConfirmed:
		return errors.New("quic: Rebind before the handshake is confirmed (RFC 9000 §9)")
	}
	select {
	case <-c.closed:
		return errClosed
	default:
	}
	old := c.pc
	c.pc = pc
	c.migrations++
	// The old path's measurements say nothing about the new one (§9.4).
	c.rtt = rttEstimator{}
	c.cc = newNewReno(c.maxDatagram)
	c.ptoCount = 0
	c.switchToFreshPeerCIDLocked()
	c.startPathChallengeLocked(time.Now())
	c.pathProbe.fatal = c.pathProbe.pending
	c.kickTimer()
	// Jolt the read loop out of the old socket: it re-reads c.pc on its
	// next iteration. Done under mu so the loop cannot re-arm a later
	// deadline on the old socket in between — it checks rebound() after
	// arming and before reading, both under this same lock.
	_ = old.SetReadDeadline(time.Now())
	return nil
}

// startPathChallengeLocked opens path validation (§8.2): eight random bytes,
// in a datagram expanded to the 1200-byte minimum (§8.2.1 — the probe is
// also an MTU probe) or as far toward it as the amplification budget allows,
// never through loss recovery: a stale challenge proves nothing, so the
// timer loop retransmits a fresh send of the same bytes on a probe-timeout
// backoff instead. Callers hold mu.
func (c *Conn) startPathChallengeLocked(now time.Time) {
	data, err := randomID(8)
	if err != nil {
		// No entropy to challenge with. The path stays unvalidated and
		// amplification-limited; the idle timer judges the rest.
		return
	}
	copy(c.pathProbe.data[:], data)
	c.pathProbe.pending = true
	c.pathProbe.fatal = false
	c.pathProbe.target, c.pathProbe.targetCID = nil, nil
	c.pathProbe.retransmits = 0
	c.pathProbe.sentAt = now
	c.pathProbe.sent, c.pathProbe.full = false, false
	c.sendPathChallengeLocked()
}

// sendPathChallengeLocked writes one PATH_CHALLENGE datagram to the current
// peer address — or, for a client probing the server's preferred address,
// to that address under the identifier offered for it — expanded to 1200
// bytes, or to what the §8.1 budget of a still-unvalidated path allows
// (§8.2.1). A migration announced by a 40-byte datagram funds a 120-byte
// probe, not a 1200-byte one; padding past the budget would withhold the
// probe outright and leave the path unvalidated for good. When the budget
// grows enough for a full-size probe, the challenge data is renewed, so
// that only an answer to a full-size datagram counts as one. Callers hold
// mu.
func (c *Conn) sendPathChallengeLocked() {
	full := c.pathProbe.target != nil || !c.amplActive || 3*c.amplRecv-c.amplSent >= 1200
	switch {
	case !c.pathProbe.sent:
		c.pathProbe.full = full
	case full && !c.pathProbe.full:
		if data, err := randomID(8); err == nil {
			copy(c.pathProbe.data[:], data)
			c.pathProbe.full = true
		}
	case !full:
		c.pathProbe.full = false
	}
	c.pathProbe.sent = true
	payload := AppendVarint(nil, framePathChallenge)
	payload = append(payload, c.pathProbe.data[:]...)
	// The control queue leaves first, in its own packet: the probe is
	// never retransmitted, and grants riding it would be lost with it.
	c.flushControlLocked()
	_ = c.sendPacketLocked(spaceApplication, payload, sendOpts{
		noRetrans: true, to: c.pathProbe.target, dcid: c.pathProbe.targetCID, padTo: 1200,
	})
}

// sendOldPathChallengeLocked sends the §9.3.3 challenge to the last
// validated path, under the identifier that path used: expanded to 1200,
// since that address is proven and no budget applies off the path in use.
// Callers hold mu.
func (c *Conn) sendOldPathChallengeLocked() {
	var dcid []byte
	for i := range c.peerCIDs {
		if c.peerCIDs[i].seq == c.lastValidatedSeq {
			dcid = c.peerCIDs[i].cid
		}
	}
	if dcid == nil {
		// Retired since: sending under any other identifier would link
		// the two paths (§9.5). The fallback on give-up still stands.
		c.pathProbe.oldPending = false
		return
	}
	payload := AppendVarint(nil, framePathChallenge)
	payload = append(payload, c.pathProbe.oldData[:]...)
	c.flushControlLocked()
	_ = c.sendPacketLocked(spaceApplication, payload, sendOpts{
		noRetrans: true, to: c.lastValidatedPeer, via: c.lastValidatedPC, dcid: dcid, padTo: 1200,
	})
}

// revertToValidatedLocked is §9.3.2's fallback: sending returns to the last
// validated peer address and socket, under the identifier used there (or a
// fresh one if the peer has retired it since — never the one the abandoned
// path saw, §9.5), with no amplification limit and the abandoned path's
// probe dropped. Path state resets once more (§9.4): the abandoned path's
// short history polluted it. Not counted as a migration — it undoes one.
// With no validated path to return to, §9.3.2 says to close silently.
// Callers hold mu.
func (c *Conn) revertToValidatedLocked() {
	c.pathProbe.pending, c.pathProbe.oldPending = false, false
	c.amplActive, c.amplRecv, c.amplSent = false, 0, 0
	if c.lastValidatedPeer == nil {
		if c.closeErr == nil {
			c.closeErr = fmt.Errorf("%w: the migrated path never validated and no validated path remains", ErrQUIC)
		}
		c.closeSent = true            // silently: §9.3.2 discards the state, it says nothing
		go func() { _ = c.Close() }() // Close takes mu; it cannot run under it
		return
	}
	c.peer, c.pc = c.lastValidatedPeer, c.lastValidatedPC
	restored := false
	for i := range c.peerCIDs {
		if e := &c.peerCIDs[i]; e.seq == c.lastValidatedSeq {
			c.dcid, c.currentPeerSeq = e.cid, e.seq
			c.rollSpinLocked()
			restored = true
		}
	}
	if !restored {
		c.switchToFreshPeerCIDLocked()
	}
	c.resetPathStateLocked()
	c.kickTimer()
}

// handlePathResponse validates the migrated path when the peer echoes the
// outstanding challenge: the amplification limit disarms and traffic flows
// at full rate. §8.2.3 deliberately accepts the answer from any address —
// what validates a path is the challenge data coming back, not where from —
// and a response that echoes nothing outstanding is ignored rather than
// fatal: it may be the answer to a probe already given up on.
func (c *Conn) handlePathResponse(f Frame) {
	c.mu.Lock()
	if c.pathProbe.oldPending && bytes.Equal(f.Data, c.pathProbe.oldData[:]) {
		// The old path is alive. Nothing moves on that alone: the peer's
		// non-probing packets there are what move the connection back.
		c.pathProbe.oldPending = false
	}
	if c.pathProbe.pending && bytes.Equal(f.Data, c.pathProbe.data[:]) {
		c.pathProbe.pending = false
		c.pathProbe.fatal = false
		c.pathProbe.oldPending = false
		c.amplActive = false
		switch {
		case c.pathProbe.target != nil:
			c.adoptPreferredPathLocked()
		case !c.pathProbe.full:
			// The address is validated, the path MTU is not: §8.2.1's
			// second validation, now that no budget stands in the way.
			c.startPathChallengeLocked(time.Now())
			c.kickTimer()
		}
	}
	c.mu.Unlock()
}

// pathProbeDeadlineLocked drives the challenge retransmission from the timer
// loop: fires a fresh send on a probe-timeout backoff while the path stays
// unvalidated, and after [pathProbeMaxRetransmits] unanswered resends stops
// probing — the limit stays armed and the idle timer owns what happens to a
// peer that never answers. Returns the next deadline, zero when none is
// armed. Callers hold mu.
func (c *Conn) pathProbeDeadlineLocked(now time.Time) time.Time {
	if !c.pathProbe.pending {
		return time.Time{}
	}
	interval := c.rtt.pto(0) << min(c.pathProbe.retransmits, 10)
	deadline := c.pathProbe.sentAt.Add(interval)
	if now.Before(deadline) {
		return deadline
	}
	if c.pathProbe.retransmits >= pathProbeMaxRetransmits {
		c.pathProbe.pending = false
		if !c.isClient {
			// A migrated path that never validated, or validated its
			// address but not 1200 bytes of MTU: either way not a path
			// QUIC can use (§9.3.2, §14).
			c.revertToValidatedLocked()
			return time.Time{}
		}
		// A preferred address the server never answered at: the
		// connection stays where it is, on a path that works (§9.6.2
		// allows the client to ignore the offer), and nothing is wrong.
		c.pathProbe.target, c.pathProbe.targetCID = nil, nil
		if c.pathProbe.fatal {
			// A client's Rebind whose new path the server never answered
			// on. The old socket is the caller's and probably closed by
			// now: nothing to fall back to, so the connection ends, with
			// the reason rather than a later idle timeout.
			c.pathProbe.fatal = false
			if c.closeErr == nil {
				c.closeErr = fmt.Errorf("quic: the path Rebind moved to never validated (%d PATH_CHALLENGEs unanswered)", pathProbeMaxRetransmits+1)
			}
			c.sendCloseLocked(transportNoError, false)
			go func() { _ = c.Close() }() // Close takes mu; it cannot run under it
		}
		return time.Time{}
	}
	c.pathProbe.retransmits++
	c.pathProbe.sentAt = now
	c.sendPathChallengeLocked()
	if c.pathProbe.oldPending {
		c.sendOldPathChallengeLocked()
	}
	return now.Add(c.rtt.pto(0) << min(c.pathProbe.retransmits, 10))
}
