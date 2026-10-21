package quic

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"net"
	"time"
)

// Connection-identifier rotation and server-side passive migration, RFC 9000
// §5.1 and §9.
//
// The shape in one paragraph: every connection issues alternative
// identifiers for itself (NEW_CONNECTION_ID) once its handshake confirms — a
// [Listener]-managed one registers them in the listener's demux table so a
// packet naming any of them still routes, one that owns its socket needs no
// table, every datagram on it being its own; every connection pools the
// identifiers its *peer* issues and honours Retire Prior To by switching
// what it sends under; and a
// server that sees a 1-RTT packet arrive from a new address follows the
// client there — new path unvalidated and amplification-limited until a
// PATH_CHALLENGE round trip proves it, congestion and round-trip state reset
// because the old path's numbers say nothing about the new one, the old path
// challenged too (§9.3.3), and a new path that never validates abandoned
// for the last validated one (§9.3.2).

// pathProbeMaxRetransmits is how many times an unanswered PATH_CHALLENGE is
// resent before this end gives up on a path. A server whose migrated path
// never validated reverts to the last validated one (RFC 9000 §9.3.2) —
// it does not kill the connection: if the peer is gone from both, the idle
// timer is the one deadline that already exists for exactly that silence.
const pathProbeMaxRetransmits = 3

// cidRegistrar is what a connection issues its additional identifiers
// through. A [Listener] gives each connection it manages the ability to add
// and remove routing entries in the shared demux table; a connection that
// owns its socket — [Dial], standalone [Accept] — gets an [ownerRegistrar],
// which routes nothing. Issuing is only a SHOULD (RFC 9000 §5.1.1), but a
// peer that got no identifier cannot follow this end across a NAT rebind:
// §9.5 forbids sending under one identifier to two destination addresses,
// so a server with no spare one cannot probe the client's new address —
// quic-go logs "skipping validation of new path … since no connection ID is
// available" and keeps sending to the address the NAT abandoned, until the
// idle timer ends the connection (the runner's rebind-port and rebind-addr
// cases, 2026-10-10, before Dial issued any).
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

// ownerRegistrar is the registrar of a connection that owns its socket —
// [Dial], standalone [Accept]. Routing is nothing: every datagram the socket
// yields belongs to this one connection whatever identifier it names, and
// receive already matches any entry of localCIDs. What remains is minting
// identifiers of scid's length (the short header is parsed at that length)
// and deriving the reset token each NEW_CONNECTION_ID must carry (§19.15)
// from a per-connection key, so none is stored. No reset is ever sent from
// here — the socket goes with the connection — which §10.3 allows: the
// token only has to be unpredictable.
type ownerRegistrar struct {
	resetKey [32]byte
}

func newOwnerRegistrar() (*ownerRegistrar, error) {
	r := &ownerRegistrar{}
	if _, err := rand.Read(r.resetKey[:]); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *ownerRegistrar) addLocalCID(c *Conn) ([]byte, error) {
	return randomID(len(c.scid))
}

func (r *ownerRegistrar) removeLocalCID([]byte) {}

func (r *ownerRegistrar) resetTokenFor(cid []byte) [16]byte {
	mac := hmac.New(sha256.New, r.resetKey[:])
	mac.Write(cid)
	var token [16]byte
	copy(token[:], mac.Sum(nil))
	return token
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
	// retireOld, with retireSeq, marks a [Conn.Rebind] probe whose success
	// retires the peer identifier the old path sent under (§5.1.2: retire
	// what is no longer used on the address it was used on). Retired on
	// validation rather than at once: a probe that fails ends the
	// connection, and until then the old identifier may still be answering
	// stragglers. The retirement is what makes the peer issue a fresh
	// identifier for the next Rebind, which §9.5 requires (ErrNoFreshConnectionID).
	retireOld bool
	retireSeq uint64
	// target and targetCID are set while a client probes the server's
	// preferred address (§9.6.2): the challenge goes there, under that
	// identifier, while everything else stays on the path in use. A
	// matching response moves the connection; silence leaves it where it
	// is, without error — the path in use still works.
	target    net.Addr
	targetCID []byte
	// via is set while a server probes the client from its preferred-address
	// socket (§9.6.2: "the server MUST probe on the path toward the client
	// from its preferred address") before the connection has moved there:
	// the challenge leaves by that socket, under targetCID, while
	// everything else still leaves by the socket in use. The client's first
	// non-probing packet on that socket moves the connection and adopts
	// the identifier; silence drops the probe and moves nothing.
	via net.PacketConn
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
// connection on the handshake's path. Internal: a caller sees the move
// through [Conn.Migrations], and only this package's tests need the
// address itself.
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
// min(the peer's active_connection_id_limit, this end's own), issuing
// each new one as a NEW_CONNECTION_ID on the control queue — which rides the
// next packet's retransmittable payload, so a lost one is resent like any
// other control frame. This end's own limit caps it whatever the peer's
// invites: each identifier is a demux-table entry, so the figure a
// deployment chose for the peer's identifiers (4 by default, enough for a
// NAT rebind and a deliberate rotation to overlap) is the one it pays for
// its own. No-op before the handshake confirms (§5.1.1 wants
// them in 1-RTT packets the peer is known able to read), or on a connection
// a test built by hand with no registrar at all.
//
// At most limit NEW_CONNECTION_ID frames wait on the queue at once. Each
// RETIRE_CONNECTION_ID the peer sends costs it two bytes and would make
// this end queue a 29-byte replacement, and the sequences are predictable,
// so one 1200-byte packet could retire four hundred identifiers — the
// control queue grew ten times faster than the peer's line. Once the queue
// holds its limit, a retirement drops the identifier without a replacement
// (§5.1.1 only asks that one SHOULD follow, and lets an endpoint limit
// what it issues); nextLocalSeq stops growing, so the next invented
// sequence is the §19.16 violation handleRetireConnectionID refuses, and a
// conformant peer, which can only retire what it was sent, never meets the
// bound. The deficit is made up when the queue drains: flushControlLocked
// tops up after every processed packet. Callers hold mu.
func (c *Conn) issueLocalCIDsLocked() {
	if c.registrar == nil || !c.handshakeConfirmed {
		return
	}
	limit := uint64(2) // the RFC 9000 §18.2 default when the peer announced nothing
	if c.hasPeerParams && c.peerParams.ActiveConnIDLimit != 0 {
		limit = c.peerParams.ActiveConnIDLimit
	}
	limit = min(limit, c.params.ActiveConnIDLimit)
	for uint64(len(c.localCIDs)) < limit && uint64(c.controlNewCIDs) < limit { //nolint:gosec // G115: a count, never negative
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
		c.controlNewCIDs++
	}
}

// handleRetireConnectionID acts on the peer retiring one of this end's
// identifiers (§19.16): the entry is dropped from the demux table and a
// replacement issued, keeping the active count — within the bound
// issueLocalCIDsLocked explains. The two violations the
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
		c.rxProgress = true
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
	c.rxProgress = true
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
// sequence among those, and falling back to a used one: its callers keep
// the local address, where §9.5's MUST NOT does not bite. Callers hold mu.
func (c *Conn) switchToFreshPeerCIDLocked() bool {
	pick := c.pickPeerCIDLocked(false)
	if pick < 0 {
		return false
	}
	c.adoptPeerCIDAtLocked(pick)
	return true
}

// pickPeerCIDLocked chooses the pooled peer identifier a switch would move
// to — never the current one; one never used on any path before one that
// was, the lowest sequence among equals — or -1 when there is none. With
// unusedOnly, a used identifier is not a candidate: Rebind sends from a new
// local address, where §9.5 forbids reusing one. Callers hold mu.
func (c *Conn) pickPeerCIDLocked(unusedOnly bool) int {
	pick := -1
	for i := range c.peerCIDs {
		e := &c.peerCIDs[i]
		if e.seq == c.currentPeerSeq || (unusedOnly && e.used) {
			continue
		}
		if pick == -1 ||
			(!e.used && c.peerCIDs[pick].used) ||
			(e.used == c.peerCIDs[pick].used && e.seq < c.peerCIDs[pick].seq) {
			pick = i
		}
	}
	return pick
}

// adoptPeerCIDAtLocked moves this end's sends to the pooled identifier at
// index i. Callers hold mu.
func (c *Conn) adoptPeerCIDAtLocked(i int) {
	e := &c.peerCIDs[i]
	e.used = true
	c.dcid = e.cid
	c.currentPeerSeq = e.seq
	// A fresh identifier starts a fresh linkability epoch; a spin value
	// carried across the rotation would tie the two (§17.4).
	c.rollSpinLocked()
}

// retirePeerCIDLocked retires one pooled peer identifier this end chose to
// stop using (§5.1.2): the RETIRE_CONNECTION_ID is queued and the entry
// leaves the pool, so nothing here sends under it again (§19.16). The
// control queue's bound on retirements is the peer's budget, counted
// against what its Retire Prior To forces; a retirement of this end's own
// that would cross it is not queued, and the identifier stays pooled as
// used — a Rebind never picks it, and the peer keeps one identifier more
// than it would. Callers hold mu.
func (c *Conn) retirePeerCIDLocked(seq uint64) {
	if seq == c.currentPeerSeq {
		return
	}
	if err := c.queueRetireLocked(seq); err != nil {
		return
	}
	kept := c.peerCIDs[:0]
	for _, e := range c.peerCIDs {
		if e.seq != seq {
			kept = append(kept, e)
		}
	}
	c.peerCIDs = kept
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

// loopbackMigration reports whether a move from addr from to addr to would
// land on a loopback address the peer was not using: a *net.UDPAddr on
// 127.0.0.0/8 or ::1 (IPv4-mapped forms unmapped, as sameAddr compares),
// reached from any other address. An address of another type is never
// loopback.
func loopbackMigration(from, to net.Addr) bool {
	return isLoopback(to) && !isLoopback(from)
}

func isLoopback(a net.Addr) bool {
	ua, ok := a.(*net.UDPAddr)
	return ok && ua.AddrPort().Addr().Unmap().IsLoopback()
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
// sending there with it — provided its own §9.6.2 probe left from that
// socket, under an identifier set aside for the path (§9.5); without one
// it stays where it is. §9.4's reset still applies, because the path is
// new even if the peer is not.
//
// A server that announced disable_active_migration still follows: §9 lets
// it either drop the packets from the new address or validate the path
// and follow, and this is the second way. The announcement tells the
// client not to try, not this server to refuse.
func (c *Conn) maybeMigrate(frames []Frame, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	addrChanged := c.rxFrom != nil && !sameAddr(c.rxFrom, c.peer)
	viaChanged := c.rxVia != nil && c.rxVia != c.pc
	if !addrChanged && !viaChanged {
		// On the path in use: the identifier this packet named is the
		// path's, whatever rotation the peer made (§5.1.2).
		c.pathLocalSeq = c.rxLocalCIDSeq
		return
	}
	if c.isClient || !c.handshakeConfirmed {
		return
	}
	if !c.rxHighest || probingOnly(frames) {
		// A probing packet from a new address moves nothing and is not
		// challenged either. RFC 9000 §9.6.3 says a server SHOULD validate
		// a client's new address on a probe from it; not taken: a client
		// that probes before it moves (§9.3.1) is answered and challenged
		// at its first non-probing packet, one round trip later, and a
		// probe on the preferred socket already draws this end's own
		// challenge (preferredPathChallengeLocked, §9.6.2) — where a
		// challenge per probing datagram would let a peer that probes from
		// everywhere make this end probe everywhere, one pathProbe at a
		// time. To revisit with a pathProbe per candidate path.
		return
	}
	if addrChanged && loopbackMigration(c.peer, c.rxFrom) {
		// RFC 9000 §21.5.6: "Endpoints SHOULD NOT allow connections or
		// migration to a loopback address if the same service was
		// previously available at a different interface" — a loopback
		// service often has no authentication of its own, and a peer
		// that forges its source as 127.0.0.1 would have this end send it
		// up to three times what it received. The packet is processed;
		// nothing leaves for the new address, and the peer that really
		// moved there is told nothing. No option to lift it (the section
		// allows one): no deployment of this package reaches a service
		// over loopback after reaching it elsewhere; to revisit on that
		// case. The section's next paragraph, on link-local and private
		// addresses, is not taken: it carries "a significant risk of
		// connectivity failure" the text itself names.
		return
	}
	if addrChanged {
		c.migrateLocked(c.rxFrom, c.rxVia, now)
		return
	}
	if c.pathProbe.via != c.rxVia && len(c.dcid) > 0 {
		// No probe went out from this socket, so no identifier is set
		// aside for it, and sending from it under the one in use would
		// link the two paths (§9.5). The connection stays where it is.
		return
	}
	// The old socket goes on being read after the move. RFC 9000 §9.6.2
	// says the server SHOULD drop newer packets received on the old
	// address; not taken: what still arrives there is the client's flight
	// toward the address it was using a round trip ago — acknowledgements
	// and data this end wants, reordered or late — and a peer that keeps
	// sending there gains nothing, since nothing is sent back from it. The
	// section's MUST is kept: non-probing packets leave from the new
	// socket only. To revisit if a measurement shows a client stuck on the
	// old address because its packets keep being answered.
	c.pc = c.rxVia
	c.pathLocalSeq = c.rxLocalCIDSeq
	if c.pathProbe.via == c.rxVia {
		// The client answered — or is answering — the probe this end
		// sent from this socket (§9.6.2); the identifier it went under
		// is the new path's from here on (§9.5).
		c.adoptProbedCIDLocked()
	}
	c.migrations++
	c.resetPathStateLocked(now)
}

// resetPathStateLocked is §9.4's bill for a new path: congestion window and
// round-trip estimate restart from their initial state, because the old
// path's measurements are not evidence about the new one. Callers hold mu.
// The datagram size restarts from 1200 with them: the old path's DPLPMTUD
// proof is not evidence about the new one either (RFC 9000 §14.3), and the
// probe leaves once the path is validated. The flight stays recorded — what
// is unacknowledged is still owed — but dated: packets sent before now were
// sent on the old path, and §9.4 says they "MUST NOT contribute to
// congestion control or RTT estimation for the new path"
// (sentTracker.pathStart).
func (c *Conn) resetPathStateLocked(now time.Time) {
	c.rtt = rttEstimator{}
	c.resetPMTULocked()
	// ECN is validated per path (§13.4.2): the new one starts its test.
	c.ecnRecv = ecnSocket(c.pc)
	c.ecn.reset(c.ecnRecv)
	c.cc = newController(c.params.Congestion, c.maxDatagram, &c.delivery)
	c.ptoCount = 0
	for i := range c.spaces {
		c.spaces[i].sent.pathStart = now
	}
}

// migrateLocked switches the send path to the peer's new address. Everything
// the old path proved is treated as unproven again: the address pays the
// §8.1 amplification limit until a PATH_CHALLENGE round trip validates it,
// and the congestion window and round-trip estimate restart from their
// initial state because the old path's measurements are not evidence about
// the new one (§9.4). The destination identifier moves to the one the
// address's challenge was answered under, if the peer probed first, else to
// a never-used one if the peer issued any (§9.5) — retiring nothing: the
// old identifier may still be answering stragglers on the old path. via,
// when non-nil, is the local socket the move also lands on (the listener's
// preferred-address one, §9.6). Callers hold mu.
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
		c.revertToValidatedLocked(now)
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
	c.pathLocalSeq = c.rxLocalCIDSeq
	c.migrations++
	c.amplActive = true
	c.amplRecv = int64(c.rxDatagramLen)
	c.amplSent = 0
	c.resetPathStateLocked(now)
	if c.answeredFrom == nil || !sameAddr(to, c.answeredFrom) || !c.adoptPeerCIDLocked(c.answeredCID) {
		c.switchToFreshPeerCIDLocked()
	}
	c.answeredFrom, c.answeredCID = nil, nil
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
	data, err := randomID(8)
	if err != nil {
		return // no entropy to challenge with; the connection stays where it is
	}
	// The challenge goes to the target only. §8.2.2: a PATH_RESPONSE
	// "validates the path on which the PATH_CHALLENGE was sent", and the
	// answer does not say which path that was. The same data sent on the
	// path in use too was answered there whenever the server could not yet
	// probe from its preferred socket (no identifier of ours had reached
	// it), and that answer moved this client to an address the server never
	// answered at: the client sent from there, the server stayed behind.
	c.startPathChallengeData(data, now)
	c.pathProbe.target = c.preferred
	c.pathProbe.targetCID = cid
	c.sendPathChallengeLocked()
	c.kickTimer()
}

// adoptPreferredPathLocked completes a client's move to the server's
// preferred address once its probe is answered: sends go there, under the
// offered identifier, with the path state reset (§9.4). Callers hold mu.
func (c *Conn) adoptPreferredPathLocked(now time.Time) {
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
	c.resetPathStateLocked(now)
	c.kickTimer()
}

// ErrMigrationDisabled is what [Conn.Rebind] returns when the server
// announced disable_active_migration (RFC 9000 §18.2) and the connection
// still sends to the address the handshake used.
var ErrMigrationDisabled = errors.New("quic: the server announced disable_active_migration (RFC 9000 §18.2)")

// ErrNoFreshConnectionID is returned by [Conn.Rebind] when the server has
// issued no connection identifier this end has never sent under: RFC 9000
// §9.5 forbids reusing one from another local address, so the connection
// stays where it is. A server issues identifiers with NEW_CONNECTION_ID —
// after the handshake, and again when this end retires the one a previous
// Rebind left behind — so the caller waits and retries; a server that
// issues none cannot be migrated to.
var ErrNoFreshConnectionID = errors.New("quic: no connection identifier never used on another path; wait for the server's NEW_CONNECTION_ID (RFC 9000 §9.5)")

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
// any other path (§9.5): without one, Rebind is refused with
// [ErrNoFreshConnectionID] and nothing changes — the caller waits for the
// server's next NEW_CONNECTION_ID and retries. Once the new path is
// validated, the identifier the old path sent under is retired (§5.1.2),
// which is what makes the server issue a replacement for the next Rebind.
//
// The old socket stays the caller's: Rebind stops reading it before it
// returns, and the caller closes it afterwards. Passing the socket already
// in use is allowed and means "the address behind this socket changed":
// the path is re-validated under a fresh identifier without replacing
// anything.
//
// Refused before the handshake is confirmed — §9 forbids migrating
// earlier — on a server, which never migrates, on a closed connection, and
// toward a peer that uses zero-length connection IDs (§9.5: the new path
// would be linked to the old by its address alone).
// Refused with [ErrMigrationDisabled] when the server announced
// disable_active_migration (§18.2), unless the connection has since moved
// to the preferred address that server offered: the parameter forbids a
// new local address only towards the address the handshake used.
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
	case len(c.dcid) == 0:
		// §9.5: with zero-length identifiers every path of the peer's is
		// told apart by address alone, so the new path is trivially linked
		// to the old and the section says not to migrate. The library's
		// own server never issues an empty identifier; a third-party one
		// may.
		return errors.New("quic: Rebind toward a peer that uses zero-length connection IDs (RFC 9000 §9.5)")
	case c.hasPeerParams && c.peerParams.DisableActiveMigration &&
		(c.preferred == nil || !sameAddr(c.preferred, c.peer)):
		// §18.2: "MUST NOT use a new local address when sending to the
		// address that the peer used during the handshake" — and sending
		// to the preferred address once the move is done (adoptPreferred-
		// PathLocked set c.peer to it) is not sending to that address.
		return ErrMigrationDisabled
	}
	select {
	case <-c.closed:
		return errClosed
	default:
	}
	// §9.5: a new local address sends under an identifier never used on
	// another one. Decided before anything moves, so a refusal leaves the
	// socket, the path state and the probe exactly as they were. Until
	// 2026-10-21 the switch's result was ignored, and a pool with nothing
	// fresh — before the server's NEW_CONNECTION_ID, or one Rebind past
	// what it issued — sent from the new socket under an identifier the
	// old path had already used.
	fresh := c.pickPeerCIDLocked(true)
	if fresh < 0 {
		return ErrNoFreshConnectionID
	}
	now := time.Now()
	old := c.pc
	c.pc = pc
	c.migrations++
	// The old path's measurements say nothing about the new one (§9.4).
	c.resetPathStateLocked(now)
	previous := c.currentPeerSeq
	c.adoptPeerCIDAtLocked(fresh)
	c.startPathChallengeLocked(now)
	c.pathProbe.fatal = c.pathProbe.pending
	c.pathProbe.retireOld, c.pathProbe.retireSeq = true, previous
	// One challenge, toward the address in use. RFC 9000 §9.6.3 says a
	// client that migrates before it has moved to the server's preferred
	// address SHOULD validate both addresses concurrently from its new
	// one; not taken: the preferred address is probed at confirmation
	// (adoptPreferredPathLocked), so a later Rebind normally finds the
	// move done, and pathProbe is one in-flight challenge — a second,
	// concurrent one is a probe per candidate path, the same change the
	// server's §9.6.3 SHOULD waits for (maybeMigrate). What the client
	// loses is the direct hop to the preferred address from its new
	// address when the two migrations race; it reaches it on the next
	// confirmation-time probe or stays on the original address, which
	// §9.6.3 allows.
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
	c.startPathChallengeData(data, now)
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
	if c.pathProbe.sent {
		// A resend carries fresh data: RFC 9000 §8.2.1 wants unpredictable
		// data in every PATH_CHALLENGE frame, and §13.3 "a different
		// payload each time". The response is matched against the latest
		// data only (handlePathResponse): a late answer to an earlier
		// challenge is ignored, and the next resend covers it. Until
		// 2026-10-21 the data was renewed only when the amplification
		// budget let the probe grow to full size.
		if data, err := randomID(8); err == nil {
			copy(c.pathProbe.data[:], data)
		}
	}
	c.pathProbe.full = full
	c.pathProbe.sent = true
	payload := AppendVarint(nil, framePathChallenge)
	payload = append(payload, c.pathProbe.data[:]...)
	// The control queue leaves first, in its own packet: the probe is
	// never retransmitted, and grants riding it would be lost with it.
	c.flushControlLocked()
	_ = c.sendPacketLocked(spaceApplication, payload, sendOpts{
		noRetrans: true, to: c.pathProbe.target, via: c.pathProbe.via, dcid: c.pathProbe.targetCID, padTo: 1200,
	})
}

// answerPathChallengesLocked sends the PATH_RESPONSE frames one packet's
// challenges earned — responses, already encoded back to back — in one
// datagram to the address the packet came from (§9.3: back to the source,
// which during a migration is not the path in use), exactly once: never
// via loss recovery, since the peer retries with a fresh challenge and a
// stale response proves nothing (§8.2.2). Hence a packet of its own,
// noRetrans, with the control queue flushed first so no grant hitches a
// ride out of loss recovery.
//
// §8.2.2: the datagram MUST be expanded to 1200 bytes — the probe is also
// an MTU probe — except past the anti-amplification limit, which the
// section itself says happens only when the challenge did not arrive
// expanded. On the path in use the §8.1 books cover the send; toward any
// other address the books are Conn.offPath, seeded by the challenge's own
// datagram: a ≥1200-byte challenge earns a 1200-byte answer, a small one
// gets a small one — never a 1200-byte reflection of a few spoofed-source
// bytes — and writePacketLocked keeps the sum under three times what that
// address sent. Nine bytes per response, so one packet answers a packet
// full of challenges; a larger packet's responses take as many packets as
// needed, the first one expanded. Callers hold mu.
func (c *Conn) answerPathChallengesLocked(responses []byte, now time.Time) {
	c.flushControlLocked()
	onPath := c.rxFrom == nil || sameAddr(c.rxFrom, c.peer)
	padTo := 0
	if onPath || c.rxDatagramLen >= 1200 {
		padTo = 1200
	}
	// A challenge that reached a server's preferred-address socket is
	// answered with this end's own probe of that path in the same packet
	// (§9.6.2), under the identifier that path gets.
	probe, dcid := c.preferredPathChallengeLocked(now)
	if c.rxVia != nil && c.rxVia != c.pc && dcid == nil && len(c.dcid) > 0 {
		// §9.5: "An endpoint MUST NOT reuse a connection ID when sending
		// from more than one local address". The challenge arrived on a
		// socket the connection does not send from, and the client has
		// given no identifier that path could take: nothing leaves that
		// socket. The client's validation of the path fails, and §9.6.1
		// keeps it on the original address — a stay, not a fault. Recorded
		// in ADR 0015, against §19.17's MUST.
		return
	}
	if dcid == nil && !onPath && len(c.dcid) > 0 {
		dcid = c.newPathCIDLocked()
	}
	const responseLen = 1 + 8 // type, data
	per := c.maxStreamChunk() / responseLen * responseLen
	for len(responses) > 0 {
		n := min(len(responses), per)
		payload := responses[:n:n]
		if len(probe) > 0 {
			payload = append(payload, probe...)
			probe = nil
		}
		_ = c.sendPacketLocked(spaceApplication, payload, sendOpts{noRetrans: true, to: c.rxFrom, via: c.rxVia, dcid: dcid, padTo: padTo})
		responses = responses[n:]
		padTo = 0
	}
}

// preferredPathChallengeLocked is the server half of §9.6.2, taken when a
// PATH_CHALLENGE arrives on the preferred-address socket from the client's
// address while the connection still sends from the main one: the client
// is validating the path the server offered, and the server "MUST probe on
// the path toward the client from its preferred address". The challenge
// rides in the same packet as the answer to the client's — the first
// datagram this end sends from that socket carries both — under a peer
// identifier never used on another path (§9.5: a new local address needs
// a new identifier). Returns the frame to append and the identifier to
// send under, both nil when nothing is to be probed: a client, an
// unconfirmed handshake, a challenge that arrived on the socket in use or
// from a new address (the latter is a migration, §9.3's business), or no
// pooled identifier for the path — the caller then sends nothing from that
// socket, unless the client uses zero-length identifiers, which every path
// shares (§5.1.1). A probe from this socket already pending has its data
// repeated, since the client's repeat means the first answer was lost.
// Callers hold mu.
func (c *Conn) preferredPathChallengeLocked(now time.Time) (frame, dcid []byte) {
	if c.isClient || !c.handshakeConfirmed || c.rxVia == nil || c.rxVia == c.pc ||
		(c.rxFrom != nil && !sameAddr(c.rxFrom, c.peer)) {
		return nil, nil
	}
	if c.pathProbe.pending && c.pathProbe.via == c.rxVia {
		return append(AppendVarint(nil, framePathChallenge), c.pathProbe.data[:]...), c.pathProbe.targetCID
	}
	data, err := randomID(8)
	if err != nil {
		return nil, nil
	}
	cid, _, ok := c.pickFreshPeerCIDLocked()
	if !ok && len(c.dcid) > 0 {
		return nil, nil
	}
	c.startPathChallengeData(data, now)
	c.pathProbe.via, c.pathProbe.targetCID = c.rxVia, cid
	c.pathProbe.sent, c.pathProbe.full = true, true
	return append(AppendVarint(nil, framePathChallenge), data...), cid
}

// newPathCIDLocked chooses the peer identifier a PATH_RESPONSE to an
// address other than the path's goes under. §9.5: a peer that probes a new
// path does so under a new identifier of this end, and the answer MUST NOT
// name an identifier already used toward another address, or an observer
// links the two paths — so the answer takes a pooled identifier never used
// on any path, remembered so that a repeat of the challenge, and the
// migration that follows, use the same one. The section's exception is the
// NAT rebinding: the challenge named the identifier the path in use
// already does, and the current identifier may serve. A client probing the
// server's preferred address answers under the identifier set aside for
// it (§9.6.2). An empty pool — the peer issued none beyond the one in use
// — answers under the current identifier rather than not at all: §9.3
// owes the response, and a conformant peer, which issues identifiers up
// to active_connection_id_limit before probing, never gets here. Returns
// nil for "the one in use". Callers hold mu.
func (c *Conn) newPathCIDLocked() []byte {
	if c.pathProbe.target != nil && sameAddr(c.rxFrom, c.pathProbe.target) {
		return c.pathProbe.targetCID
	}
	if c.rxLocalCIDSeq == c.pathLocalSeq {
		return nil
	}
	if c.answeredFrom != nil && sameAddr(c.rxFrom, c.answeredFrom) {
		for i := range c.peerCIDs {
			if bytes.Equal(c.peerCIDs[i].cid, c.answeredCID) {
				return c.answeredCID
			}
		}
		// Retired since the first answer: this one takes another.
	}
	cid, _, ok := c.pickFreshPeerCIDLocked()
	if !ok {
		return nil
	}
	c.answeredFrom, c.answeredCID = c.rxFrom, cid
	return cid
}

// startPathChallengeData arms the probe state around data without sending
// anything: the caller is putting the challenge in a packet of its own.
// Callers hold mu.
func (c *Conn) startPathChallengeData(data []byte, now time.Time) {
	copy(c.pathProbe.data[:], data)
	c.pathProbe.pending = true
	c.pathProbe.fatal = false
	c.pathProbe.target, c.pathProbe.targetCID, c.pathProbe.via = nil, nil, nil
	c.pathProbe.retransmits = 0
	c.pathProbe.sentAt = now
	c.pathProbe.sent, c.pathProbe.full = false, false
}

// pickFreshPeerCIDLocked marks as used and returns the pooled peer identifier
// of lowest sequence never used on any path, other than the one in use —
// without switching to it. Callers hold mu.
func (c *Conn) pickFreshPeerCIDLocked() (cid []byte, seq uint64, ok bool) {
	pick := -1
	for i := range c.peerCIDs {
		e := &c.peerCIDs[i]
		if e.used || e.seq == c.currentPeerSeq {
			continue
		}
		if pick == -1 || e.seq < c.peerCIDs[pick].seq {
			pick = i
		}
	}
	if pick == -1 {
		return nil, 0, false
	}
	e := &c.peerCIDs[pick]
	e.used = true
	return e.cid, e.seq, true
}

// adoptProbedCIDLocked makes the identifier a server probed the client under
// from its preferred socket the one in use, now that the connection sends
// from that socket. Callers hold mu.
func (c *Conn) adoptProbedCIDLocked() {
	c.adoptPeerCIDLocked(c.pathProbe.targetCID)
	c.pathProbe.targetCID, c.pathProbe.via = nil, nil
}

// adoptPeerCIDLocked moves this end's sends to the pooled peer identifier
// given — one already set aside for the path being moved to — and reports
// whether it is still in the pool. Callers hold mu.
func (c *Conn) adoptPeerCIDLocked(cid []byte) bool {
	for i := range c.peerCIDs {
		if e := &c.peerCIDs[i]; bytes.Equal(e.cid, cid) {
			c.dcid, c.currentPeerSeq = e.cid, e.seq
			c.rollSpinLocked()
			return true
		}
	}
	return false
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
func (c *Conn) revertToValidatedLocked(now time.Time) {
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
	c.resetPathStateLocked(now)
	c.kickTimer()
}

// handlePathResponse validates the migrated path when the peer echoes the
// outstanding challenge: the amplification limit disarms and traffic flows
// at full rate. §8.2.3 deliberately accepts the answer from any address —
// what validates a path is the challenge data coming back, not where from —
// and a response that echoes nothing outstanding is ignored rather than
// fatal: it may be the answer to a probe already given up on.
func (c *Conn) handlePathResponse(f Frame, now time.Time) {
	c.mu.Lock()
	if c.pathProbe.oldPending && bytes.Equal(f.Data, c.pathProbe.oldData[:]) {
		// The old path is alive. Nothing moves on that alone: the peer's
		// non-probing packets there are what move the connection back.
		c.pathProbe.oldPending = false
		c.rxProgress = true
	}
	if c.pathProbe.pending && bytes.Equal(f.Data, c.pathProbe.data[:]) {
		c.rxProgress = true
		c.pathProbe.pending = false
		c.pathProbe.fatal = false
		c.pathProbe.oldPending = false
		c.amplActive = false
		if c.pathProbe.retireOld {
			// The new path answers: the identifier the old one sent under
			// has no address left to serve (§5.1.2), and retiring it is
			// what makes the peer issue the next one.
			c.pathProbe.retireOld = false
			c.retirePeerCIDLocked(c.pathProbe.retireSeq)
		}
		switch {
		case c.pathProbe.target != nil:
			c.adoptPreferredPathLocked(now)
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
	// RFC 9000 §9.4: the timer is RFC 9002 §6.2.1's PTO, max_ack_delay
	// included as in the application space — "MUST NOT be more aggressive"
	// leaves no room to drop the term because a PATH_RESPONSE is not
	// delayed the way an ACK is. Until 2026-10-21 the term was zero.
	interval := c.rtt.pto(c.peerMaxAckDelayLocked()) << min(c.pathProbe.retransmits, 10)
	deadline := c.pathProbe.sentAt.Add(interval)
	if now.Before(deadline) {
		return deadline
	}
	if c.pathProbe.retransmits >= pathProbeMaxRetransmits {
		c.pathProbe.pending = false
		if c.pathProbe.via != nil {
			// A server's probe from its preferred socket that the client
			// never answered, while the client never moved either: the
			// connection is where it always was, on the socket in use,
			// and nothing is wrong (§9.6.2 lets the client stay).
			c.pathProbe.via, c.pathProbe.targetCID = nil, nil
			return time.Time{}
		}
		if !c.isClient {
			// A migrated path that never validated, or validated its
			// address but not 1200 bytes of MTU: either way not a path
			// QUIC can use (§9.3.2, §14).
			c.revertToValidatedLocked(now)
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
	return now.Add(c.rtt.pto(c.peerMaxAckDelayLocked()) << min(c.pathProbe.retransmits, 10)) // §9.4, as above
}
