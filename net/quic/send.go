package quic

import (
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"time"
)

// The send path. Every packet leaves through sendPacketLocked, which is what
// makes the bookkeeping single-sourced: packet numbers, in-flight accounting,
// retransmission state, acknowledgement piggybacking and Initial padding all
// happen in one place, under the connection's lock.

// sendOpts is what a caller declares about the payload it is sending.
type sendOpts struct {
	// waitCwnd blocks until the congestion window has room. Stream data
	// waits; control frames, acknowledgements and probes do not — an ACK is
	// how the window reopens, and a probe is how a stall is detected, so
	// making either wait would be the deadlock it exists to break.
	waitCwnd bool
	// deadline bounds that wait (Stream.SetWriteDeadline); zero waits
	// without bound.
	deadline time.Time
	// ackOnly marks a packet that elicits no acknowledgement and is not
	// counted in flight (RFC 9002 §2).
	ackOnly bool
	// carriesAck says an ACK frame rode along with the payload: what
	// completes this end's answer to the peer's key update (RFC 9001 §6.2).
	carriesAck bool
	// noRetrans drops the payload from loss recovery: a probe resends bytes
	// that are already queued elsewhere, and recording them again would
	// duplicate them on the next loss.
	noRetrans bool
	// to overrides the destination address, nil meaning the peer's current
	// one. Only a PATH_RESPONSE uses it (RFC 9000 §9.3: the answer goes
	// back to the address the challenge came from, which during a
	// migration is not the path in use).
	to net.Addr
	// via overrides the socket the datagram leaves from, nil meaning the
	// connection's own. Only a PATH_RESPONSE uses it: §8.2.2 wants the
	// answer on the path the challenge arrived by, and on a server with a
	// preferred address (§9.6) that may be the other socket.
	via net.PacketConn
	// dcid overrides the destination identifier a short header names, nil
	// meaning the one in use. Only a client's probe of the server's
	// preferred address uses it: that path is reached under the identifier
	// the server offered for it (§9.6.1), while the path in use keeps its
	// own.
	dcid []byte
	// own declares the payload freshly allocated and never touched again by
	// the caller, so the retransmission record may keep it instead of
	// copying it — one whole-payload copy per packet on the hot paths that
	// build their frame per call (Stream.Write, the retransmission queue).
	own bool
	// padTo expands a 1-RTT datagram with PADDING to this size — or, on a
	// path still under the §8.1 limit, to as much of it as the budget
	// allows. PATH_CHALLENGE and PATH_RESPONSE use it: §8.2.1 wants them
	// expanded "unless the anti-amplification limit does not permit", and
	// a probe padded past the budget is not sent at all.
	padTo int
	// pmtuProbe marks a DPLPMTUD probe (pmtu.go): recorded in flight so its
	// acknowledgement proves the size and its loss moves to the next try,
	// but released from flight without a congestion event when lost.
	pmtuProbe bool
}

// maxStreamChunk is how much stream payload fits one packet, leaving room
// for the short header, the frame header and the AEAD tag.
func (c *Conn) maxStreamChunk() int {
	return c.maxDatagram - 64
}

// sendPacketLocked seals payload as one packet in space's level and writes it
// as one datagram. Callers hold mu.
func (c *Conn) sendPacketLocked(space int, payload []byte, opts sendOpts) error {
	if c.draining || c.closeSent {
		return errClosed
	}
	sp := &c.spaces[space]
	if sp.discarded || sp.sealer == nil {
		return fmt.Errorf("%w: nothing to seal packets with at level %d", ErrQUIC, space)
	}

	if opts.waitCwnd {
		for {
			ready := c.cc.canSend(len(payload) + 64)
			if ready {
				// The window has room; the pacer decides *when*. A deficit
				// arms a deadline the timer loop answers with a broadcast —
				// an ACK arriving sooner broadcasts too, and the loop simply
				// re-prices the wait.
				now := time.Now()
				rate := c.cc.pacingRate(c.rtt.current())
				c.pacer.refill(now, rate)
				d := c.pacer.delay(rate)
				if d == 0 {
					break
				}
				if until := now.Add(d); c.paceDeadline.IsZero() || until.Before(c.paceDeadline) {
					c.paceDeadline = until
					c.kickTimer()
				}
			}
			if err := c.waitForCreditLocked(opts.deadline); err != nil {
				return err
			}
			if c.draining || c.closeSent {
				return errClosed
			}
			if sp.discarded || sp.sealer == nil {
				return fmt.Errorf("%w: the keys for level %d were discarded while waiting", ErrQUIC, space)
			}
		}
	}

	// Anything owed rides along: this space's acknowledgement, and the
	// grant and control queues (application space) — as much of them as
	// fits beside the payload under maxDatagram, cut on a frame boundary;
	// the rest waits for the next packet, or for flushControlLocked, which
	// sends packet after packet until the queues are empty. Grants ride any
	// packet — their cwnd exemption is theirs, not the carrier's — while
	// the control queue rides only when the window has room for the extra
	// bytes: a cwnd-exempt carrier (a grant flush, a probe) must not
	// smuggle past the window what flushControlLocked just deferred for
	// lacking it.
	//
	// A noRetrans packet carries neither: nothing records it for recovery,
	// so frames riding it would be gone for good if it were lost — or
	// withheld by the amplification budget, which skips such a packet
	// outright. They stay queued for the next packet that recovery covers.
	//
	// The ACK is encoded first — its size decides the room left — and
	// settled last: an empty call from flushControlLocked, when the window
	// has no room for the control queue, sends nothing and owes it still.
	now := time.Now()
	var ack []byte
	if sp.ack.owes(now) {
		ack = sp.ack.encodeAck(c.ackScratch[:0], now)
		c.ackScratch = ack
		// An ACK that does not fit beside the payload under maxDatagram
		// leaves ahead in a packet of its own, as flushAcksLocked would
		// send it. A peer that opens maxAckRanges gaps makes the frame a
		// few hundred bytes, and a full stream chunk (maxStreamChunk)
		// leaves room for the header, the tag and a short ACK, not that
		// one: carried anyway, the datagram would exceed what the path
		// and the peer accept (§14.1 — the peer may discard it outright).
		// The chunk is not cut, so a full chunk stays one packet; the
		// extra datagram is the price of the peer's gaps.
		if len(payload) > 0 && c.packetOverhead(space)+len(ack)+len(payload) > c.maxDatagram {
			_ = c.writePacketLocked(sp, space, sp.ack.appendAck(nil, now), nil, sendOpts{ackOnly: true}, now)
			ack = nil
		}
	}
	retransPart := payload
	if space == spaceApplication && !opts.noRetrans && (len(c.grants) > 0 || len(c.control) > 0) {
		room := c.maxDatagram - c.shortPacketOverhead() - len(ack) - len(payload)
		if queued := c.takeQueuedLocked(room, len(payload)); len(queued) > 0 {
			retransPart = append(queued, payload...)
			// Control frames are cheap to lose only if someone resends
			// them; they carry window and stream grants, so they are
			// retransmittable and ride the same recovery as the payload.
			payload = retransPart
		}
	}
	if len(payload) == 0 {
		return nil
	}
	if len(ack) > 0 {
		payload = append(ack[:len(ack):len(ack)], payload...)
		sp.ack.settle()
		opts.carriesAck = true
	}

	return c.writePacketLocked(sp, space, payload, retransPart, opts, now)
}

// shortPacketOverhead is what a 1-RTT packet costs on the wire beyond its
// payload: the short header, the packet number and the AEAD tag.
func (c *Conn) shortPacketOverhead() int {
	const pnLen = 4
	return 1 + len(c.dcid) + pnLen + 16
}

// packetOverhead is what a packet of space costs on the wire beyond its
// payload: its header, the packet number and the AEAD tag. A long header's
// Length field is counted at two bytes, which covers any packet under
// maxDatagram.
func (c *Conn) packetOverhead(space int) int {
	if space == spaceApplication {
		return c.shortPacketOverhead()
	}
	const pnLen = 4
	n := 1 + 4 + 1 + len(c.dcid) + 1 + len(c.scid) + 2 + pnLen + 16
	if space == spaceInitial {
		n += varintBytes(uint64(len(c.retryToken))) + len(c.retryToken)
	}
	return n
}

// takeQueuedLocked lifts what fits in room bytes off the grant queue, then
// the control queue, whole frames only, and returns it as a fresh slice —
// nil when nothing fits or the congestion window refuses the control bytes
// (carried is what the packet already holds, counted against the window
// with them). Both queues are this end's own encoding, so cutting them is
// a walk with the one frame parser. Callers hold mu.
func (c *Conn) takeQueuedLocked(room, carried int) []byte {
	var out []byte
	if n, _, _ := cutFrames(c.grants, room); n > 0 {
		out = append(out, c.grants[:n]...)
		c.grants = c.grants[n:]
		if len(c.grants) == 0 {
			c.grants = nil
		}
		room -= n
	}
	if n, retires, newCIDs := cutFrames(c.control, room); n > 0 && c.cc.canSend(carried+len(out)+n+64) {
		out = append(out, c.control[:n]...)
		c.control = c.control[n:]
		c.controlRetires -= retires
		c.controlNewCIDs -= newCIDs
		if len(c.control) == 0 {
			c.control, c.controlRetires, c.controlNewCIDs = nil, 0, 0
		}
	}
	return out
}

// cutFrames is the length of the longest prefix of q that ends on a frame
// boundary and is no longer than room, with how many RETIRE_CONNECTION_ID
// and NEW_CONNECTION_ID frames that prefix holds — the two queue counters
// that must follow the frames out. The whole queue when it fits, without a
// walk; q holds frames this end encoded, so the parse cannot fail, and a
// failure would only leave the rest queued.
func cutFrames(q []byte, room int) (n, retires, newCIDs int) {
	if len(q) <= room {
		return len(q), 0, 0
	}
	for n < len(q) {
		f, rest, err := parseFrame(q[n:])
		if err != nil {
			return n, retires, newCIDs
		}
		l := len(q) - n - len(rest)
		if n+l > room {
			return n, retires, newCIDs
		}
		n += l
		switch f.Type {
		case frameRetireConnectionID:
			retires++
		case frameNewConnectionID:
			newCIDs++
		}
	}
	return n, retires, newCIDs
}

// initialPadding is how many zero bytes an Initial packet's payload needs so
// the full datagram reaches 1200 bytes (RFC 9000 §14.1), given the header
// already written, the packet number's width, and the payload's length
// before padding — all excluding the length field and the 16-byte AEAD tag,
// which this adds itself.
func initialPadding(headerLen, pnLen, payloadLen int) int {
	return initialPaddingAfter(0, headerLen, pnLen, payloadLen)
}

// initialPaddingAfter is initialPadding for a packet that closes a datagram
// already holding before bytes of other packets (§12.2 coalescing): the
// padding tops the whole datagram up to 1200, not this packet alone.
//
// The length field's own width depends on what it is about to describe,
// which depends on how much padding closes the gap to 1200 — a circularity
// solved by a fixed point: guess a width, size the padding against it, and
// re-check the width the padded total would actually need. There is at most
// one varint threshold (64, 16384, ...) between "no padding" and 1200
// bytes, so this settles in at most two passes; the loop bounds it anyway
// rather than trust that argument blindly.
func initialPaddingAfter(before, headerLen, pnLen, payloadLen int) int {
	width := varintBytes(uint64(pnLen + payloadLen + 16)) //nolint:gosec // G115: a sum of packet lengths, positive
	for range 4 {
		pad := max(1200-(before+headerLen+width+pnLen+payloadLen+16), 0)
		if got := varintBytes(uint64(pnLen + payloadLen + pad + 16)); got != width { //nolint:gosec // G115: a sum of packet lengths, positive
			width = got
			continue
		}
		return pad
	}
	panic("quic: initialPadding did not converge")
}

// pendingPacket is a packet built but not yet sealed: everything
// takePNLocked consumes the next packet number of sp. In the application
// space it leaves a number unused about once per congestion window (RFC
// 9000 §21.4), so that a peer acknowledging numbers it never received
// can be caught; see sentTracker.skipNumber. The gap stays a number short
// of §12.3's 2^62-1 ceiling, which writePacketLocked checked just above.
func (c *Conn) takePNLocked(sp *space, space int) uint64 {
	if space == spaceApplication && sp.nextPN+1 < 1<<62-1 {
		window := uint64(10) // RFC 9002 §7.2's initial window, for a connection not yet set up
		if cwnd := c.cc.window(); c.maxDatagram > 0 && cwnd > 0 {
			window = uint64(cwnd / c.maxDatagram) //nolint:gosec // G115: both are positive
		}
		if sp.sent.skipNumber(sp.nextPN, window) {
			sp.nextPN++
		}
	}
	pn := sp.nextPN
	sp.nextPN++
	return pn
}

// writePacketLocked decided — header up to the Length field, payload,
// packet number, destination — waiting for the one thing it cannot know
// alone, how much §14.1 padding it carries. When Initial and Handshake
// packets coalesce into one datagram (§12.2), only the packet that closes
// the datagram pads, and only once the datagram's size is known.
type pendingPacket struct {
	sp          *space
	space       int
	prefix      []byte // the long header up to, not including, Length
	payload     []byte
	retransPart []byte
	pn          uint64
	opts        sendOpts
	now         time.Time
	dest        net.Addr
	onPath      bool
}

// gatherLocked turns datagram coalescing on or off. On: writePacketLocked
// accumulates Initial and Handshake packets into one datagram and writes
// nothing. Off: whatever gathered is flushed, padded as §14.1 asks, in one
// write. The receive-and-pump sequence runs with it on, so the ACK a
// datagram provokes and the CRYPTO flight TLS answers it with leave
// together — where before, each padded itself to 1200 bytes and left
// alone. Callers hold mu.
func (c *Conn) gatherLocked(on bool) {
	c.gather = on
	if !on {
		c.flushDatagramLocked()
	}
}

// gatheredLenLocked is the size the gathered datagram would have if sealed
// now, padding excluded. Callers hold mu.
func (c *Conn) gatheredLenLocked() int {
	n := len(c.dgram)
	if p := c.dgramPend; p != nil {
		n += sealedLen(len(p.prefix), len(p.payload), 0)
	}
	return n
}

// sealedLen is a long-header packet's size on the wire: header prefix,
// Length field, packet number, payload with pad, AEAD tag.
func sealedLen(prefixLen, payloadLen, pad int) int {
	const pnLen = 4
	return prefixLen + varintBytes(uint64(pnLen+payloadLen+pad+16)) + pnLen + payloadLen + pad + 16 //nolint:gosec // G115: a sum of packet lengths, positive
}

// cryptoRoomLocked is how much CRYPTO payload still fits beside what the
// gathered datagram holds, header and frame overhead deducted — so a
// Handshake chunk sized to it fills the room an Initial's padding would
// otherwise waste. Zero when not gathering or when the room is too small
// to be worth a packet; the caller then takes its ordinary chunk, which
// starts a new datagram. Callers hold mu.
func (c *Conn) cryptoRoomLocked() int {
	if !c.gather || (len(c.dgram) == 0 && c.dgramPend == nil) {
		return 0
	}
	// Long header: first byte, version, two length-prefixed identifiers, a
	// token length (Initial only; Handshake carries none, but one byte of
	// slack is cheaper than a branch), a two-byte Length, four-byte packet
	// number, 16-byte tag; then the CRYPTO frame's type, offset and length
	// varints at their widest plausible.
	overhead := 1 + 4 + 1 + len(c.dcid) + 1 + len(c.scid) + 1 + 2 + 4 + 16 + 1 + 8 + 4
	room := c.maxDatagram - c.gatheredLenLocked() - overhead
	if room < 32 {
		return 0
	}
	return room
}

// flushDatagramLocked seals the pending packet — with the §14.1 padding when
// an Initial in the datagram requires it — and writes the datagram. Callers
// hold mu.
func (c *Conn) flushDatagramLocked() {
	if c.dgramPend == nil && len(c.dgram) == 0 {
		return
	}
	c.flushBatchLocked() // what was sent before leaves before
	if p := c.dgramPend; p != nil {
		c.dgramPend = nil
		pad := 0
		if c.dgramPad {
			pad = initialPaddingAfter(len(c.dgram), len(p.prefix), 4, len(p.payload))
		}
		c.dgram, _ = c.sealLocked(c.dgram, p, pad)
	}
	if len(c.dgram) > 0 {
		_ = c.writeLocked(c.pc, c.dgram, c.peer)
	}
	c.dgram = c.dgram[:0]
	c.dgramPad = false
}

// writePacketLocked is the wire half of sendPacketLocked: header, seal,
// padding, the datagram write, and the recovery bookkeeping — or, while
// gathering, everything but the seal and the write, which
// flushDatagramLocked finishes for the whole datagram at once.
func (c *Conn) writePacketLocked(sp *space, space int, payload, retransPart []byte, opts sendOpts, now time.Time) error {
	const pnLen = 4
	// Only Initial and Handshake packets coalesce, and never a
	// CONNECTION_CLOSE, which must leave now. Anything else first flushes
	// what gathered, so order on the wire is the order of sending.
	gathering := c.gather && space != spaceApplication && !c.closeSent
	if !gathering {
		c.flushDatagramLocked()
	}
	// The header and the sealed packet live in per-connection scratch:
	// both are consumed before this function returns — the header copied
	// into the sealed packet, the packet handed to WriteTo, which may not
	// retain it — and mu serialises every writer, so neither is ever alive
	// twice. Two allocations per packet, measured away.
	header := c.hdrScratch[:0]
	defer func() { c.hdrScratch = header[:0] }()
	needPad := false
	if space == spaceApplication {
		header = append(header, 0x40|c.spin|c.ku.phase|byte(pnLen-1)) // short header, fixed bit set, §17.4 spin, RFC 9001 §6 key phase
		if opts.dcid != nil {
			header = append(header, opts.dcid...)
		} else {
			header = append(header, c.dcid...)
		}
	} else {
		typ := byte(packetInitial)
		if space == spaceHandshake {
			typ = packetHandshake
		}
		header = append(header, 0xc0|typ<<4|byte(pnLen-1), 0, 0, 0, 1, byte(len(c.dcid))) //nolint:gosec // G115: a connection ID is at most 20 bytes; version 1
		header = append(header, c.dcid...)
		header = append(header, byte(len(c.scid))) //nolint:gosec // G115: a connection ID is at most 20 bytes
		header = append(header, c.scid...)
		if typ == packetInitial {
			header = AppendVarint(header, uint64(len(c.retryToken)))
			header = append(header, c.retryToken...)
		}
		// An Initial must leave in a 1200-byte datagram (RFC 9000 §14.1) —
		// clients expand *every* datagram carrying one (ACKs and closes
		// included: a conformant server discards the smaller ones, and a
		// discarded ACK is a whole flight spuriously retransmitted), servers
		// the ack-eliciting ones, so path MTU is proven both ways. The
		// padding is PADDING frames *inside* a packet: zeros appended after
		// one would be parsed as a next packet and dropped as garbage. Alone,
		// the Initial pads itself; coalesced, the packet closing the datagram
		// does (flushDatagramLocked), so Handshake payload fills the room.
		needPad = typ == packetInitial && (c.isClient || !opts.ackOnly)
		if needPad && !gathering {
			if pad := initialPadding(len(header), pnLen, len(payload)); pad > 0 {
				payload = append(payload, make([]byte, pad)...)
			}
		}
	}
	// Where this datagram goes: the peer's current address, unless the
	// caller is answering a PATH_CHALLENGE back to the address it came from.
	dest := c.peer
	onPath := true
	if opts.to != nil && !sameAddr(opts.to, c.peer) {
		dest, onPath = opts.to, false
	}
	// An off-path destination has books of its own when it is the address
	// the datagram in hand came from (Conn.offPath): what goes there is
	// bounded like any unvalidated address. A client's probe of the
	// server's preferred address is off the path too, but that address
	// sent nothing yet and is not being answered — it is not bounded.
	budgeted := !onPath && c.offPath.addr != nil && sameAddr(dest, c.offPath.addr)
	if opts.padTo > 0 && space == spaceApplication {
		target := opts.padTo
		if c.amplActive && onPath {
			target = min(target, int(3*c.amplRecv-c.amplSent))
		}
		if budgeted {
			target = min(target, int(3*c.offPath.recv-c.offPath.sent))
		}
		if pad := target - (len(header) + pnLen + len(payload) + 16); pad > 0 {
			payload = append(payload, make([]byte, pad)...)
		}
	}
	// The datagram's size on the wire, for the amplification check: known
	// exactly before sealing. While gathering, the padding a closing Initial
	// will add is still to come — counted as the room it fills.
	size := len(header) + pnLen + len(payload) + 16
	if space != spaceApplication {
		size = sealedLen(len(header), len(payload), 0)
		if gathering && needPad {
			size = max(size, 1200-c.gatheredLenLocked())
		}
	}
	// RFC 9000 §8.1: this end has not validated the peer's address, so it
	// may not send more than three times what that address has sent it —
	// the limit that keeps an unvalidated client from turning this server
	// into traffic aimed at somebody else. The accounting is the current
	// path's; an off-path PATH_RESPONSE answers a challenge inside an
	// authenticated packet, which only the peer can produce — not an
	// amplification primitive.
	if c.amplActive && onPath && c.amplSent+int64(size) > 3*c.amplRecv {
		// The packet is withheld, not dropped. Handshake bytes exist only
		// here — crypto/tls does not re-emit them — so losing them would
		// kill every handshake whose first flight overflows the budget,
		// which a real certificate chain does. The payload goes back to
		// the front of the retransmission queue, and retransmitLocked
		// sends it once the peer's next datagram raises amplRecv.
		// Acknowledgements and probes regenerate from current state, so
		// they are simply skipped. No packet number was consumed.
		if !opts.ackOnly && !opts.noRetrans && len(retransPart) > 0 {
			sp.retrans = append(sp.retrans, nil)
			copy(sp.retrans[1:], sp.retrans)
			if opts.own {
				sp.retrans[0] = retransPart
			} else {
				sp.retrans[0] = append([]byte(nil), retransPart...)
			}
		}
		return nil
	}
	if budgeted && c.offPath.sent+int64(size) > 3*c.offPath.recv {
		// Past what the off-path address has earned: nothing leaves. The
		// only thing sent there is a PATH_RESPONSE, which regenerates — the
		// peer sends another challenge if it still needs one (§8.2.2).
		return nil
	}

	// RFC 9000 §12.3: a sender whose packet number reaches 2^62-1 closes
	// without a CONNECTION_CLOSE — there is no number left to send one
	// under. Key update renews keys, never numbers, so this guard is its
	// own.
	if sp.nextPN >= 1<<62-1 {
		err := fmt.Errorf("%w: the packet-number space is exhausted", ErrQUIC)
		if c.closeErr == nil {
			c.closeErr = err
		}
		go func() { _ = c.Close() }() // Close takes mu; it cannot run under it
		return err
	}

	// RFC 9001 §6.6: AES-GCM keys are spent after 2^23 sealed packets. At
	// the 1-RTT level the sender MUST rotate before that (§6), which it
	// may once the handshake is confirmed and a packet under the current
	// keys was acknowledged (§6.1) — the §6.5 wait is a SHOULD, and the
	// limit a MUST, so it does not apply here. Where rotation is not yet
	// permitted, or at the levels whose keys never rotate, the connection
	// ends with AEAD_LIMIT_REACHED instead of degrading the cipher. The
	// check skips the CONNECTION_CLOSE itself (closeSent is set before it
	// is written, and the refusal below reserved it a slot under the limit).
	if !c.closeSent && sp.sealer.sealed >= sp.sealer.k.confLimit-1 {
		if space == spaceApplication {
			if ok, err := c.initiateKeyUpdateLocked(); err != nil {
				return err
			} else if ok {
				return c.writePacketLocked(sp, space, payload, retransPart, opts, now)
			}
		}
		err := &transportError{
			code: transportAEADLimitReached,
			err:  fmt.Errorf("%w: this key sealed its 2^23 packets and no key update was possible", ErrQUIC),
		}
		if c.closeErr == nil {
			c.closeErr = err
		}
		c.sendCloseLocked(transportAEADLimitReached, false)
		go func() { _ = c.Close() }() // Close takes mu; it cannot run under it
		return err
	}

	if gathering {
		// A packet that would not fit beside what gathered closes that
		// datagram and opens the next. The pending packet is sealed when
		// its successor arrives — it is then known not to be the closer —
		// and the header prefix is copied out of the scratch it was built
		// in.
		if c.gatheredLenLocked()+size > c.maxDatagram {
			c.flushDatagramLocked()
		}
		if p := c.dgramPend; p != nil {
			c.dgramPend = nil
			c.dgram, _ = c.sealLocked(c.dgram, p, 0)
		}
		pn := c.takePNLocked(sp, space)
		c.dgramPend = &pendingPacket{
			sp: sp, space: space, prefix: append([]byte(nil), header...), payload: payload,
			retransPart: retransPart, pn: pn, opts: opts, now: now, dest: dest, onPath: onPath,
		}
		if needPad {
			c.dgramPad = true
		}
		return nil
	}

	pn := c.takePNLocked(sp, space)
	p := pendingPacket{
		sp: sp, space: space, prefix: header, payload: payload,
		retransPart: retransPart, pn: pn, opts: opts, now: now, dest: dest, onPath: onPath,
	}
	if c.batching && space == spaceApplication && opts.via == nil && onPath {
		return c.batchLocked(&p, size)
	}
	c.flushBatchLocked()
	pkt, err := c.sealLocked(c.pktScratch[:0], &p, 0)
	if err != nil {
		return err
	}
	c.pktScratch = pkt
	pc := c.pc
	if opts.via != nil {
		pc = opts.via
	}
	return c.writeLocked(pc, pkt, dest)
}

// sealLocked finishes a built packet — Length field and packet number for a
// long header, pad bytes of PADDING, the seal — appends it to dst, and does
// the recovery bookkeeping: amplification spent, the sent record, bytes in
// flight, pacing, the idle clock. A long-header prefix is completed in
// place when dst is the pending packet's own copy, or into scratch when it
// is the per-packet header. Callers hold mu.
//
// The bookkeeping comes before the write on purpose: batched and coalesced
// packets are sealed long before their datagram leaves, and a write that
// fails is a packet the network lost — recovery declares it so and resends
// its payload, releasing the window it held, exactly as for a loss on the
// wire. Nothing here is sized past maxDatagram (sendPacketLocked cuts the
// queues to it), so the one failure a correct packet could cause itself,
// EMSGSIZE, does not arise.
func (c *Conn) sealLocked(dst []byte, p *pendingPacket, pad int) ([]byte, error) {
	const pnLen = 4
	sp := p.sp
	if sp.discarded || sp.sealer == nil {
		// The keys left between building and sealing — a Handshake packet
		// retiring the Initial space under an Initial still pending. What
		// it carried is gone with the space, as discardSpaceLocked says.
		return dst, nil
	}
	payload := p.payload
	if pad > 0 {
		payload = append(payload, make([]byte, pad)...)
	}
	header := p.prefix
	if p.space != spaceApplication {
		// The length covers the packet number and the sealed payload, tag
		// included.
		header = AppendVarint(header, uint64(pnLen+len(payload)+16))
	}
	pnOffset := len(header)
	for i := pnLen - 1; i >= 0; i-- {
		header = append(header, byte(p.pn>>(8*uint(i)))) //nolint:gosec // G115: keeps the low octet of each shift
	}
	before := len(dst)
	out, err := sp.sealer.Seal(dst, header, payload, p.pn, pnOffset, pnLen)
	if err != nil {
		return dst, err
	}
	size := len(out) - before
	if c.amplActive && p.onPath {
		c.amplSent += int64(size)
	} else if !p.onPath && c.offPath.addr != nil && sameAddr(p.dest, c.offPath.addr) {
		c.offPath.sent += int64(size)
	}
	// RFC 9001 §6.2: an acknowledgement sent under the keys that answered
	// the peer's rotation, covering the packet that carried it, is what
	// completes the update — from here the peer may rotate again. The
	// packet is covered once it was recorded for acknowledgement, which
	// sp.largest reaching it proves (processPacket sets both together).
	if p.space == spaceApplication && c.ku.responseDue && (p.opts.ackOnly || p.opts.carriesAck) && sp.largest >= c.ku.responsePN {
		c.ku.responseDue = false
	}

	// A client's first Handshake packet is the moment its Initial keys are
	// done for good (RFC 9001 §4.9.1).
	if p.space == spaceHandshake && c.isClient {
		c.discardSpaceLocked(spaceInitial)
	}

	if p.opts.ackOnly {
		return out, nil
	}
	record := sentPacket{pn: p.pn, sentAt: p.now, size: size, ackEliciting: true, pmtuProbe: p.opts.pmtuProbe}
	c.delivery.onSent(&record, c.cc.bytesInFlight() == 0)
	if p.opts.pmtuProbe {
		// Counted here, not where the probe was decided: a packet refused
		// before sealing (number space or key exhausted) was never a try.
		c.pmtu.inFlight, c.pmtu.probePN = true, p.pn
		c.pmtu.tries++
	}
	if !p.opts.noRetrans && len(p.retransPart) > 0 {
		if p.opts.own {
			record.payload = p.retransPart
		} else {
			record.payload = append([]byte(nil), p.retransPart...)
		}
	}
	sp.sent.record(record)
	sp.lastElicit = p.now
	c.cc.onSent(p.now, size)
	// Every send spends pacing tokens — probes, ACKs and grants occupy the
	// wire like anything else — but only the waitCwnd gate above ever waits
	// on them (the pacer's doc says why the rest must not).
	c.pacer.refill(p.now, c.cc.pacingRate(c.rtt.current()))
	c.pacer.spend(size)
	// The first ack-eliciting send since the last receive restarts the idle
	// clock (§10.1): the peer is about to owe an answer. Later sends into the
	// same silence do not — an endpoint transmitting at a dead peer must not
	// keep extending its own idle timeout with every retransmission.
	if c.idleTimeout != 0 && !c.elicitingSinceRecv {
		c.idleDeadline = p.now.Add(c.idleTimeout)
	}
	c.elicitingSinceRecv = true
	c.armKeepAliveLocked(p.now)
	c.kickTimer()
	return out, nil
}

// sendCrypto sends handshake bytes as CRYPTO frames at their level, split to
// fit the datagram — a certificate chain does not fit one packet.
func (c *Conn) sendCrypto(l tls.QUICEncryptionLevel, data []byte) error {
	space := spaceFor(l)
	c.mu.Lock()
	defer c.mu.Unlock()
	for len(data) > 0 {
		n := min(len(data), c.maxStreamChunk())
		// Beside a gathered Initial, a chunk cut to the room left fills the
		// datagram the Initial would otherwise pad with zeros (§14.1): the
		// ServerHello's Initial and the first of the certificate chain
		// travel together instead of in two 1200-byte datagrams.
		if room := c.cryptoRoomLocked(); room > 0 && room < n {
			n = room
		}
		frame := AppendVarint(nil, frameCrypto)
		frame = AppendVarint(frame, c.spaces[space].cryptoOff)
		frame = AppendVarint(frame, uint64(n)) //nolint:gosec // G115: a byte count, positive
		frame = append(frame, data[:n]...)
		c.spaces[space].cryptoOff += uint64(n) //nolint:gosec // G115: a byte count, positive
		data = data[n:]
		// CRYPTO obeys the congestion window like everything else (RFC 9002
		// §7) — but it must not *wait* on it: the credit arrives on the same
		// loop that is sending this, during the handshake. What does not fit
		// queues behind the lost payloads, and retransmitLocked drains it as
		// acknowledgements reopen the window.
		if !c.cc.canSend(len(frame) + 64) {
			sp := &c.spaces[space]
			sp.retrans = append(sp.retrans, frame)
			continue
		}
		if err := c.sendPacketLocked(space, frame, sendOpts{}); err != nil {
			return err
		}
	}
	return nil
}

// flushControlLocked sends whatever control frames are queued, now, in
// their own packets — as many as maxDatagram needs. Used when nothing else
// is about to leave and the peer needs them to make progress — a MAX_DATA
// the peer is blocked on does not wait for this end to have data of its
// own.
//
// Grants flush regardless of the congestion window: they are how a blocked
// peer resumes sending, and withholding them until ACKs reopen the window is
// a deadlock when the peer's sending is what produces those ACKs. The rest
// of the control queue obeys cwnd (RFC 9002 §7): when there is no room it
// stays queued, rides the next packet that passes the window, or flushes on
// a later call — one runs after every processed packet, so an ACK that
// reopens the window releases it.
//
// Before each packet it tops the issued connection identifiers back up: a
// retirement that found the queue full (issueLocalCIDsLocked) left a
// deficit, made up here — this runs after every processed packet — once
// the packet before freed the room.
func (c *Conn) flushControlLocked() {
	if c.spaces[spaceApplication].sealer == nil {
		return
	}
	// The DPLPMTUD probe waits on the same things the control queue does —
	// a validated path, room in the window — and is retried from the same
	// place.
	c.maybeProbePMTULocked()
	for {
		c.issueLocalCIDsLocked()
		before := len(c.grants) + len(c.control)
		if before == 0 {
			return
		}
		if err := c.sendPacketLocked(spaceApplication, nil, sendOpts{}); err != nil {
			return
		}
		if len(c.grants)+len(c.control) == before {
			return // the window has no room for what is left; it waits
		}
	}
}

// flushAcksLocked answers every space that owes an acknowledgement with
// nothing else to carry it.
func (c *Conn) flushAcksLocked(now time.Time) {
	for space := range c.spaces {
		sp := &c.spaces[space]
		if sp.discarded || sp.sealer == nil || !sp.ack.owes(now) {
			continue
		}
		payload := sp.ack.appendAck(nil, now)
		if len(payload) == 0 {
			continue
		}
		_ = c.writePacketLocked(sp, space, payload, nil, sendOpts{ackOnly: true}, now)
	}
}

// queueLostLocked puts the payloads of packets a detection pass declared
// lost back on the space's send queue, and moves the counters Stats reports
// for it: packets lost, and payload bytes that will go again. Both detection
// paths (an ACK, the loss timer) end here, so a count lives in one place.
func (c *Conn) queueLostLocked(sp *space, out ackOutcome) {
	sp.retrans = append(sp.retrans, out.lost...)
	c.lostPackets += out.lostCount
	for _, payload := range out.lost {
		c.bytesRetransmitted += uint64(len(payload))
	}
}

// retransmitLocked drains the queues of lost payloads, best effort under the
// congestion window: what does not fit now goes when the next ACK opens the
// window again.
func (c *Conn) retransmitLocked() {
	for space := range c.spaces {
		sp := &c.spaces[space]
		if sp.discarded || sp.sealer == nil {
			continue
		}
		for len(sp.retrans) > 0 {
			payload := sp.retrans[0]
			if !c.cc.canSend(len(payload) + 64) {
				return
			}
			sp.retrans = sp.retrans[1:]
			left := len(sp.retrans)
			if err := c.sendPacketLocked(space, payload, sendOpts{own: true}); err != nil {
				return
			}
			if len(sp.retrans) > left {
				// The anti-amplification budget blocked the send and the
				// payload is back at the head of the queue: nothing more
				// can leave until the peer's next datagram raises it.
				return
			}
		}
	}
}

// sendProbeLocked answers a probe timeout in one space with two datagrams
// (RFC 9002 §6.2.4 allows two, so that one lost probe does not cost a whole
// doubled interval; probes may exceed the congestion window, §7): the two
// oldest unacknowledged payloads — they are the most likely lost — or, when
// only one is left to resend, that one twice. A ping goes out alone when
// there is nothing to resend but an answer is still owed. The queue of
// declared losses goes first; the rest is lifted out of the flight record
// (liftOldestResendable says why) and sent as new packets.
//
// Two datagrams every time, not only when two payloads wait: a client whose
// ClientHello spans two Initial packets and whose second one is lost holds
// exactly one payload to resend, and an amplification-limited server can
// send nothing until that payload arrives. Each interval doubles, so the
// eighth datagram leaves after 7 intervals at two per probe and after 63 at
// one — past the five seconds a server waits for a handshake, on a path of
// 30 ms. Two copies of one payload cost one spurious duplicate per probe on
// a flight of one packet, the price §6.2.4 names for the same payload
// "ensuring the highest priority frames arrive first".
//
// Two is also what a server's first flight needs: a ServerHello with a
// post-quantum key share spans two Initial packets, and a client that
// holds only the first cannot read anything the server sends after it.
// A probe carries old data, not new (§6.2.4 prefers new data but permits
// this): writers here are synchronous — unsent data lives in a blocked
// Stream.Write's own stack, not in a queue a probe could pull from — and
// what a probe answers is "did my oldest packet die?", which resending that
// packet answers in one round trip where new data would take two.
func (c *Conn) sendProbeLocked(space int) {
	sp := &c.spaces[space]
	if sp.discarded || sp.sealer == nil {
		return
	}
	const probes = 2
	var payloads [probes][]byte
	n := 0
	for ; n < probes && len(sp.retrans) > 0; n++ {
		payloads[n] = sp.retrans[0]
		sp.retrans = sp.retrans[1:]
	}
	// Lifted before any is sent: the copies are recorded as they go, and
	// the newest of them must not be the next one lifted.
	for ; n < probes; n++ {
		p, ok := sp.sent.liftOldestResendable()
		if !ok {
			break
		}
		if sp.sent.onPath(&p) { // a previous path's packet never counted here (RFC 9000 §9.4)
			c.cc.onLifted(p.size)
		}
		payloads[n] = p.payload
	}
	switch n {
	case 0:
		_ = c.sendPacketLocked(space, []byte{framePing}, sendOpts{})
		return
	case 1:
		payloads[1] = payloads[0]
		n = probes
	}
	for _, payload := range payloads[:n] {
		_ = c.sendPacketLocked(space, payload, sendOpts{own: true})
	}
}

// waitForCreditLocked parks the caller until credit arrives, the connection
// ends, or deadline passes (zero: never; os.ErrDeadlineExceeded when it
// does). The read loop must never call it: the read loop is what delivers
// the credit.
//
// The deadline is checked before parking, not after: the timer loop's
// broadcast at the deadline wakes the writer, whose own loop comes back
// here — still out of credit — and finds the time passed.
func (c *Conn) waitForCreditLocked(deadline time.Time) error {
	select {
	case <-c.closed:
		return errClosed
	default:
	}
	if !deadline.IsZero() && !time.Now().Before(deadline) {
		return os.ErrDeadlineExceeded
	}
	// Nothing built waits with the writer, and while it sleeps the
	// batch is closed: whoever takes mu meanwhile — the read loop and its
	// acknowledgements — writes at once, as outside Stream.write.
	batching := c.batching
	c.flushBatchLocked()
	c.batching = false
	c.cond.Wait()
	c.batching = batching
	select {
	case <-c.closed:
		return errClosed
	default:
	}
	return nil
}

// sendBlockedFramesLocked tells the peer this end is out of credit, once per
// limit (RFC 9000 §4.1): the peer is entitled to know it is the bottleneck,
// and a probe of the same name is what unsticks a lost MAX_DATA.
//
// It also arms the periodic re-announcement (§4.1's second half): a writer
// can stay parked here longer than either end's idle timeout, and the one
// *_BLOCKED frame sent at the limit — once acknowledged — leaves nothing
// ack-eliciting in flight to keep the connection alive while it waits.
func (c *Conn) sendBlockedFramesLocked(s *Stream) {
	if c.connSend.avail() == 0 && !c.connSend.blockedSent {
		c.connSend.blockedSent = true
		c.control = appendFrame(c.control, frameDataBlocked, c.connSend.max)
	}
	if s.sendQ.avail() == 0 && !s.sendQ.blockedSent {
		s.sendQ.blockedSent = true
		c.control = appendFrame(c.control, frameStreamDataBlocked, s.id, s.sendQ.max)
	}
	if c.blockedResend.IsZero() {
		if ivl := c.blockedResendInterval(); ivl > 0 {
			c.blockedResend = time.Now().Add(ivl)
			c.kickTimer()
		}
	}
	c.flushControlLocked()
}

// blockedResendInterval is the cadence of §4.1's periodic *_BLOCKED
// re-announcement: a third of the smaller announced idle timeout, so a
// parked sender speaks up several times before either end could declare the
// connection idle. Zero — no re-announcement — when neither end enforces an
// idle timeout, because then there is nothing for the silence to kill.
func (c *Conn) blockedResendInterval() time.Duration {
	limit := c.idleTimeout
	if c.hasPeerParams && c.peerParams.MaxIdleTimeout != 0 {
		peer := c.peerParams.MaxIdleTimeout
		if limit == 0 || peer < limit {
			limit = peer
		}
	}
	if limit == 0 {
		return 0
	}
	return max(limit/3, 10*time.Millisecond)
}

// resendBlockedFramesLocked is the timer half of §4.1: while anything is
// still parked at a flow-control limit it re-queues that limit's *_BLOCKED
// frame — deliberately past the once-per-limit flag, which governs the
// immediate announcement, not the keepalive — and re-arms itself. When
// nothing is blocked any more it disarms.
func (c *Conn) resendBlockedFramesLocked(now time.Time) {
	blocked := false
	if c.connSend.avail() == 0 && c.connSend.blockedSent {
		c.control = appendFrame(c.control, frameDataBlocked, c.connSend.max)
		blocked = true
	}
	for _, s := range c.streams {
		if s.sendQ.avail() == 0 && s.sendQ.blockedSent {
			c.control = appendFrame(c.control, frameStreamDataBlocked, s.id, s.sendQ.max)
			blocked = true
		}
	}
	if !blocked {
		c.blockedResend = time.Time{}
		return
	}
	c.flushControlLocked()
	c.blockedResend = now.Add(c.blockedResendInterval())
}

// creditConsumed reports application-consumed stream bytes to flow control,
// queueing the grants the consumption earned. Called by Stream.Read with no
// locks held.
func (c *Conn) creditConsumed(streamID, n uint64, streamGrant bool, nextStreamMax uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if streamGrant {
		c.grants = appendFrame(c.grants, frameMaxStreamData, streamID, nextStreamMax)
	}
	if c.connRecv.consume(n) {
		c.grants = appendFrame(c.grants, frameMaxData, c.connRecv.nextMax())
	}
	c.flushControlLocked()
}

// kickTimer wakes the timer loop to re-read the deadlines it sleeps on.
func (c *Conn) kickTimer() {
	select {
	case c.timerKick <- struct{}{}:
	default:
	}
}

// writeLocked sends one datagram and keeps the one thing a failed send can
// say that matters: on Linux, the ICMP port-unreachable answering an
// earlier datagram on a connected socket is reported by whichever syscall
// comes next, often a send. The handshake counts those refusals alongside
// the ones its reads see, so a Dial to a closed port fails on the first
// probe rather than waiting for a read to be the one that hears it.
//
// Every failure is also counted (WriteFailures): most callers send best
// effort — loss recovery resends what a failed write lost — so the error
// stops there, and the count is what keeps a socket refusing every send
// from being silent.
func (c *Conn) writeLocked(pc net.PacketConn, b []byte, to net.Addr) error {
	if c.closing.capturing {
		c.keepClosingDatagramLocked(pc, b, to) // the CONNECTION_CLOSE, for the closing state
	}
	_, err := pc.WriteTo(b, to)
	return c.wroteLocked(err)
}

// wroteLocked is writeLocked's accounting of one write's outcome, shared
// with the batched writes of flushBatchLocked. Callers hold mu.
func (c *Conn) wroteLocked(err error) error {
	if err != nil {
		c.writeFailures++
		if isRefusal(err) {
			c.writeRefusals++
			c.writeRefusal = err
			if c.hsSyncRead && c.incoming == nil {
				// Wake the handshake's blocking read so it re-counts the
				// refusals now rather than at its deadline (hsWake).
				c.hsWake = true
				_ = c.pc.SetReadDeadline(time.Now())
			}
		}
	}
	return err
}

// maxBatchSegments and maxBatchBytes bound one batch: the kernel's
// UDP_MAX_SEGMENTS on the oldest kernels that offer segmentation (4.18),
// and what one UDP datagram — the unit sendmsg hands over before cutting —
// may carry.
const (
	maxBatchSegments = 64
	maxBatchBytes    = 65507
)

// batchLocked seals p at the end of the batch instead of writing it. size
// is its exact sealed length, known before sealing (writePacketLocked): a
// packet that cannot join — larger than the batch's segments, after a
// shorter one closed it, or past the bounds — first flushes the batch and
// opens the next. The recovery bookkeeping is sealLocked's, as for any
// packet; only the write is deferred, to flushBatchLocked. Callers hold mu.
func (c *Conn) batchLocked(p *pendingPacket, size int) error {
	if c.batchN > 0 && (c.batchShort || size > c.batchSeg ||
		c.batchN == maxBatchSegments || len(c.batch)+size > maxBatchBytes) {
		c.flushBatchLocked()
	}
	out, err := c.sealLocked(c.batch, p, 0)
	if err != nil {
		return err
	}
	size = len(out) - len(c.batch)
	c.batch = out
	switch {
	case c.batchN == 0:
		c.batchSeg = size
	case size < c.batchSeg:
		c.batchShort = true
	}
	c.batchN++
	return nil
}

// flushBatchLocked writes what batchLocked gathered: in one system call
// where the socket offers segmentation (writeSegments), one write per
// datagram where it does not. The destination is the peer's current
// address: batching only takes packets bound for it, and the peer cannot
// change while the batch is open — that takes mu, which a batching writer
// releases only through waitForCreditLocked, which flushes first. It
// keeps the first write error in batchErr, which Stream.write returns as
// it returned WriteTo's before; the packets themselves are recorded, and
// loss recovery resends them like any lost datagram. Callers hold mu.
func (c *Conn) flushBatchLocked() {
	if c.batchN == 0 {
		return
	}
	b, seg, n := c.batch, c.batchSeg, c.batchN
	c.batch, c.batchN, c.batchShort = c.batch[:0], 0, false
	if n > 1 && !c.noGSO {
		var ok bool
		var err error
		c.oobScratch, ok, err = writeSegments(c.pc, b, seg, c.peer, c.oobScratch)
		if ok {
			c.segmented++
			c.keepBatchErr(c.wroteLocked(err))
			return
		}
		c.noGSO = true
	}
	for len(b) > 0 {
		k := min(seg, len(b))
		c.keepBatchErr(c.writeLocked(c.pc, b[:k], c.peer))
		b = b[k:]
	}
}

func (c *Conn) keepBatchErr(err error) {
	if c.batchErr == nil {
		c.batchErr = err
	}
}
