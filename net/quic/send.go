package quic

import (
	"crypto/tls"
	"fmt"
	"net"
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
	// ackOnly marks a packet that elicits no acknowledgement and is not
	// counted in flight (RFC 9002 §2).
	ackOnly bool
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
				c.pacer.refill(now, c.cc.paceWindow(), c.rtt.current())
				d := c.pacer.delay(c.cc.paceWindow(), c.rtt.current())
				if d == 0 {
					break
				}
				if until := now.Add(d); c.paceDeadline.IsZero() || until.Before(c.paceDeadline) {
					c.paceDeadline = until
					c.kickTimer()
				}
			}
			if err := c.waitForCreditLocked(); err != nil {
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

	// Anything owed rides along: the grant and control queues (application
	// space), and this space's acknowledgement. Grants ride any packet —
	// their cwnd exemption is theirs, not the carrier's — while the control
	// queue rides only when the window has room for the extra bytes: a
	// cwnd-exempt carrier (a grant flush, a probe) must not smuggle past the
	// window what flushControlLocked just deferred for lacking it.
	//
	// A noRetrans packet carries neither: nothing records it for recovery,
	// so frames riding it would be gone for good if it were lost — or
	// withheld by the amplification budget, which skips such a packet
	// outright. They stay queued for the next packet that recovery covers.
	retransPart := payload
	var head []byte
	if space == spaceApplication && !opts.noRetrans && (len(c.grants) > 0 || len(c.control) > 0) {
		head = append(head, c.grants...)
		c.grants = nil
		if len(c.control) > 0 && c.cc.canSend(len(head)+len(payload)+len(c.control)+64) {
			head = append(head, c.control...)
			c.control, c.controlRetires = nil, 0
		}
		if len(head) > 0 {
			retransPart = append(head[:len(head):len(head)], payload...)
			// Control frames are cheap to lose only if someone resends
			// them; they carry window and stream grants, so they are
			// retransmittable and ride the same recovery as the payload.
			payload = retransPart
		}
		head = nil
	}
	now := time.Now()
	if sp.ack.owes(now) {
		head = sp.ack.appendAck(head, now)
		if len(head) > 0 {
			payload = append(head, payload...)
		}
	}

	return c.writePacketLocked(sp, space, payload, retransPart, opts, now)
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
		header = append(header, 0x40|c.spin|byte(pnLen-1)) // short header, fixed bit set, §17.4 spin
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
	if opts.padTo > 0 && space == spaceApplication {
		target := opts.padTo
		if c.amplActive && onPath {
			target = min(target, int(3*c.amplRecv-c.amplSent))
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

	// RFC 9001 §6.6: AES-GCM keys are spent after 2^23 sealed packets, and
	// without key update (not implemented; see the package documentation)
	// there is no fresh key to move to — the connection must end with
	// AEAD_LIMIT_REACHED instead of degrading the cipher. The check skips
	// the CONNECTION_CLOSE itself (closeSent is set before it is written,
	// and the refusal below reserved it a slot under the limit).
	//
	// This check also subsumes RFC 9000 §12.3's 2^62-1 packet-number
	// ceiling: nextPN only advances alongside sealed, the sealer never
	// rekeys, and 2^23 is thirty-nine powers of two short of the ceiling —
	// so no separate guard exists, and one would be dead code until key
	// update lands. If key update ever does, §12.3 needs its own check.
	if !c.closeSent && sp.sealer.sealed >= sp.sealer.k.confLimit-1 {
		err := &transportError{
			code: transportAEADLimitReached,
			err:  fmt.Errorf("%w: this key sealed its 2^23 packets and key update is not implemented", ErrQUIC),
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
		pn := sp.nextPN
		sp.nextPN++
		c.dgramPend = &pendingPacket{
			sp: sp, space: space, prefix: append([]byte(nil), header...), payload: payload,
			retransPart: retransPart, pn: pn, opts: opts, now: now, dest: dest, onPath: onPath,
		}
		if needPad {
			c.dgramPad = true
		}
		return nil
	}

	pn := sp.nextPN
	sp.nextPN++
	p := pendingPacket{
		sp: sp, space: space, prefix: header, payload: payload,
		retransPart: retransPart, pn: pn, opts: opts, now: now, dest: dest, onPath: onPath,
	}
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
	}

	// A client's first Handshake packet is the moment its Initial keys are
	// done for good (RFC 9001 §4.9.1).
	if p.space == spaceHandshake && c.isClient {
		c.discardSpaceLocked(spaceInitial)
	}

	if p.opts.ackOnly {
		return out, nil
	}
	record := sentPacket{pn: p.pn, sentAt: p.now, size: size, ackEliciting: true}
	if !p.opts.noRetrans && len(p.retransPart) > 0 {
		if p.opts.own {
			record.payload = p.retransPart
		} else {
			record.payload = append([]byte(nil), p.retransPart...)
		}
	}
	sp.sent.record(record)
	sp.lastElicit = p.now
	c.cc.onSent(size)
	// Every send spends pacing tokens — probes, ACKs and grants occupy the
	// wire like anything else — but only the waitCwnd gate above ever waits
	// on them (the pacer's doc says why the rest must not).
	c.pacer.refill(p.now, c.cc.paceWindow(), c.rtt.current())
	c.pacer.spend(size)
	// The first ack-eliciting send since the last receive restarts the idle
	// clock (§10.1): the peer is about to owe an answer. Later sends into the
	// same silence do not — an endpoint transmitting at a dead peer must not
	// keep extending its own idle timeout with every retransmission.
	if c.idleTimeout != 0 && !c.elicitingSinceRecv {
		c.idleDeadline = p.now.Add(c.idleTimeout)
	}
	c.elicitingSinceRecv = true
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

// flushControlLocked sends whatever control frames are queued, now, in their
// own packet. Used when nothing else is about to leave and the peer needs
// them to make progress — a MAX_DATA the peer is blocked on does not wait
// for this end to have data of its own.
//
// Grants flush regardless of the congestion window: they are how a blocked
// peer resumes sending, and withholding them until ACKs reopen the window is
// a deadlock when the peer's sending is what produces those ACKs. The rest
// of the control queue obeys cwnd (RFC 9002 §7): when there is no room it
// stays queued, rides the next packet that passes the window, or flushes on
// a later call — one runs after every processed packet, so an ACK that
// reopens the window releases it.
func (c *Conn) flushControlLocked() {
	if c.spaces[spaceApplication].sealer == nil {
		return
	}
	payload := c.grants
	c.grants = nil
	if len(c.control) > 0 && c.cc.canSend(len(payload)+len(c.control)+64) {
		payload = append(payload, c.control...)
		c.control, c.controlRetires = nil, 0
	}
	if len(payload) == 0 {
		return
	}
	_ = c.sendPacketLocked(spaceApplication, payload, sendOpts{})
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

// sendProbeLocked answers a probe timeout in one space: resend the oldest
// unacknowledged payload — it is the most likely lost, and probes may exceed
// the congestion window (RFC 9002 §6.2.4, §7) — or ping if there is nothing
// to resend but an answer is still owed.
// A probe carries old data, not new (RFC 9002 §6.2.4 prefers new data but
// permits this): writers here are synchronous — unsent data lives in a
// blocked Stream.Write's own stack, not in a queue a probe could pull from —
// and what a probe answers is "did my oldest packet die?", which resending
// that packet answers in one round trip where new data would take two.
func (c *Conn) sendProbeLocked(space int) {
	sp := &c.spaces[space]
	if sp.discarded || sp.sealer == nil {
		return
	}
	if len(sp.retrans) > 0 {
		payload := sp.retrans[0]
		sp.retrans = sp.retrans[1:]
		_ = c.sendPacketLocked(space, payload, sendOpts{own: true})
		return
	}
	if p, ok := sp.sent.oldestEliciting(); ok && len(p.payload) > 0 {
		_ = c.sendPacketLocked(space, p.payload, sendOpts{noRetrans: true})
		return
	}
	_ = c.sendPacketLocked(space, []byte{framePing}, sendOpts{})
}

// waitForCreditLocked parks the caller until credit arrives or the
// connection ends. The read loop must never call it: the read loop is what
// delivers the credit.
func (c *Conn) waitForCreditLocked() error {
	select {
	case <-c.closed:
		return errClosed
	default:
	}
	c.cond.Wait()
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
	_, err := pc.WriteTo(b, to)
	if err != nil {
		c.writeFailures++
		if isRefusal(err) {
			c.writeRefusals++
			c.writeRefusal = err
		}
	}
	return err
}
