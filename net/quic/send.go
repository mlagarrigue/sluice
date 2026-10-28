package quic

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/mlagarrigue/sluice/internal/varint"
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
	//
	// RFC 9002 §7 says a sender MUST NOT send if bytes in flight would
	// exceed the window "unless PTO or entering recovery", and this is where
	// that rule is kept and where it is knowingly not: the window gates
	// stream data (here), the retransmission queue and the control queue
	// (canSend in retransmitLocked and takeQueuedLocked), which is every
	// send that can be large or repeated. A RESET_STREAM, a STOP_SENDING,
	// the keepalive PING and the PING of a graceful close go out ungated
	// (the sites say "see sendOpts.waitCwnd"): each is a few dozen bytes,
	// one per application action or timer, and holding it behind the window
	// would hold what it frees — a reset waits on the data it cancels, a
	// keepalive misses the idle clock it serves. Flow-control grants are the
	// same choice, written at takeQueuedLocked. The excess is bounded by the
	// count of such events, as a probe's is by the PTO (§7.5).
	waitCwnd bool
	// deadline bounds that wait (Stream.SetWriteDeadline); zero waits
	// without bound.
	deadline time.Time
	// recheck is asked, after every wake from that wait — and from the
	// wait for keys (awaitSealLocked), which releases mu too — whether the
	// payload is still owed; a non-nil answer abandons the send and is
	// what the caller returns. The wait releases mu, so the state the
	// caller checked before building the frame may be gone by the time the
	// window reopens — and for a STREAM frame that state is the stream's.
	// Callers hold mu when it runs.
	//
	// What it is for. A writer parked here had already built its frame at
	// s.writeOff and charged its credit. A STOP_SENDING arriving meanwhile
	// is read on the receive goroutine, which abandons this end's sending
	// direction and answers RESET_STREAM with a final size of exactly that
	// s.writeOff (§3.5, onStopSendingLocked) — then broadcasts, and the
	// writer woke up and sent its frame at the offset just declared final.
	// RFC 9000 §3.3 forbids a STREAM frame after RESET_STREAM, and §4.5
	// has the receiver answer bytes past the final size with
	// FINAL_SIZE_ERROR: two sluice ends closed the connection on it, and
	// so do the peers that apply §4.5 — quic-go, ngtcp2. The whole
	// connection dies, with every other request in flight on it, because
	// one request was cancelled.
	recheck func() error
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
	// close marks this end's own CONNECTION_CLOSE: the one packet
	// writePacketLocked still writes once closeSent is set, and one that
	// never coalesces — it must leave now.
	close bool
}

// maxStreamChunk is how much stream payload fits one packet, leaving room
// for the short header, the frame header and the AEAD tag.
func (c *Conn) maxStreamChunk() int {
	return c.maxDatagram - 64
}

// sendPacketLocked seals payload as one packet in space's level and writes it
// as one datagram. Callers hold mu.
//
// sendPacketLocked runs in this order: send nothing once the connection is
// closing or draining; wait for the space's keys, rechecking what the caller
// built if it waited; wait for the congestion window and the pacer when the
// caller asks; read the clock once; put the acknowledgement this space owes in
// front, unless the packet leaves off the path in use; append the grants and
// control frames owed in the application space; then seal and write the
// datagram (writePacketLocked).
func (c *Conn) sendPacketLocked(space int, payload []byte, opts sendOpts) error {
	if c.draining || c.closeSent {
		return errClosed
	}
	sp := &c.spaces[space]
	waited := !c.canSealLocked(sp, space)
	if err := c.awaitSealLocked(sp, space); err != nil {
		return err
	}
	if waited && opts.recheck != nil {
		// The wait for keys releases mu as the wait for the window below
		// does, and the caller's frame was built before it: a Reset in
		// between declared the final size at the bytes written so far, and
		// this frame would go past it (RFC 9000 §19.4, FINAL_SIZE_ERROR at
		// the peer).
		if err := opts.recheck(); err != nil {
			return err
		}
	}

	if opts.waitCwnd {
		// The first packet of a write is where it asks whether the sender
		// had run out of data (writeBeginLocked): on this clock reading, and
		// only if the window has room at once — a write that waits for the
		// window found it full.
		check := c.limitCheck
		c.limitCheck = false
		for {
			// Lost data first (RFC 9000 §13.3): a writer woken by the
			// acknowledgement that opened the window may reach here before
			// the retransmission pass that follows the packet's frames,
			// and would otherwise spend the window on new data while the
			// queue waits. The pass takes what the window and the pacer
			// allow; the check below then sees the window it left.
			if len(c.spaces[space].retrans) > 0 {
				c.retransmitLocked()
			}
			ready := c.cc.canSend(len(payload) + 64)
			if ready {
				// The window has room; the pacer decides *when*. A deficit
				// arms a deadline the timer loop answers with a broadcast —
				// an ACK arriving sooner broadcasts too, and the loop simply
				// re-prices the wait.
				now := time.Now()
				if check && c.checkAppLimitedLocked(now) {
					c.appLimitedMarks++
				}
				rate := c.cc.pacingRate(c.rtt.current())
				c.pacer.refill(now, rate)
				d := c.pacer.delay(rate)
				if d == 0 {
					c.writeSent = now
					break
				}
				c.armPaceLocked(now.Add(d))
			}
			check = false
			if err := c.waitForCreditLocked(opts.deadline); err != nil {
				return err
			}
			if c.draining || c.closeSent {
				return errClosed
			}
			if err := c.awaitSealLocked(sp, space); err != nil {
				return err
			}
			if opts.recheck != nil {
				if err := opts.recheck(); err != nil {
					return err
				}
			}
			// The datagram size may have fallen while this waited — a
			// black hole detected, a migration, a Rebind — and a payload
			// cut for the old size would leave larger than the path was
			// just found to carry (§14.2). The caller cuts it again.
			if c.packetOverhead(space)+len(payload) > c.maxDatagram {
				return errRefit
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
	//
	// An ACK not yet owed rides too: §13.2.1 says an endpoint SHOULD send an
	// ACK frame with other frames when there are new ack-eliciting packets
	// to acknowledge. Carrying only an owed one left the delayed ACK of a
	// request to its own datagram 25 ms after the response had gone without
	// it — one ACK-only packet per request on a server answering small
	// requests, measured at 0.6 per request with 20,000 requests over
	// HTTP/3.
	now := time.Now()
	var ack []byte
	// Not off the path in use: a datagram toward another address or from
	// another socket is a probe — PATH_CHALLENGE, PATH_RESPONSE, PADDING
	// (§9.1) — and an ACK would make it non-probing, which a server must
	// not send toward an address it has seen no non-probing packet from
	// (§9) nor from its preferred address before that path is validated
	// (§9.6.2). The ACK waits for the next packet on the path; an answer
	// on the path itself keeps it (§9.3.3 has that one non-probing).
	offPath := (opts.via != nil && opts.via != c.pc) || (opts.to != nil && !sameAddr(opts.to, c.peer))
	// In the Initial and Handshake spaces an ACK rides every packet once
	// anything was received there, owed or not — §13.2.1's "when sending a
	// packet for any reason, an endpoint SHOULD attempt to include an ACK
	// frame if one has not been sent recently". A handshake whose one ACK
	// was lost otherwise leaves the peer blind until it next elicits one:
	// runner handshakeloss against quiche, the client's two ACKs of the
	// ServerHello's first half both dropped while four of its probes got
	// through bare, and quiche's PTO resending only that first half, never
	// the lost tail, until Dial expired. A few bytes in packets that leave
	// rarely, and the client's Initials are padded to 1200 anyway.
	resend := space != spaceApplication && len(sp.ack.ranges) > 0
	if owed := sp.ack.owes(now); !offPath && (owed || sp.ack.pending() || resend) {
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
		// extra datagram is the price of the peer's gaps. One not yet owed
		// simply stays behind for its timer.
		//
		// The same holds for the congestion window (RFC 9002 §7): the gates
		// that let this packet through — the writer's above, the
		// retransmission and CRYPTO passes — counted the payload and 64
		// bytes of header, tag and slack, which a short ACK fits in and a
		// gapped one does not. An ACK-only packet is not in flight, so
		// leaving ahead keeps the flight under the window; a packet over
		// the window without its ACK is cwnd-exempt (a probe, a grant
		// flush) and keeps it. The application space only: a client's
		// Initial is padded to 1200 bytes whatever it carries, and the
		// handshake spaces' ACKs stay short.
		size := c.packetOverhead(space) + len(ack) + len(payload)
		if len(payload) > 0 && (size > c.maxDatagram || space == spaceApplication && c.ackOverflowsWindowLocked(size, len(ack))) {
			if owed {
				_ = c.writePacketLocked(sp, space, sp.ack.appendAck(nil, now), nil, sendOpts{ackOnly: true}, now)
			}
			ack = nil
		}
	}
	retransPart := payload
	if space == spaceApplication && !opts.noRetrans && (len(c.grants) > 0 || len(c.control) > 0) {
		room := c.maxDatagram - c.packetOverhead(space) - len(ack) - len(payload)
		if queued := c.takeQueuedLocked(room, len(ack)+len(payload)); len(queued) > 0 {
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

// ackOverflowsWindowLocked reports whether a packet of size bytes, ack of
// them its ACK frame, would fit the congestion window without the ACK and
// does not with it. Read off the window, not through canSend: the payload
// leaves either way, so this is no refusal for §7.8 to count. Callers hold
// mu.
func (c *Conn) ackOverflowsWindowLocked(size, ack int) bool {
	room := c.cc.window() - c.cc.bytesInFlight()
	return size > room && size-ack <= room
}

// shortPacketOverhead is what a 1-RTT packet costs on the wire beyond its
// payload: the short header, the packet number and the AEAD tag.
func (c *Conn) shortPacketOverhead() int {
	const pnLen = 4
	return 1 + len(c.dcid) + pnLen + 16
}

// sendsEarlyLocked reports whether an application packet sent now leaves in
// 0-RTT: a DialEarly client before its 1-RTT keys (writePacketLocked).
// Callers hold mu.
func (c *Conn) sendsEarlyLocked() bool {
	return c.spaces[spaceApplication].sealer == nil && c.earlySealer != nil
}

// errRefit is sendPacketLocked's answer to a waiting writer whose payload no
// longer fits the datagram size it woke to: nothing was sent, and the
// writer builds its frame again for the size now in force.
var errRefit = errors.New("quic: the datagram size fell while the packet waited")

// packetOverhead is what a packet of space costs on the wire beyond its
// payload: its header, the packet number and the AEAD tag. A long header's
// Length field is counted at two bytes, which covers any packet under
// maxDatagram. An application packet sent in 0-RTT carries a long header,
// sixteen bytes or so more than the short one it is counted at otherwise.
// Callers hold mu.
func (c *Conn) packetOverhead(space int) int {
	if space == spaceApplication && !c.sendsEarlyLocked() {
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
// which this adds itself. Alone in its datagram, a packet's header is far
// too short for the padding to leave its Length field without a consistent
// width (initialPaddingAfter), so the width is the minimal one and dropped.
func initialPadding(headerLen, pnLen, payloadLen int) int {
	pad, _ := initialPaddingAfter(0, headerLen, pnLen, payloadLen)
	return pad
}

// initialPaddingAfter is initialPadding for a packet that closes a datagram
// already holding before bytes of other packets (§12.2 coalescing): the
// padding tops the whole datagram up to 1200, not this packet alone. It also
// returns the width the Length field is to be encoded on, which sealLocked
// honours.
//
// The length field's own width depends on what it is about to describe,
// which depends on how much padding closes the gap to 1200 — a circularity
// solved in two steps: size the padding against the width the unpadded
// length needs, and if the padded length crossed a varint threshold (64,
// 16384, ...), widen the field and shrink the padding by as much. There is
// at most one threshold between "no padding" and 1200 bytes, so one step
// back is all it takes — and the shrunk padding may land the length right
// under the threshold again, where no width is consistent with its own
// padding (before + headerLen = 1135 with a closing packet of 43 payload
// bytes or fewer: width 1 pads the length to 64, width 2 to 63). The wider
// field then stays, encoded on more bytes than its value needs: RFC 9000 §16
// allows every field but the frame type to be, and the datagram still
// lands on 1200 exactly. A fixed-point loop here panicked on that input.
func initialPaddingAfter(before, headerLen, pnLen, payloadLen int) (pad, lengthWidth int) {
	lengthWidth = varintBytes(uint64(pnLen + payloadLen + 16)) //nolint:gosec // G115: a sum of packet lengths, positive
	pad = max(1200-(before+headerLen+lengthWidth+pnLen+payloadLen+16), 0)
	if got := varintBytes(uint64(pnLen + payloadLen + pad + 16)); got > lengthWidth { //nolint:gosec // G115: a sum of packet lengths, positive
		lengthWidth = got
		pad = max(1200-(before+headerLen+lengthWidth+pnLen+payloadLen+16), 0)
	}
	return pad, lengthWidth
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
	early       bool   // a 0-RTT packet: long header, sealed with Conn.earlySealer
	prefix      []byte // the long header up to, not including, Length
	payload     []byte
	retransPart []byte
	pn          uint64
	opts        sendOpts
	now         time.Time
	dest        net.Addr
	onPath      bool
	ecn         uint8 // the codepoint its datagram leaves with (§13.4.2)
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
// an Initial in the datagram requires it — and writes the datagram. A
// connection that entered draining since the packets were gathered drops
// them instead: §10.2.2, "an endpoint in the draining state MUST NOT send
// any packets", and a peer's CONNECTION_CLOSE coalesced behind an
// ack-eliciting packet arrives after that packet's acknowledgement was
// gathered. The guards of writePacketLocked and sendPacketLocked cannot
// see a packet already built. Callers hold mu.
func (c *Conn) flushDatagramLocked() {
	if c.dgramPend == nil && len(c.dgram) == 0 {
		return
	}
	if c.draining {
		c.dgramPend, c.dgram, c.dgramPad = nil, c.dgram[:0], false
		return
	}
	c.flushBatchLocked() // what was sent before leaves before
	if p := c.dgramPend; p != nil {
		c.dgramPend = nil
		pad, lengthWidth := 0, 0
		if c.dgramPad {
			pad, lengthWidth = initialPaddingAfter(len(c.dgram), len(p.prefix), 4, len(p.payload))
		}
		// sealLocked fails only on Seal's invariant guards (a header
		// sample too short), which the constant 4-byte packet number makes
		// unreachable: there is no error here to handle.
		c.dgram, _ = c.sealLocked(c.dgram, p, pad, lengthWidth)
	}
	if len(c.dgram) > 0 {
		_ = c.writeLocked(c.pc, c.dgram, c.peer, c.dgramECN)
	}
	c.dgram = c.dgram[:0]
	c.dgramPad = false
}

// writePacketLocked is the wire half of sendPacketLocked: header, seal,
// padding, the datagram write, and the recovery bookkeeping — or, while
// gathering, everything but the seal and the write, which
// flushDatagramLocked finishes for the whole datagram at once.
//
// In order:
//
//  1. Refuse a connection that is draining or has said its close, and
//     decide whether this packet coalesces (Initial and Handshake only).
//  2. Build the header for the packet's space — long, or short with the key
//     phase, or 0-RTT for a DialEarly client before its 1-RTT keys.
//  3. Choose the destination (the peer, or a path being probed) and its
//     padding, and size the datagram.
//  4. Check the limits that may stop it: the amplification limit before the
//     peer's address is validated (§8.1), on the path or off it, the packet
//     number space (§12.3) and the AEAD's confidentiality limit (RFC 9001
//     §6.6).
//  5. Gathering: queue the packet for flushDatagramLocked. Otherwise take
//     the packet number, seal, and write — or keep it for the write batch.
func (c *Conn) writePacketLocked(sp *space, space int, payload, retransPart []byte, opts sendOpts, now time.Time) error {
	// Every packet passes here — acknowledgements, probes, retransmissions
	// and control frames included, not only what sendPacketLocked guards —
	// so this is where the two closed states refuse them. RFC 9000 §10.2.1:
	// once this end's CONNECTION_CLOSE left, it "MUST NOT send further
	// packets" other than that close; §10.2.2: an endpoint in the draining
	// state "MUST NOT send any packets". The close itself is the exception
	// (sendCloseLocked sets closeSent before writing it, and the closing
	// state replays the kept datagram without coming through here).
	if c.draining || (c.closeSent && !opts.close) {
		return errClosed
	}
	const pnLen = 4
	// A DialEarly client before its 1-RTT keys sends application data in
	// 0-RTT packets: a long header of type 1, sealed with the early keys
	// (RFC 9001 §4.6, RFC 9000 §17.2.3). Known here because the coalescing
	// decision below turns on it.
	early := space == spaceApplication && sp.sealer == nil && c.earlySealer != nil
	// Only Initial and Handshake packets coalesce, and never a
	// CONNECTION_CLOSE, which must leave now. Anything else first flushes
	// what gathered, so order on the wire is the order of sending.
	//
	// 0-RTT packets coalesce too, and for a reason of their own: they carry
	// a long header, so they have a Length field and may precede another
	// packet in the same datagram, which a short-header 1-RTT packet never
	// can (§12.2 — it has no Length field, so it is always the last packet
	// of its datagram). They coalesce under gathering, as the Initial and
	// Handshake packets do — gatherLocked(false) closes the datagram, and a
	// Retry's resend of the 0-RTT flight (handleRetry) runs under it — and
	// otherwise only while another writer is inside [Stream.Write] or
	// waiting for mu to get there (writersIn), because only then does the
	// gathered datagram have a known closer: the last writer out flushes
	// it. One writer on its own sends at once.
	//
	// What that is worth. A client that opens many streams at once and
	// writes a small request on each — 40 requests the moment the
	// connection opens, which is the QUIC Interop Runner's zerortt case and
	// the shape of any multiplexed request burst — would otherwise send one
	// datagram per write and a second for the end of each stream: some 140
	// datagrams of about 200 bytes for 10 KB of requests. A path that queues
	// 25 datagrams drops most of such a burst, and RFC 9001 §4.6.1 has the
	// lost 0-RTT data resent in 1-RTT packets once the keys are there,
	// which the zerortt case counts against the client (at most
	// 0.5 x FILENAMELEN x NUM_FILES bytes in 1-RTT). Any path with a queue
	// does the same: a sender that pays a datagram per frame loses the ones
	// the queue has no room for, and pays for them twice.
	gathering := !opts.close && ((c.gather && (space != spaceApplication || early)) ||
		(early && c.writersIn.Load() > 1))
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
	if space == spaceApplication && !early {
		header = append(header, 0x40|c.spin|c.ku.phase|byte(pnLen-1)) // short header, fixed bit set, §17.4 spin, RFC 9001 §6 key phase
		if opts.dcid != nil {
			header = append(header, opts.dcid...)
		} else {
			header = append(header, c.dcid...)
		}
	} else {
		typ := byte(packetInitial)
		switch {
		case early:
			typ = packetZeroRTT
		case space == spaceHandshake:
			typ = packetHandshake
		}
		header = append(header, 0xc0|typ<<4|byte(pnLen-1), 0, 0, 0, 1, byte(len(c.dcid))) //nolint:gosec // G115: a connection ID is at most 20 bytes; version 1
		header = append(header, c.dcid...)
		header = append(header, byte(len(c.scid))) //nolint:gosec // G115: a connection ID is at most 20 bytes
		header = append(header, c.scid...)
		if typ == packetInitial {
			header = varint.Append(header, uint64(len(c.retryToken)))
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
	// The path left behind unproven keeps books of its own, because it is
	// neither of the two addresses above: §9.3.3's challenge is resent
	// there by the probe timer, after the move gave amplRecv and amplSent
	// to the new path (pathProbe.oldRecv).
	oldBudgeted := !onPath && !c.pathProbe.oldValidated &&
		c.pathProbe.oldPeer != nil && sameAddr(dest, c.pathProbe.oldPeer)
	if opts.padTo > 0 && space == spaceApplication {
		target := opts.padTo
		if c.amplActive && onPath {
			target = min(target, int(3*c.amplRecv-c.amplSent))
		}
		if budgeted {
			target = min(target, int(3*c.offPath.recv-c.offPath.sent))
		}
		if oldBudgeted {
			// §8.2.1: the datagram is expanded to 1200 "unless the
			// anti-amplification limit for the path does not permit
			// sending a datagram of this size". It then validates the
			// address and not the path MTU, which is what the section
			// expects and what a later full-size validation settles.
			target = min(target, int(3*c.pathProbe.oldRecv-c.pathProbe.oldSent))
		}
		if pad := target - (len(header) + pnLen + len(payload) + 16); pad > 0 {
			payload = append(payload, make([]byte, pad)...)
		}
	}
	// RFC 9000 §10.3: every packet SHOULD be at least 22 bytes longer than
	// the identifier this end asks the peer to send under, so that the
	// peer's stateless reset — 21 bytes at the least — is never the
	// shortest thing addressed here. A short header is 21 bytes with the
	// peer's identifier, the packet number and the tag: a PING toward a
	// peer with short identifiers fell under it. Long headers carry both
	// identifiers and clear it. An ACK-only packet padded here stays out of
	// the flight like any ACK-only packet: a few bytes, never acknowledged
	// for their own sake (RFC 9002 §2 counts PADDING in flight so that a
	// padded probe is congestion-controlled, which this is not). A copy:
	// payload may be the head of a retransmission chunk whose tail shares
	// its array.
	if space == spaceApplication && !early {
		if pad := 22 + len(c.scid) - (len(header) + pnLen + len(payload) + 16); pad > 0 {
			payload = append(payload[:len(payload):len(payload)], make([]byte, pad)...)
		}
	}
	// The datagram's size on the wire, for the amplification check: known
	// exactly before sealing. While gathering, the padding a closing Initial
	// will add is still to come — counted as the room it fills.
	size := len(header) + pnLen + len(payload) + 16
	if space != spaceApplication || early {
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
		// sends it once the peer's next datagram raises amplRecv. An ACK
		// was settled when it was encoded, so its debt is owed again
		// (rearm) for the flush that follows that datagram; probes
		// regenerate from current state and are simply skipped. No packet
		// number was consumed.
		if opts.ackOnly || opts.carriesAck {
			sp.ack.rearm()
		}
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
	if oldBudgeted && c.pathProbe.oldSent+int64(size) > 3*c.pathProbe.oldRecv {
		// Same for the unproven path left behind: the challenge is given
		// up rather than sent past the budget. A legitimate peer still
		// there answers the one that did go, or sends from that address
		// again, which raises the budget.
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
	// §6.6 has an endpoint that reaches a limit "stop using the connection
	// and only send stateless resets", and in the next sentence makes it
	// RECOMMENDED to close with AEAD_LIMIT_REACHED before getting there:
	// the second is what happens here, one CONNECTION_CLOSE under the keys
	// in use and nothing more under them — the MUST describes the state of
	// an endpoint that went past the limit without closing, which this one
	// never does. The integrity limit (receive.go) reads §6.6 the same way.
	keys := sp.sealer
	if early {
		keys = c.earlySealer
	}
	if !c.closeSent && keys.sealed >= keys.k.confLimit-1 {
		if space == spaceApplication && !early {
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
			c.dgram, _ = c.sealLocked(c.dgram, p, 0, 0) // unreachable error, as in flushDatagramLocked
		}
		if c.dgramPend == nil && len(c.dgram) == 0 {
			// The datagram's mark is decided when it opens; every packet
			// coalesced into it is recorded with the same one.
			c.dgramECN = c.ecn.mark()
		}
		pn := c.takePNLocked(sp, space)
		c.dgramPend = &pendingPacket{
			sp: sp, space: space, early: early, prefix: append([]byte(nil), header...), payload: payload,
			retransPart: retransPart, pn: pn, opts: opts, now: now, dest: dest, onPath: onPath, ecn: c.dgramECN,
		}
		if needPad {
			c.dgramPad = true
		}
		return nil
	}

	pn := c.takePNLocked(sp, space)
	p := pendingPacket{
		sp: sp, space: space, early: early, prefix: header, payload: payload,
		retransPart: retransPart, pn: pn, opts: opts, now: now, dest: dest, onPath: onPath, ecn: c.ecn.mark(),
	}
	if c.batching && space == spaceApplication && !early && opts.via == nil && onPath {
		return c.batchLocked(&p, size)
	}
	c.flushBatchLocked()
	pkt, err := c.sealLocked(c.pktScratch[:0], &p, 0, 0)
	if err != nil {
		return err
	}
	c.pktScratch = pkt
	pc := c.pc
	if opts.via != nil {
		pc = opts.via
	}
	return c.writeLocked(pc, pkt, dest, p.ecn)
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
// queues to it, under the packet's real header, and sends a waiting writer
// back to cut its frame again when the size fell meanwhile), but maxDatagram is what the path search believes, not what
// the outgoing interface will carry: a probe above the interface MTU comes
// back EMSGSIZE, since the socket forbids fragmenting (udp.Configure), and
// that needs no special case either — the probe is never acknowledged, and
// the search reads its loss and caps the path where the interface does
// (pmtu.go).
//
// pad is the §14.1 padding to append to the payload, and lengthWidth the
// width to encode the long header's Length field on when it is wider than
// the value needs (initialPaddingAfter); zero means the minimal encoding.
func (c *Conn) sealLocked(dst []byte, p *pendingPacket, pad, lengthWidth int) ([]byte, error) {
	const pnLen = 4
	sp := p.sp
	sealer := sp.sealer
	if p.early {
		sealer = c.earlySealer
	}
	if sp.discarded || sealer == nil {
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
	if p.space != spaceApplication || p.early {
		// The length covers the packet number and the sealed payload, tag
		// included.
		length := uint64(pnLen + len(payload) + 16)
		if lengthWidth > varintBytes(length) {
			header = appendVarintOn(header, length, lengthWidth)
		} else {
			header = varint.Append(header, length)
		}
	}
	pnOffset := len(header)
	for i := pnLen - 1; i >= 0; i-- {
		header = append(header, byte(p.pn>>(8*uint(i)))) //nolint:gosec // G115: keeps the low octet of each shift
	}
	before := len(dst)
	out, err := sealer.Seal(dst, header, payload, p.pn, pnOffset, pnLen)
	if err != nil {
		return dst, err
	}
	size := len(out) - before
	switch {
	case c.amplActive && p.onPath:
		c.amplSent += int64(size)
	case !p.onPath && c.offPath.addr != nil && sameAddr(p.dest, c.offPath.addr):
		c.offPath.sent += int64(size)
	case !p.onPath && !c.pathProbe.oldValidated && c.pathProbe.oldPeer != nil &&
		sameAddr(p.dest, c.pathProbe.oldPeer):
		c.pathProbe.oldSent += int64(size)
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

	if p.ecn == ecnECT0 {
		p.sp.ecnSent++ // ACK-only packets included: the peer counts them too
	}
	if p.opts.ackOnly {
		return out, nil
	}
	record := sentPacket{pn: p.pn, sentAt: p.now, size: size, ackEliciting: true, pmtuProbe: p.opts.pmtuProbe, ect0: p.ecn == ecnECT0}
	c.delivery.onSent(&record, c.cc.bytesInFlight())
	if p.opts.pmtuProbe {
		// Counted here, not where the probe was decided: a packet refused
		// before sealing (number space or key exhausted) was never a try.
		c.pmtu.inFlight, c.pmtu.probePN = true, p.pn
		c.pmtu.probes++
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
		if space == spaceInitial && len(c.retryToken) > 0 {
			// After a Retry the client's Initial carries the token, which
			// maxStreamChunk's slack does not cover (§14.2).
			n = min(n, c.maxStreamChunk()-varintBytes(uint64(len(c.retryToken)))-len(c.retryToken))
		}
		// Beside a gathered Initial, a chunk cut to the room left fills the
		// datagram the Initial would otherwise pad with zeros (§14.1): the
		// ServerHello's Initial and the first of the certificate chain
		// travel together instead of in two 1200-byte datagrams.
		if room := c.cryptoRoomLocked(); room > 0 && room < n {
			n = room
		}
		frame := varint.Append(nil, frameCrypto)
		frame = varint.Append(frame, c.spaces[space].cryptoOff)
		frame = varint.Append(frame, uint64(n)) //nolint:gosec // G115: a byte count, positive
		frame = append(frame, data[:n]...)
		c.spaces[space].cryptoOff += uint64(n) //nolint:gosec // G115: a byte count, positive
		data = data[n:]
		// CRYPTO obeys the congestion window like everything else (RFC 9002
		// §7) — but it must not *wait* on it: the credit arrives on the same
		// loop that is sending this, during the handshake. What does not fit
		// queues behind the lost payloads, and retransmitLocked drains it as
		// acknowledgements reopen the window.
		//
		// Behind them whatever stopped them: once a chunk waits, every later
		// one queues after it, in offset order. The anti-amplification
		// budget withholds a packet by putting it back at the head of the
		// queue, which is right for the head retransmitLocked just took and
		// reversed a fresh certificate chain chunk by chunk: the budget
		// lifted, the server sent its Handshake CRYPTO from offset 8870
		// down to 4326, and kwik, which buffers the 4096 out-of-order bytes
		// RFC 9000 §7.5 requires and no more, closed with
		// CRYPTO_BUFFER_EXCEEDED on the first (runner amplificationlimit).
		sp := &c.spaces[space]
		if len(sp.retrans) > 0 || !c.cc.canSend(len(frame)+64) {
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

// armPaceLocked asks the timer loop to come back at until — unless an
// earlier pace deadline is already armed — for whoever the pacer stopped:
// a parked writer, or the retransmission pass. Callers hold mu.
func (c *Conn) armPaceLocked(until time.Time) {
	if c.paceDeadline.IsZero() || until.Before(c.paceDeadline) {
		c.paceDeadline = until
		c.kickTimer()
	}
}

// retransmitLocked drains the queues of lost payloads, best effort under the
// congestion window and the pacer: what the window refuses goes when the
// next ACK opens it again; what the pacer's bucket does not hold whole goes
// at the pace deadline, when the timer loop runs the next pass. One pass
// therefore sends at most one bucket — the initial window, the burst RFC
// 9002 §7.7 permits — rather than whatever the window leaves after an ACK
// that releases most of a large flight and declares the rest lost.
// Nothing parks here: the read and timer loops are the callers.
func (c *Conn) retransmitLocked() {
	// The read loop calls this after every packet it receives, and almost
	// always every queue is empty: then there is nothing to pace, and the
	// clock read, the controller's rate and the bucket's refill below would
	// be paid per packet for nothing. Skipping the refill changes nothing
	// either — it credits the time elapsed since the last one, capped at
	// the burst, so the next refill credits the same total.
	if len(c.spaces[spaceInitial].retrans) == 0 && len(c.spaces[spaceHandshake].retrans) == 0 &&
		len(c.spaces[spaceApplication].retrans) == 0 {
		c.pruneResetIDsLocked()
		return
	}
	rate := c.cc.pacingRate(c.rtt.current())
	c.pacer.refill(time.Now(), rate)
	for space := range c.spaces {
		sp := &c.spaces[space]
		// canSealLocked, not the space's own keys: the 0-RTT payloads a
		// Retry put back (handleRetry) go again in 0-RTT, before the 1-RTT
		// keys exist.
		if !c.canSealLocked(sp, space) {
			continue
		}
		for len(sp.retrans) > 0 {
			payload, rest := c.fitLocked(space, sp.retrans[0])
			if len(payload) == 0 {
				sp.retrans = sp.retrans[1:] // nothing of it is owed any more
				continue
			}
			if !c.cc.canSend(len(payload) + 64) {
				return
			}
			if d := c.pacer.until(len(payload)+c.packetOverhead(space), rate); d > 0 {
				c.armPaceLocked(time.Now().Add(d))
				return
			}
			if rest != nil {
				sp.retrans[0] = rest
			} else {
				sp.retrans = sp.retrans[1:]
			}
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
	c.pruneResetIDsLocked()
}

// fitLocked cuts a payload recorded for retransmission to what one packet
// of space carries under maxDatagram, returning the part to send now and
// the rest (nil when it all fits). A payload sent under a larger datagram
// size — before a black hole or a new path brought it back to 1200
// (pmtu.go) — would otherwise be resent whole into the path that just lost
// it. The frames of a stream reset since leave the payload first
// (pruneResetLocked); a payload with nothing left comes back empty, and
// the caller owes nothing for it. Callers hold mu.
func (c *Conn) fitLocked(space int, payload []byte) (now, rest []byte) {
	if space == spaceApplication && len(c.resetIDs) > 0 {
		if payload = c.pruneResetLocked(payload); len(payload) == 0 {
			return nil, nil
		}
	}
	return fitFrames(payload, c.maxDatagram-c.packetOverhead(space))
}

// streamDataRecvdLocked reports whether s's sending direction is in RFC 9000
// §3.1's terminal "Data Recvd" state: its FIN has been sent and every byte
// up to it acknowledged. From there nothing may be sent for the stream, a
// RESET_STREAM included (§3.3), and §3.5 asks for one on STOP_SENDING only
// in "Ready" or "Send" ("Data Sent" may defer it). The state is read on
// demand, from the payloads still tracked in the application space — the
// packets in flight and the lost ones waiting to go again — rather than
// kept per packet or per stream: an acknowledged packet leaves the record,
// a lost one puts its payload on the queue, and a payload a probe lifted
// travels in the probe, which is tracked too; so a STREAM frame of s found
// nowhere is a STREAM frame acknowledged. The walk costs a parse of what
// is tracked on the event that asks, and nothing per packet or per ACK; and
// it is walked once per stream, since "Data Recvd" is terminal and the
// verdict is kept (s.dataRecvd). A peer that holds its own direction open
// may ask again with every STOP_SENDING, and each walk would cost the
// whole flight. Callers hold mu.
func (c *Conn) streamDataRecvdLocked(s *Stream) bool {
	if s.dataRecvd {
		return true
	}
	if !s.sendClosed || s.writeReset {
		return false
	}
	sp := &c.spaces[spaceApplication]
	for i := range sp.sent.packets {
		if carriesStream(sp.sent.packets[i].payload, s.id) {
			return false
		}
	}
	for _, payload := range sp.retrans {
		if carriesStream(payload, s.id) {
			return false
		}
	}
	s.dataRecvd = true
	return true
}

// carriesStream reports whether payload holds a STREAM frame of stream id.
// A payload that does not parse, which cannot happen, is read as carrying
// it: the safe answer keeps the stream out of "Data Recvd".
func carriesStream(payload []byte, id uint64) bool {
	for n := 0; n < len(payload); {
		f, tail, err := parseFrame(payload[n:])
		if err != nil {
			return true
		}
		if f.IsStream() && f.StreamID == id {
			return true
		}
		n = len(payload) - len(tail)
	}
	return false
}

// noteResetLocked records that this end reset stream id, for the replay
// filter: the mark is the next application packet number, so that every
// packet that may carry a STREAM frame of the stream is older than it — the
// RESET_STREAM itself leaves at the mark and carries none.
//
// The control queue is filtered at the same moment: a STREAM_DATA_BLOCKED a
// parked writer queued waits there for the congestion window, which the
// RESET_STREAM does not wait for, and would leave after it — §3.3 forbids
// the frame in "Reset Sent". Callers hold mu.
func (c *Conn) noteResetLocked(id uint64) {
	if c.resetIDs == nil {
		c.resetIDs = make(map[uint64]uint64)
	}
	c.resetIDs[id] = c.spaces[spaceApplication].nextPN
	c.control = c.pruneResetLocked(c.control)
}

// pruneResetLocked drops from payload — a lost packet's frames, about to go
// again — the STREAM and STREAM_DATA_BLOCKED frames of the streams this end
// has reset since: after RESET_STREAM an endpoint ceases transmission and
// retransmission of both (RFC 9000 §3.3, §19.4). Every other frame goes as
// it was, the RESET_STREAM above all when that is what was lost. The
// payload comes back untouched, without a copy, when nothing is dropped;
// a payload that does not parse, which cannot happen, goes whole.
func (c *Conn) pruneResetLocked(payload []byte) []byte {
	var kept []byte
	n := 0
	for n < len(payload) {
		f, tail, err := parseFrame(payload[n:])
		if err != nil {
			return payload
		}
		l := len(payload) - n - len(tail)
		drop := f.IsStream() || f.Type == frameStreamDataBlocked
		if drop {
			_, drop = c.resetIDs[f.StreamID]
		}
		switch {
		case drop && kept == nil:
			kept = append(make([]byte, 0, len(payload)), payload[:n]...)
		case !drop && kept != nil:
			kept = append(kept, payload[n:n+l]...)
		}
		n += l
	}
	if kept == nil {
		return payload
	}
	return kept
}

// pruneResetIDsLocked forgets the reset streams nothing in flight predates:
// with the retransmission queue empty and every packet in flight sent at or
// after a stream's mark, no STREAM frame of it can come back. Called at the
// end of a replay pass, which is when the queue is known empty.
func (c *Conn) pruneResetIDsLocked() {
	sp := &c.spaces[spaceApplication]
	if len(c.resetIDs) == 0 || len(sp.retrans) > 0 {
		return
	}
	oldest := sp.nextPN
	if len(sp.sent.packets) > 0 {
		oldest = sp.sent.packets[0].pn
	}
	for id, mark := range c.resetIDs {
		if mark <= oldest {
			delete(c.resetIDs, id)
		}
	}
}

// fitFrames cuts payload, frames this end encoded, so that the first part
// is no longer than room: whole frames while they fit, then the first one
// that does not, split in two if it carries stream or handshake data —
// the same bytes at the same offsets, the FIN on the second half. A first
// frame that cannot be split leaves whole, as it was sent before; so does
// a payload that does not parse, which cannot happen.
func fitFrames(payload []byte, room int) (now, rest []byte) {
	if len(payload) <= room {
		return payload, nil
	}
	n := 0
	for n < len(payload) {
		f, tail, err := parseFrame(payload[n:])
		if err != nil {
			return payload, nil
		}
		l := len(payload) - n - len(tail)
		if n+l <= room {
			n += l
			continue
		}
		if first, second := splitDataFrame(f, room-n); first != nil {
			return append(payload[:n:n], first...), append(second, tail...)
		}
		break
	}
	if n == 0 {
		return payload, nil
	}
	return payload[:n], payload[n:]
}

// splitDataFrame re-encodes a STREAM or CRYPTO frame as two: the first no
// longer than room, carrying as many bytes as fit, the second the rest.
// Nil when f is another type or room leaves no byte of data.
func splitDataFrame(f Frame, room int) (first, second []byte) {
	stream := f.IsStream()
	if !stream && f.Type != frameCrypto {
		return nil, nil
	}
	header := 1 + varintBytes(f.offset) + varintBytes(uint64(max(room, 0))) //nolint:gosec // G115: non-negative
	if stream {
		header += varintBytes(f.StreamID)
	}
	k := room - header
	if k <= 0 || k >= len(f.Data) {
		return nil, nil
	}
	encode := func(dst []byte, off uint64, data []byte, fin bool) []byte {
		if stream {
			dst = varint.Append(dst, FrameStream|streamOFF|streamLEN|boolBit(fin, streamFIN))
			dst = varint.Append(dst, f.StreamID)
		} else {
			dst = varint.Append(dst, frameCrypto)
		}
		dst = varint.Append(dst, off)
		dst = varint.Append(dst, uint64(len(data)))
		return append(dst, data...)
	}
	first = encode(nil, f.offset, f.Data[:k], false)
	second = encode(nil, f.offset+uint64(k), f.Data[k:], f.Fin) //nolint:gosec // G115: k is positive
	return first, second
}

// sendProbeLocked answers a probe timeout in one space with two datagrams
// (RFC 9002 §6.2.4 allows two, so that one lost probe does not cost a whole
// doubled interval; probes may exceed the congestion window, §7): the two
// oldest unacknowledged payloads — they are the most likely lost — or, when
// only one is left to resend, that one twice. A ping goes out alone when
// there is nothing to resend but an answer is still owed. The queue of
// declared losses goes first; the rest is lifted from the oldest packets in
// flight, which stay there (liftOldestResendable says why), and sent as new
// packets counted on top of them (RFC 9002 §7.5).
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
	// A payload fitLocked empties — frames of a stream reset since — fills
	// no slot: the next one is looked at instead.
	n := 0
	for n < probes && len(sp.retrans) > 0 {
		var rest []byte
		payloads[n], rest = c.fitLocked(space, sp.retrans[0])
		if rest != nil {
			sp.retrans[0] = rest
		} else {
			sp.retrans = sp.retrans[1:]
		}
		if len(payloads[n]) > 0 {
			n++
		}
	}
	// Lifted before any is sent: the copies are recorded as they go, and
	// the newest of them must not be the next one lifted. The packets
	// lifted from stay in flight and in the count (RFC 9002 §7.5: probes
	// are additionally in flight); only their payloads travel.
	for n < probes {
		payload, ok := sp.sent.liftOldestResendable()
		if !ok {
			break
		}
		var rest []byte
		if payloads[n], rest = c.fitLocked(space, payload); rest != nil {
			sp.retrans = append([][]byte{rest}, sp.retrans...)
		}
		if len(payloads[n]) > 0 {
			n++
		}
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

// earlyResendLimit is how many times a server resends its Initial flight
// ahead of the probe timeout (RFC 9002 §6.2.3). The RFC finds once enough
// for a single loss; the runner's handshakecorruption case corrupts in
// bursts of up to three, and quinn's client resent its ClientHello five
// times while all seven of this end's ServerHello datagrams arrived
// corrupted and the 10 s handshake deadline ran out on a PTO backoff of
// 1, 2 and 4 s. Twice keeps the §6.2.3 warning — a server that answers
// every packet it cannot use risks an endless exchange — and each resend
// still spends the anti-amplification budget like any other.
const earlyResendLimit = 2

// resendEarlyLocked is §6.2.3's early resend: a client repeating Initial
// CRYPTO bytes this end already took has not received this end's Initial
// answer, so a server with Initial data still unacknowledged resends it now
// as a probe would, instead of waiting out the timer. Only the server: a
// client's Initial flight is the ClientHello, which the server's own
// answer acknowledges. Callers hold mu.
func (c *Conn) resendEarlyLocked() {
	if c.isClient || c.earlyResends >= earlyResendLimit {
		return
	}
	sp := &c.spaces[spaceInitial]
	if sp.discarded || sp.sealer == nil {
		return
	}
	pending := len(sp.retrans) > 0
	for i := range sp.sent.packets {
		pending = pending || (sp.sent.packets[i].ackEliciting && len(sp.sent.packets[i].payload) > 0)
	}
	if !pending {
		return
	}
	c.earlyResends++
	c.sendProbeLocked(spaceInitial)
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
	// Nor does a gathered datagram: a writer that sleeps on the window
	// while holding the 0-RTT packets of other writers would keep the peer
	// waiting for the very data that opens it.
	c.flushDatagramLocked()
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
		// Not for a stream whose sending part is closed — reset since, or
		// its FIN sent: nothing waits for its credit any more, §3.3 forbids
		// the frame after RESET_STREAM and from "Data Recvd", and "Data
		// Sent" has no need of it.
		if s.sendQ.avail() == 0 && s.sendQ.blockedSent && !s.sendClosed {
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
func (c *Conn) writeLocked(pc net.PacketConn, b []byte, to net.Addr, ecn uint8) error {
	if c.closing.capturing {
		c.keepClosingDatagramLocked(pc, b, to) // the CONNECTION_CLOSE, for the closing state
	}
	var err error
	if ecn != ecnNotECT {
		c.oobScratch, err = writeMarked(pc, b, to, ecn, c.oobScratch)
	} else {
		_, err = pc.WriteTo(b, to)
	}
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
	if c.batchN > 0 && (c.batchShort || size > c.batchSeg || p.ecn != c.batchECN ||
		c.batchN == maxBatchSegments || len(c.batch)+size > maxBatchBytes) {
		c.flushBatchLocked() // one segmented write carries one ECN mark
	}
	out, err := c.sealLocked(c.batch, p, 0, 0)
	if err != nil {
		return err
	}
	size = len(out) - len(c.batch)
	c.batch = out
	switch {
	case c.batchN == 0:
		c.batchSeg, c.batchECN = size, p.ecn
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
		c.oobScratch, ok, err = writeSegments(c.pc, b, seg, c.peer, c.batchECN, c.oobScratch)
		if ok {
			c.segmented++
			c.keepBatchErr(c.wroteLocked(err))
			return
		}
		c.noGSO = true
	}
	for len(b) > 0 {
		k := min(seg, len(b))
		c.keepBatchErr(c.writeLocked(c.pc, b[:k], c.peer, c.batchECN))
		b = b[k:]
	}
}

func (c *Conn) keepBatchErr(err error) {
	if c.batchErr == nil {
		c.batchErr = err
	}
}

// canSealLocked reports whether a packet of space can be sealed now: the
// space's own keys, or for the application space a DialEarly client's 0-RTT
// keys before the 1-RTT ones arrive. Callers hold mu.
func (c *Conn) canSealLocked(sp *space, space int) bool {
	if sp.discarded {
		return false
	}
	return sp.sealer != nil || (space == spaceApplication && c.earlySealer != nil)
}

// awaitSealLocked returns once a packet of space can be sealed, or the
// error that says it never will. The one wait is a DialEarly client whose
// 0-RTT the server refused: crypto/tls reports the refusal just before the
// 1-RTT keys, the 0-RTT keys are gone, and a stream opened in between waits
// the instant until the 1-RTT ones are in. Callers hold mu.
func (c *Conn) awaitSealLocked(sp *space, space int) error {
	for !c.canSealLocked(sp, space) {
		if sp.discarded || space != spaceApplication || !c.earlyRejected || c.draining || c.closeSent || c.closeErr != nil {
			return fmt.Errorf("%w: nothing to seal packets with at level %d", ErrQUIC, space)
		}
		c.cond.Wait()
	}
	return nil
}
