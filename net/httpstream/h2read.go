package httpstream

import (
	"errors"
	"fmt"
	"time"
)

// consume folds a batch of frames into stream state and returns the requests
// that became complete, with the identifier each arrived on.
//
// A stream error does not end the loop: the offending stream is reset and the
// frames behind it — which belong to other requests — are served. That is the
// difference the split made, and the reason the loop looks like this rather
// than returning at the first refusal.
func (s *h2Server) consume(frames []h2Frame) ([]Request, []uint32, error) {
	var (
		ready []Request
		ids   []uint32
	)
	for _, f := range frames {
		st, err := s.frame(f)
		if err != nil {
			if se, ok := errors.AsType[streamError](err); ok {
				if err := s.reset(se); err != nil {
					return nil, nil, err
				}
				continue
			}
			return nil, nil, err
		}
		if st != nil {
			// The request is the responder's now; the stream was retired
			// by finish, and nothing here keeps it.
			ready = append(ready, st.req)
			ids = append(ids, st.id)
		}
	}
	ready, ids = s.handOver(ready, ids)
	if err := s.expireAssembly(); err != nil {
		return nil, nil, err
	}
	return ready, ids, nil
}

// handOver appends to ready the requests of the frame batch whose head is
// complete and whose body is still arriving, with the body as a stream: a
// body whole in the batch that brought its head was finished and is in ready
// already, so small requests still batch (docs/design/adr/0009, article 4).
//
// The stream changes counts as finish has a request change them: it leaves
// the open ones for the ones in flight, which keeps the sum the concurrency
// bound reads — and the batch queue — what it was, and it is a request
// served, for the rapid-reset bound. It stays in streams, where its DATA is
// routed, until its body has ended or its handler has returned.
func (s *h2Server) handOver(ready []Request, ids []uint32) (reqs []Request, streams []uint32) {
	for _, st := range s.pending {
		if s.streams[st.id] != st || st.state != stateOpen {
			continue // reset, or finished, within the batch
		}
		if st.stream == nil {
			st.stream = s.newH2Body(st)
		}
		st.handed = true
		s.handed++
		s.open.Add(-1)
		s.inFlight.Add(1)
		s.served++
		req := st.req
		req.body = &bodyStream{q: st.stream}
		ready = append(ready, req)
		ids = append(ids, st.id)
		if !st.stream.unread() {
			// Ended within the batch, past what Body may hold: nothing more
			// may arrive on it.
			s.retire(st.id, stateHalfClosed)
		}
	}
	clear(s.pending)
	s.pending = s.pending[:0]
	return ready, ids
}

// expireAssembly resets every stream whose request has been assembling for
// longer than ReadTimeout.
//
// It runs once per frame batch, which is exactly often enough: the only way a
// stalled stream's memory is held is by the peer sending *something* — a PING,
// a WINDOW_UPDATE, a byte of another stream — to keep the frame-level clock
// alive, and each of those somethings arrives through consume. A peer that
// sends nothing at all is the frame reader's own timeout's to end.
//
// A stream handed over with its body streaming has no such deadline — each
// wait for its bytes has ReadTimeout, on the handler's side — and is
// forgotten here once its handler has returned: nobody reads it, and its
// response resets it if the peer is still sending.
func (s *h2Server) expireAssembly() error {
	if s.open.Load() == 0 && s.handed == 0 {
		return nil
	}
	now := time.Now()
	s.expired = s.expired[:0]
	for id, st := range s.streams {
		if st.handed {
			if st.stream.isClosed() {
				s.retire(id, stateClosed)
			}
			continue
		}
		if now.After(st.deadline) {
			s.expired = append(s.expired, id)
		}
	}
	for _, id := range s.expired {
		err := s.reset(streamError{id: id, code: errCancel, err: fmt.Errorf(
			"%w: stream %d assembled nothing complete within the read timeout", ErrH2Protocol, id)})
		if err != nil {
			return err
		}
	}
	return nil
}

// reset ends one stream: RST_STREAM to the peer, the state closed on this
// side, and whatever the responder had queued for it dropped.
//
// It counts against the same allowance a peer's own RST_STREAM does. A peer
// that only ever sends malformed requests would otherwise hold a connection
// open indefinitely for the cost of one refusal each, which is the rapid-reset
// shape with the roles swapped.
func (s *h2Server) reset(se streamError) error {
	if s.streams[se.id] == nil && s.closed[se.id] == stateHalfClosed && s.w.responded(se.id) {
		// Its request ended and its answer reached END_STREAM: "closed" in
		// §5.1's state machine, where this server's books still say
		// half-closed. A late frame on it is the peer's fault and answered
		// with nothing, as below; the books catch up so the next one is
		// tolerated without asking.
		s.closed[se.id] = stateClosed
	}
	if s.streams[se.id] == nil && s.closed[se.id] == stateClosed {
		// The stream is closed already — the peer reset it, or it finished —
		// and RFC 9113 §5.1 forbids any frame but PRIORITY on a closed
		// stream: the failure is noted on this side and nothing is sent.
		// Not counted against the reset allowance either: no reset left.
		s.w.forget(se.id)
		return nil
	}
	if err := s.countReset(); err != nil {
		return fmt.Errorf("%w: %w", err, se.err)
	}
	if st := s.streams[se.id]; st != nil && st.stream != nil {
		st.stream.fail(se.err)
	}
	s.retire(se.id, stateClosed)
	s.w.forget(se.id)
	if se.status != 0 {
		return s.w.refuse(se.id, se.status, se.head, se.code)
	}
	return s.w.control32(frameRSTStream, se.id, se.code)
}

// countReset charges one stream reset, from either end, against the
// rapid-reset allowance: four batches' worth of frames, plus two for every
// request this connection has completed. The RST_STREAM branch of frame
// states what "completed" has to mean for the bound to hold.
func (s *h2Server) countReset() error {
	s.resets++
	if s.resets > s.cfg.MaxFramesPerBatch*4+2*s.served {
		return fmt.Errorf("%w: %w: %d stream resets against %d completed requests",
			ErrH2Protocol, errCalm, s.resets, s.served)
	}
	return nil
}

// idle reports a stream the client has not opened (RFC 9113 §5.1): an even
// identifier, which only a server opens and this one opens none, or an odd
// one past the highest the client has used. A stream that has fallen out of
// the closed-stream ring is not idle — its identifier is at or below lastID.
func (s *h2Server) idle(id uint32) bool {
	return id%2 == 0 || id > s.lastID
}

// retire moves a stream out of the set the peer may still send on, and
// remembers it for a while.
//
// The two destinations are not the same news. A request that arrived whole is
// half-closed: nothing more may be sent on it, and DATA that arrives anyway is
// the refusal RFC 9113 §5.1 asks for — a body that resumes after its own
// END_STREAM grows past the bound it was already measured against. A stream
// that was reset, by either end, is closed: frames for it may already be on
// the wire and are tolerated rather than refused, which is what §5.1 asks for
// in the other direction.
//
// Kept rather than deleted, for two reasons beyond politeness. A trailing
// header block still has to reach the HPACK decoder — a block that is not
// decoded mis-decodes every block after it, on every stream, for the rest of
// the connection. And a trailing DATA still has to be counted against the
// connection window and replenished, because the peer already debited it
// there; a server that quietly drops it stalls the connection a few resets
// later with nothing in the logs to say why.
//
// What is kept is the state alone, an identifier and a byte, because that is
// all a late frame needs: the stream's own fields are an open stream's, and a
// connection that has served MaxConcurrentStreams requests holds that many
// retired streams for as long as it lives. A header block still arriving when
// the stream is retired is handed to late, where its remaining fragments land.
//
// The memory is bounded to MaxConcurrentStreams entries, oldest evicted first.
func (s *h2Server) retire(id uint32, state h2State) {
	st := s.streams[id]
	if st == nil {
		// Half-closed to closed; a stream already closed, or forgotten,
		// stays as it is.
		if s.closed[id] == stateHalfClosed {
			s.closed[id] = state
		}
		return
	}
	delete(s.streams, id)
	if st.handed {
		// In flight since handOver, not open; its body is failed if it has
		// not ended, which wakes a handler waiting on it.
		s.handed--
		st.stream.fail(errBodyConnClosed)
	} else {
		s.open.Add(-1)
	}
	// The body stops counting against the aggregate assembly bound here,
	// whichever way the stream left the open state: completed, it is the
	// handler's and bounded by the concurrency limit; reset, it is freed.
	s.assembling -= len(st.body)
	if len(st.block) > 0 {
		// Fragments of a block still arriving: they must reach the HPACK
		// decoder whole, or every later block mis-decodes.
		s.late = h2Stream{id: id, block: st.block, declared: -1}
	}
	s.remember(id, state)
}

// remember records a retired stream, evicting the oldest past the bound.
func (s *h2Server) remember(id uint32, state h2State) {
	s.closed[id] = state
	s.closedRing = append(s.closedRing, id)
	for len(s.closedRing) > s.cfg.MaxConcurrentStreams {
		delete(s.closed, s.closedRing[0])
		s.w.evict(s.closedRing[0])
		s.closedRing = s.closedRing[1:]
	}
}

// lookup returns what this end knows of a stream the client opened: the
// stream itself while it is open, late standing in for it once it is
// retired, nil once it is forgotten.
// h2OpenedWindow is how many of the most recent odd identifiers h2Server
// remembers as opened or skipped (h2OpenedWords words of 64 bits). Past it,
// an identifier is taken as opened: the tolerant answer, which costs the
// decode of one block and nothing else. 256 bytes a connection.
const (
	h2OpenedWords  = 32
	h2OpenedWindow = 64 * h2OpenedWords
)

// openID records that the client opened id, which must be odd and above
// lastID, and advances lastID. The odd identifiers skipped on the way are
// marked as never opened, so that a frame on one of them is refused as
// §5.1.1 asks rather than tolerated as a late frame.
func (s *h2Server) openID(id uint32) {
	if (id-s.lastID)/2 > h2OpenedWindow {
		clear(s.opened[:])
	} else {
		for skipped := s.lastID + 1 + s.lastID%2; skipped < id; skipped += 2 {
			s.opened[(skipped>>1)%h2OpenedWindow/64] &^= 1 << ((skipped >> 1) % 64)
		}
	}
	s.opened[(id>>1)%h2OpenedWindow/64] |= 1 << ((id >> 1) % 64)
	s.lastID = id
}

// wasOpened reports whether the client opened id, an odd identifier at or
// below lastID — or whether it is too old to tell, which reads as opened.
func (s *h2Server) wasOpened(id uint32) bool {
	if (s.lastID-id)/2 >= h2OpenedWindow {
		return true
	}
	return s.opened[(id>>1)%h2OpenedWindow/64]&(1<<((id>>1)%64)) != 0
}

func (s *h2Server) lookup(id uint32) *h2Stream {
	if st := s.streams[id]; st != nil {
		return st
	}
	state, ok := s.closed[id]
	if !ok {
		return nil
	}
	if s.late.id != id {
		s.late = h2Stream{id: id, declared: -1}
	}
	s.late.state = state
	return &s.late
}

func (s *h2Server) frame(f h2Frame) (*h2Stream, error) {
	switch f.Type {
	case frameSettings:
		if f.Flags&flagAck != 0 {
			// The peer has processed this end's SETTINGS: streams it opens
			// from here start at the advertised window (§6.9.2).
			//
			// No SETTINGS_TIMEOUT (§6.5.3's MAY): this end sends one SETTINGS,
			// and every bound it carries is enforced here from the first
			// byte, acknowledged or not — a peer that never acknowledges has
			// its streams start at the default window, which slows it and
			// costs this end nothing, until the idle timeout ends the
			// connection as it would any silent one.
			s.settingsAcked = true
			return nil, nil
		}
		table, haveTable, err := s.applySettings(f.Payload)
		if err != nil {
			return nil, err
		}
		return nil, s.w.ackSettings(table, haveTable)

	case framePing:
		if f.Flags&flagAck != 0 {
			if s.drainStep.Load() == drainBegun && string(f.Payload) == drainPing {
				// The round trip the first GOAWAY allowed for is over: every
				// stream the peer opened before it saw that GOAWAY is in,
				// and the second GOAWAY names the last of them. Set before
				// the frame goes out so a HEADERS later in this very batch
				// is refused rather than taken past the identifier it names.
				s.goingAway = true
				s.goAwayWith(s.lastID, errNoError)
				s.drainStep.Store(drainAnnounced)
			}
			return nil, nil
		}
		return nil, s.w.control(h2Frame{Type: framePing, Flags: flagAck, Payload: f.Payload})

	case frameWindowUpdate:
		inc := int32(be32(f.Payload) & 0x7fffffff) //nolint:gosec // G115: masked to 31 bits
		if f.StreamID != 0 && s.idle(f.StreamID) {
			// §5.1: on an idle stream only HEADERS and PRIORITY may arrive,
			// anything else is a connection error. A stream error is not an
			// option either, since RST_STREAM must never name an idle stream.
			return nil, fmt.Errorf("%w: WINDOW_UPDATE on stream %d, which is idle", ErrH2Protocol, f.StreamID)
		}
		if inc == 0 {
			// §6.9 splits the refusal: a zero increment on the connection is
			// a connection error, on a stream it costs only that stream.
			if f.StreamID == 0 {
				return nil, fmt.Errorf("%w: a WINDOW_UPDATE of zero", ErrH2Protocol)
			}
			return nil, streamFail(f.StreamID, errProtocolError,
				"%w: a WINDOW_UPDATE of zero on stream %d", ErrH2Protocol, f.StreamID)
		}
		if f.StreamID == 0 {
			return nil, s.w.creditConn(inc)
		}
		// Forwarded whatever this side's stream state says: a stream whose
		// request is complete is still one this end is writing a response on,
		// and the credit for it is exactly what that response is waiting for.
		return nil, s.w.credit(f.StreamID, inc)

	case frameRSTStream:
		// The error code the peer gives is not read, here or in GOAWAY
		// below: no behaviour of this server depends on why a client
		// cancelled, so an unknown code and INTERNAL_ERROR (§7's MAY) would
		// be treated alike anyway — ignored, which is what §7 requires of
		// an unknown one.
		//
		// Rapid reset: a client may legally cancel, and a client that cancels
		// endlessly makes a server allocate stream state endlessly for free.
		// CVE-2023-44487 is that, and the bound is the answer.
		//
		// The bound is a rate rather than a lifetime count: an allowance of
		// four batches' worth of frames, plus two cancellations for every
		// request this connection has actually completed. A long-lived
		// connection that serves requests may go on cancelling; one that only
		// cancels runs out. It is expressed against work done rather than
		// against a clock, so it holds while a connection is stalled and it
		// can be tested without one.
		//
		// "Completed" means answered, not merely assembled. The
		// CVE-2023-44487 pattern is HEADERS with END_STREAM followed at once
		// by RST_STREAM: the HEADERS completes a request — served goes up —
		// and the reset cancels it. Counted as served, each pair earned the
		// allowance for two more resets and the bound never tripped; the
		// handler ran for every cancelled request. A reset of a request that
		// is still in the responder's hands takes its credit back, so the
		// pattern spends the flat allowance and nothing else.
		if s.idle(f.StreamID) {
			return nil, fmt.Errorf("%w: RST_STREAM on stream %d, which is idle", ErrH2Protocol, f.StreamID)
		}
		st := s.streams[f.StreamID]
		if s.closed[f.StreamID] == stateHalfClosed || st != nil && st.handed {
			s.served--
		}
		if err := s.countReset(); err != nil {
			return nil, err
		}
		if st != nil && st.stream != nil {
			st.stream.fail(errBodyReset)
		}
		s.retire(f.StreamID, stateClosed)
		// The cancellation reaches the response being written for it, which
		// is what stops a producer working for a caller who walked away,
		// and the handler's context once no request of its batch is left.
		s.w.forget(f.StreamID)
		s.calls.leave(f.StreamID)
		return nil, nil

	case frameGoAway:
		s.goingAway = true
		return nil, nil

	case framePriority:
		if len(f.Payload) != 5 {
			// §6.3 makes the wrong length a stream error, FRAME_SIZE_ERROR —
			// not the connection error the other fixed-length frames rate —
			// which is why the shape check leaves it to this handler. On an
			// idle stream there is nothing to reset (§5.1: RST_STREAM never
			// names one), so it is the connection's, as nghttp2 and x/net
			// also make it.
			if s.idle(f.StreamID) {
				return nil, fmt.Errorf("%w: %w: PRIORITY is %d bytes on idle stream %d, want 5",
					ErrH2Protocol, errFrameSize, len(f.Payload), f.StreamID)
			}
			return nil, streamFail(f.StreamID, errFrameSizeError,
				"%w: %w: PRIORITY is %d bytes, want 5", ErrH2Protocol, errFrameSize, len(f.Payload))
		}
		return nil, nil // deprecated by RFC 9113 and ignored, as it allows

	case framePushPromise:
		// §8.4: only a server pushes, and this one has said ENABLE_PUSH is 0.
		return nil, fmt.Errorf("%w: a client sent PUSH_PROMISE", ErrH2Protocol)

	case frameHeaders, frameContinuation:
		return s.headers(f)

	case frameData:
		return s.data(f)
	}
	return nil, nil
}

// headers takes one HEADERS or CONTINUATION frame of a stream, in this order:
// find the stream (stream); strip the padding and the priority fields of a
// HEADERS frame; append the fragment to the stream's block, and wait for the
// rest unless the frame ends the headers; decode the whole block — always,
// since HPACK's table is shared — into the storage the stream's state allows;
// refuse a block over MaxHeaders fields; then one case per state — a block on
// a half-closed stream is refused, a block after the head is the trailer
// section (checked, kept or discarded, and the stream finished), and a first
// block is the request's head (buildRequest), refused with a status or a
// reset, or admitted.
func (s *h2Server) headers(f h2Frame) (*h2Stream, error) {
	st, err := s.stream(f.StreamID)
	if err != nil {
		return nil, err
	}
	block := f.Payload
	// Pad stripping is HEADERS-only, by both conditions below being exact:
	// CONTINUATION defines no flag besides END_HEADERS (§6.10), so a
	// CONTINUATION carrying the PADDED bit has the bit ignored — as §4.1
	// requires for undefined flags — and its *whole* payload is header block
	// fragment. Reading a pad length out of it instead would let the peer
	// carve bytes out of the HPACK stream that this end's decoder sees and a
	// conformant intermediary's does not, which is a dynamic-table desync.
	//
	// The pad's length is checked, its bytes are not — see data for why. A
	// missing pad length or priority block is PROTOCOL_ERROR rather than
	// §4.2's FRAME_SIZE_ERROR: the generic code §5.4 allows, see ADR 0008.
	if f.Type == frameHeaders && f.Flags&flagPadded != 0 {
		if len(block) == 0 {
			return nil, fmt.Errorf("%w: a padded HEADERS with no pad length", ErrH2Protocol)
		}
		pad := int(block[0])
		block = block[1:]
		if pad > len(block) {
			return nil, fmt.Errorf("%w: HEADERS padding of %d exceeds the block", ErrH2Protocol, pad)
		}
		block = block[:len(block)-pad]
	}
	if f.Type == frameHeaders && f.Flags&flagPriority != 0 {
		if len(block) < 5 {
			return nil, fmt.Errorf("%w: HEADERS with priority but no priority fields", ErrH2Protocol)
		}
		block = block[5:]
	}

	// [Frames] has already refused an interleaved block, so appending here is
	// appending to this stream's own block. END_STREAM rides on the HEADERS
	// frame and has to be remembered: the CONTINUATION that closes the block
	// cannot carry it, so reading the flag off the closing frame loses it.
	st.block = append(st.block, block...)
	if f.Type == frameHeaders && f.Flags&flagEndStream != 0 {
		st.endStream = true
	}
	if !f.EndsHeaders() {
		return nil, nil
	}

	// The block is decoded whatever happens to the stream, and that ordering
	// is not an accident: HPACK carries a dynamic table across a connection,
	// so a block left undecoded mis-decodes every block after it — on every
	// other stream. A closed stream's block is decoded and dropped; a
	// refusal after this point is a stream error, which by then can be one
	// because the shared decoder is already consistent.
	dst := st.headers
	switch {
	case st.state != stateOpen:
		dst = s.discard[:0]
	case st.handed:
		// The trailers of a streamed body: the request went to the handler
		// before they arrived, so they are checked and not kept, and the
		// head's storage, which the handler is reading, is left alone.
		dst = s.discard[:0]
	case st.gotHead:
		// Trailers, decoded behind the head's fields in the same storage:
		// Request.Headers is capped at its own length, so neither overwrites
		// the other, and a stream whose head left room costs no allocation.
		dst = st.headers[len(st.headers):]
	}
	fields, err := s.dec.Decode(dst, st.block)
	st.block = st.block[:0]
	if st.state != stateOpen {
		st.block = nil // a retired stream keeps no scratch warm
	}
	if err != nil {
		return nil, s.blockRefused(st, err)
	}
	if st.state == stateClosed {
		// Tolerated, and before the field-count bound below on purpose: the
		// stream has already been reset, and refusing an oversized block on
		// it would send a second RST_STREAM — and charge a second reset —
		// for a stream that is gone.
		s.discard = fields
		return nil, nil
	}
	// The field-count bound the h1 parser applies as it reads, applied here
	// at the first moment the count exists. The byte bound the decoder
	// enforces is not this bound: a block of near-empty fields fits the
	// bytes while making every [Request.Get] walk hundreds of entries. A
	// stream error, not a connection error — the table is already consistent.
	if fieldCount(fields) > s.cfg.MaxHeaders {
		return nil, statusFail(st.id, 431, nil, "%w: over %d header fields", ErrTooLarge, s.cfg.MaxHeaders)
	}

	switch {
	case st.state == stateHalfClosed:
		return nil, streamFail(st.id, errStreamClosed,
			"%w: HEADERS on stream %d after it ended", ErrH2Protocol, st.id)
	case st.gotHead:
		// A trailer section, held to the head's bounds and delivered in
		// Request.Trailers — but it ends the message, and §8.1 makes a
		// pseudo-header in it malformed: a trailer that carries :status or
		// :path is a second request's head pretending to be the end of this
		// one.
		st.gotTrailers = true
		// §8.1: the trailer block is the one that ends the stream, so a
		// second HEADERS without END_STREAM is a malformed request, a stream
		// error. Found by h2spec (8.1/1): before this check the block was
		// decoded and the server then waited for a DATA or HEADERS that
		// never came, holding the stream and its slot in the concurrency
		// bound until the idle timeout — one of several such requests
		// would quietly pin MaxConcurrentStreams.
		if !st.endStream {
			return nil, streamFail(st.id, errProtocolError,
				"%w: a trailer section on stream %d without END_STREAM", ErrH2Protocol, st.id)
		}
		// The rules the head's fields passed, connection-specific fields
		// included (checkRequestTrailer says why). A bare reset, no 400:
		// §8.2.1 exempts a fault in a trailer section from the status.
		for _, h := range fields {
			if err := checkRequestTrailer(h, ErrH2Protocol); err != nil {
				return nil, streamError{id: st.id, code: errProtocolError, err: err}
			}
		}
		if st.stream != nil {
			// A streamed body: the trailers go with it, read through
			// Request.StreamTrailers once it has ended.
			st.stream.setTrailers(fields)
		}
		if st.handed {
			s.discard = fields
			return s.finish(st)
		}
		if len(fields) > 0 && st.stream == nil {
			st.req.Trailers = fields
		}
		return s.finish(st)
	}

	st.gotHead = true
	req, declared, err := buildRequest(fields, ErrH2Protocol)
	if err != nil {
		if errors.Is(err, errConnect) {
			// Well formed and not served: a status on the stream, as the
			// bounds get one, where a reset would call a conforming
			// request malformed (RFC 9110 §15.6.2).
			return nil, statusFail(st.id, 501, nil, "%w", err)
		}
		// §8.1.1: a malformed request is a stream error. It is the whole
		// point of the per-stream path — one bad request among a hundred
		// costs that one. The method is looked up again for the 400's
		// shape: buildRequest returned nothing.
		var method []byte
		for _, h := range fields {
			if string(h.Name) == ":method" {
				method = h.Value
				break
			}
		}
		return nil, malformedFail(st.id, method, err)
	}
	st.headers, st.declared, st.req = req.Headers, declared, req
	st.req.Headers = req.Headers[:len(req.Headers):len(req.Headers)]
	if bound := max(s.cfg.MaxBodyBytes, s.cfg.MaxStreamedBodyBytes); declared > bound {
		// A content-length past every bound a body is held to: refused
		// before any of it is read, and before the 100 the client may be
		// waiting for, as the HTTP/1.1 reader refuses it.
		return nil, statusFail(st.id, 413, st.req.Method, "%w: a body over %d bytes", ErrTooLarge, bound)
	}
	if st.endStream {
		return s.finish(st)
	}
	if s.cfg.BodyDelivery == BodyStreamed {
		// The body may stream: handOver decides at the end of the batch,
		// and the 100 a client waits for goes out when the handler first
		// waits for the body (bodyQueue.next).
		s.pending = append(s.pending, st)
		return nil, nil
	}
	if expectsContinue(st.req) {
		// The body is yet to come and the client is waiting to be told to
		// send it: the interim 100 goes out now, RFC 9110 §10.1.1, as the
		// HTTP/1.1 reader sends it the moment a head is complete. Not
		// omitted when DATA is already behind the HEADERS in the same read
		// (the MAY of §10.1.1): that is not known here, and a 100 the client
		// did not wait for costs it a few bytes.
		if err := s.w.interim(st.id, 100); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

// blockRefused turns the decoder's error for a stream's header block into
// this connection's: an HPACK fault is a connection error, the table being in
// doubt; a list over its bound is a 431 on the stream, because the decoder
// read the block to its end and the table is consistent (RFC 9113 §10.5.1).
// On a stream already closed it is nothing at all, for the reason the
// field-count bound in headers gives.
func (s *h2Server) blockRefused(st *h2Stream, err error) error {
	switch {
	case !errors.Is(err, errHeadersTooLarge):
		return err
	case st.state == stateClosed:
		return nil
	}
	return statusFail(st.id, 431, nil, "%w", err)
}

func (s *h2Server) data(f h2Frame) (*h2Stream, error) {
	st := s.lookup(f.StreamID)
	if st == nil {
		// An even identifier can never have been opened: those are the
		// server's to open (§5.1.1) and this server opens none, so DATA on
		// one is not a late frame for a forgotten stream — it is a frame on a
		// stream that never existed, and tolerating it would window-account
		// traffic the peer can send forever without ever owing a request.
		if f.StreamID == 0 || f.StreamID%2 == 0 || f.StreamID > s.lastID {
			return nil, fmt.Errorf("%w: DATA on stream %d, which was never opened",
				ErrH2Protocol, f.StreamID)
		}
		// A stream this end closed long enough ago to have forgotten. The
		// peer still debited the connection window for it, so it is counted
		// and nothing else.
		return nil, s.consumeConnWindow(f)
	}
	if !st.gotHead && st.state == stateOpen {
		return nil, fmt.Errorf("%w: DATA on stream %d before its headers", ErrH2Protocol, f.StreamID)
	}
	// Whatever the stream's state, the payload was debited from the
	// connection window by the peer and has to be accounted here.
	if err := s.consumeConnWindow(f); err != nil {
		return nil, err
	}
	switch {
	case st.state == stateClosed:
		return nil, nil // tolerated within the grace period, and now counted
	case st.state == stateHalfClosed:
		return nil, streamFail(st.id, errStreamClosed,
			"%w: DATA on stream %d after END_STREAM", ErrH2Protocol, st.id)
	case st.gotTrailers:
		return nil, streamFail(st.id, errProtocolError,
			"%w: DATA on stream %d after its trailers", ErrH2Protocol, st.id)
	}

	body := f.Payload
	if f.Flags&flagPadded != 0 {
		// The pad's length is checked, its bytes are not: §6.1 says a
		// receiver "is not obligated to verify padding" and MAY make a
		// non-zero byte a connection error. Verifying would cost a pass over
		// every pad byte to catch a peer that is already non-conformant and
		// gains nothing by it — the pad carries no semantic (§10.7 says what
		// it is for) and is cut off the body whatever it holds.
		if len(body) == 0 {
			return nil, fmt.Errorf("%w: padded DATA with no pad length", ErrH2Protocol)
		}
		pad := int(body[0])
		body = body[1:]
		if pad > len(body) {
			return nil, fmt.Errorf("%w: DATA padding of %d exceeds the frame", ErrH2Protocol, pad)
		}
		body = body[:len(body)-pad]
	}
	if st.stream == nil && len(st.body)+len(body) > s.cfg.MaxBodyBytes {
		if s.cfg.BodyDelivery == BodyWhole {
			return nil, statusFail(st.id, 413, st.req.Method, "%w: a body over %d bytes", ErrTooLarge, s.cfg.MaxBodyBytes)
		}
		// Past what Body may hold, before the batch has ended: the body
		// streams from here. The stream is in pending, as every stream with
		// a head and a body to come is when bodies stream.
		st.stream = s.newH2Body(st)
	}
	last := f.Flags&flagEndStream != 0
	if st.stream != nil {
		// The stream's window goes back as the handler consumes, not here,
		// so the window bounds what the body holds and the aggregate bound
		// below, which is the bodies assembled whole, does not apply.
		if err := st.stream.receive(int32(len(f.Payload)), body, last); err != nil { //nolint:gosec // G115: bounded by MaxFrameSize
			return nil, err
		}
		if last {
			return s.finish(st)
		}
		return nil, nil
	}
	// The aggregate bound, beside the per-stream one: windows are granted
	// back as DATA is buffered here, so flow control does not bound what a
	// connection's unfinished uploads hold together — this does. See
	// [Config.MaxBodyBytes] for the factor. A reset rather than a 413:
	// this request alone is under its bound, so it is not the one that is
	// too large, and the status would say otherwise.
	if s.assembling+len(body) > 4*s.cfg.MaxBodyBytes {
		return nil, streamFail(st.id, errEnhanceYourCalm,
			"%w: over %d bytes buffered across this connection's unfinished requests",
			ErrTooLarge, 4*s.cfg.MaxBodyBytes)
	}
	st.body = append(st.body, body...)
	s.assembling += len(body)

	// The stream's own window, granted back as its body is consumed.
	// Replenishing only the connection's leaves the peer stalled at the 65535
	// bytes its stream started with, so an upload between 64 KiB and
	// MaxBodyBytes — a size the configuration says is allowed — hangs until
	// the read timeout kills the connection.
	//
	// The whole frame payload counts, padding included: that is what the peer
	// debited (RFC 9113 §6.9.1).
	used := int32(len(f.Payload)) //nolint:gosec // G115: bounded by MaxFrameSize
	if used > st.recvWind+st.windowGrace {
		// §6.9.1 wants FLOW_CONTROL_ERROR for a peer that sends more than it
		// was granted. Harmless here — the body is bounded by MaxBodyBytes
		// whatever the window says — so it is conformance rather than safety,
		// and it is a stream error rather than a connection one.
		return nil, streamFail(st.id, errFlowControlError,
			"%w: %w: %d bytes on stream %d against a window of %d",
			ErrH2Protocol, errFlowControl, used, st.id, st.recvWind)
	}
	st.recvWind -= used

	// Not on a stream the peer has just ended: nothing more will arrive on
	// it, and the grant would name a stream that is closing.
	initial := int32(s.cfg.InitialWindowSize) //nolint:gosec // G115: capped by withDefaults
	// Rounded up: at a window of 1, initial/2 is 0 and a window at 0 would
	// never be granted back. When bodies stream, a body still arriving at
	// the end of this batch is handed over and its window goes back as the
	// handler consumes (bodyQueue), so nothing is granted here: the bytes the
	// handler has not read stay spent.
	if !last && s.cfg.BodyDelivery == BodyWhole && st.recvWind < (initial+1)/2 {
		inc := initial - st.recvWind
		st.recvWind = initial
		if err := s.w.control32(frameWindowUpdate, st.id, uint32(inc)); err != nil { //nolint:gosec // G115: positive by construction
			return nil, err
		}
	}
	if last {
		return s.finish(st)
	}
	return nil, nil
}

// consumeConnWindow debits a DATA frame from the connection's receive window
// and tops it back up, refusing a peer that spent more than it was granted.
//
// It runs for every DATA frame including the ones on a closed stream, which is
// the trap in tolerating those: the peer debited them at the connection level
// whatever this end thinks of the stream, and skipping the accounting stalls
// the connection a few resets later.
func (s *h2Server) consumeConnWindow(f h2Frame) error {
	used := int32(len(f.Payload)) //nolint:gosec // G115: bounded by MaxFrameSize
	if used > s.recvWind {
		return fmt.Errorf("%w: %w: %d bytes against a connection window of %d",
			ErrH2Protocol, errFlowControl, used, s.recvWind)
	}
	s.recvWind -= used
	if s.recvWind >= s.connWind/2 {
		return nil
	}
	inc := s.connWind - s.recvWind
	s.recvWind = s.connWind
	return s.w.control32(frameWindowUpdate, 0, uint32(inc)) //nolint:gosec // G115: positive by construction
}

// finish completes a request and hands its stream over to the responder.
//
// The stream moves to closed here rather than when its answer is written:
// nothing more may arrive on it, and its slot in the concurrency bound moves
// from open to inFlight in the same step, which is what keeps the sum — and
// therefore the batch queue — bounded.
func (s *h2Server) finish(st *h2Stream) (*h2Stream, error) {
	if st.state != stateOpen {
		return nil, streamFail(st.id, errStreamClosed,
			"%w: stream %d ended twice", ErrH2Protocol, st.id)
	}
	if st.stream != nil {
		// A streamed body's end: the request is the handler's already, or
		// is handed over at the end of this batch, so nothing is returned.
		// A content-length mismatch is the same malformed request, reset
		// without the 400, since the handler is answering it.
		if err := st.stream.end(); err != nil {
			return nil, streamError{id: st.id, code: errProtocolError, err: err}
		}
		if !st.handed {
			// Ended within the batch that converted it: handOver takes it.
			return nil, nil
		}
		s.retire(st.id, stateHalfClosed)
		return nil, nil
	}
	// §8.1.1: a content-length that disagrees with the DATA sum is a
	// malformed request, checked here because END_STREAM is the first moment
	// the sum is final — it catches the short body, the long one, and the
	// "content-length: 10" with END_STREAM on the HEADERS alike.
	if st.declared >= 0 && st.declared != len(st.body) {
		return nil, malformedFail(st.id, st.req.Method,
			fmt.Errorf("%w: content-length %d against a body of %d bytes", ErrH2Protocol, st.declared, len(st.body)))
	}
	st.req.Body = st.body
	s.served++ // what the rapid-reset bound is measured against
	s.inFlight.Add(1)
	s.retire(st.id, stateHalfClosed)
	return st, nil
}

// stream returns the state for an identifier, opening it if the client may.
func (s *h2Server) stream(id uint32) (*h2Stream, error) {
	if id == 0 || id%2 == 0 {
		// Even identifiers are the server's to open, and zero is the
		// connection. A client using either is not addressing a request.
		return nil, fmt.Errorf("%w: a client opened stream %d", ErrH2Protocol, id)
	}
	if st := s.lookup(id); st != nil {
		return st, nil
	}
	if id <= s.lastID {
		// An identifier the client has already passed, on a stream neither
		// open nor in the closed-stream ring. Two streams look like this,
		// and §5.1 wants them answered in opposite ways. One the client
		// never opened — a stream identifier going backwards — is a
		// connection error (§5.1.1). One the client opened, this end
		// retired, and MaxConcurrentStreams later retirements pushed out of
		// the ring is a closed stream that may still have frames on the
		// wire: the trailers of a request this end reset with a 413 while
		// the client was already sending them behind a hundred other
		// requests. §5.1 "closed" makes those frames the peer's right, to
		// be minimally processed and discarded. The opened bitmap tells the
		// two apart for the h2OpenedWindow most recent identifiers.
		if !s.wasOpened(id) {
			return nil, fmt.Errorf("%w: stream %d goes backwards past stream %d",
				ErrH2Protocol, id, s.lastID)
		}
		// Decoded and dropped, as DATA on a forgotten stream is counted and
		// dropped (data). The other answers are each wrong somewhere: a
		// connection error loses every other stream to one late block from
		// a conformant client; a STREAM_CLOSED reset is a frame on a stream
		// that may well be closed, which §5.1 forbids, and a reset charged
		// against the rapid-reset allowance for a stream that is gone.
		// Decoding costs what the peer can already demand of any stream it
		// opens. §5.1 does allow a connection error once a signal shows the
		// peer received the RST_STREAM; the ring filling is not that signal,
		// since the peer may have enqueued all those streams before it did.
		//
		// The block lands in late, as a ring-held stream's does, and in the
		// closed state: headers decodes it into discard and keeps nothing.
		// lastID is left alone — nothing opened — and the stream is not
		// remembered again, which would evict a stream the ring still needs.
		if s.late.id != id {
			s.late = h2Stream{id: id, declared: -1}
		}
		s.late.state = stateClosed
		return &s.late, nil
	}
	if s.goingAway || int(s.open.Load())+int(s.inFlight.Load()) >= s.cfg.MaxConcurrentStreams {
		// §5.1.2 makes this a stream error, and REFUSED_STREAM is the code
		// that tells the client nothing was processed — so it may retry the
		// request elsewhere rather than assume it half-happened. The same
		// refusal answers a stream opened after the peer's own GOAWAY.
		//
		// Refused is not the same as unseen. The stream is registered, in
		// the closed state, because two things about it still matter to the
		// connection that survives it: its header block must reach the
		// shared HPACK decoder (§4.3 — an undecoded block desynchronises the
		// dynamic table, and every later block on every stream decodes
		// wrong), and its identifier must advance lastID (§5.1 — a client
		// that POSTed at this server's own advertised limit already has DATA
		// on the wire behind the HEADERS, and a lastID left behind makes
		// that DATA read as a frame on an idle stream, a connection error).
		// Returning the closed stream gets both for free: headers decodes
		// its blocks and drops them, data counts its frames and tolerates
		// them, and exactly one RST_STREAM — this one — answers the lot.
		if err := s.countReset(); err != nil {
			return nil, err
		}
		s.openID(id)
		s.remember(id, stateClosed)
		if err := s.w.control32(frameRSTStream, id, errRefusedStream); err != nil {
			return nil, err
		}
		return s.lookup(id), nil
	}
	s.openID(id)
	st := &h2Stream{
		id: id, declared: -1, recvWind: int32(s.cfg.InitialWindowSize), //nolint:gosec // G115: capped by withDefaults
		deadline: time.Now().Add(s.cfg.ReadTimeout),
	}
	if !s.settingsAcked && s.cfg.InitialWindowSize < defaultInitialWindow {
		// RFC 9113 §6.9.3: "the receiver MUST be prepared to receive data
		// that exceeds this window size, since the sender might send data
		// that exceeds the lower limit prior to processing the SETTINGS
		// frame". The window itself stays at the advertised size, so the
		// WINDOW_UPDATE arithmetic matches the peer's once it has read the
		// SETTINGS (§6.9.2 adjusts its active windows by the difference);
		// the overshoot the default allowed is tolerated on top.
		st.windowGrace = int32(defaultInitialWindow - s.cfg.InitialWindowSize) //nolint:gosec // G115: both under 2^31-1
	}
	s.streams[id] = st
	s.open.Add(1)
	s.w.open(id)
	return st, nil
}

// validFieldValue reports a field value free of the bytes RFC 9110 §5.5
// keeps out of field content: CR and LF, which let one field smuggle a
// second field or a second request into whatever re-serialises this one
// toward HTTP/1.1, plus the other C0 controls (HTAB excepted) and DEL — the
// same octet rules the h1 parser enforces in parseHeaderLine and the writers
// enforce outbound via hasFieldControl. A value edged with SP or HTAB is
// refused too (RFC 9113 §8.2.1, RFC 9114 §4.1.2): the h1 parser trims that
// whitespace, so two hops would not agree on what the value was.
func validFieldValue(v []byte) bool {
	for _, c := range v {
		if (c < 0x20 && c != '\t') || c == 0x7f {
			return false
		}
	}
	return !edgedWithWhitespace(v)
}

// edgedWithWhitespace reports a value that starts or ends with SP or HTAB —
// what RFC 9110 §5.5's field-content grammar never produces, and what the
// RFC 9113 §8.2.1 and RFC 9114 §4.1.2 minimal validation refuses.
func edgedWithWhitespace(v []byte) bool {
	if len(v) == 0 {
		return false
	}
	first, last := v[0], v[len(v)-1]
	return first == ' ' || first == '\t' || last == ' ' || last == '\t'
}

// parseContentLength parses a decimal content-length the way the h1 parser
// does: digits only, overflow refused per digit rather than detected after
// the wrap — a wrapped length is a small "valid" number, which is exactly the
// disagreement the check exists to prevent.
func parseContentLength(v []byte) (int, error) {
	if len(v) == 0 {
		return 0, fmt.Errorf("%w: an empty content-length", ErrH2Protocol)
	}
	n := 0
	for _, c := range v {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("%w: content-length %q is not a number", ErrH2Protocol, v)
		}
		if n > maxWindow/10 {
			return 0, fmt.Errorf("%w: content-length %q overflows", ErrH2Protocol, v)
		}
		n = n*10 + int(c-'0')
	}
	return n, nil
}

// applySettings validates every entry in order, then applies the last value
// each identifier carried.
//
// Validate-all, apply-last matters for the one setting whose application is
// not O(1): SETTINGS_INITIAL_WINDOW_SIZE walks every live stream window under
// the writer's lock, and nothing in the protocol stops a frame from carrying
// the same identifier thousands of times — a frame full of repeats was a way
// to buy millions of locked map operations for a few kilobytes of wire. The
// result is the one the in-order rule produces anyway: settings are
// overwrites, so the last value is the state either way, and the window delta
// is computed against the previously *applied* value in both readings.
//
// SETTINGS_HEADER_TABLE_SIZE is returned rather than applied: it changes what
// the next header block this end sends must begin with, and that is decided
// where the ACK is queued (see [h2Writer.ackSettings]).
func (s *h2Server) applySettings(payload []byte) (table uint32, haveTable bool, err error) {
	var (
		haveWindow, haveFrame bool
		window, frame         uint32
	)
	for len(payload) >= 6 {
		id := uint32(payload[0])<<8 | uint32(payload[1])
		v := be32(payload[2:6])
		payload = payload[6:]
		switch id {
		case settingHeaderTableSize:
			table, haveTable = v, true
		case settingInitialWindowSize:
			if v > maxWindow {
				return 0, false, fmt.Errorf("%w: %w: an initial window of %d", ErrH2Protocol, errFlowControl, v)
			}
			window, haveWindow = v, true
		case settingMaxFrameSize:
			if v < defaultMaxFrameSize || v > maxFrameSize {
				return 0, false, fmt.Errorf("%w: a maximum frame size of %d", ErrH2Protocol, v)
			}
			frame, haveFrame = v, true
		case settingMaxHeaderListSize:
			// Read by the responder, which frames the heads; zero, which no
			// peer can usefully mean, stays "unlimited" with the default.
			s.peerMaxHeaderList.Store(v)
		case settingEnablePush:
			if v > 1 {
				return 0, false, fmt.Errorf("%w: ENABLE_PUSH of %d", ErrH2Protocol, v)
			}
		}
	}
	if haveWindow {
		if err := s.w.setInitialWindow(int32(window)); err != nil { //nolint:gosec // G115: bounded above
			return 0, false, err
		}
	}
	if haveFrame {
		s.w.setMaxFrame(int(frame))
	}
	return table, haveTable, nil
}
