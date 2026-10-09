package quic

import (
	"cmp"
	"fmt"
	"io"
	"slices"
	"sync"
)

// Stream is one QUIC stream, read and written as bytes.
//
// Data arrives out of order in general — each STREAM frame carries its offset
// — so a stream holds what it cannot yet deliver and returns it when the gap
// fills. Retransmissions overlap what was already delivered as a matter of
// course, and are merged rather than refused; see [reassembly].
//
// The two directions fail independently, as RFC 9000 §2.4 draws them: a
// RESET_STREAM from the peer abandons what it was sending and surfaces to
// [Stream.Read]; a STOP_SENDING from the peer — or a local [Stream.Reset] —
// abandons what this end was sending and surfaces to [Stream.Write].
//
// Locking: the receive side (reassembly, the read buffer, the peer's reset)
// lives under the stream's own lock; the send side (offsets, credit, this
// end's reset) lives under the connection's, which serialises every packet
// written. The lock order is c.mu → s.mu: [Conn.maybeRemoveLocked] takes s.mu
// (via [Stream.recvFinished]) while holding c.mu, so s.mu must never be held
// while taking c.mu.
type Stream struct {
	id   uint64
	conn *Conn

	mu    sync.Mutex
	ready chan struct{}
	rasm  reassembly
	buf   []byte // ordered bytes waiting for Read; unused in batch mode
	fin   bool
	finAt uint64
	recvQ recvQuota
	// readReset records a RESET_STREAM from the peer: the request was
	// abandoned, and a reader learns it as an error rather than as an end of
	// file — a caller that cannot tell the two apart hands a truncated body
	// to whoever asked for it.
	readReset bool
	readCode  uint64
	recvDone  bool // FIN fully reassembled, or reset: nothing more will come

	// The send side, under conn.mu.
	writeOff   uint64
	sendQ      sendQuota
	writeReset bool // this end sent RESET_STREAM, or the peer sent STOP_SENDING
	writeCode  uint64
	sendClosed bool // FIN or RESET_STREAM sent: the peer knows this side's size
}

// ID reports the stream's identifier. Its two low bits say who opened it and
// whether it is bidirectional (RFC 9000 §2.1).
func (s *Stream) ID() uint64 { return s.id }

func newStream(c *Conn, id uint64) *Stream {
	s := &Stream{id: id, conn: c, ready: make(chan struct{}, 1)}
	if c.hasPeerParams {
		s.sendQ.max = initialStreamSendLimit(c.peerParams, id, c.isClient)
	}
	s.recvQ = newRecvQuota(initialStreamRecvLimit(c.params, id, c.isClient))
	// A unidirectional stream only ever carries bytes one way: the direction
	// that does not exist is born closed, so lifecycle accounting does not
	// wait forever for a FIN nobody can send.
	if id&0x02 != 0 {
		if (id&1 == 0) == c.isClient {
			s.recvDone = true // this end opened it; the peer never writes
		} else {
			s.sendClosed = true // the peer opened it; this end never writes
		}
	}
	return s
}

// signal wakes a blocked Read without ever blocking itself.
func (s *Stream) signal() {
	select {
	case s.ready <- struct{}{}:
	default:
	}
}

// deliver files one STREAM frame's bytes. It returns the newly ordered delta
// appended to dst — the caller decides whether that lands in the read buffer
// or in a batch callback — along with whether the stream's receive side just
// finished, and how far the peer's high-water mark moved, which the caller
// charges against the connection's flow-control account.
//
// Errors are connection errors by design: a final size that moves, bytes past
// it, or bytes past the granted window are protocol violations on an
// authenticated packet.
func (s *Stream) deliver(f Frame, dst []byte) (delta []byte, finished bool, highGrowth uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	end := f.offset + uint64(len(f.Data))
	if s.fin && (end > s.finAt || (f.Fin && end != s.finAt)) {
		return dst, false, 0, &transportError{
			code: transportFinalSize,
			err:  fmt.Errorf("%w: stream %d grew past its declared final size", ErrQUIC, s.id),
		}
	}
	// §4.5's other direction: a FIN that declares a final size below bytes
	// already received retracts data the peer provably sent. Without this, a
	// short FIN marked the stream finished at the shorter size and the bytes
	// past it were silently dropped — abandon has always had the equivalent
	// check for RESET_STREAM.
	if f.Fin && end < s.recvQ.highest {
		return dst, false, 0, &transportError{
			code: transportFinalSize,
			err:  fmt.Errorf("%w: stream %d declared a final size below bytes already received", ErrQUIC, s.id),
		}
	}
	if f.Fin {
		s.fin, s.finAt = true, end
	}
	oldHigh := s.recvQ.highest
	if err := s.recvQ.receive(end); err != nil {
		return dst, false, 0, fmt.Errorf("stream %d: %w", s.id, err)
	}
	highGrowth = s.recvQ.highest - oldHigh

	if s.readReset {
		// Frames still in flight behind the peer's own reset: counted above,
		// carried nowhere. The final size already bounded them.
		return dst, false, highGrowth, nil
	}

	dst, err = s.rasm.addThenTake(f.offset, f.Data, dst)
	if err != nil {
		return dst, false, highGrowth, fmt.Errorf("stream %d: %w", s.id, err)
	}
	if s.fin && s.rasm.consumed >= s.finAt && !s.recvDone {
		s.recvDone = true
		finished = true
	}
	return dst, finished, highGrowth, nil
}

// keepForRead stores a delivered delta for [Stream.Read] and wakes it. Batch
// mode skips this: the callback is the consumer, and buffering a second copy
// for a Read nobody calls is memory spent on nothing.
func (s *Stream) keepForRead(delta []byte, finished bool) {
	s.mu.Lock()
	s.buf = append(s.buf, delta...)
	s.mu.Unlock()
	if len(delta) > 0 || finished {
		s.signal()
	}
}

// Read returns what has arrived in order, blocking until something has.
//
// A stream the peer reset reports that reset as an error — never as an end of
// file, since a reader that cannot tell "the message is complete" from "the
// sender gave up" hands a truncated body to whoever asked for it. What it
// reports *first*, though, is whatever had already arrived in order: the
// bytes are real and were delivered before the sender changed its mind. Read
// to the error.
//
// There is no per-read timeout: the connection's idle timeout is what bounds
// a peer that went away, and a stream that is quiet because its producer is
// slow is not an error.
func (s *Stream) Read(p []byte) (int, error) {
	for {
		s.mu.Lock()
		if len(s.buf) > 0 {
			n := copy(p, s.buf)
			s.buf = s.buf[n:]
			if !s.conn.readTaken.Load() {
				s.conn.readTaken.Store(true)
			}
			grant := s.recvQ.consume(uint64(n)) //nolint:gosec // G115: a byte count, positive
			var nextMax uint64
			if grant {
				nextMax = s.recvQ.nextMax()
			}
			s.mu.Unlock()
			s.conn.creditConsumed(s.id, uint64(n), grant, nextMax) //nolint:gosec // G115: a byte count, positive
			return n, nil
		}
		done := s.fin && s.rasm.consumed >= s.finAt
		abandoned, code := s.readReset, s.readCode
		s.mu.Unlock()
		if abandoned {
			return 0, fmt.Errorf("%w: stream %d was reset with code %#x", ErrQUIC, s.id, code)
		}
		if done {
			return 0, io.EOF
		}
		select {
		case <-s.ready:
		case <-s.conn.closed:
			return 0, errClosed
		}
	}
}

// Write sends bytes as STREAM frames, waiting for credit when the peer's
// flow-control grant or the congestion window is spent. The wait ends when
// credit arrives, the stream is reset by either end, or the connection
// closes — a peer that stays healthy but never grants credit holds the
// writer, which is what transport flow control means.
func (s *Stream) Write(p []byte) (int, error) {
	return s.write(p, false)
}

// CloseWrite sends the FIN, which is what tells the peer a request or a
// response is complete.
func (s *Stream) CloseWrite() error {
	_, err := s.write(nil, true)
	return err
}

func (s *Stream) write(p []byte, fin bool) (int, error) {
	written := 0
	c := s.conn
	c.mu.Lock()
	defer c.mu.Unlock()
	for {
		if s.writeReset {
			return written, fmt.Errorf("%w: stream %d is reset (code %#x) and takes no more writes",
				ErrQUIC, s.id, s.writeCode)
		}
		if s.sendClosed {
			return written, fmt.Errorf("%w: stream %d is closed for writing", ErrQUIC, s.id)
		}
		rest := len(p) - written
		if rest == 0 && !fin {
			return written, nil
		}
		credit := min(s.sendQ.avail(), c.connSend.avail())
		n := min(rest, int(min(credit, uint64(c.maxStreamChunk())))) //nolint:gosec // G115: bounded by maxStreamChunk, an int
		if rest > 0 && n == 0 {
			// Out of flow-control credit. Say so once per limit — the peer
			// is entitled to know it is the bottleneck (§4.1) — then wait
			// for MAX_DATA / MAX_STREAM_DATA to arrive on the read loop.
			c.sendBlockedFramesLocked(s)
			if err := c.waitForCreditLocked(); err != nil {
				return written, err
			}
			continue
		}

		last := written+n >= len(p)
		frame := AppendVarint(nil, FrameStream|streamOFF|streamLEN|boolBit(fin && last, streamFIN))
		frame = AppendVarint(frame, s.id)
		frame = AppendVarint(frame, s.writeOff)
		frame = AppendVarint(frame, uint64(n))
		frame = append(frame, p[written:written+n]...)
		if err := c.sendPacketLocked(spaceApplication, frame, sendOpts{waitCwnd: true, own: true}); err != nil {
			return written, err
		}
		s.writeOff += uint64(n)
		s.sendQ.sent += uint64(n)
		c.connSend.sent += uint64(n)
		written += n
		if last {
			if fin {
				s.sendClosed = true
				c.maybeRemoveLocked(s)
			}
			return written, nil
		}
	}
}

// Reset abandons this end's sending direction with an application error code
// (RFC 9000 §19.4).
//
// It is the per-stream error channel a multiplexed transport has and a
// sequential one does not: one request that cannot be served ends on its own
// stream, and the connection — with every other request in flight on it —
// carries on. To refuse a request whose body may still be arriving, pair it
// with [Stream.StopSending], which is the half that tells the peer to stop
// transmitting (RFC 9114 §4.1.1 uses both).
//
// The final size sent is what this end has written so far, which is what the
// peer needs to close its own flow-control accounting for the stream.
func (s *Stream) Reset(code uint64) error {
	c := s.conn
	c.mu.Lock()
	defer c.mu.Unlock()
	if s.writeReset {
		return nil // already abandoned; saying so twice tells the peer nothing
	}
	s.writeReset, s.writeCode = true, code
	s.sendClosed = true
	frame := appendFrame(nil, FrameResetStream, s.id, code, s.writeOff)
	err := c.sendPacketLocked(spaceApplication, frame, sendOpts{})
	c.maybeRemoveLocked(s)
	c.cond.Broadcast() // a writer blocked on credit for this stream gives up
	return err
}

// StopSending asks the peer to stop transmitting on this stream (RFC 9000
// §3.5): the request it carries will not be served, so every byte still in
// flight is effort wasted at both ends. The peer answers with RESET_STREAM,
// which is what finally closes the receiving direction's accounting.
func (s *Stream) StopSending(code uint64) error {
	c := s.conn
	c.mu.Lock()
	defer c.mu.Unlock()
	frame := appendFrame(nil, frameStopSending, s.id, code)
	return c.sendPacketLocked(spaceApplication, frame, sendOpts{})
}

// abandon records a RESET_STREAM that arrived, releasing whoever is reading.
// It returns how far the peer's declared final size moved this stream's
// high-water mark, and how many bytes the application will now never consume
// — both owed to the connection-level flow-control account.
func (s *Stream) abandon(code, finalSize uint64) (highGrowth, neverConsumed uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fin && finalSize != s.finAt {
		return 0, 0, fmt.Errorf("%w: stream %d reset to a different final size", ErrQUIC, s.id)
	}
	if finalSize < s.recvQ.highest {
		return 0, 0, fmt.Errorf("%w: stream %d reset below bytes already received", ErrQUIC, s.id)
	}
	if s.readReset {
		return 0, 0, nil
	}
	oldHigh := s.recvQ.highest
	if err := s.recvQ.receive(finalSize); err != nil {
		return 0, 0, fmt.Errorf("stream %d: %w", s.id, err)
	}
	s.readReset, s.readCode = true, code
	s.recvDone = true
	// Only bytes never delivered to the application are settled here. Bytes
	// already delivered but not yet credited — this packet's own STREAM delta
	// when STREAM and RESET_STREAM share a packet, or bytes parked in the read
	// buffer — are credited by whoever consumes them (settleBatchConsumption,
	// or Read draining to the reset error). Counting them here too used to
	// over-grant the connection window by exactly that delta.
	neverConsumed = finalSize - s.rasm.consumed
	s.recvQ.consumed += neverConsumed
	s.signal()
	return finalSize - oldHigh, neverConsumed, nil
}

// onStopSending records the peer's refusal to keep reading: this end's
// sending direction is abandoned and answered with RESET_STREAM, as §3.5
// requires. Runs on the read loop, under conn.mu.
func (s *Stream) onStopSendingLocked(code uint64) error {
	if s.writeReset {
		return nil
	}
	s.writeReset, s.writeCode = true, code
	s.sendClosed = true
	frame := appendFrame(nil, FrameResetStream, s.id, code, s.writeOff)
	err := s.conn.sendPacketLocked(spaceApplication, frame, sendOpts{})
	s.conn.maybeRemoveLocked(s)
	s.conn.cond.Broadcast()
	return err
}

// WasReset reports whether the stream was abandoned — by either end, in
// either direction — and with what code.
func (s *Stream) WasReset() (uint64, bool) {
	s.mu.Lock()
	readReset, readCode := s.readReset, s.readCode
	s.mu.Unlock()
	if readReset {
		return readCode, true
	}
	c := s.conn
	c.mu.Lock()
	defer c.mu.Unlock()
	return s.writeCode, s.writeReset
}

// recvFinished reports whether nothing more will arrive, under s.mu.
func (s *Stream) recvFinished() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.recvDone
}

// deliveredSoFar is the offset the ordered prefix has reached, which is what
// a batch delta's Offset field reports.
func (s *Stream) deliveredSoFar() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rasm.consumed
}

func boolBit(b bool, bit uint64) uint64 {
	if b {
		return bit
	}
	return 0
}

// AcceptStream returns the next stream the peer opened. When the queue is
// full the connection's read loop blocks rather than dropping or failing —
// a server that stops accepting stops reading, and the peer feels it as
// back-pressure through flow control.
func (c *Conn) AcceptStream() (*Stream, error) {
	select {
	case s := <-c.accepted:
		return s, nil
	case <-c.closed:
		return nil, errClosed
	}
}

// OpenStream opens a bidirectional stream from this end, within what the
// peer's MAX_STREAMS grant allows. Refusal is an error rather than a wait:
// the callers this package has are tests and clients, and a client is better
// told than parked.
func (c *Conn) OpenStream() (*Stream, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.hasPeerParams {
		return nil, fmt.Errorf("%w: opening a stream before the handshake delivered the peer's limits", ErrQUIC)
	}
	if c.openedBidi >= c.peerMaxStreamsBidi {
		c.control = appendFrame(c.control, frameStreamsBlocked, c.peerMaxStreamsBidi)
		c.flushControlLocked()
		return nil, fmt.Errorf("%w: the peer allows %d bidirectional streams and all are in use",
			ErrQUIC, c.peerMaxStreamsBidi)
	}
	base := uint64(1)
	if c.isClient {
		base = 0
	}
	id := base + 4*c.openedBidi
	c.openedBidi++
	s := newStream(c, id)
	c.streams[id] = s
	return s, nil
}

// Stream returns the stream with this identifier, opening the bookkeeping
// for it if nobody has yet. It is how a server answers on the stream a
// request arrived on, and how it opens its own unidirectional streams (the
// HTTP/3 control stream). A stream that already closed comes back non-nil
// but refuses writes — answering on a stream the peer is done with is not
// an error worth a connection.
//
// Unlike [Conn.OpenStream], Stream deliberately does not check the peer's
// MAX_STREAMS grant. Picking a stream by identifier is for streams whose
// numbers the application protocol already fixed — HTTP/3's control stream
// is stream 3, and its transport parameters mandate a grant that covers it —
// and for answering on streams the peer itself opened, where the limit being
// spent is the peer's, not this end's. The caller therefore owns conformance
// to the grant; to open a stream of this end's choosing under flow control,
// use OpenStream.
func (c *Conn) Stream(id uint64) *Stream {
	c.mu.Lock()
	defer c.mu.Unlock()
	if s := c.streams[id]; s != nil {
		return s
	}
	s := newStream(c, id)
	if _, closed := c.closedIDs[id]; closed {
		s.writeReset = true
		s.sendClosed = true
		s.recvDone = true
		return s // a tombstone, deliberately not re-registered
	}
	localInitiated := (id&1 == 0) == c.isClient
	if localInitiated {
		count := id/4 + 1
		if id&0x02 != 0 {
			c.openedUni = max(c.openedUni, count)
		} else {
			c.openedBidi = max(c.openedBidi, count)
		}
	}
	c.streams[id] = s
	return s
}

// maybeRemoveLocked retires a stream once both directions are done: the
// receive side has delivered its FIN or been reset, and the send side has
// said its final word (FIN or RESET_STREAM). The identifier is remembered so
// that frames still in flight are tolerated rather than reopening the
// stream, and — for peer-initiated streams — the slot is granted back with
// MAX_STREAMS, which is what stops a long-lived connection dying of stream
// arithmetic. Callers hold c.mu.
func (c *Conn) maybeRemoveLocked(s *Stream) {
	if !s.sendClosed {
		return
	}
	if _, present := c.streams[s.id]; !present {
		return // already retired
	}
	if !s.recvFinished() {
		return
	}
	delete(c.streams, s.id)
	c.closedIDs[s.id] = struct{}{}
	c.closedOrder = append(c.closedOrder, s.id)
	// The closed set is a memory, and memories are bounded: oldest first,
	// because frames for a stream closed long ago come from a peer that is
	// not confused but hostile, and re-refusing them cheaply is enough.
	for len(c.closedOrder) > 1024 {
		delete(c.closedIDs, c.closedOrder[0])
		c.closedOrder = c.closedOrder[1:]
	}

	if (s.id&1 == 0) != c.isClient { // peer-initiated: grant the slot back
		if s.id&0x02 != 0 {
			c.localMaxStreamsUni++
			c.grants = appendFrame(c.grants, frameMaxStreams|1, c.localMaxStreamsUni)
		} else {
			c.localMaxStreamsBidi++
			c.grants = appendFrame(c.grants, frameMaxStreams, c.localMaxStreamsBidi)
		}
	}
}

// OnStreamFrames registers a callback that receives each packet's stream
// deltas as a group, ordered within each stream by the transport.
//
// It is the seam an all-batch layer needs. [Conn.AcceptStream] gives streams
// one at a time, which is the ordinary shape and throws away what the
// network already knew: a datagram carried frames for several streams at
// once, and they belong together. A caller that wants a batch takes it here.
//
// # What the callback receives
//
// Frames with Type FrameStream carry ordered byte deltas: the transport has
// already reassembled retransmissions and reordering, so Data always extends
// the stream exactly where the previous delivery ended, and Fin marks the
// delta that completes the stream. Frames with Type FrameResetStream report
// a peer that abandoned the stream; whatever was assembled from it should be
// forgotten. Everything borrows the connection's buffers and is valid only
// for the call — the batch contract.
//
// # Registering switches the connection to batch mode
//
// The accept queue stops filling and per-stream read buffering stops: the
// callback is the consumer, and buffering a second copy for a Read nobody
// calls would hold window credit against memory nothing will release.
// [Stream.Read] on streams that arrive after the switch returns nothing.
// The switch is one-way, and mixing the modes is not supported: register
// before consuming anything, which is what ServeH3 does. A caller that
// Reads first and registers after gets what no Read took, per stream, as
// one delta each — arrival order across streams and resets are lost then.
//
// # What arrived first is not lost
//
// The read loop starts inside [Accept] and [Dial], so packets can arrive
// before a caller has had a chance to register. Those deltas are held —
// bounded by the connection receive window, since none of them has been
// credited yet — and replayed here, in order, on the goroutine that
// registers. The replay settles flow control exactly as live delivery does:
// automatically, or not at all under [Conn.SetManualCredit].
//
// The callback runs on the connection's read goroutine and must not block on
// this connection — blocking is what stops the read loop, which is the
// back-pressure a caller wants and never a way to wait for something the
// read loop must deliver.
func (c *Conn) OnStreamFrames(f func([]Frame) error) {
	c.deliverMu.Lock()
	defer c.deliverMu.Unlock()
	c.onFrames = f
	held, lost := c.pending, c.pendingLost
	c.pending, c.pendingBytes = nil, 0
	if f == nil {
		return
	}
	c.mu.Lock()
	c.batchMode = true
	streams := make([]*Stream, 0, len(c.streams))
	for _, s := range c.streams {
		streams = append(streams, s)
	}
	c.mu.Unlock()

	// Read buffers stop here: nothing will Read them, and every byte in them
	// is either in held already or, when held was abandoned, about to be
	// replayed from them. Nothing is credited for them here — the replay
	// settles what it delivers, once.
	var leftovers []Frame
	for _, s := range streams {
		s.mu.Lock()
		if len(s.buf) > 0 {
			leftovers = append(leftovers, Frame{Type: FrameStream, StreamID: s.id, Data: s.buf, Fin: s.recvDone})
		}
		s.buf = nil
		s.mu.Unlock()
	}
	// A Read that took bytes did so under its stream's lock before the loop
	// above took it, so the flag is visible now: held then repeats bytes a
	// Read already credited, and the buffers are the only faithful source.
	if lost || c.readTaken.Load() {
		c.pendingLost = true
		slices.SortFunc(leftovers, func(a, b Frame) int { return cmp.Compare(a.StreamID, b.StreamID) })
		held = nil
		if len(leftovers) > 0 {
			held = [][]Frame{leftovers}
		}
	}

	for _, batch := range held {
		if err := f(batch); err != nil {
			c.fail(err)
			_ = c.Close()
			return
		}
		c.settleBatchConsumption(batch)
	}
}

// cloneFrames copies frames out of the buffers they borrow, which is what
// holding them past the call that parsed them requires.
func cloneFrames(fs []Frame) []Frame {
	out := make([]Frame, len(fs))
	for i, f := range fs {
		f.Data = append([]byte(nil), f.Data...)
		out[i] = f
	}
	return out
}
