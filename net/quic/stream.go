package quic

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"sync"
	"time"
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
// (via [Stream.recvState]) while holding c.mu, so s.mu must never be held
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
	// readEnded says the end — the FIN's last delta, or the reset — has been
	// handed to the reader's side: the delta is in buf, or the batch callback
	// has returned. Read reports EOF or the reset on this flag, never on
	// recvDone: deliver sets recvDone under mu on the read loop *before* the
	// delta reaches buf, and a Read looping between the two saw an empty buf
	// and a finished stream, and returned EOF with the last frame's bytes
	// still in flight. quic-go's client found that: one echo in twenty came
	// back a frame short (interop/quicgo_test.go).
	readEnded bool
	// endTaken says the application is done with the receive side: a Read
	// took the last byte or returned the end — EOF or the reset — or it
	// called StopSending. It is
	// RFC 9000 §3.2's "Data Read" (or "Reset Read"), and what a stream the
	// peer opened waits for before its MAX_STREAMS slot is granted back.
	endTaken bool
	// readDeadline is when a parked Read gives up (SetReadDeadline); zero
	// means never. The connection's timer loop wakes the stream when it
	// matures (Conn.streamDeadline).
	readDeadline time.Time

	// The send side, under conn.mu.
	writeOff   uint64
	sendQ      sendQuota
	writeReset bool // this end sent RESET_STREAM, or the peer sent STOP_SENDING
	writeCode  uint64
	sendClosed bool // FIN or RESET_STREAM sent: the peer knows this side's size
	queued     bool // waiting in the accept queue (Conn.acceptQ), under conn.mu
	// writeDeadline is when a Write parked on credit or the congestion
	// window gives up (SetWriteDeadline); zero means never.
	writeDeadline time.Time
}

// ID reports the stream's identifier. Its two low bits say who opened it and
// whether it is bidirectional (RFC 9000 §2.1).
func (s *Stream) ID() uint64 { return s.id }

// sendOnly says the stream is unidirectional and this end opened it (RFC
// 9000 §2.1): the peer never writes on it.
func (s *Stream) sendOnly() bool {
	return s.id&0x02 != 0 && (s.id&1 == 0) == s.conn.isClient
}

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
	if finished {
		s.readEnded = true
	}
	s.mu.Unlock()
	if len(delta) > 0 || finished {
		s.signal()
	}
}

// endForRead marks the stream's end as handed over — a reset dispatched to
// the reader, or a finished stream whose batch callback has returned — and
// wakes a Read waiting on it.
func (s *Stream) endForRead() {
	s.mu.Lock()
	s.readEnded = true
	s.mu.Unlock()
	s.signal()
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
// Without a deadline ([Stream.SetReadDeadline]) the wait is unbounded: the
// connection's idle timeout is what bounds a peer that went away, and a
// stream that is quiet because its producer is slow is not an error. With
// one, a Read that has nothing to deliver when it passes returns
// [os.ErrDeadlineExceeded], as a [net.Conn] does.
//
// A unidirectional stream this end opened ([Conn.OpenUniStream], or
// [Conn.Stream] with a local unidirectional identifier) has no receive side:
// Read refuses it with an error at once rather than waiting for bytes the
// peer may never send.
//
// A stream the peer opened gives its MAX_STREAMS slot back only once Read has
// taken its last byte or its reset, or [Stream.StopSending] said the rest
// will not be read (RFC 9000 §3.2): a stream accepted and left unread holds
// its slot, and the peer opens no stream in its place.
func (s *Stream) Read(p []byte) (int, error) {
	if s.sendOnly() {
		return 0, fmt.Errorf("%w: stream %d is unidirectional and this end opened it: nothing to read", ErrQUIC, s.id)
	}
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
			// The last byte of a finished stream is its end taken: a
			// reader that knows where its message ends need not come back
			// for the EOF before the stream may retire.
			last := len(s.buf) == 0 && s.readEnded && !s.endTaken
			if last {
				s.endTaken = true
			}
			s.mu.Unlock()
			s.conn.creditConsumed(s.id, uint64(n), grant, nextMax) //nolint:gosec // G115: a byte count, positive
			if last {
				s.conn.retireTaken(s)
			}
			return n, nil
		}
		ended := s.readEnded
		abandoned, code := s.readReset, s.readCode
		deadline := s.readDeadline
		first := ended && !s.endTaken
		if first {
			s.endTaken = true
		}
		s.mu.Unlock()
		if first {
			s.conn.retireTaken(s)
		}
		if ended {
			if abandoned {
				return 0, fmt.Errorf("%w: stream %d was reset with code %#x", ErrQUIC, s.id, code)
			}
			return 0, io.EOF
		}
		if !deadline.IsZero() && !time.Now().Before(deadline) {
			return 0, os.ErrDeadlineExceeded
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
// credit arrives, the stream is reset by either end, the connection closes,
// or the write deadline passes ([Stream.SetWriteDeadline], reported as
// [os.ErrDeadlineExceeded] after the bytes that did go out). Without a
// deadline, a peer that stays healthy but never grants credit holds the
// writer, which is what transport flow control means.
func (s *Stream) Write(p []byte) (int, error) {
	return s.write(p, false)
}

// SetReadDeadline bounds every Read from now on, as [net.Conn] does: a Read
// still parked when t passes returns [os.ErrDeadlineExceeded], and so does
// every later one until the deadline is moved. The zero time removes it.
// Setting a deadline wakes a Read already parked, so the new bound applies
// to it too.
func (s *Stream) SetReadDeadline(t time.Time) error {
	s.mu.Lock()
	s.readDeadline = t
	s.mu.Unlock()
	s.signal()
	s.conn.armStreamDeadline(t)
	return nil
}

// SetWriteDeadline bounds every Write from now on: one parked on
// flow-control credit or the congestion window when t passes returns
// [os.ErrDeadlineExceeded] with the count of bytes already sent, and later
// ones return it at once until the deadline is moved. The zero time removes
// it. It is what lets a server stop waiting on a peer that keeps the
// connection alive but never grants credit.
func (s *Stream) SetWriteDeadline(t time.Time) error {
	c := s.conn
	c.mu.Lock()
	s.writeDeadline = t
	c.armStreamDeadlineLocked(t)
	c.cond.Broadcast() // a writer parked on credit re-reads its deadline
	c.mu.Unlock()
	return nil
}

// SetDeadline sets both the read and the write deadline.
func (s *Stream) SetDeadline(t time.Time) error {
	_ = s.SetReadDeadline(t)
	return s.SetWriteDeadline(t)
}

// armStreamDeadline records a stream deadline for the timer loop: the
// earliest one armed is what the loop sleeps on.
func (c *Conn) armStreamDeadline(t time.Time) {
	c.mu.Lock()
	c.armStreamDeadlineLocked(t)
	c.mu.Unlock()
}

func (c *Conn) armStreamDeadlineLocked(t time.Time) {
	if t.IsZero() {
		return // nothing to wait for; a stale earlier deadline costs one spurious pass
	}
	if c.streamDeadline.IsZero() || t.Before(c.streamDeadline) {
		c.streamDeadline = t
		c.kickTimer()
	}
}

// expireStreamDeadlinesLocked wakes whatever a matured stream deadline
// holds — the stream's Read, the writers parked on cond — and returns the
// earliest deadline still to come, zero when none is set. Callers hold mu;
// taking s.mu under it is the one order the two locks allow.
func (c *Conn) expireStreamDeadlinesLocked(now time.Time) time.Time {
	var next time.Time
	earlier := func(t time.Time) {
		if !t.IsZero() && now.Before(t) && (next.IsZero() || t.Before(next)) {
			next = t
		}
	}
	wakeWriters := false
	for _, s := range c.streams {
		s.mu.Lock()
		rd := s.readDeadline
		s.mu.Unlock()
		if !rd.IsZero() && !now.Before(rd) {
			s.signal()
		}
		earlier(rd)
		if wd := s.writeDeadline; !wd.IsZero() && !now.Before(wd) {
			wakeWriters = true
		}
		earlier(s.writeDeadline)
	}
	if wakeWriters {
		c.cond.Broadcast()
	}
	return next
}

// CloseWrite sends the FIN, which is what tells the peer a request or a
// response is complete.
func (s *Stream) CloseWrite() error {
	_, err := s.write(nil, true)
	return err
}

// WriteClose writes p and then the FIN, as Write followed by CloseWrite
// does, in one call: the FIN rides the last STREAM frame of p, so a p that
// fits one packet leaves as one datagram where the two calls send two. It
// waits for credit as Write does and returns what Write returns; the FIN
// goes only once all of p has, so an error leaves the stream open for
// writing, as a failed Write does.
func (s *Stream) WriteClose(p []byte) (int, error) {
	return s.write(p, true)
}

func (s *Stream) write(p []byte, fin bool) (int, error) {
	c := s.conn
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writeBeginLocked()
	// The packets one call builds leave together (batchLocked): a 4 KiB
	// write is four datagrams, and one system call carries the four.
	c.batching, c.batchErr = true, nil
	written, err := s.writeLocked(p, fin)
	c.batching = false
	c.flushBatchLocked()
	c.writeEndLocked()
	if err == nil {
		err = c.batchErr
	}
	c.batchErr = nil
	return written, err
}

// writeLocked is write's loop, run under mu with batching on.
func (s *Stream) writeLocked(p []byte, fin bool) (int, error) {
	written := 0
	c := s.conn
	// A deadline already passed fails the write before it sends anything,
	// as net.Conn does: the caller set it to stop, and stop includes the
	// writes that follow the one it interrupted.
	if dl := s.writeDeadline; !dl.IsZero() && !time.Now().Before(dl) {
		return 0, os.ErrDeadlineExceeded
	}
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
			if err := c.waitForCreditLocked(s.writeDeadline); err != nil {
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
		opts := sendOpts{waitCwnd: true, own: true, deadline: s.writeDeadline}
		if err := c.sendPacketLocked(spaceApplication, frame, opts); err != nil {
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
// peer needs to close its own flow-control accounting for the stream. A
// direction already finished — FIN sent and every byte acknowledged — has
// nothing left to abandon, and nothing is sent (RFC 9000 §3.3).
func (s *Stream) Reset(code uint64) error {
	c := s.conn
	c.mu.Lock()
	defer c.mu.Unlock()
	if s.writeReset {
		return nil // already abandoned; saying so twice tells the peer nothing
	}
	if c.streamDataRecvdLocked(s) {
		return nil // finished and acknowledged: "Data Recvd", nothing may leave
	}
	s.writeReset, s.writeCode = true, code
	s.sendClosed = true
	c.noteResetLocked(s.id)
	frame := appendFrame(nil, FrameResetStream, s.id, code, s.writeOff)
	err := c.sendPacketLocked(spaceApplication, frame, sendOpts{}) // ungated: see sendOpts.waitCwnd
	c.maybeRemoveLocked(s)
	c.cond.Broadcast() // a writer blocked on credit for this stream gives up
	return err
}

// StopSending asks the peer to stop transmitting on this stream (RFC 9000
// §3.5): the request it carries will not be served, so every byte still in
// flight is effort wasted at both ends. The peer answers with RESET_STREAM,
// which is what finally closes the receiving direction's accounting.
//
// Nothing is sent when there is nothing left to stop — the peer has reset
// the stream already, or its FIN has arrived whole — since §3.5 says
// STOP_SENDING "SHOULD only be sent for a stream that has not been reset
// by the peer" and the HTTP/3 server calls this after its own Reset, when
// either may well have happened; the application's intent is kept all the
// same. On a unidirectional stream this end opened the peer has no sending
// part to stop and would answer STREAM_STATE_ERROR (§19.5): an error, no
// frame.
func (s *Stream) StopSending(code uint64) error {
	c := s.conn
	if s.sendOnly() {
		return fmt.Errorf("%w: STOP_SENDING on stream %d, which this end opened send-only", ErrQUIC, s.id)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	// Stopping is how an application says it will not read the rest: the
	// peer's RESET_STREAM, when it comes, ends the receive side with
	// nothing left for the application to take (§3.2).
	s.mu.Lock()
	s.endTaken = true
	over := s.readReset || s.recvDone
	s.mu.Unlock()
	if over {
		c.maybeRemoveLocked(s)
		return nil
	}
	frame := appendFrame(nil, FrameStopSending, s.id, code)
	err := c.sendPacketLocked(spaceApplication, frame, sendOpts{}) // ungated: see sendOpts.waitCwnd
	c.maybeRemoveLocked(s)
	c.flushControlLocked()
	return err
}

// abandon records a RESET_STREAM that arrived, releasing whoever is reading.
// It returns how far the peer's declared final size moved this stream's
// high-water mark, and how many bytes the application will now never consume
// — both owed to the connection-level flow-control account.
func (s *Stream) abandon(code, finalSize uint64) (highGrowth, neverConsumed uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// §4.5: a RESET_STREAM that changes a known final size, or declares one
	// below bytes already received, is FINAL_SIZE_ERROR like deliver's two.
	if s.fin && finalSize != s.finAt {
		return 0, 0, &transportError{
			code: transportFinalSize,
			err:  fmt.Errorf("%w: stream %d reset to a different final size", ErrQUIC, s.id),
		}
	}
	if finalSize < s.recvQ.highest {
		return 0, 0, &transportError{
			code: transportFinalSize,
			err:  fmt.Errorf("%w: stream %d reset below bytes already received", ErrQUIC, s.id),
		}
	}
	if s.readReset {
		return 0, 0, nil
	}
	oldHigh := s.recvQ.highest
	if err := s.recvQ.receive(finalSize); err != nil {
		return 0, 0, fmt.Errorf("stream %d: %w", s.id, err)
	}
	// The reset's final size is the stream's final size from here on, held
	// exactly as a FIN's: a later STREAM frame past it, a FIN elsewhere, or
	// a second RESET_STREAM naming another size is FINAL_SIZE_ERROR through
	// the checks above and deliver's. Until 2026-10-21 only a FIN set these,
	// so a reset stream accepted a larger reset or data beyond its end.
	s.fin, s.finAt = true, finalSize
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
// requires. stopped reports that this STOP_SENDING is what abandoned it — not
// a retransmission, nor one crossing this end's own reset — the one the
// application is told of. Runs on the read loop, under conn.mu.
func (s *Stream) onStopSendingLocked(code uint64) (stopped bool, err error) {
	if s.writeReset {
		return false, nil
	}
	if s.conn.streamDataRecvdLocked(s) {
		// Everything was delivered and acknowledged before the peer asked
		// to stop: the direction is in "Data Recvd", from which §3.3 lets
		// nothing leave, and there is nothing to abandon or to tell.
		return false, nil
	}
	s.writeReset, s.writeCode = true, code
	s.sendClosed = true
	s.conn.noteResetLocked(s.id)
	frame := appendFrame(nil, FrameResetStream, s.id, code, s.writeOff)
	err = s.conn.sendPacketLocked(spaceApplication, frame, sendOpts{}) // ungated: see sendOpts.waitCwnd
	s.conn.maybeRemoveLocked(s)
	s.conn.cond.Broadcast()
	return true, err
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

// recvState reports, under s.mu, whether nothing more will arrive and
// whether the application has taken the end (endTaken).
func (s *Stream) recvState() (finished, taken bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.recvDone, s.endTaken
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

// AcceptStream returns the next stream the peer opened. It has three ways
// out: the stream, once the peer opens one; ctx's error, verbatim, once ctx
// is cancelled or past its deadline — a graceful shutdown, a timeout, an
// abandoned request — so the caller can stop waiting without ending the
// connection; and an error once the connection ends. A stream already
// queued when ctx is already done may come out either way.
//
// The queue never blocks the connection's read loop. What bounds it is
// MAX_STREAMS: a stream the peer opened gives its slot back only once it has
// come out of here and been read to its end (see [Stream.Read]), so the
// queue holds at most the InitialMaxStreamsBidi plus InitialMaxStreamsUni
// streams this end announced, and a server that stops accepting leaves the
// peer waiting for credit while every stream already accepted carries on.
//
// [Listener.Accept] takes no context: it has the shape of net.Listener's
// Accept, and [Listener.Close] unblocks it without touching the connections
// already accepted. A connection has no such switch — the only thing above
// it that could wake its acceptors is [Conn.Close], which ends it — hence the
// context here.
//
// A stream the peer opened before the connection ended is still handed
// out, ahead of the error that says it ended: the peer's data on it has
// arrived — a [Conn.Shutdown] on its side waited for exactly that — and a
// select that picked at random between the queue and the closed signal
// handed the error one time in a hundred, and lost the stream.
func (c *Conn) AcceptStream(ctx context.Context) (*Stream, error) {
	for {
		c.mu.Lock()
		if len(c.acceptQ) > 0 {
			s := c.acceptQ[0]
			c.acceptQ[0] = nil
			c.acceptQ = c.acceptQ[1:]
			if len(c.acceptQ) > 0 {
				c.signalAccept() // another acceptor may be waiting
			}
			s.queued = false
			// Read through Conn.Stream before it was accepted, the stream
			// waited for this to retire.
			c.maybeRemoveLocked(s)
			if len(c.grants) > 0 {
				c.flushControlLocked()
			}
			c.mu.Unlock()
			return s, nil
		}
		c.mu.Unlock()
		select {
		case <-c.acceptReady:
		case <-c.closed:
			c.mu.Lock()
			empty := len(c.acceptQ) == 0
			c.mu.Unlock()
			if empty {
				return nil, errClosed
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// signalAccept wakes one waiting AcceptStream without ever blocking.
func (c *Conn) signalAccept() {
	select {
	case c.acceptReady <- struct{}{}:
	default:
	}
}

// OpenStream opens a bidirectional stream from this end, within what the
// peer's MAX_STREAMS grant allows (RFC 9000 §4.6). When the grant is spent
// it waits for the peer to raise it rather than refusing: a client that
// opens a stream per request would otherwise have to poll, and the peer
// grants more as soon as it retires a finished stream. The wait ends in one
// of three ways — the credit arrives and the stream comes out; the context
// ends and its error comes back verbatim, the connection untouched; the
// connection closes meanwhile and the error says so, whatever the context.
// A context that is already done still opens a stream when credit is at
// hand: only the wait is cancellable.
//
// On the way into the wait — or into the refusal, when a context already
// done finds no credit — this end tells the peer it is blocked with
// STREAMS_BLOCKED (§19.14), once per exhaustion of the limit rather than
// once per wake-up — the frame is a hint for the peer's accounting, not a
// retry. With [context.Background] the wait lasts as long as the peer
// withholds credit, which the idle timeout bounds: a peer that never grants
// and never speaks ends the connection, and the wait with it.
func (c *Conn) OpenStream(ctx context.Context) (*Stream, error) {
	return c.open(ctx, false)
}

// OpenUniStream opens a unidirectional stream from this end — one this end
// writes and the peer only reads — within the peer's MAX_STREAMS grant for
// unidirectional streams (RFC 9000 §4.6). It waits, gives up and reports
// exactly as [Conn.OpenStream] does: the credit arrives and the stream comes
// out; the context ends and its error comes back verbatim; the connection
// closes meanwhile and the error says so. STREAMS_BLOCKED of the
// unidirectional kind (§19.14) goes out once per exhaustion of the limit.
//
// The stream it returns refuses [Stream.Read] with an error rather than
// blocking: the peer never writes on it, so nothing could ever arrive. For
// a unidirectional stream whose number the application protocol fixes —
// HTTP/3's control stream is stream 3 — use [Conn.Stream], which picks by
// identifier and leaves conformance to the grant to the caller.
func (c *Conn) OpenUniStream(ctx context.Context) (*Stream, error) {
	return c.open(ctx, true)
}

// open is the body of OpenStream and OpenUniStream: the two differ only in
// which counter and grant they read and in the identifier's type bit.
func (c *Conn) open(ctx context.Context, uni bool) (*Stream, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.hasPeerParams {
		return nil, fmt.Errorf("%w: opening a stream before the handshake delivered the peer's limits", ErrQUIC)
	}
	opened, limit := c.localStreamCounters(uni)
	if *opened >= *limit {
		if err := c.waitStreamCreditLocked(ctx, uni); err != nil {
			return nil, err
		}
	}
	base := uint64(1)
	if c.isClient {
		base = 0
	}
	if uni {
		base |= 0x02
	}
	id := base + 4**opened
	*opened++
	s := newStream(c, id)
	c.streams[id] = s
	return s, nil
}

// localStreamCounters returns how many streams of the kind this end has
// opened and how many the peer allows, for open and its wait to read and
// advance the same pair. Callers hold c.mu.
func (c *Conn) localStreamCounters(uni bool) (opened, limit *uint64) {
	if uni {
		return &c.openedUni, &c.peerMaxStreamsUni
	}
	return &c.openedBidi, &c.peerMaxStreamsBidi
}

// waitStreamCreditLocked parks under mu until the peer's grant for the
// kind of stream exceeds what this end has opened, the context ends, or
// the connection closes. The waits ride c.cond, which the MAX_STREAMS
// handler broadcasts when the limit rises and closeOnce on the way out; the
// context's expiry broadcasts too, under mu, so a Wait that has not yet
// parked cannot miss it. Called only once a wait is certain: the common
// path, credit at hand, pays nothing for the timer.
func (c *Conn) waitStreamCreditLocked(ctx context.Context, uni bool) error {
	stop := context.AfterFunc(ctx, func() {
		c.mu.Lock()
		c.cond.Broadcast()
		c.mu.Unlock()
	})
	defer stop()
	opened, limit := c.localStreamCounters(uni)
	told, frame := &c.bidiBlockedTold, uint64(frameStreamsBlocked)
	if uni {
		told, frame = &c.uniBlockedTold, frameStreamsBlocked|1
	}
	for *opened >= *limit {
		select {
		case <-c.closed:
			return errClosed
		default:
		}
		// Before the context's test: a caller that only tries, its context
		// already done, is blocked all the same, and §4.6 asks that the peer
		// hear of it.
		if !*told {
			*told = true
			c.control = appendFrame(c.control, frame, *limit)
			c.flushControlLocked()
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		c.cond.Wait()
	}
	return nil
}

// ErrStreamLimit is what [Conn.Stream] returns for an identifier past the
// stream limit of its kind: for a stream this end would open, past the
// MAX_STREAMS the peer granted; for a stream the peer would open, past the
// limit this end announced. Nothing is registered for it.
var ErrStreamLimit = errors.New("quic: stream identifier past the stream limit")

// Stream returns the stream with this identifier, opening the bookkeeping
// for it if nobody has yet. It is how a server answers on the stream a
// request arrived on, and how it opens its own unidirectional streams (the
// HTTP/3 control stream). A stream that already closed comes back non-nil
// but refuses writes — answering on a stream the peer is done with is not
// an error worth a connection.
//
// Picking a stream by identifier is for streams whose numbers the
// application protocol already fixed — HTTP/3's control stream is stream 3
// — and for answering on streams the peer itself opened. Unlike
// [Conn.OpenStream], Stream does not wait for credit: an identifier this
// end would open past the peer's MAX_STREAMS grant, or one the peer would
// open past the limit this end announced, is refused with [ErrStreamLimit]
// and allocates nothing. Within the grant, a local identifier used out of
// order opens every lower-numbered stream of its kind (RFC 9000 §2.1).
func (c *Conn) Stream(id uint64) (*Stream, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if s := c.streams[id]; s != nil {
		return s, nil
	}
	// Within what has been opened and absent from the table means closed
	// (see Conn.streams): a tombstone, deliberately not re-registered.
	count := id/4 + 1
	uni := id&0x02 != 0
	localInitiated := (id&1 == 0) == c.isClient
	opened, limit := c.seenPeerBidi, c.localMaxStreamsBidi
	switch {
	case localInitiated && uni:
		opened, limit = c.openedUni, c.peerMaxStreamsUni
	case localInitiated:
		opened, limit = c.openedBidi, c.peerMaxStreamsBidi
	case uni:
		opened, limit = c.seenPeerUni, c.localMaxStreamsUni
	}
	if count <= opened {
		s := newStream(c, id)
		s.writeReset = true
		s.sendClosed = true
		s.recvDone = true
		return s, nil
	}
	if count > limit {
		return nil, ErrStreamLimit
	}
	if !localInitiated {
		s := newStream(c, id)
		c.streams[id] = s
		return s, nil
	}
	// Using a local identifier out of order opens every lower-numbered
	// stream of its kind (RFC 9000 §2.1): they are registered so a frame the
	// peer may now legitimately send on one of them — MAX_STREAM_DATA,
	// STOP_SENDING — finds a stream and not an error, and so the table's
	// rule above stays true. The ones the caller never uses live until
	// the connection closes, which is the price of the caller's choice.
	first := opened
	if uni {
		c.openedUni = count
	} else {
		c.openedBidi = count
	}
	for n := first + 1; n <= count; n++ {
		nid := (n-1)*4 + id&0x03
		c.streams[nid] = newStream(c, nid)
	}
	return c.streams[id], nil
}

// maybeRemoveLocked retires a stream once both directions are done: the
// receive side has delivered its FIN or been reset, and the send side has
// said its final word (FIN or RESET_STREAM). Leaving the table is what marks
// the stream closed (see Conn.streams): frames still in flight for it are
// tolerated rather than reopening it, for as long as the connection lives.
// For peer-initiated streams the slot is granted back with MAX_STREAMS,
// which is what stops a long-lived connection dying of stream arithmetic.
//
// A stream the peer opened retires later than that: once it has left the
// accept queue and the application has taken its end (endTaken) — RFC 9000
// §3.2's "Data Read", not "Data Recvd". Retiring on the FIN alone granted
// the slot back while the stream still sat unaccepted, its bytes unread, so
// a peer opening unidirectional streams with a FIN faster than the
// application accepted them was bounded by nothing MAX_STREAMS counts. In
// batch mode the callback is the consumer and has already returned when this
// runs, so the FIN is the end taken.
// Callers hold c.mu.
func (c *Conn) maybeRemoveLocked(s *Stream) {
	if !s.sendClosed {
		return
	}
	if _, present := c.streams[s.id]; !present {
		return // already retired
	}
	finished, taken := s.recvState()
	if !finished {
		return
	}
	peerInitiated := (s.id&1 == 0) != c.isClient
	if peerInitiated && !c.batchMode && (s.queued || !taken) {
		return
	}
	delete(c.streams, s.id)
	if peerInitiated { // grant the slot back
		c.returnStreamSlotLocked(s.id&0x02 != 0)
	}
}

// retireTaken retires a stream whose end the application has just taken,
// and sends the MAX_STREAMS grant that may have earned. Called by
// Stream.Read with no locks held.
func (c *Conn) retireTaken(s *Stream) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.maybeRemoveLocked(s)
	if len(c.grants) > 0 {
		c.flushControlLocked()
	}
}

// returnStreamSlotLocked grants a retired peer stream's slot back with
// MAX_STREAMS, an eighth of the initial limit at a time rather than one
// slot per stream. The grant leaves at once in a packet of its own
// (flushControlLocked), so one per stream was one datagram per request on
// a connection serving small requests — measured at 1.0 per request over
// HTTP/3, as many as the responses. RFC 9000 §4.6 leaves when and how many
// to the implementation; returning an eighth at a time keeps the peer's
// credit within an eighth of what one-per-stream would give it, and the
// grant never waits on the peer saying it is blocked, which §4.6 forbids:
// it follows retirements alone. A limit under 8 still returns every slot.
// The limit is raised as the grant is queued, not when it leaves — the
// same choice as recvQuota.nextMax, with the same reason. Callers hold c.mu.
func (c *Conn) returnStreamSlotLocked(uni bool) {
	limit, retired, initial, typ := &c.localMaxStreamsBidi, &c.retiredBidi, c.params.InitialMaxStreamsBidi, uint64(frameMaxStreams)
	if uni {
		limit, retired, initial, typ = &c.localMaxStreamsUni, &c.retiredUni, c.params.InitialMaxStreamsUni, frameMaxStreams|1
	}
	*retired++
	if *retired < max(1, initial/8) {
		return
	}
	*limit += *retired
	*retired = 0
	c.grants = appendFrame(c.grants, typ, *limit)
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
// forgotten. A frame with Type FrameStopSending reports a peer that stopped
// reading a stream this end sends on: the transport has already abandoned
// the sending side and answered RESET_STREAM (RFC 9000 §3.5), so further
// writes fail, and [Stream.WasReset] has the peer's code. It comes once per
// stream, in the packet's order, even in a packet that carries nothing else.
// Everything borrows the connection's buffers and is valid only
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
// one delta each — arrival order across streams, resets and STOP_SENDING
// are lost then.
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
