package httpstream

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

// The write side of an HTTP/2 connection.
//
// It is one goroutine and it is the only one that touches the socket for
// writing. That is the whole of its contract: frames arrive from the read
// goroutine (control) and from the handler goroutine (responses), and they
// leave in whole frames, in the order this scheduler chose.
//
// # Why it exists
//
// Before it, everything ran on the goroutine that read the socket — framing,
// the handler, and streamed response bodies. Two consequences followed, and
// both were connection-fatal rather than theoretical. A streamed response
// could only spend the flow-control credit granted before it started, because
// the goroutine that would read the WINDOW_UPDATE was the one blocked writing;
// past 65535 bytes it gave up and took the connection with it. And while a
// handler ran, the connection was deaf: no PING answered, no SETTINGS ACKed,
// no RST_STREAM seen — a peer that uses PING for liveness kills a connection
// that goes quiet.
//
// Splitting the write side off answers both. Waiting for credit is now a wait
// on a condition variable that the read goroutine signals, and control frames
// go out while a response is stalled because they are a separate queue that
// jumps ahead of the data.
//
// # The scheduling policy, stated
//
// Round-robin over the streams that have data queued, one frame each per pass,
// repeated until the round's byte budget ([H2Config.MaxWriteBufferBytes]) is
// spent or nothing can progress. One stream alone therefore gets the whole
// budget and pays one syscall for it; eight streams share it evenly and none
// is starved by a large neighbour. No priorities: RFC 9113 deprecated the
// scheme and RFC 9218's replacement is not implemented here, which is a choice
// rather than an omission — see the ADR.
//
// Within one batch the buffered bodies are all queued at once and interleave.
// A streamed body does not interleave with another streamed body of the same
// batch: its producer is pulled by the handler goroutine, one at a time, and
// a batch's buffers may not be retained past the call that received them (S14).
// The ADR records that trade and what would reverse it.

// errStreamGone reports a stream that ended — reset by the peer, or given up
// on — while a response for it was still queued. It is not a connection
// failure: the response is dropped and the connection carries on.
var errStreamGone = errors.New("httpstream: the stream ended before its response was written")

// errWriteStalled reports a stream the peer never granted credit for. It is
// bounded by [H2Config.WriteTimeout] rather than waited on forever, which is
// what keeps a shutdown from hanging on a peer that stopped reading.
var errWriteStalled = errors.New("httpstream: the peer granted no window before the write timeout")

// h2Job is one stream's contribution to the write queue: a framed head that
// needs no credit, and a payload that does.
//
// For a job [h2Writer.deliver] queues, data is borrowed from whoever queued
// it and must stay valid until the job reports done — which is what makes
// deliver a blocking call. For a streamed body's job ([h2Writer.extend]) data
// is the job's own buffer, grown under the lock while the job is still queued,
// so that the producer runs ahead of the socket by at most one round's budget
// and the batches that arrive during a write leave together in the next one.
type h2Job struct {
	id uint32

	// queued is true while the job sits in h2Writer.jobs: the one state in
	// which more data may be appended to it. It is cleared wherever a job
	// leaves the queue — framed whole, dropped, expired or failed.
	queued bool

	// head is already framed: HEADERS and whatever CONTINUATION frames the
	// block needed. RFC 9113 §6.9 flow-controls DATA and nothing else, so it
	// goes out on the first pass whatever the window says.
	head []byte

	data []byte
	off  int
	end  bool // END_STREAM rides on the last DATA frame

	// closes says the stream is over once this job is written, whether the
	// flag went out on a DATA frame or on the head. It is what releases the
	// send window, and it is not the same question as end: a response with no
	// body carries END_STREAM on its HEADERS and never emits a DATA frame at
	// all.
	closes bool

	headSent bool
	endSent  bool

	// deadline is refreshed every time the job writes something. It bounds
	// the wait for credit, not the response.
	deadline time.Time

	done bool
	err  error
}

func (j *h2Job) complete() bool {
	return j.headSent && j.off == len(j.data) && (!j.end || j.endSent)
}

type h2Writer struct {
	conn net.Conn
	cfg  H2Config

	mu   sync.Mutex
	cond *sync.Cond
	done chan struct{}

	// ctrl holds framed control frames — SETTINGS, PING ACK, WINDOW_UPDATE,
	// RST_STREAM, GOAWAY. They are written before any DATA of the same round,
	// which is the point: a PING answered while a response waits for credit
	// is the difference between a connection the peer keeps and one it kills
	// for silence.
	ctrl []byte

	jobs     []*h2Job // queued response data
	inflight []*h2Job // completed in this round, waiting on the socket write
	turn     int      // round-robin cursor
	out      []byte   // one round's frames, reused
	// expiry is park's stall deadline, one timer re-armed per wakeup rather
	// than one AfterFunc allocated per wakeup — see park for the numbers.
	expiry *time.Timer

	// connWind and windows are the peer's grants, and they live here rather
	// than on the read side because this is the goroutine that spends them.
	// The read goroutine credits them through credit and creditConn.
	connWind int32
	windows  map[uint32]int32
	initial  int32
	// maxFrame is the largest payload a frame leaves with: the peer's
	// SETTINGS_MAX_FRAME_SIZE, capped by setMaxFrame.
	maxFrame int

	// The peer's SETTINGS_HEADER_TABLE_SIZE, and the dynamic table size
	// update the next header block owes it. RFC 7541 §4.2: after the setting
	// changes, the first header block sent MUST open with the smallest size
	// seen since the last block, then the final one if it differs. The
	// encoder never indexes, so its table is always empty and any size is
	// true of it — but a decoder that saw the setting shrink expects the
	// update regardless, and nghttp2 answers its absence with
	// COMPRESSION_ERROR. Decided here, under the lock the ACK is queued
	// under, because the peer applies the setting when it reads the ACK: a
	// head that leaves after the ACK owes the update even if it was framed
	// before the SETTINGS arrived.
	peerTable   uint32
	tableUpdate bool
	tableMin    uint32
	tableFinal  uint32

	stopping bool
	err      error
}

// hpackEncoderTableSize is the most dynamic table this end's encoder ever
// claims: the protocol's initial value. A peer that allows more is not told
// so — nothing would use it.
const hpackEncoderTableSize = 4096

func newH2Writer(c net.Conn, cfg H2Config) *h2Writer {
	w := &h2Writer{
		conn:      c,
		cfg:       cfg,
		done:      make(chan struct{}),
		windows:   make(map[uint32]int32),
		connWind:  defaultInitialWindow,
		initial:   defaultInitialWindow,
		maxFrame:  defaultMaxFrameSize,
		peerTable: hpackEncoderTableSize,
	}
	w.cond = sync.NewCond(&w.mu)
	return w
}

// run is the write goroutine. It ends when the connection fails, or when stop
// has been called and everything queued before it has gone out.
func (w *h2Writer) run() {
	defer close(w.done)
	for {
		w.mu.Lock()
		for {
			if w.err != nil {
				w.mu.Unlock()
				return
			}
			if len(w.ctrl) > 0 || w.runnable() {
				break
			}
			if w.stopping && len(w.jobs) == 0 {
				w.mu.Unlock()
				return
			}
			w.park()
		}

		w.out = w.out[:0]
		w.out = append(w.out, w.ctrl...)
		w.ctrl = w.ctrl[:0]
		w.inflight = w.fill()
		buf := w.out
		// The control queue has room again, so a read goroutine blocked on
		// its bound can carry on reading.
		w.cond.Broadcast()
		w.mu.Unlock()

		err := w.writeAll(buf)

		w.mu.Lock()
		if err != nil {
			w.setErr(err)
		} else {
			for _, j := range w.inflight {
				j.done = true
			}
			w.inflight = nil
		}
		w.cond.Broadcast()
		w.mu.Unlock()
	}
}

// park waits for something to do, under the deadline the earliest stalled job
// imposes.
//
// A job with data and no credit is a peer that granted nothing. Waiting for it
// forever is what would deadlock a shutdown — the read goroutine has stopped,
// so no WINDOW_UPDATE is ever coming — so the wait is bounded by the same
// clock a blocked socket write is bounded by, and the stream is reset when it
// runs out.
// The expiry timer is one per writer, re-armed per wakeup: an AfterFunc
// allocated per wakeup costs 2 allocations (128 B) and about
// 1.6x the time of Reset, which allocates nothing (BenchmarkPark, median of
// ten: 302 ns against 185 ns, 2026-10-04), and a stalled
// round wakes once per WINDOW_UPDATE the peer trickles in. Reset on a timer
// whose function may be running is safe since Go 1.23, and the callback
// re-checks state under the lock anyway.
func (w *h2Writer) park() {
	if d, ok := w.earliest(); ok {
		if w.expiry == nil {
			w.expiry = time.AfterFunc(time.Until(d), func() {
				w.mu.Lock()
				w.expire()
				w.cond.Broadcast()
				w.mu.Unlock()
			})
		} else {
			w.expiry.Reset(time.Until(d))
		}
		defer w.expiry.Stop()
	}
	w.cond.Wait()
}

func (w *h2Writer) earliest() (time.Time, bool) {
	var best time.Time
	for _, j := range w.jobs {
		if best.IsZero() || j.deadline.Before(best) {
			best = j.deadline
		}
	}
	return best, !best.IsZero()
}

// expire gives up on jobs the peer never granted credit for, resetting their
// streams so the peer learns the response is not coming.
func (w *h2Writer) expire() {
	now := time.Now()
	kept := w.jobs[:0]
	for _, j := range w.jobs {
		if now.Before(j.deadline) {
			kept = append(kept, j)
			continue
		}
		j.err = fmt.Errorf("%w: stream %d", errWriteStalled, j.id)
		j.done, j.queued = true, false
		delete(w.windows, j.id)
		// CANCEL rather than a code that blames the peer: it granted less
		// than we wanted, which it is entitled to do, and we are the side
		// that stopped waiting. A frame this end built cannot fail to frame,
		// and a failure here would have nowhere to go anyway — the job it
		// would report on has already been answered.
		_ = w.appendCtrl32(frameRSTStream, j.id, errCancel)
	}
	w.jobs = kept
	w.turn = 0
}

// runnable reports whether any queued job can put a byte on the wire now.
func (w *h2Writer) runnable() bool {
	for _, j := range w.jobs {
		if !j.headSent {
			return true
		}
		left := len(j.data) - j.off
		if left == 0 {
			return true // the trailing END_STREAM costs no credit
		}
		if w.connWind > 0 && w.windows[j.id] > 0 {
			return true
		}
	}
	return false
}

// fill appends one round of frames to w.out and returns the jobs it finished.
//
// Round-robin, one frame per job per pass, until the byte budget is spent or a
// whole pass writes nothing. The cursor moves by one job per round so that the
// stream which went first this time goes last next time.
func (w *h2Writer) fill() []*h2Job {
	if len(w.jobs) == 0 {
		return nil
	}
	budget := max(w.cfg.MaxWriteBufferBytes, w.maxFrame)
	for progress := true; progress && budget > 0; {
		progress = false
		for k := 0; k < len(w.jobs) && budget > 0; k++ {
			j := w.jobs[(w.turn+k)%len(w.jobs)]
			if j.done {
				continue
			}
			n, err := w.emit(j)
			if err != nil {
				j.err, j.done = err, true
				continue
			}
			if n > 0 {
				progress = true
				budget -= n
			}
		}
	}

	var finished []*h2Job
	kept := w.jobs[:0]
	for _, j := range w.jobs {
		switch {
		case j.done: // failed inside emit; already answered
			j.queued = false
		case j.complete():
			if j.closes {
				// The stream is over on this side; its window is nobody's to
				// spend now.
				delete(w.windows, j.id)
			}
			j.queued = false
			finished = append(finished, j)
		default:
			kept = append(kept, j)
		}
	}
	w.jobs = kept
	if len(w.jobs) > 0 {
		w.turn = (w.turn + 1) % len(w.jobs)
	} else {
		w.turn = 0
	}
	return finished
}

// emit appends at most one frame of j and reports the payload bytes it spent
// from the round's budget.
func (w *h2Writer) emit(j *h2Job) (int, error) {
	if !j.headSent {
		if w.tableUpdate {
			head, err := w.prefixTableUpdate(j)
			if err != nil {
				return 0, err
			}
			j.head = head
			w.tableUpdate = false
		}
		w.out = append(w.out, j.head...)
		j.headSent = true
		j.deadline = time.Now().Add(w.cfg.WriteTimeout)
		return len(j.head), nil
	}
	left := len(j.data) - j.off
	if left == 0 {
		if j.end && !j.endSent {
			out, err := appendH2Frame(w.out, h2Frame{Type: frameData, Flags: flagEndStream, StreamID: j.id})
			if err != nil {
				return 0, err
			}
			w.out, j.endSent = out, true
		}
		return 0, nil
	}
	n := min(left, w.maxFrame, int(w.connWind), int(w.windows[j.id]))
	if n <= 0 {
		return 0, nil
	}
	var flags byte
	if j.end && j.off+n == len(j.data) {
		flags = flagEndStream
	}
	out, err := appendH2Frame(w.out, h2Frame{
		Type: frameData, Flags: flags, StreamID: j.id, Payload: j.data[j.off : j.off+n],
	})
	if err != nil {
		return 0, err
	}
	spent := int32(n) //nolint:gosec // G115: n is the minimum of two int32 windows
	w.out = out
	j.off += n
	j.endSent = j.endSent || flags&flagEndStream != 0
	w.connWind -= spent
	w.windows[j.id] -= spent
	j.deadline = time.Now().Add(w.cfg.WriteTimeout)
	return n, nil
}

// deliver queues jobs and returns when every one of them has been written, has
// been dropped because its stream is gone, or has run out of time.
//
// It returns an error only when the write side itself failed, which ends the
// connection. A per-stream outcome is left on the job, where the caller reads
// it and decides — that is the whole difference between one bad stream and a
// dead connection.
func (w *h2Writer) deliver(jobs []*h2Job) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return w.err
	}
	deadline := time.Now().Add(w.cfg.WriteTimeout)
	for _, j := range jobs {
		if _, alive := w.windows[j.id]; !alive || w.stopping {
			j.err, j.done = errStreamGone, true
			continue
		}
		j.deadline = deadline
		j.queued = true
		w.jobs = append(w.jobs, j)
	}
	w.cond.Broadcast()
	for {
		if w.err != nil {
			return w.err
		}
		pending := false
		for _, j := range jobs {
			if !j.done {
				pending = true
				break
			}
		}
		if !pending {
			return nil
		}
		w.cond.Wait()
	}
}

// extend queues data for a streamed body on id and returns the job carrying
// it, without waiting for the wire.
//
// The job still queued from the previous batch, if there is one, takes the
// bytes: they go out in the same round as what it already holds, in one DATA
// frame where the window allows, and the last of them may carry END_STREAM
// when [h2Writer.finish] catches the job in time. Otherwise — the previous
// job is being written, has been written, or was dropped — a new job opens on
// whichever of cur and spare is free. Both are the caller's and alternate:
// one is at most in flight while the other is queued, so the one not in use
// is always done by the time it is needed, and its buffer can be reused.
//
// The only wait is for room: a queued job holding one round's budget or more
// of unframed bytes ([H2Config.MaxWriteBufferBytes]) is a peer that reads
// slower than the producer yields, and the producer then runs at the window's
// pace, bounded by the same deadline every stalled job has. This is the whole
// difference between a batch on HTTP/2 and on HTTP/3 before it: deliver
// returned once the bytes were on the wire, so three batches of eight bytes
// cost three writes, six goroutine hand-offs and three TCP segments the
// client woke on one at a time — about 70 µs each against a loopback —
// where quic-go's Stream.Write copies, returns, and lets the send loop pack
// what has accumulated into one packet.
//
// The outcome of a job that left the queue is on the job, as with deliver:
// cur.err set means the stream is gone and the pull should stop.
func (w *h2Writer) extend(id uint32, cur, spare *h2Job, data []byte) (*h2Job, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for cur != nil && cur.queued {
		if w.err != nil {
			return cur, w.err
		}
		if len(cur.data)-cur.off < w.cfg.MaxWriteBufferBytes {
			cur.data = append(cur.data, data...)
			w.cond.Broadcast()
			return cur, nil
		}
		w.cond.Wait()
	}
	if cur != nil && cur.err != nil {
		return cur, nil // the stream is over; the caller reads why
	}
	j, err := w.claim(id, cur, spare)
	if err != nil || j.err != nil {
		return j, err
	}
	j.data = append(j.data[:0], data...)
	w.jobs = append(w.jobs, j)
	w.cond.Broadcast()
	return j, nil
}

// claim takes a free job of the pair for a new streamed-body job on id and
// resets it, waiting for a write in progress to release one. Called with the
// lock held. A stream that is gone is answered on the job, as deliver does.
func (w *h2Writer) claim(id uint32, cur, spare *h2Job) (*h2Job, error) {
	j := cur
	if j == nil || !j.done {
		j = spare
	}
	for !j.done {
		if w.err != nil {
			return j, w.err
		}
		w.cond.Wait()
	}
	if w.err != nil {
		return j, w.err
	}
	data := j.data[:0]
	*j = h2Job{id: id, headSent: true, data: data, deadline: time.Now().Add(w.cfg.WriteTimeout)}
	if _, alive := w.windows[id]; !alive || w.stopping {
		j.err, j.done = errStreamGone, true
		return j, nil
	}
	j.queued = true
	return j, nil
}

// finish ends a streamed body: END_STREAM rides on the last DATA frame of the
// job still queued when there is one, and on an empty DATA frame of its own
// otherwise. It returns once the stream's last job has been written — or
// dropped, which is not this connection's failure.
func (w *h2Writer) finish(id uint32, cur, spare *h2Job) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	j := cur
	if j != nil && j.queued {
		j.end, j.closes = true, true
		w.cond.Broadcast()
	} else {
		var err error
		if j, err = w.claim(id, cur, spare); err != nil || j.err != nil {
			return err
		}
		j.end, j.closes = true, true
		w.jobs = append(w.jobs, j)
		w.cond.Broadcast()
	}
	for !j.done {
		if w.err != nil {
			return w.err
		}
		w.cond.Wait()
	}
	return nil
}

// control queues a control frame, blocking while the queue is at its bound.
//
// Blocking is the design: the read goroutine is the only caller, and a peer
// that sends PING faster than this end can answer must be made to wait on TCP
// rather than have its frames buffered without limit. It is the S1 bound the
// project asks for, put where the pressure is.
func (w *h2Writer) control(f h2Frame) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.controlLocked(f)
}

func (w *h2Writer) controlLocked(f h2Frame) error {
	for len(w.ctrl) >= w.cfg.MaxPendingControlBytes && w.err == nil && !w.stopping {
		w.cond.Wait()
	}
	if w.err != nil {
		return w.err
	}
	if err := w.appendCtrl(f); err != nil {
		return err
	}
	w.cond.Broadcast()
	return nil
}

// ackSettings queues the ACK for a peer's SETTINGS and, in the same critical
// section, records what a SETTINGS_HEADER_TABLE_SIZE in it obliges the next
// header block to say. One section, so that no head can leave between the
// ACK and the obligation, in either order.
func (w *h2Writer) ackSettings(table uint32, haveTable bool) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.controlLocked(h2Frame{Type: frameSettings, Flags: flagAck}); err != nil {
		return err
	}
	if haveTable && table != w.peerTable {
		w.peerTable = table
		size := min(table, hpackEncoderTableSize)
		if !w.tableUpdate || size < w.tableMin {
			w.tableMin = size
		}
		w.tableFinal = size
		w.tableUpdate = true
	}
	return nil
}

// prefixTableUpdate reframes j's head with the dynamic table size updates
// the peer is owed at the front of its block. It runs once per change of the
// setting, so it allocates rather than disturb the borrowed head in place.
func (w *h2Writer) prefixTableUpdate(j *h2Job) ([]byte, error) {
	block := make([]byte, 0, len(j.head)+10)
	if w.tableMin < w.tableFinal {
		block = appendHPACKInt(block, uint64(w.tableMin), 5, 0x20)
	}
	block = appendHPACKInt(block, uint64(w.tableFinal), 5, 0x20)
	var flags byte
	for rest := j.head; len(rest) >= frameHeaderSize; {
		n := int(rest[0])<<16 | int(rest[1])<<8 | int(rest[2])
		if rest[3] == frameHeaders {
			flags = rest[4] &^ flagEndHeaders
		}
		// appendHead framed these, without padding or priority, so the
		// payload is the block fragment and nothing else.
		block = append(block, rest[frameHeaderSize:frameHeaderSize+n]...)
		rest = rest[frameHeaderSize+n:]
	}
	return appendHeaderFrames(nil, j.id, flags, block, w.maxFrame)
}

// control32 and appendCtrl32 queue a control frame whose payload is one
// 32-bit word — RST_STREAM's code, WINDOW_UPDATE's increment — from a stack
// array: AppendFrame copies the payload into ctrl, so nothing needs the heap
// slice a per-frame make would cost.
func (w *h2Writer) control32(typ byte, id, v uint32) error {
	var p [4]byte
	be32put(p[:], v)
	return w.control(h2Frame{Type: typ, StreamID: id, Payload: p[:]})
}

func (w *h2Writer) appendCtrl32(typ byte, id, v uint32) error {
	var p [4]byte
	be32put(p[:], v)
	return w.appendCtrl(h2Frame{Type: typ, StreamID: id, Payload: p[:]})
}

func (w *h2Writer) appendCtrl(f h2Frame) error {
	out, err := appendH2Frame(w.ctrl, f)
	if err != nil {
		return err
	}
	w.ctrl = out
	return nil
}

// open registers a stream's send window at the peer's current
// SETTINGS_INITIAL_WINDOW_SIZE — the current one, not the protocol's default:
// a peer that shrank it before opening this stream expects the smaller one.
func (w *h2Writer) open(id uint32) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, known := w.windows[id]; !known {
		w.windows[id] = w.initial
	}
}

// forget drops a stream: whatever was queued for it is answered errStreamGone,
// which is how a peer's RST_STREAM reaches a handler still producing a body.
func (w *h2Writer) forget(id uint32) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.windows, id)
	kept := w.jobs[:0]
	for _, j := range w.jobs {
		if j.id != id {
			kept = append(kept, j)
			continue
		}
		j.err, j.done, j.queued = errStreamGone, true, false
	}
	w.jobs = kept
	w.turn = 0
	w.cond.Broadcast()
}

// alive reports whether a response for id may still be written: the stream
// was not reset or given up on, and the write side neither failed nor began
// to stop. It is a lock and a map lookup, asked once per producer batch.
func (w *h2Writer) alive(id uint32) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, open := w.windows[id]
	return open && w.err == nil && !w.stopping
}

// credit adds to a stream's send window. A stream this end has finished with
// is not an error: WINDOW_UPDATE for a stream that just closed is ordinary.
func (w *h2Writer) credit(id uint32, inc int32) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	cur, alive := w.windows[id]
	if !alive {
		return nil
	}
	next, err := addWindow(cur, inc)
	if err != nil {
		// §6.9.1 makes a stream window overflow a stream error, and now there
		// is a per-stream error path to make it one.
		return streamError{id: id, code: errFlowControlError, err: err}
	}
	w.windows[id] = next
	w.cond.Broadcast()
	return nil
}

func (w *h2Writer) creditConn(inc int32) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	next, err := addWindow(w.connWind, inc)
	if err != nil {
		return err // connection-level: a connection error, as §6.9.1 says
	}
	w.connWind = next
	w.cond.Broadcast()
	return nil
}

// setInitialWindow applies SETTINGS_INITIAL_WINDOW_SIZE, which is the one
// setting that reaches backwards (§6.9.2): the delta applies to every stream
// already open, and it is a delta against the previous value rather than
// against the protocol's default.
func (w *h2Writer) setInitialWindow(v int32) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	delta := v - w.initial
	w.initial = v
	for id, cur := range w.windows {
		next, err := addWindow(cur, delta)
		if err != nil {
			return err
		}
		w.windows[id] = next
	}
	w.cond.Broadcast()
	return nil
}

// setMaxFrame applies the peer's SETTINGS_MAX_FRAME_SIZE, capped at this
// end's own [H2Config.MaxFrameSize].
//
// The peer's value is a ceiling it will accept, not a size this end must
// use (RFC 9113 §4.2). Taken as is, a peer advertising 16 MiB made one DATA
// frame — and the round buffer sized by it, which is retained for the
// connection's life — as large as its window allowed: 4 MiB in one frame was
// reproduced. The cap keeps that buffer bounded by this end's configuration.
func (w *h2Writer) setMaxFrame(n int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.maxFrame = min(n, w.cfg.MaxFrameSize)
}

// stop ends the write side once everything already queued has gone out.
func (w *h2Writer) stop() {
	w.mu.Lock()
	w.stopping = true
	w.cond.Broadcast()
	w.mu.Unlock()
}

// failure reports what ended the write side, if anything did.
func (w *h2Writer) failure() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.err
}

// setErr records a write failure and releases everyone waiting on the write
// side. Called with the lock held.
//
// It closes the socket too. Nothing this end can say will reach the peer once
// a write has failed, and leaving the read side open means the reader goes on
// parsing frames and handing batches to a responder whose every hand-off fails
// — busy work for a connection that is already over, until the read side times
// out on its own. Closing makes the reader's next read fail, which is the
// signal it already knows how to unwind on.
func (w *h2Writer) setErr(err error) {
	if w.err == nil {
		w.err = err
		_ = w.conn.Close()
	}
	for _, j := range w.jobs {
		if !j.done {
			j.err, j.done = w.err, true
		}
		j.queued = false
	}
	for _, j := range w.inflight {
		j.err, j.done = w.err, true
	}
	w.jobs, w.inflight = nil, nil
}

func (w *h2Writer) writeAll(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	if err := w.conn.SetWriteDeadline(time.Now().Add(w.cfg.WriteTimeout)); err != nil {
		return err
	}
	_, err := w.conn.Write(buf)
	return err
}
