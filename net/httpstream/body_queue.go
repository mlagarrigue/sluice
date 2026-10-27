package httpstream

import (
	"fmt"
	"os"
	"sync"
	"time"
)

// bodyQueue is an HTTP/2 or HTTP/3 request body that streams
// (docs/design/adr/0009, article 4): the connection's reader queues each DATA
// frame's bytes as they arrive, the handler takes them as it pulls, and the
// stream's flow-control window goes back to the peer as the handler consumes
// — not as the bytes arrive, which is what makes the window, rather than this
// end's memory, the backpressure. The connection's window goes back as the
// bytes arrive, so that one body left unread does not stall the others.
//
// The two protocols differ in who keeps the window. HTTP/2's is this end's
// arithmetic (h2 set): a grant never takes window plus held bytes past the
// stream's initial window, and nothing is granted on arrival while bodies
// stream (see h2Server.data). HTTP/3's is the QUIC transport's, run on manual
// credit (release set): every byte delivered is released once at the stream
// level, here, when the handler is done with it or it is dropped. Either way
// the peer can send no more than the stream window allows, so the window
// bounds what a streamed body holds, and MaxConcurrentStreams times it what a
// connection's streamed bodies hold — in place of the aggregate bound the
// bodies assembled whole are under.
//
// The bytes sit in a ring that starts small and doubles toward what the
// window lets arrive, so a short upload does not pay for all of it. What the
// handler holds — the piece the last pull returned — is released at the next
// pull, and only then counts as consumed: until then it is still memory this
// end holds, and its window stays spent.
//
// It is shared by two goroutines, the reader and the handler's call, and
// every field below mu is read and written under it. The writer and the
// transport are called with mu released: they may block, and the reader,
// which takes mu on every DATA frame of the stream, must not wait behind
// them.
type bodyQueue struct {
	id      uint64
	limit   int // the most a body may carry, whole or streamed
	timeout time.Duration

	// h2 is the HTTP/2 connection whose window this is, nil over HTTP/3;
	// initial is the window it grants, InitialWindowSize.
	h2      *h2Server
	initial int32
	// release gives n bytes of stream-level credit back to the QUIC
	// transport, nil over HTTP/2.
	release func(n int)
	// interim writes the 100 Continue a client is waiting for.
	interim func() error
	// onClose, when set, is told once the handler has returned from a body
	// that had not ended: the reader may forget the stream.
	onClose func()

	// wake is signalled, without blocking, when bytes arrive or the body
	// ends; the handler waits on it with ReadTimeout.
	wake  chan struct{}
	timer *time.Timer

	mu sync.Mutex
	// buf is the ring; the held bytes run from head, n of them, and the
	// first viewed of those are the piece the handler holds.
	buf      []byte
	head     int
	n        int
	viewed   int
	window   int32 // HTTP/2: what the peer may still send on this stream
	grace    int32 // HTTP/2: the overshoot tolerated above window, see h2Stream.windowGrace
	total    int   // bytes received
	declared int   // the content-length the head stated, or -1

	expect  bool // the client waits for 100 Continue before it sends the body
	sent100 bool
	ended   bool  // the body's end arrived: what is held is all there is
	closed  bool  // the handler returned
	err     error // the body failed: what the handler reads next
}

// bodyMinRing is the ring a streamed body starts with when nothing is
// buffered yet: one default HTTP/2 frame.
const bodyMinRing = defaultMaxFrameSize

// errBodyReset reports a streamed body the client reset before its end.
var errBodyReset = fmt.Errorf("%w: the client reset the stream mid-body", ErrMalformed)

// errBodyConnClosed reports a streamed body whose connection ended first.
var errBodyConnClosed = fmt.Errorf("%w: the connection closed mid-body", ErrMalformed)

// newBodyQueue starts a streamed body with what was buffered before it began
// to stream as its first bytes.
func newBodyQueue(cfg Config, id uint64, first []byte, declared int, expect bool) *bodyQueue {
	b := &bodyQueue{
		id: id, limit: max(cfg.MaxBodyBytes, cfg.MaxStreamedBodyBytes), timeout: cfg.ReadTimeout,
		wake: make(chan struct{}, 1), total: len(first), declared: declared, expect: expect,
	}
	if len(first) > 0 {
		b.buf = append(make([]byte, 0, max(len(first), bodyMinRing)), first...)
		b.buf = b.buf[:cap(b.buf)]
		b.n = len(first)
	}
	if b.total > b.limit {
		b.failLocked(fmt.Errorf("%w: a streamed body over %d bytes", ErrTooLarge, b.limit))
	}
	return b
}

// newH2Body turns st, whose head is complete and whose body has not ended,
// into a streamed body. Called by the reader.
func (s *h2Server) newH2Body(st *h2Stream) *bodyQueue {
	b := newBodyQueue(s.cfg, uint64(st.id), st.body, st.declared, expectsContinue(st.req))
	b.h2 = s
	b.initial = int32(s.cfg.InitialWindowSize) //nolint:gosec // G115: capped by resolve
	b.window, b.grace = st.recvWind, st.windowGrace
	b.interim = func() error { return s.w.interim(st.id, 100) }
	s.assembling -= len(st.body)
	st.body = nil
	return b
}

// receive queues one HTTP/2 DATA frame's bytes: used is the frame's payload
// as the peer debited it, data the body bytes once its padding is cut.
// Called by the reader, which has already charged the connection window.
func (b *bodyQueue) receive(used int32, data []byte, last bool) error {
	b.mu.Lock()
	if used > b.window+b.grace {
		window := b.window
		b.mu.Unlock()
		// §6.9.1, as for a stream assembled whole (see h2Server.data).
		return streamFail(uint32(b.id), errFlowControlError, //nolint:gosec // G115: an HTTP/2 stream identifier
			"%w: %w: %d bytes on stream %d against a window of %d",
			ErrH2Protocol, errFlowControl, used, b.id, window)
	}
	b.window -= used
	b.addLocked(data)
	// Padding is spent window that is never held: it is owed back now, or
	// a body made mostly of padding would stall with nothing to consume.
	inc := int32(0)
	if !last {
		inc = b.grantLocked()
	}
	b.mu.Unlock()
	b.signal()
	return b.sendGrant(inc)
}

// add queues one HTTP/3 DATA frame's payload, or the part of it a packet
// carried. Called by the assembler, which has already given the connection
// level its credit; the stream level's comes back as the handler consumes,
// or now for bytes nobody will read.
func (b *bodyQueue) add(data []byte) {
	b.mu.Lock()
	dropped := b.addLocked(data)
	b.mu.Unlock()
	b.signal()
	b.giveBack(dropped)
}

// addLocked appends data to the ring and reports the bytes it dropped
// instead: all of them for a body already failed or abandoned, and those
// that take it past its bound, which fails it with ErrTooLarge — the handler
// reads that and answers, and the stream is stopped after the answer, as any
// body left unread is.
//
// Bytes dropped because the handler returned are still counted in total:
// the count serves end's framing check (RFC 9113 §8.1.1, RFC 9114 §4.1.2),
// not retention, and a client that has not yet seen the RST_STREAM(NO_ERROR)
// sent after the response finishes its body as §8.1 expects it to. Counted
// short, a well-formed request was found malformed at its END_STREAM and
// reset PROTOCOL_ERROR, its response lost if it was waiting for credit.
// Bytes dropped by a failure are not counted: end compares nothing then.
func (b *bodyQueue) addLocked(data []byte) (dropped int) {
	if b.overLocked() {
		if b.err == nil {
			b.total += len(data)
		}
		return len(data)
	}
	if b.total+len(data) > b.limit {
		dropped = b.n + len(data)
		b.failLocked(fmt.Errorf("%w: a streamed body over %d bytes", ErrTooLarge, b.limit))
		return dropped
	}
	b.push(data)
	b.total += len(data)
	return 0
}

// push copies data into the ring, growing it when the held bytes and data
// do not fit. The piece the handler holds may sit in the old array: a grown
// ring is a new one, and the old one stays as it is for as long as the
// handler's slice keeps it.
func (b *bodyQueue) push(data []byte) {
	if len(data) == 0 {
		return
	}
	if b.n+len(data) > len(b.buf) {
		size := max(len(b.buf), bodyMinRing)
		for size < b.n+len(data) {
			size *= 2
		}
		grown := make([]byte, size)
		k := copy(grown, b.buf[b.head:min(b.head+b.n, len(b.buf))])
		copy(grown[k:], b.buf[:b.n-k])
		b.buf, b.head = grown, 0
	}
	tail := (b.head + b.n) % len(b.buf)
	k := copy(b.buf[tail:], data)
	copy(b.buf, data[k:])
	b.n += len(data)
}

// grantLocked returns the HTTP/2 window increment owed to the peer, and
// counts it as granted, once it reaches half the initial window: the bytes
// the window and the held bytes leave short of it. Nothing is owed to a body
// that has ended, failed or been abandoned — the stream is closing — nor
// over HTTP/3, whose transport keeps the window.
func (b *bodyQueue) grantLocked() int32 {
	if b.h2 == nil || b.ended || b.overLocked() {
		return 0
	}
	owed := b.initial - int32(b.n) - b.window //nolint:gosec // G115: n is bounded by the window
	if owed < (b.initial+1)/2 {
		return 0
	}
	b.window += owed
	return owed
}

// overLocked reports a body nobody will read past this point: it failed, or
// its handler returned. The frame that finds it so is no fault of the peer's.
func (b *bodyQueue) overLocked() bool {
	return b.err != nil || b.closed
}

func (b *bodyQueue) sendGrant(inc int32) error {
	if inc <= 0 {
		return nil
	}
	return b.h2.w.control32(frameWindowUpdate, uint32(b.id), uint32(inc)) //nolint:gosec // G115: an HTTP/2 stream identifier, a positive increment
}

// giveBack returns n consumed or dropped bytes' stream-level credit to the
// QUIC transport; over HTTP/2 the window arithmetic does instead.
func (b *bodyQueue) giveBack(n int) {
	if n > 0 && b.release != nil {
		b.release(n)
	}
}

func (b *bodyQueue) signal() {
	select {
	case b.wake <- struct{}{}:
	default:
	}
}

// end marks the body's end: what is held is all there is. It returns the
// content-length mismatch that makes the request malformed (RFC 9113
// §8.1.1, RFC 9114 §4.1.2), when the head stated one. Called by the reader.
func (b *bodyQueue) end() error {
	b.mu.Lock()
	if b.declared >= 0 && b.declared != b.total && b.err == nil {
		err := fmt.Errorf("%w: content-length %d against a body of %d bytes", ErrMalformed, b.declared, b.total)
		dropped := b.n
		b.failLocked(err)
		b.mu.Unlock()
		b.signal()
		b.giveBack(dropped)
		return err
	}
	b.ended = true
	b.mu.Unlock()
	b.signal()
	return nil
}

// fail ends the body with err, unless it has ended or failed already.
func (b *bodyQueue) fail(err error) {
	b.mu.Lock()
	if b.ended || b.err != nil {
		b.mu.Unlock()
		return
	}
	dropped := b.n
	b.failLocked(err)
	b.mu.Unlock()
	b.signal()
	b.giveBack(dropped)
}

func (b *bodyQueue) failLocked(err error) {
	if b.err == nil {
		b.err = err
	}
	b.dropLocked()
}

// dropLocked lets go of the held bytes: the handler will not read them. The
// caller gives their credit back once mu is released.
func (b *bodyQueue) dropLocked() {
	b.buf, b.head, b.n, b.viewed = nil, 0, 0, 0
}

// close is the handler's return: the body is over for it, read or not, and
// what is held is let go. Called by the handler's call.
func (b *bodyQueue) close() {
	b.mu.Lock()
	b.closed = true
	dropped, ended := b.n, b.ended
	b.dropLocked()
	b.mu.Unlock()
	b.giveBack(dropped)
	if !ended && b.onClose != nil {
		b.onClose()
	}
}

// unread reports whether the client may still be sending the body: it had
// not ended, cleanly, by the time this is asked. Such a stream is stopped
// after its response (RFC 9113 §8.1, RFC 9114 §4.1).
func (b *bodyQueue) unread() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return !b.ended
}

// isClosed reports whether the handler has returned. Called by the reader,
// which then forgets the stream: nobody reads it any more.
func (b *bodyQueue) isClosed() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.closed
}

// next returns the next piece of the body, nil once it has ended, or the
// error that ended it. The piece is valid until the next call. Called by
// the handler.
func (b *bodyQueue) next() ([]byte, error) {
	b.mu.Lock()
	freed := b.viewed
	if b.viewed > 0 {
		b.head = (b.head + b.viewed) % len(b.buf)
		b.n -= b.viewed
		b.viewed = 0
		if b.n == 0 {
			b.head = 0
		}
	}
	inc := b.grantLocked()
	for {
		switch {
		case b.closed:
			b.mu.Unlock()
			b.giveBack(freed)
			return nil, errBodyAfterReturn
		case b.err != nil:
			err := b.err
			b.mu.Unlock()
			b.giveBack(freed)
			return nil, err
		case b.n > 0:
			k := min(b.n, len(b.buf)-b.head)
			piece := b.buf[b.head : b.head+k : b.head+k]
			b.viewed = k
			b.mu.Unlock()
			b.giveBack(freed)
			return piece, b.sendGrant(inc)
		case b.ended:
			b.mu.Unlock()
			b.giveBack(freed)
			return nil, nil
		}
		// Nothing held: the handler waits for the peer. The 100 a client
		// may be waiting for goes out at this first wait, so a handler that
		// answers without reading never invites the body.
		continue100 := b.expect && !b.sent100
		b.sent100 = b.sent100 || continue100
		b.mu.Unlock()
		b.giveBack(freed)
		freed = 0
		if err := b.sendGrant(inc); err != nil {
			return nil, err
		}
		inc = 0
		if continue100 && b.interim != nil {
			if err := b.interim(); err != nil {
				return nil, err
			}
		}
		if !b.wait() {
			b.mu.Lock()
			if b.n == 0 && !b.ended && b.err == nil && !b.closed {
				b.err = fmt.Errorf("%w: no body bytes on stream %d within the read timeout %v",
					os.ErrDeadlineExceeded, b.id, b.timeout)
			}
			continue
		}
		b.mu.Lock()
	}
}

// wait blocks until the reader signals or ReadTimeout passes, and reports
// which.
func (b *bodyQueue) wait() bool {
	if b.timer == nil {
		b.timer = time.NewTimer(b.timeout)
	} else {
		b.timer.Reset(b.timeout)
	}
	select {
	case <-b.wake:
		b.timer.Stop()
		return true
	case <-b.timer.C:
		return false
	}
}
