package httpstream

import (
	"fmt"
	"os"
	"sync"
	"time"
)

// h2Body is an HTTP/2 request body that streams (docs/design/adr/0009,
// article 4): the reader queues each DATA frame's bytes as it arrives, the
// handler takes them as it pulls, and the stream's window goes back to the
// peer as the handler consumes — not as the bytes arrive, which is what
// makes the window, rather than this end's memory, the backpressure.
//
// The bytes sit in a ring. What the peer may send is bounded by the window
// this end granted, and a grant never takes window plus held bytes past the
// stream's initial window — nothing is granted on arrival while bodies
// stream (see data) — so the ring never needs more than that, plus the
// overshoot a stream opened before the SETTINGS acknowledgement may carry.
// That is the bound on what a streamed body holds, and MaxConcurrentStreams
// times it the bound across a connection, in place of the aggregate bound
// the bodies assembled whole are under. The ring starts small and doubles
// toward the window, so a short upload does not pay for all of it.
//
// What the handler holds — the piece the last pull returned — is released
// at the next pull, and only then counts as consumed: until then it is
// still memory this end holds, and its window stays spent.
//
// It is shared by two goroutines, the reader and the handler's call, and
// every field below mu is read and written under it. The writer is called
// with mu released: its control queue may block, and the reader, which
// takes mu on every DATA frame of this stream, must not wait behind it.
type h2Body struct {
	s       *h2Server
	id      uint32
	initial int32 // the stream window this end grants, InitialWindowSize
	limit   int   // the most a body may carry, whole or streamed
	timeout time.Duration

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
	window   int32 // what the peer may still send on this stream
	grace    int32 // the overshoot tolerated above window, see h2Stream.windowGrace
	total    int   // bytes received
	declared int   // the content-length the head stated, or -1

	expect  bool // the client waits for 100 Continue before it sends the body
	sent100 bool
	ended   bool  // END_STREAM arrived: what is held is all there is
	closed  bool  // the handler returned
	err     error // the body failed: what the handler reads next
}

// h2BodyMinRing is the ring a streamed body starts with when nothing is
// buffered yet: one default frame.
const h2BodyMinRing = defaultMaxFrameSize

// errBodyReset reports a streamed body the client reset before its end.
var errBodyReset = fmt.Errorf("%w: the client reset the stream mid-body", ErrMalformed)

// errBodyConnClosed reports a streamed body whose connection ended first.
var errBodyConnClosed = fmt.Errorf("%w: the connection closed mid-body", ErrMalformed)

// newH2Body turns st, whose head is complete and whose body has not ended,
// into a streamed body, with what st has buffered so far as its first bytes.
// Called by the reader.
func (s *h2Server) newH2Body(st *h2Stream) *h2Body {
	b := &h2Body{
		s: s, id: st.id,
		initial:  int32(s.cfg.InitialWindowSize), //nolint:gosec // G115: capped by resolve
		limit:    max(s.cfg.MaxBodyBytes, s.cfg.MaxStreamedBodyBytes),
		timeout:  s.cfg.ReadTimeout,
		wake:     make(chan struct{}, 1),
		window:   st.recvWind,
		grace:    st.windowGrace,
		total:    len(st.body),
		declared: st.declared,
		expect:   expectsContinue(st.req),
	}
	if len(st.body) > 0 {
		b.buf = append(make([]byte, 0, max(len(st.body), h2BodyMinRing)), st.body...)
		b.buf = b.buf[:cap(b.buf)]
		b.n = len(st.body)
		s.assembling -= len(st.body)
		st.body = nil
	}
	if b.total > b.limit {
		b.failLocked(fmt.Errorf("%w: a streamed body over %d bytes", ErrTooLarge, b.limit))
	}
	return b
}

// receive queues one DATA frame's bytes: used is the frame's payload as the
// peer debited it, data the body bytes once its padding is cut. Called by
// the reader, which has already charged the connection window. A body
// already failed or abandoned takes nothing more — the bytes are dropped,
// and the window they spent is never granted back.
func (b *h2Body) receive(used int32, data []byte, last bool) error {
	b.mu.Lock()
	if used > b.window+b.grace {
		window := b.window
		b.mu.Unlock()
		// §6.9.1, as for a stream assembled whole (see data).
		return streamFail(b.id, errFlowControlError,
			"%w: %w: %d bytes on stream %d against a window of %d",
			ErrH2Protocol, errFlowControl, used, b.id, window)
	}
	b.window -= used
	if b.overLocked() {
		b.mu.Unlock()
		return nil
	}
	if b.total+len(data) > b.limit {
		// The handler reads ErrTooLarge and answers; the stream is reset
		// after the answer, as any body left unread is.
		b.failLocked(fmt.Errorf("%w: a streamed body over %d bytes", ErrTooLarge, b.limit))
		b.mu.Unlock()
		b.signal()
		return nil
	}
	b.push(data)
	b.total += len(data)
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

// push copies data into the ring, growing it when the held bytes and data
// do not fit. The piece the handler holds may sit in the old array: a grown
// ring is a new one, and the old one stays as it is for as long as the
// handler's slice keeps it.
func (b *h2Body) push(data []byte) {
	if len(data) == 0 {
		return
	}
	if b.n+len(data) > len(b.buf) {
		size := max(len(b.buf), h2BodyMinRing)
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

// grantLocked returns the increment owed to the peer, and counts it as
// granted, once it reaches half the initial window: the bytes the window
// and the held bytes leave short of it. Nothing is owed to a body that has
// ended, failed or been abandoned — the stream is closing.
func (b *h2Body) grantLocked() int32 {
	if b.ended || b.overLocked() {
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
func (b *h2Body) overLocked() bool {
	return b.err != nil || b.closed
}

func (b *h2Body) sendGrant(inc int32) error {
	if inc <= 0 {
		return nil
	}
	return b.s.w.control32(frameWindowUpdate, b.id, uint32(inc)) //nolint:gosec // G115: positive
}

func (b *h2Body) signal() {
	select {
	case b.wake <- struct{}{}:
	default:
	}
}

// end marks END_STREAM: what is held is all there is. It returns the
// content-length mismatch §8.1.1 makes the request malformed for, when the
// head stated one. Called by the reader.
func (b *h2Body) end() error {
	b.mu.Lock()
	defer b.signal()
	defer b.mu.Unlock()
	if b.declared >= 0 && b.declared != b.total && b.err == nil {
		err := fmt.Errorf("%w: content-length %d against a body of %d bytes", ErrH2Protocol, b.declared, b.total)
		b.failLocked(err)
		return err
	}
	b.ended = true
	return nil
}

// fail ends the body with err, unless it has ended or failed already.
func (b *h2Body) fail(err error) {
	b.mu.Lock()
	if b.ended {
		b.mu.Unlock()
		return
	}
	b.failLocked(err)
	b.mu.Unlock()
	b.signal()
}

func (b *h2Body) failLocked(err error) {
	if b.err == nil {
		b.err = err
	}
	b.dropLocked()
}

// dropLocked lets go of the held bytes: the handler will not read them.
func (b *h2Body) dropLocked() {
	b.buf, b.head, b.n, b.viewed = nil, 0, 0, 0
}

// close is the handler's return: the body is over for it, read or not, and
// what is held is let go. Called by the handler's call.
func (b *h2Body) close() {
	b.mu.Lock()
	b.closed = true
	b.dropLocked()
	b.mu.Unlock()
}

// unread reports whether the client may still be sending the body: it had
// not ended, cleanly, by the time this is asked. Such a stream is reset
// after its response (RFC 9113 §8.1).
func (b *h2Body) unread() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return !b.ended
}

// isClosed reports whether the handler has returned. Called by the reader,
// which then forgets the stream: nobody reads it any more.
func (b *h2Body) isClosed() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.closed
}

// next returns the next piece of the body, nil once it has ended, or the
// error that ended it. The piece is valid until the next call. Called by
// the handler.
func (b *h2Body) next() ([]byte, error) {
	b.mu.Lock()
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
			return nil, errBodyAfterReturn
		case b.err != nil:
			err := b.err
			b.mu.Unlock()
			return nil, err
		case b.n > 0:
			k := min(b.n, len(b.buf)-b.head)
			piece := b.buf[b.head : b.head+k : b.head+k]
			b.viewed = k
			b.mu.Unlock()
			return piece, b.sendGrant(inc)
		case b.ended:
			b.mu.Unlock()
			return nil, nil
		}
		// Nothing held: the handler waits for the peer. The 100 a client
		// may be waiting for goes out at this first wait, so a handler that
		// answers without reading never invites the body.
		continue100 := b.expect && !b.sent100
		b.sent100 = b.sent100 || continue100
		b.mu.Unlock()
		if err := b.sendGrant(inc); err != nil {
			return nil, err
		}
		inc = 0
		if continue100 {
			if err := b.s.w.interim(b.id, 100); err != nil {
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
func (b *h2Body) wait() bool {
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
