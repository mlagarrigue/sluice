package httpstream

import (
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/mlagarrigue/sluice"
)

// Requests reads a connection as a stream of request batches.
//
// A batch is what arrived together: one read of the socket, parsed into as
// many complete requests as it held. A client that waits for each answer
// produces batches of one; a client that pipelines produces the batches this
// package exists to show. Nothing is ever waited for to fill a batch — the
// latency a gateway spends deliberately (see [gateway.Config.Within]) is not
// spent here, because the requests are already on the wire or they are not.
//
// Every field of every [Request] borrows the connection's read buffer, which
// is refilled after the batch is yielded. Retaining one requires a copy, the
// same rule as every other batch in this library.
//
// A client's Expect: 100-continue is answered here too, with the interim
// response [write100Continue] sends, before the body that follows is read —
// not something the caller asks for or sees.
//
// The stream ends when the client closes, when [Config.MaxRequestsPerConn] is
// reached, when a request says Connection: close, or when a deadline expires.
// A parse failure ends it too and is reported by [sluice.Source.Err]; the
// caller answers with a status and closes, since a stream whose framing is in
// doubt cannot be resynchronised.
func Requests(c net.Conn, cfg Config) sluice.Source[Request] {
	cfg = cfg.withDefaults()
	r := &reader{conn: c, cfg: cfg}
	return sluice.NewSource(sluice.Stream[Request](r.stream), func() error { return r.err })
}

// maxBuffer is the most one connection may hold at once: a single request at
// its ceiling. It is also what bounds a batch — requests are batched when
// they fit in one buffer together, so a workload of small JSON calls batches
// well and one of megabyte uploads does not batch at all, which is the honest
// behaviour rather than a configurable that would let a client choose how
// much memory to occupy.
func (c Config) maxBuffer() int {
	return c.MaxRequestLineBytes + c.MaxHeaderBytes + c.MaxBodyBytes
}

type reader struct {
	conn   net.Conn
	cfg    Config
	buf    []byte
	n      int // bytes held in buf
	served int
	err    error

	// deadline is the current request's absolute read deadline, held fixed
	// once set rather than re-armed on every read. A sliding per-read
	// deadline is not a timeout on the request at all: a client sending one
	// byte every few seconds keeps re-arming it and holds the connection
	// forever, which is the slowloris attack the sliding version claims to
	// answer and does not. Zero means unset; it is set the first time a read
	// is needed for a request that has not fully arrived yet, and cleared
	// once that request's bytes are consumed.
	deadline time.Time

	// sent100 reports whether this pending request already got its interim
	// 100 Continue, so a client whose body keeps this connection in fill's
	// loop for several reads is not sent the line more than once.
	sent100 bool

	// batch, headers and bounds are reused across batches, like every other
	// accumulator in this library. headers is one flat arena for the whole
	// batch — the requests in a batch coexist, so each needs its own header
	// storage, and a slice per request would allocate per request. It is
	// [postgres.Rows] applied to a different wire protocol.
	batch   []Request
	headers []Header
	bounds  [][2]int // each request's span in headers

	// pending is what the last batch consumed, compacted at the start of the
	// next round rather than before that batch was yielded: its elements
	// borrow the buffer, and shifting the tail down over them while the
	// consumer still holds them is the aliasing bug the batch contract warns
	// about, arriving from inside the operator instead of from a caller.
	pending int

	// progress is how far the chunked body of the request still arriving
	// has been validated, so each read resumes the walk rather than
	// restarting it — see chunkProgress. It indexes buf, and consume keeps
	// it pointed at the same request as the buffer shifts under it.
	progress chunkProgress
}

func (r *reader) stream(yield func(sluice.Batch[Request]) bool) {
	for {
		batch, ok := r.next()
		if !ok {
			return
		}
		if !yield(sluice.Batch[Request]{Items: batch}) {
			return
		}
		if !r.keepAlive(batch) {
			return
		}
	}
}

// keepAlive reports whether the connection may serve another batch. The last
// request of the batch decides, since responses go back in order and anything
// after a Connection: close would have nobody to read it.
func (r *reader) keepAlive(batch []Request) bool {
	if len(batch) == 0 || !batch[len(batch)-1].KeepAlive {
		return false
	}
	return r.served < r.cfg.MaxRequestsPerConn
}

// next fills the buffer and parses everything complete in it.
func (r *reader) next() ([]Request, bool) {
	for {
		if r.pending > 0 {
			r.consume(r.pending)
			r.pending = 0
			// The previous request (or batch) is fully off the buffer, so
			// whatever comes next starts its own clock rather than inheriting
			// what was left of the last one.
			r.deadline = time.Time{}
			r.sent100 = false
		}
		// Parse first: the previous read may have left whole requests behind
		// the one it completed, which is what pipelining looks like from here.
		batch, used, err := r.parseBuffered()
		if err != nil {
			r.err = err
			return nil, false
		}
		if len(batch) > 0 {
			r.pending = used
			r.served += len(batch)
			return batch, true
		}
		if err := r.fill(); err != nil {
			r.err = err
			return nil, false
		}
	}
}

// parseBuffered takes as many complete requests as the buffer holds, stopping
// at the batch bound.
func (r *reader) parseBuffered() (batch []Request, used int, err error) {
	r.batch, r.headers, r.bounds = r.batch[:0], r.headers[:0], r.bounds[:0]
	// A batch may not serve more than the connection has left, and it may
	// not carry a request beyond one that already said Connection: close —
	// both bounds are on how many requests this call may take, not just how
	// many fit in the buffer.
	limit := min(r.cfg.MaxRequestsPerBatch, r.cfg.MaxRequestsPerConn-r.served)
	for len(r.batch) < limit {
		start := len(r.headers)
		if r.progress.at != used {
			// Saved for a request that is not this one — it cannot still be
			// pending, since the buffer only moves on by whole requests.
			r.progress = chunkProgress{}
		}
		req, n, arena, perr := parseRequest(r.buf[used:r.n], r.cfg, r.headers, &r.progress)
		r.headers = arena
		if errors.Is(perr, errIncomplete) {
			r.progress.at = used
		}
		if errors.Is(perr, errExpectContinue) {
			// Only when no earlier request of this pass is waiting for its
			// final response: an interim 100 written now would reach the
			// client *before* those finals, out of the order RFC 9112 §9.3
			// promises. Breaking with what is already parsed lets the caller
			// answer it; the next pass re-parses this request from the front
			// of the buffer with the batch empty, and sends the 100 then.
			if len(r.batch) > 0 {
				break
			}
			if !r.sent100 {
				if werr := r.write100Continue(); werr != nil {
					return nil, 0, werr
				}
				r.sent100 = true
			}
			break
		}
		if errors.Is(perr, errIncomplete) {
			break
		}
		if perr != nil {
			return nil, 0, perr
		}
		r.bounds = append(r.bounds, [2]int{start, len(r.headers)})
		r.batch = append(r.batch, req)
		used += n
		if !req.KeepAlive {
			// The client said this is the last request on the connection;
			// anything buffered behind it would get an answer nobody reads.
			break
		}
		if req.http10 {
			// An HTTP/1.0 response that is streamed has no chunked coding to
			// end it, so its body runs to the connection's close (see
			// writeStreamedBatch) and nothing can follow it. Whether it is streamed
			// is the handler's choice, made after this batch is cut — so the
			// batch is cut here, rather than hand the handler requests whose
			// answers would be dropped after it acted on them. Pipelining
			// HTTP/1.0 keep-alive clients get batches of one, which is all
			// that is lost.
			break
		}
		if len(r.batch) == limit && r.served+len(r.batch) >= r.cfg.MaxRequestsPerConn {
			// The connection's own request budget ended this batch, whether
			// or not it also happened to fill it: tell the client so it does
			// not pipeline into a socket that is about to close. Checking
			// limit < MaxRequestsPerBatch here would miss exactly the batches
			// that run full — the common case whenever MaxRequestsPerConn is
			// a multiple of MaxRequestsPerBatch, which the defaults are.
			r.batch[len(r.batch)-1].KeepAlive = false
			break
		}
	}
	// The header views are taken now, not during the loop: appending to the
	// arena may move its backing array, so a sub-slice handed out earlier
	// would point at the array the next request outgrew.
	for i := range r.batch {
		r.batch[i].Headers = r.headers[r.bounds[i][0]:r.bounds[i][1]]
	}
	return r.batch, used, nil
}

// fill reads once, growing the buffer toward the ceiling but never past it.
func (r *reader) fill() error {
	limit := r.cfg.maxBuffer()
	if r.n >= limit {
		// The buffer is full and nothing in it parsed, so the request at its
		// front is larger than the bounds allow.
		return fmt.Errorf("%w: a request fills the %d-byte buffer without completing", ErrTooLarge, limit)
	}
	if len(r.buf) == r.n {
		grown := min(max(2*len(r.buf), 4<<10), limit)
		next := make([]byte, grown)
		copy(next, r.buf[:r.n])
		r.buf = next
	}

	// Idle while nothing has arrived: that deadline is free to slide, since
	// nothing is owed to a client that has not spoken yet. Once part of a
	// request has arrived, the deadline is fixed the first time it is needed
	// and held there — see the field comment on deadline — so a request has
	// exactly ReadTimeout to arrive in full, not ReadTimeout since whenever
	// it last sent a byte.
	idle := r.n == 0
	deadline := time.Now().Add(r.cfg.IdleTimeout)
	if !idle {
		if r.deadline.IsZero() {
			r.deadline = time.Now().Add(r.cfg.ReadTimeout)
		}
		deadline = r.deadline
	}
	if err := r.conn.SetReadDeadline(deadline); err != nil {
		return err
	}

	n, err := r.conn.Read(r.buf[r.n:])
	r.n += n
	if err != nil {
		if errors.Is(err, io.EOF) {
			if r.n == 0 {
				return io.EOF // a clean close between requests
			}
			return fmt.Errorf("%w: the connection closed mid-request", ErrMalformed)
		}
		if idle && isTimeout(err) {
			// Nobody is owed an answer: no request had started, so there is
			// nothing to say 408 about — see statusFor in serve.go.
			return fmt.Errorf("%w: %w", errIdleTimeout, err)
		}
		return err
	}
	return nil
}

// write100Continue answers a conformant client's Expect: 100-continue: it is
// waiting for this before it sends the body, and this package is waiting for
// the body — both sides blocking would be a deadlock broken only by the read
// deadline, well after RFC 9110 §10.1.1 says to send the interim response.
func (r *reader) write100Continue() error {
	if err := r.conn.SetWriteDeadline(time.Now().Add(r.cfg.WriteTimeout)); err != nil {
		return err
	}
	_, err := r.conn.Write([]byte("HTTP/1.1 100 Continue\r\n\r\n"))
	return err
}

// consume drops what was parsed, keeping the tail that has not been.
func (r *reader) consume(used int) {
	copy(r.buf, r.buf[used:r.n])
	r.n -= used
	// The pending request's saved walk indexes the buffer; it shifted down
	// with everything else. It cannot sit inside what was consumed — that
	// was whole requests, and a whole request clears its progress.
	if r.progress.at < used {
		r.progress = chunkProgress{}
	} else {
		r.progress.at -= used
	}
}
