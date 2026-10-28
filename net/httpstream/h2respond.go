package httpstream

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/mlagarrigue/sluice"
)

// h2Applied is a batch the handler has answered, on its way back to the
// responder to be framed.
type h2Applied struct {
	b   h2Batch
	res []Response
	err error
}

// call runs the handler over one batch and sends what it returned to the
// responder. The reader starts it on a goroutine of its own, so that a batch
// that arrives while the handler is busy with another is not held behind it. A handler
// that waits on something per call — a gateway's window, a round trip —
// otherwise paid that wait once per batch, in sequence: a stream that
// arrived during a call waited for that call, then for its own, where
// HTTP/1.1 carries the same request on a connection of its own and waits
// once. Measured on the comparison study's unit scenario (a 2 ms gateway
// window, 8,000 requests per second, 256 and 1,024 connections), the HTTP/2
// p99 went from 1.35-1.63 times the HTTP/1.1 figure to 0.96-1.06, and the
// p50 from 1.12-1.22 to 0.96-1.00.
//
// The calls in flight on a connection are bounded by
// [Config.MaxConcurrentStreams], since each holds a stream slot; the send on
// out never blocks. Starting the goroutine costs 2.3% of a request answered
// alone, about half a microsecond on one core (BenchmarkH2FromNetHTTP/plain);
// reusing call goroutines is the known way down if that ever matters more
// than the p99 above.
func (s *h2Server) call(b h2Batch, out chan<- *h2Applied) {
	answered := false
	defer func() {
		if !answered {
			// runtime.Goexit in the handler (a t.FailNow, say) runs the
			// deferred calls and ends this goroutine without a result;
			// apply's recover sees no panic. Left unanswered, the batch
			// keeps inFlight up for good: the connection never goes idle
			// and its stream slots never come back.
			closeBodies(b.reqs)
			out <- &h2Applied{b: b, err: errors.New("httpstream: the handler exited its goroutine without answering")}
		}
	}()
	s.peer.stamp(b.reqs)
	res, err := apply(b.ctx, s.handler, sluice.Batch[Request]{Items: b.reqs}, s.cfg.EarlyData)
	closeBodies(b.reqs)
	if err != nil {
		reportFailure(s.cfg.logger(), err)
	}
	answered = true
	out <- &h2Applied{b: b, res: res, err: err}
}

// closeBodies ends the streamed bodies of a batch whose handler has
// returned: they are valid during the call and no longer.
func closeBodies(reqs []Request) {
	for i := range reqs {
		if body := reqs[i].body; body != nil {
			body.q.close()
		}
	}
}

// answer hands what the handler returned for one batch to the writer. It is
// the responder goroutine.
//
// The heads of a batch are framed into one buffer and queued together, so the
// writer can interleave their bodies rather than serve them in order — a
// large response no longer holds a small one behind it. The buffer is the
// batch's own, an h2Sent: deliver returns once it is queued, so the next
// batch is answered while this one waits for credit, and the writer hands
// the buffer back once it has finished with every byte of it.
//
// Every request of b is answered once: by the writer, for a buffered answer,
// when its job leaves its hands; here for the others — a stream reset
// because nothing could be framed, a streamed body once it has ended.
func (s *h2Server) answer(got *h2Applied) {
	b, res, err := got.b, got.res, got.err
	if b.ctx != nil {
		defer s.calls.release(b.ctx, b.ids)
	}
	if err != nil {
		// The handler panicked, or answered a number of requests that does
		// not match what it was given. Neither is the peer's doing and
		// neither is a reason to lose the other streams on this connection:
		// each request of the batch is answered 500 and the connection
		// carries on. This was a GOAWAY before the write side was split off,
		// so one handler bug cost every request in flight.
		res = make([]Response, len(b.reqs))
		for i := range res {
			res[i] = transportError(500)
		}
	}

	// Header blocks are framed at the protocol's floor, not at the peer's
	// SETTINGS_MAX_FRAME_SIZE. The heads are framed here, on the responder,
	// and written later by the writer; a peer that lowers the setting in
	// between — it may, down to this floor — would receive HEADERS over its
	// new limit and end the connection with FRAME_SIZE_ERROR. DATA reads the
	// live value at write time, so only headers needed this. A block over
	// 16384 bytes is rare, and costs one CONTINUATION more when it happens.
	const maxFrame = defaultMaxFrameSize
	s.date = dateFor(time.Now())
	sent := s.w.sent()
	s.answers = s.answers[:0]
	for i, r := range res {
		method := b.reqs[i].Method
		status := r.Status
		if status == 0 {
			status = 200
		}
		_, noBody := noLengthAndBody(status, method)
		body, stream := r.Body, r.Stream
		if noBody {
			// A HEAD or 204/304 response states nothing it shouldn't, and
			// sends nothing it shouldn't either: the producer is never
			// pulled, the same way the HTTP/1.1 path never writes res.Body
			// for these.
			body, stream = nil, nil
		}
		a := h2Answer{id: b.ids[i], from: len(sent.out), body: body, stream: stream, job: -1}
		if rb := b.reqs[i].body; rb != nil {
			// A body the client may still be sending: RST_STREAM(NO_ERROR)
			// after the response tells it to stop (RFC 9113 §8.1).
			a.unread = rb.q.unread()
		}
		if stream != nil {
			a.length, a.trailers = r.ContentLength, r.Trailers
		}
		out, err := s.appendHead(sent.out, a.id, method, r, maxFrame)
		mid := len(out)
		if err == nil && r.Trailers != nil && !noBody && !r.streamed() {
			// A body in hand has its trailers framed now, behind its head:
			// a refusal is still a 500 rather than a reset.
			out, err = s.appendTrailers(out, a.id, r, maxFrame)
		}
		if err != nil {
			// A response this package will not put on the wire — an injected
			// header, a status no version of HTTP carries, a body and a
			// stream at once. The caller's bug, answered as one, on its own
			// stream and nobody else's.
			sent.out = sent.out[:a.from]
			a.body, a.stream = nil, nil
			a.trailers = nil
			r = transportError(500)
			if out, err = s.appendHead(sent.out, a.id, method, r, maxFrame); err != nil {
				// The peer's SETTINGS_MAX_HEADER_LIST_SIZE is under the
				// line's own two fields: the status alone, which fits
				// wherever any answer does.
				r = Response{Status: 500}
				out, err = s.appendHead(sent.out, a.id, method, r, maxFrame)
			}
			if err != nil {
				_ = s.w.reset(a.id, errInternalError)
				s.w.forget(a.id)
				a.skip = true
				s.answers = append(s.answers, a)
				continue
			}
			if !noBody {
				a.body = r.Body
			}
			mid = len(out)
		}
		sent.out = out
		a.mid, a.to = mid, len(sent.out)
		s.answers = append(s.answers, a)
	}

	// Two passes, because appending to sent.out may move it: the heads are
	// sliced only once the buffer has stopped growing.
	for i := range s.answers {
		a := &s.answers[i]
		if a.skip {
			continue
		}
		a.job = len(sent.jobs)
		var tail []byte
		if a.mid < a.to {
			tail = sent.out[a.mid:a.to]
		}
		sent.jobs = append(sent.jobs, h2Job{
			id:   a.id,
			head: sent.out[a.from:a.mid],
			data: a.body,
			tail: tail,
			end:  len(a.body) > 0 || tail != nil,
			// A response with neither a body nor a stream carries END_STREAM
			// on its head, so it closes the stream without a DATA frame.
			closes:     a.stream == nil,
			resetAfter: a.unread,
		})
	}
	// The jobs are read through sent only until deliver: from there the
	// writer may finish them all and hand sent to the next batch. What a
	// streamed answer needs of its head job is kept aside first.
	heads := s.heads[:0]
	for i := range s.answers {
		if a := &s.answers[i]; a.stream != nil && a.job >= 0 {
			heads = append(heads, &sent.jobs[a.job])
		}
	}
	s.heads = heads
	failed := s.w.deliver(sent) != nil

	// The streamed ones follow, one at a time, each once its head is out —
	// its DATA may not overtake its HEADERS. Each is pulled here, on this
	// goroutine, so a producer never runs ahead of the socket and what is
	// held is one batch rather than the whole answer — and, since the writer
	// waits for credit instead of refusing, a streamed response may be any
	// size the peer is willing to receive.
	k := 0
	for i := range s.answers {
		a := &s.answers[i]
		switch {
		case a.skip:
			s.answered()
		case a.stream != nil:
			if !failed && s.w.await(heads[k]) == nil {
				s.streamBody(a.id, a.stream, a.length, a.trailers, a.unread)
			}
			k++
			s.answered()
		}
	}
}

// answered counts one request as answered: its stream no longer counts
// against [Config.MaxConcurrentStreams] on this end's side, and the
// responder rechecks whether a drained connection is done.
func (s *h2Server) answered() {
	s.inFlight.Add(-1)
	select {
	case s.settled <- struct{}{}:
	default:
	}
}

// appendHead frames a response's HEADERS, and its CONTINUATION frames if the
// block does not fit one. s.date is the batch's Date value, set by answer.
func (s *h2Server) appendHead(dst []byte, id uint32, method []byte, r Response, maxFrame int) ([]byte, error) {
	if err := r.check(); err != nil {
		return nil, err
	}
	status := r.Status
	if status == 0 {
		status = 200
	}
	if err := checkStatus(status); err != nil {
		return nil, err
	}
	noLength, noBody := noLengthAndBody(status, method)
	block := appendHPACKStatus(s.block[:0], status)
	// listSize is RFC 9113 §6.5.2's measure — names and values uncompressed,
	// plus 32 per field — of what this block decodes to at the peer.
	listSize := uint64(len(":status") + 3 + 32)
	dated, err := datedBy(r.Headers)
	if err != nil {
		return nil, err
	}
	if !dated {
		// RFC 9110 §6.6.1's Date, as the h1 writer adds it and where: on
		// every final response the handler did not date itself, first after
		// :status. The static table names the field (entry 33); the value
		// is the batch's, formatted once.
		block = appendHPACKInt(block, hpackDateIndex, 4, 0x00)
		block = appendHPACKString(block, s.date)
		listSize += uint64(len("date")+len(s.date)) + 32
	}
	s.names = s.names[:0]
	for _, h := range r.Headers {
		// h.Name and h.Value stay []byte into appendHPACK: a string binding
		// here allocates twice per response field on the encode hot path.
		// A name the handler spelled in HTTP/1.1 case goes on the wire
		// lowercase (RFC 9113 §8.2.1), from a copy: the handler's bytes are
		// borrowed, never written. lowerName is one pass over the name,
		// which is what the refusal it replaces cost.
		name := lowerName(h.Name, &s.names)
		// The h1 writer's octet rule: HPACK carries any byte, and a name
		// with a space or a colon is a second field at the first hop that
		// re-serialises this response toward HTTP/1.1.
		if !isToken(name) {
			return nil, fmt.Errorf("%w: response header %q is not a token", ErrH2Protocol, h.Name)
		}
		if hasCRLF(h.Value) {
			return nil, ErrHeaderInjection
		}
		if hasFieldControl(h.Value) {
			return nil, fmt.Errorf("%w: the value of %q holds a control character", ErrH2Protocol, h.Name)
		}
		if err := checkResponseField(name); err != nil {
			return nil, err
		}
		// The output half of §8.2.2's rule, mirrored on the input half
		// above: a response generated with a connection-specific field is
		// malformed, and a conformant client MUST reject it — so a handler's
		// HTTP/1.1 habit ("connection: close") must fail here, as the
		// caller's bug it is, rather than there, as the whole response.
		// Content-length is the h1 writer's refusal for the h1 writer's
		// reason: framing is this package's to state, and a second opinion
		// on it is the smuggling class arriving from the inside.
		// TE, whose §8.2.2 exception holds in a request alone, was refused
		// above by checkResponseField as a request's field.
		if connectionSpecific(name) {
			return nil, fmt.Errorf("%w: the connection-specific response field %q", ErrH2Protocol, h.Name)
		}
		if string(name) == "content-length" {
			return nil, fmt.Errorf("%w: content-length is set by the writer, not the caller", ErrH2Protocol)
		}
		block = appendHPACK(block, name, h.Value)
		listSize += uint64(len(name)+len(h.Value)) + 32
	}
	if n, ok := statedLength(r, noLength, noBody); ok {
		var digits [20]byte
		v := strconv.AppendInt(digits[:0], n, 10)
		block = appendHPACK(block, "content-length", v)
		listSize += uint64(len("content-length")+len(v)) + 32
	}
	s.block = block
	// The peer's SETTINGS_MAX_HEADER_LIST_SIZE is advisory (§6.5.2), but a
	// peer that set it will likely refuse a longer block, and a refused
	// response is one the handler never hears about. Refused here instead,
	// it becomes the 500 a handler bug gets — the same treatment HTTP/3
	// gives the peer's MAX_FIELD_SECTION_SIZE.
	if limit := s.peerMaxHeaderList.Load(); limit > 0 && listSize > uint64(limit) {
		return nil, fmt.Errorf("%w: a header list of %d bytes, over the peer's %d limit",
			ErrH2Protocol, listSize, limit)
	}

	// A block larger than one frame goes out as HEADERS followed by
	// CONTINUATION, which is what those frames are for. END_HEADERS marks the
	// last piece and nothing else may be interleaved between them — the same
	// rule this server enforces on the way in.
	flags := byte(0)
	if noBody || (len(r.Body) == 0 && !r.streamed() && r.Trailers == nil) {
		flags |= flagEndStream
	}
	return appendHeaderFrames(dst, id, flags, block, maxFrame)
}

// appendTrailers frames a response's trailer section as a header block
// carrying END_STREAM (RFC 9113 §8.1), held to [checkTrailer] and to the
// peer's SETTINGS_MAX_HEADER_LIST_SIZE as the head is. Its fields go on the
// wire lowercase, from copies, as the head's do.
func (s *h2Server) appendTrailers(dst []byte, id uint32, r Response, maxFrame int) ([]byte, error) {
	fields, err := r.trailers()
	if err != nil {
		reportPanic(s.cfg.logger(), err)
		return nil, err
	}
	block := s.block[:0]
	var listSize uint64
	s.names = s.names[:0]
	for _, h := range fields {
		name := lowerName(h.Name, &s.names)
		block = appendHPACK(block, name, h.Value)
		listSize += uint64(len(name)+len(h.Value)) + 32
	}
	s.block = block
	if limit := s.peerMaxHeaderList.Load(); limit > 0 && listSize > uint64(limit) {
		return nil, fmt.Errorf("%w: a trailer list of %d bytes, over the peer's %d limit",
			ErrH2Protocol, listSize, limit)
	}
	return appendHeaderFrames(dst, id, flagEndStream, block, maxFrame)
}

// appendHeaderFrames frames one header block as HEADERS and as many
// CONTINUATION frames as maxFrame makes it need; flags are the HEADERS
// frame's, END_HEADERS aside, which lands on whichever frame is last.
func appendHeaderFrames(dst []byte, id uint32, flags byte, block []byte, maxFrame int) ([]byte, error) {
	var err error
	first, rest := block, []byte(nil)
	if len(block) > maxFrame {
		first, rest = block[:maxFrame], block[maxFrame:]
	} else {
		flags |= flagEndHeaders
	}
	if dst, err = appendH2Frame(dst, h2Frame{Type: frameHeaders, Flags: flags, StreamID: id, Payload: first}); err != nil {
		return nil, err
	}
	for len(rest) > 0 {
		n := min(len(rest), maxFrame)
		var f byte
		if n == len(rest) {
			f = flagEndHeaders
		}
		if dst, err = appendH2Frame(dst, h2Frame{Type: frameContinuation, Flags: f, StreamID: id, Payload: rest[:n]}); err != nil {
			return nil, err
		}
		rest = rest[n:]
	}
	return dst, nil
}

// streamBody pulls a streamed response and hands each producer batch to the
// writer, which frames what has accumulated as however many DATA frames the
// credit and the round's budget allow.
//
// The hand-off copies the batch into the job's own buffer — the batch
// contract forbids retaining it — and returns without waiting for the wire:
// a batch that arrives while the writer is in a write joins the job still
// queued and leaves in the next write with it, and END_STREAM rides on the
// last DATA frame when the end catches that job in time. The producer still
// cannot run away: [h2Writer.extend] makes it wait once a queued job holds a
// round's budget of unframed bytes, so what is held is bounded by
// [Config.MaxWriteBufferBytes] per stream, and a stalled window stops the
// pull after [Config.WriteTimeout] as before.
//
// Before that, the hand-off blocked until the bytes were written, and a
// streamed response paid one write, two goroutine hand-offs and one TCP
// segment per batch plus one for the end: measured from the standard
// library's client over loopback, about 70 µs per batch of eight bytes
// (docs/benchmarks.md, "HTTP/2 and HTTP/3, from a real client"), a cost the
// HTTP/3 path never had because quic-go's Stream.Write does not wait.
//
// A batch the writer refuses because the stream is gone stops the pull: that
// is a peer's RST_STREAM reaching a handler that is still working for it.
// Every batch, empty or not, is also checked against the stream's and the
// connection's state first, since an empty one is never handed to the writer
// and so could not be refused by it.
//
// unread is the request's streamed body left unread: the stream is reset
// with NO_ERROR once the response's end is written.
func (s *h2Server) streamBody(id uint32, stream sluice.Stream[[]byte], length int64, trailers func() []Header, unread bool) {
	// Both jobs start free: done is what marks a job as reusable.
	s.streamJobs[0].done, s.streamJobs[1].done = true, true
	s.streamCur = nil
	stopped, wrong := false, false
	var sent int64
	perr := pull(stream, func(b sluice.Batch[[]byte]) bool {
		// Asked on every batch, empty ones included: an empty batch is never
		// handed to the writer, so neither a reset stream nor a dead
		// connection would otherwise reach a producer of cadence ticks.
		if s.readEnded.Load() || !s.w.alive(id) {
			stopped = true // cut short: no END_STREAM may claim the body whole
			return false
		}
		if overLength(&sent, length, b.Items) {
			// Past the declared length: the batch is not handed over, and
			// the stream is reset below rather than ended (RFC 9113 §8.1.1
			// makes the mismatch a malformed response).
			wrong = true
			return false
		}
		// A single chunk is handed over as it is; several are flattened
		// first, so the writer appends once under its lock.
		var data []byte
		n := 0
		for _, chunk := range b.Items {
			if len(chunk) == 0 {
				continue // an empty batch is cadence, not an end
			}
			n++
			data = chunk
		}
		if n == 0 {
			return true
		}
		if n > 1 {
			s.streamBuf = s.streamBuf[:0]
			for _, chunk := range b.Items {
				s.streamBuf = append(s.streamBuf, chunk...)
			}
			data = s.streamBuf
		}
		j, err := s.w.extend(id, s.streamCur, s.streamSpare(), data)
		s.streamCur = j
		if err != nil || j.err != nil {
			stopped = true
			return false
		}
		return true
	})
	if stopped {
		return
	}
	if perr != nil {
		reportFailure(s.cfg.logger(), perr)
	}
	if perr != nil || wrong || sent < length {
		// The producer panicked partway through its answer, or did not
		// yield the length it declared. Only its own stream pays: it is
		// reset, as the response that cannot be framed is, and the
		// connection carries on serving the others.
		_ = s.w.reset(id, errInternalError)
		s.w.forget(id)
		return
	}
	var tail []byte
	if trailers != nil {
		var err error
		if tail, err = s.appendTrailers(s.streamTail[:0], id, Response{Trailers: trailers}, defaultMaxFrameSize); err != nil {
			// The body is out: a refused trailer resets the stream, as a
			// producer that panicked does, so the client never reads the
			// answer as whole.
			_ = s.w.reset(id, errInternalError)
			s.w.forget(id)
			return
		}
		s.streamTail = tail
	}
	// The end: END_STREAM on the trailers, on the last DATA frame still
	// queued, or on an empty one of its own — none costs credit, so the end
	// always goes out.
	_ = s.w.finish(id, s.streamCur, s.streamSpare(), tail, unread)
}

// streamSpare is the streamed-body job the last batch did not go to.
func (s *h2Server) streamSpare() *h2Job {
	if s.streamCur == &s.streamJobs[0] {
		return &s.streamJobs[1]
	}
	return &s.streamJobs[0]
}

// addWindow moves a flow-control window, refusing the overflow RFC 9113
// §6.9.1 makes an error rather than letting int32 wrap — a wrapped window
// reads as a peer that granted nothing, or as room it never granted.
//
// The window may legitimately go negative: a SETTINGS_INITIAL_WINDOW_SIZE
// that shrinks applies to streams already written to (§6.9.2).
func addWindow(cur, delta int32) (int32, error) {
	next := int64(cur) + int64(delta)
	if next > maxWindow || next < -maxWindow {
		return 0, fmt.Errorf("%w: %w: a window of %d falls outside ±%d",
			ErrH2Protocol, errFlowControl, next, maxWindow)
	}
	return int32(next), nil
}
