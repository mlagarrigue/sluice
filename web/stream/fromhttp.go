package stream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"maps"
	"net/http"
	"net/textproto"
	"net/url"
	"os"
	"runtime"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/net/httpstream"
)

// FromHTTP serves a net/http handler over the native transport: the
// [http.ServeMux] and [web] helpers a program answers HTTP/1.1 and HTTP/2
// with answer HTTP/3 as well, without a second set of routes. It is what
// quic-go's http3 server does for the Go ecosystem, on this library's
// transport; [ServeH3] is the call that runs it beside [web.ServeTLS]. A nil
// h is [http.DefaultServeMux], as for an [http.Server].
//
// # The request
//
// Each request of a batch reaches h on a goroutine of its own, as net/http's
// servers run one per request, and the batch is answered once every call has
// returned or flushed. The *http.Request is the one net/http would build:
// the target parsed by net/url, the header canonicalised with Host moved to
// Request.Host, Proto from the protocol ALPN chose ("HTTP/3.0" over QUIC),
// RemoteAddr, TLS, RequestURI, and a context that is the batch's, cancelled
// when the handler returns or its client goes away. The body reads as it
// arrives. As net/http asks of an HTTP/1.x handler, read what you need of it
// before you flush: the body is the transport's only while the batch is being
// answered, and a read after a flush fails.
//
// # The response
//
// What the handler writes is buffered and goes out whole with its length,
// as a small answer does under net/http. A Flush — [http.Flusher], or
// [http.ResponseController] — sends the head and turns the rest into a stream
// the transport pulls at the client's pace: each later Flush, and the
// handler's return, hands over what was written since, and a Flush waits
// while the client is behind. Past 64 KiB written without one the answer
// streams by itself, so an export is never held whole in memory. A
// Content-Type the handler did not set is sniffed from the first 512 bytes,
// as net/http does unless a Content-Encoding is set; a Content-Length the
// handler set frames a streamed answer and is otherwise the writer's;
// trailers declared in a Trailer field, or set under [http.TrailerPrefix],
// go out after the body.
//
// # Where it differs from net/http
//
// The transport refuses what HTTP/2 and HTTP/3 forbid on a response, so the
// adapter drops it first, as net/http's HTTP/2 server does: the
// connection-specific fields (Connection, Keep-Alive, Proxy-Connection,
// Transfer-Encoding, Upgrade), the fields only a request carries (Host, TE,
// Expect), a field whose name is not a token or whose value still holds a
// control byte once CR and LF have become spaces, and a Date that is not an
// IMF-fixdate. A 1xx other than 100 and 101 leaves at once through
// [httpstream.Request.Interim], with the header as it stands, except where
// the transport cannot send one: a pipelined HTTP/1.1 request behind
// another, an HTTP/1.0 client. A deadline set through
// [http.ResponseController] holds as under net/http, beside the
// transport's [httpstream.Config] bounds: a write past it fails and cuts the
// answer, a read begun past it fails (see SetWriteDeadline and
// SetReadDeadline on the writer).
//
// A panic other than [http.ErrAbortHandler] fails that response alone and
// reaches the server's Logger with its value and the handler's stack — a
// 500, or the stream reset once the head is out; one raised after a Flush on
// an answer that has no body (a HEAD, a 204) goes to [slog.Default], since
// that answer has already left. ErrAbortHandler ends the response
// unterminated, without a log line, as under net/http.
//
// The goroutine per request is the per-request handler's price, which the
// batch stages of this package do not pay; its figure is in the guide
// (docs/guide/httpstream.md, "web/stream").
func FromHTTP(h http.Handler) httpstream.Handler {
	if h == nil {
		h = http.DefaultServeMux
	}
	return func(ctx context.Context, b sluice.Batch[httpstream.Request]) sluice.Batch[httpstream.Response] {
		calls := make([]httpCall, len(b.Items))
		var decided sync.WaitGroup
		decided.Add(len(calls))
		for i := range calls {
			c := &calls[i]
			c.decided = &decided
			if c.prepare(ctx, &b.Items[i]) {
				go c.run(h)
			}
		}
		decided.Wait()
		out := make([]httpstream.Response, len(calls))
		for i := range calls {
			// The call lets go of its answer, so that a stream the
			// transport drops is reachable from nothing (begin's cleanup).
			out[i], calls[i].resp = calls[i].resp, httpstream.Response{}
			// A handler still running has flushed, and the body is the
			// transport's no longer: a read from here on fails.
			calls[i].body.end()
		}
		return sluice.Batch[httpstream.Response]{Items: out}
	}
}

// streamAfter is how much a handler may write without a Flush before its
// answer streams by itself, and how much a streamed one gathers between two
// hand-overs: large enough that a JSON answer leaves whole with its length,
// small enough that an export is never held in memory.
const streamAfter = 64 << 10

// The three states of an answer: buffered until something decides it,
// then streamed, or written to nowhere when it has no body.
const (
	buffering = iota
	streaming
	discarding
)

var (
	errClientGone     = errors.New("stream: the client is no longer reading this response")
	errBodyAfterFlush = errors.New("stream: the request body is read after the response began; read it before flushing, as net/http asks")
)

// httpCall is one request under FromHTTP: the *http.Request handed to the
// handler, the [http.ResponseWriter] it writes to, and the answer it
// becomes. Every field but body is written by the handler's goroutine
// alone; the batch reads resp once decided has been released, the transport
// reads the chunks it is sent, and gone is the only way back.
type httpCall struct {
	req     *http.Request
	cancel  context.CancelFunc
	batch   context.Context
	decided *sync.WaitGroup
	body    requestBody

	head        http.Header
	status      int
	wroteHeader bool
	fields      []httpstream.Header
	declared    []string // the trailer names a Trailer field announced
	length      int64    // the Content-Length the handler set, or -1
	sniff       bool     // a Content-Type is the adapter's to find
	state       int
	buf         []byte
	resp        httpstream.Response
	isDecided   bool

	chunks   chan []byte
	gone     chan struct{}
	goneOnce sync.Once
	failure  *handlerPanic // a panic after the head left, re-raised by the stream

	// writeDeadline is what SetWriteDeadline set, zero for none; timedOut
	// is set once a write met it, and the answer is then cut.
	writeDeadline time.Time
	timedOut      bool
}

// SetWriteDeadline is what [http.ResponseController] calls: a write, a flush
// or a hand-over still waiting for the transport past t fails with
// [os.ErrDeadlineExceeded], and the answer then ends unterminated — net/http
// closes the connection under a write that misses its deadline, so that a
// client never takes a cut answer for a whole one. A zero t lifts it. The
// transport's [httpstream.Config] bounds still hold beside it.
func (c *httpCall) SetWriteDeadline(t time.Time) error {
	c.writeDeadline = t
	return nil
}

// SetReadDeadline is what [http.ResponseController] calls: a read of the
// body begun past t fails with [os.ErrDeadlineExceeded]. A read already
// waiting for the client when t passes returns at the next bytes or at the
// transport's ReadTimeout, which bounds every wait for a body. A zero t
// lifts it.
func (c *httpCall) SetReadDeadline(t time.Time) error {
	var n int64
	if !t.IsZero() {
		n = t.UnixNano()
	}
	c.body.deadline.Store(n)
	return nil
}

// pastWrite reports whether a write now misses the write deadline, and cuts
// the answer the first time it does.
func (c *httpCall) pastWrite() bool {
	if c.timedOut {
		return true
	}
	if c.writeDeadline.IsZero() || time.Now().Before(c.writeDeadline) {
		return false
	}
	c.timeOut()
	return true
}

// timeOut cuts the answer a write deadline ended. One not yet decided is
// answered as an abort — one byte promised and none sent — at once, so the
// batch does not wait for the handler; a streamed one ends unterminated once
// the handler returns, as one that aborts after a flush does.
func (c *httpCall) timeOut() {
	c.timedOut = true
	switch c.state {
	case buffering:
		c.state, c.buf = discarding, nil
		c.resp = httpstream.Response{Stream: func(func(sluice.Batch[[]byte]) bool) {}, ContentLength: 1}
		c.decide()
	case streaming:
		c.buf = nil
		if c.failure == nil {
			c.failure = &handlerPanic{value: http.ErrAbortHandler}
		}
	}
}

// prepare builds the request; a target net/url cannot read is answered 400
// in its place, as net/http answers it, and reports false.
func (c *httpCall) prepare(batch context.Context, r *httpstream.Request) bool {
	c.batch = batch
	target := string(r.Target)
	var u *url.URL
	if target == "*" {
		u = &url.URL{Path: "*"}
	} else {
		var err error
		if u, err = url.ParseRequestURI(target); err != nil {
			c.resp = httpstream.Response{Status: http.StatusBadRequest}
			c.decide()
			return false
		}
	}
	header := make(http.Header, len(r.Headers))
	for _, f := range r.Headers {
		if foldEqual(f.Name, "host") {
			continue
		}
		k := textproto.CanonicalMIMEHeaderKey(string(f.Name))
		header[k] = append(header[k], string(f.Value))
	}
	ctx, cancel := context.WithCancel(batch)
	req := (&http.Request{
		Method:     string(r.Method),
		URL:        u,
		Header:     header,
		Host:       string(r.Get("host")),
		RequestURI: target,
		TLS:        r.TLS,
	}).WithContext(ctx)
	req.Proto, req.ProtoMajor, req.ProtoMinor = "HTTP/1.1", 1, 1
	if r.TLS != nil {
		switch r.TLS.NegotiatedProtocol {
		case "h3":
			req.Proto, req.ProtoMajor, req.ProtoMinor = "HTTP/3.0", 3, 0
		case "h2":
			req.Proto, req.ProtoMajor, req.ProtoMinor = "HTTP/2.0", 2, 0
		}
	}
	if r.RemoteAddr.IsValid() {
		req.RemoteAddr = r.RemoteAddr.String()
	}
	req.ContentLength = -1
	if v := r.Get("content-length"); v != nil {
		if n, err := strconv.ParseInt(string(v), 10, 64); err == nil {
			req.ContentLength = n
		}
	} else if r.Body != nil {
		req.ContentLength = int64(len(r.Body))
	}
	c.body.src, c.body.req = r, req
	req.Body = &c.body
	c.req, c.cancel = req, cancel
	c.head = make(http.Header)
	c.length = -1
	return true
}

// run is the handler's goroutine.
func (c *httpCall) run(h http.Handler) {
	defer c.finish()
	h.ServeHTTP(c, c.req)
}

// finish ends the call once the handler has returned or panicked: the answer
// decided if it was not, the rest of a streamed one handed over and its end
// signalled, the request's context cancelled as net/http cancels it.
func (c *httpCall) finish() {
	p := recover()
	// Cancelled first, before the answer is decided: the batch may hand
	// the response on the moment it is, and a context still live then
	// outlived its handler for whoever reads it (a shuffled CI run saw
	// TestFromHTTPBuildsTheRequest do so). Nothing below reads it.
	c.cancel()
	c.body.end()
	if p != nil {
		c.fail(p, debug.Stack())
		return
	}
	switch c.state {
	case buffering:
		if !c.wroteHeader {
			c.WriteHeader(http.StatusOK)
		}
		c.resp = httpstream.Response{Status: c.status, Headers: c.headFields()}
		if len(c.declared) > 0 || hasTrailerPrefix(c.head) {
			c.resp.Trailers = c.trailers
		}
		if len(c.buf) > 0 {
			c.resp.Body = c.buf
		}
		c.decide()
	case streaming:
		_ = c.send()
		close(c.chunks)
	}
}

// fail answers a panic. Before the answer is decided, the response becomes
// a stream that raises it again, so that the transport fails that one
// response and logs it through its own Logger; after, the open stream
// raises it once it has handed over what was written.
func (c *httpCall) fail(p any, stack []byte) {
	f := &handlerPanic{value: p, stack: stack}
	switch c.state {
	case buffering:
		if aborted(p) {
			// One byte promised and none sent: the response ends
			// unterminated and nothing is logged, net/http's abort.
			c.resp = httpstream.Response{Stream: func(func(sluice.Batch[[]byte]) bool) {}, ContentLength: 1}
		} else {
			c.resp = httpstream.Response{Stream: func(func(sluice.Batch[[]byte]) bool) { panic(f) }}
		}
		c.decide()
	case streaming:
		c.failure = f
		close(c.chunks)
	case discarding:
		if !aborted(p) {
			slog.Default().Error("stream: the handler panicked after its bodiless answer left", "panic", p, "stack", string(stack))
		}
	}
}

// decide releases the batch's wait for this call, once.
func (c *httpCall) decide() {
	if !c.isDecided {
		c.isDecided = true
		// Closed to the handler before the batch is released, from the
		// handler's own goroutine: a read it starts after a Flush would
		// otherwise take the body's lock first, wait there for the client,
		// and hold end — and with it every answer of the batch.
		c.body.flushed.Store(true)
		c.decided.Done()
	}
}

// Header is the response header: changing it after WriteHeader changes
// nothing but the trailers, as under net/http.
func (c *httpCall) Header() http.Header { return c.head }

// WriteHeader fixes the status and takes the header as it stands.
func (c *httpCall) WriteHeader(code int) {
	if code < 100 || code > 999 {
		panic(fmt.Sprintf("invalid WriteHeader code %v", code)) // net/http's check, worded as it
	}
	if c.wroteHeader {
		return // superfluous
	}
	if code < 200 {
		// An interim response leaves at once with the header as it stands,
		// as under net/http; the final one keeps the header's later state.
		// A 100 is the transport's (Expect: 100-continue) and a 101 changes
		// protocols, which the transport does not do. One it cannot send —
		// a pipelined HTTP/1.1 request behind another, an HTTP/1.0 client —
		// is dropped, as net/http drops what it cannot write.
		if code > 101 {
			fields, _, _, _ := responseFields(c.head)
			_ = c.body.src.Interim(code, fields)
		}
		return
	}
	c.wroteHeader, c.status = true, code
	c.fields, c.declared, c.length, c.sniff = responseFields(c.head)
}

func (c *httpCall) Write(p []byte) (int, error) {
	if !c.wroteHeader {
		c.WriteHeader(http.StatusOK)
	}
	if c.pastWrite() {
		return 0, os.ErrDeadlineExceeded
	}
	switch c.state {
	case buffering:
		c.buf = append(c.buf, p...)
		if len(c.buf) >= streamAfter {
			c.begin()
		}
	case streaming:
		if c.isGone() {
			return 0, errClientGone
		}
		c.buf = append(c.buf, p...)
		if len(c.buf) >= streamAfter {
			if err := c.send(); err != nil {
				return 0, err
			}
		}
	}
	return len(p), nil
}

// Flush is [http.Flusher]'s.
func (c *httpCall) Flush() { _ = c.FlushError() }

// FlushError is what [http.ResponseController] calls: the head goes out at
// the first, what was written since at each.
func (c *httpCall) FlushError() error {
	if !c.wroteHeader {
		c.WriteHeader(http.StatusOK)
	}
	if c.pastWrite() {
		return os.ErrDeadlineExceeded
	}
	switch c.state {
	case buffering:
		c.begin()
		return nil
	case streaming:
		return c.send()
	}
	return nil
}

// begin decides a buffered answer as a streamed one, or as one without a
// body when its method or status has none, and hands over what was written.
func (c *httpCall) begin() {
	if c.req.Method == http.MethodHead || !bodyAllowed(c.status) {
		c.state = discarding
		c.resp = httpstream.Response{Status: c.status, Headers: c.headFields()}
		c.buf = nil
		c.decide()
		return
	}
	c.state = streaming
	c.chunks, c.gone = make(chan []byte), make(chan struct{})
	// A stream the transport drops unpulled — a head it refused, a
	// connection gone before the body — would leave this handler waiting in
	// a Flush for good: the token is reachable only from the stream, and
	// its cleanup releases the handler once nothing can pull it.
	tok := &streamToken{call: c}
	runtime.AddCleanup(tok, (*httpCall).abandon, c)
	c.resp = httpstream.Response{Status: c.status, Headers: c.headFields(), Stream: tok.stream}
	if c.length > 0 {
		c.resp.ContentLength = c.length
	}
	// Trailers change the framing of HTTP/1.1, so they are asked for only
	// when they can come: announced, or set later under TrailerPrefix on an
	// answer chunked anyway since its length is not known.
	if len(c.declared) > 0 || c.length <= 0 {
		c.resp.Trailers = c.trailers
	}
	c.decide()
	_ = c.send()
}

// send hands what was written since the last hand-over to the transport,
// waiting while it is behind.
func (c *httpCall) send() error {
	if c.isGone() {
		return errClientGone
	}
	if len(c.buf) == 0 {
		return nil
	}
	if c.pastWrite() {
		return os.ErrDeadlineExceeded
	}
	chunk := c.buf
	c.buf = nil // the chunk is the transport's until written
	var expired <-chan time.Time
	if !c.writeDeadline.IsZero() {
		t := time.NewTimer(time.Until(c.writeDeadline))
		defer t.Stop()
		expired = t.C
	}
	select {
	case c.chunks <- chunk:
		return nil
	case <-c.gone:
		return errClientGone
	case <-c.batch.Done():
		return c.batch.Err()
	case <-expired:
		c.timeOut()
		return os.ErrDeadlineExceeded
	}
}

// streamToken is what a streamed answer's producer hangs on: reachable from
// the transport's Response alone, so that its cleanup tells when nothing will
// pull the stream any more. It holds a pointer, which keeps it out of the
// tiny allocator, whose blocks may never be cleaned up one by one.
type streamToken struct{ call *httpCall }

// stream is the streamed answer's producer: what the handler hands over,
// in order, then its panic if it raised one.
func (t *streamToken) stream(yield func(sluice.Batch[[]byte]) bool) {
	defer runtime.KeepAlive(t) // pulling is what keeps the stream wanted
	c := t.call
	for chunk := range c.chunks {
		if !yield(sluice.Batch[[]byte]{Items: [][]byte{chunk}}) {
			c.abandon()
			return
		}
	}
	if c.failure != nil {
		panic(c.failure)
	}
}

// abandon tells the handler its client is gone: its writes fail from here
// and its context is cancelled.
func (c *httpCall) abandon() {
	c.goneOnce.Do(func() {
		close(c.gone)
		c.cancel()
	})
}

func (c *httpCall) isGone() bool {
	select {
	case <-c.gone:
		return true
	default:
		return false
	}
}

// headFields is the head as WriteHeader took it, with the Content-Type
// sniffed from what was written when the handler set none.
func (c *httpCall) headFields() []httpstream.Header {
	if !c.sniff || len(c.buf) == 0 || !bodyAllowed(c.status) {
		return c.fields
	}
	ct := http.DetectContentType(c.buf[:min(len(c.buf), 512)])
	return append(c.fields, httpstream.Header{Name: []byte("Content-Type"), Value: []byte(ct)})
}

// trailers reads the trailer values once the body is done, as net/http
// reads them from the header map when the handler returns.
func (c *httpCall) trailers() []httpstream.Header {
	var out []httpstream.Header
	for _, name := range c.declared {
		for _, v := range c.head.Values(name) {
			out = appendField(out, name, v)
		}
	}
	for k, vs := range c.head {
		if name, ok := strings.CutPrefix(k, http.TrailerPrefix); ok {
			for _, v := range vs {
				out = appendField(out, name, v)
			}
		}
	}
	return out
}

// hasTrailerPrefix reports whether the handler set a trailer it did not
// announce.
func hasTrailerPrefix(h http.Header) bool {
	for k := range h {
		if strings.HasPrefix(k, http.TrailerPrefix) {
			return true
		}
	}
	return false
}

// responseFields turns a net/http header into the transport's fields, sorted
// by name as net/http writes them, without what the transport refuses on a
// response: the Content-Length is returned apart, the Trailer field's names
// too, and whether the Content-Type is left to sniffing.
func responseFields(h http.Header) (fields []httpstream.Header, declared []string, length int64, sniff bool) {
	length = -1
	_, typed := h["Content-Type"] // present but nil: the handler asked for none
	sniff = !typed && h.Get("Content-Encoding") == ""
	for _, k := range slices.Sorted(maps.Keys(h)) {
		switch strings.ToLower(k) {
		case "content-length":
			if n, err := strconv.ParseInt(h.Get(k), 10, 64); err == nil && n >= 0 {
				length = n
			}
			continue
		case "trailer":
			for _, v := range h[k] {
				for name := range strings.SplitSeq(v, ",") {
					if name = textproto.TrimString(name); name != "" {
						declared = append(declared, textproto.CanonicalMIMEHeaderKey(name))
					}
				}
			}
			continue
		case "connection", "keep-alive", "proxy-connection", "transfer-encoding", "upgrade",
			"host", "te", "expect":
			continue
		case "date":
			if v := h.Get(k); !imfFixdate(v) {
				continue
			}
		}
		if strings.HasPrefix(k, http.TrailerPrefix) {
			continue
		}
		for _, v := range h[k] {
			fields = appendField(fields, k, v)
		}
	}
	if len(declared) > 0 {
		fields = append(fields, httpstream.Header{Name: []byte("Trailer"), Value: []byte(strings.Join(declared, ", "))})
	}
	return fields, declared, length, sniff
}

// appendField appends one field the transport will take: a name that is a
// token, a value whose CR and LF become spaces as net/http writes them, and
// no other control byte but the tab.
func appendField(dst []httpstream.Header, name, value string) []httpstream.Header {
	if name == "" || strings.IndexFunc(name, func(r rune) bool { return !isTokenRune(r) }) >= 0 {
		return dst
	}
	value = strings.NewReplacer("\r", " ", "\n", " ").Replace(value)
	for i := range len(value) {
		if b := value[i]; (b < ' ' && b != '\t') || b == 0x7f {
			return dst
		}
	}
	return append(dst, httpstream.Header{Name: []byte(name), Value: []byte(value)})
}

// isTokenRune reports whether r is a tchar (RFC 9110 §5.6.2).
func isTokenRune(r rune) bool {
	switch {
	case 'a' <= r && r <= 'z', 'A' <= r && r <= 'Z', '0' <= r && r <= '9':
		return true
	}
	return strings.ContainsRune("!#$%&'*+-.^_`|~", r)
}

// imfFixdate reports whether v is the one Date form a sender may generate
// (RFC 9110 §5.6.7).
func imfFixdate(v string) bool {
	t, err := time.Parse(http.TimeFormat, v)
	return err == nil && t.Format(http.TimeFormat) == v
}

// bodyAllowed reports whether a response with this status carries content:
// a 204, 205 or 304 does not (RFC 9110 §15.3.5, §15.3.6, §15.4.5).
func bodyAllowed(status int) bool {
	return status != http.StatusNoContent && status != http.StatusResetContent && status != http.StatusNotModified
}

// handlerPanic is a handler's panic carried to where the transport recovers
// it, with the stack where it was raised rather than the one re-raising it.
type handlerPanic struct {
	value any
	stack []byte
}

func (p *handlerPanic) Error() string {
	return fmt.Sprintf("%v\n\nraised in the handler at:\n%s", p.value, p.stack)
}

// requestBody is the request's body as an io.ReadCloser: the bytes when they
// arrived whole, the transport's stream otherwise, read only while the batch
// is being answered.
type requestBody struct {
	mu     sync.Mutex
	src    *httpstream.Request
	req    *http.Request
	whole  *bytes.Reader
	stream sluice.Source[[]byte]
	next   func() (sluice.Batch[[]byte], bool)
	stop   func()
	items  [][]byte
	ended  bool
	eof    bool
	// flushed is set when the answer is decided, before end can run: a
	// read from then on fails without waiting for the client.
	flushed atomic.Bool
	// deadline is SetReadDeadline's, in Unix nanoseconds, zero for none:
	// written by the handler while a read may hold mu.
	deadline atomic.Int64
}

func (b *requestBody) Read(p []byte) (int, error) {
	if d := b.deadline.Load(); d != 0 && time.Now().UnixNano() >= d {
		return 0, os.ErrDeadlineExceeded
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	switch {
	case b.ended, b.flushed.Load() && !b.eof:
		return 0, errBodyAfterFlush
	case b.eof:
		return 0, io.EOF
	}
	if b.src.Body != nil {
		if b.whole == nil {
			b.whole = bytes.NewReader(b.src.Body)
		}
		n, err := b.whole.Read(p)
		if err != nil {
			b.eof = true
			b.trailers()
		}
		return n, err
	}
	if b.next == nil {
		b.stream = b.src.Stream()
		b.next, b.stop = iter.Pull(iter.Seq[sluice.Batch[[]byte]](b.stream.Stream()))
	}
	for len(b.items) == 0 || len(b.items[0]) == 0 {
		if len(b.items) > 0 {
			b.items = b.items[1:]
			continue
		}
		batch, ok := b.next()
		if !ok {
			b.eof = true
			if err := b.stream.Err(); err != nil {
				return 0, err
			}
			b.trailers()
			return 0, io.EOF
		}
		b.items = batch.Items
	}
	// Copied at once: an element borrows the connection's buffer only until
	// the next is pulled.
	n := copy(p, b.items[0])
	b.items[0] = b.items[0][n:]
	return n, nil
}

// Close is the handler's: net/http lets it close the body early.
func (b *requestBody) Close() error {
	b.end()
	return nil
}

// end lets the transport's body go: from here a read fails.
func (b *requestBody) end() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.stop != nil {
		b.stop()
		b.stop = nil
	}
	b.ended = b.ended || !b.eof
}

// trailers hands the body's trailers to Request.Trailer once it has ended,
// as net/http does, whichever form the body arrived in.
func (b *requestBody) trailers() {
	if t := b.src.Trailers(); len(t) > 0 {
		b.req.Trailer = make(http.Header, len(t))
		for _, f := range t {
			b.req.Trailer.Add(string(f.Name), string(f.Value))
		}
	}
}

// aborted reports whether a handler's panic is net/http's way of aborting
// a response on purpose.
func aborted(p any) bool {
	err, ok := p.(error)
	return ok && errors.Is(err, http.ErrAbortHandler)
}
