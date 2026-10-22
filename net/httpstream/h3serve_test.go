package httpstream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/internal/dgram"
	"github.com/mlagarrigue/sluice/net/quic"
)

// startH3 serves one HTTP/3 connection over an in-memory datagram pair, with
// the server's transport parameters given, and returns the client end, a
// cancel for the server's context, and what ServeH3Conn returned once it has.
func startH3(t *testing.T, params quic.TransportParameters, handle Handler) (*quic.Conn, context.CancelFunc, <-chan error) {
	t.Helper()
	return startH3With(t, Config{}, params, quic.DefaultParameters(), handle)
}

// startH3With is startH3 with the server's Config and the client's transport
// parameters chosen too.
func startH3With(t *testing.T, cfg Config, params, clientParams quic.TransportParameters, handle Handler) (*quic.Conn, context.CancelFunc, <-chan error) {
	t.Helper()
	serverPC, clientPC := dgram.Pair()
	t.Cleanup(func() { _ = serverPC.Close(); _ = clientPC.Close() })

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	serverUp := make(chan error, 1)
	served := make(chan error, 1)
	serverDone := make(chan struct{})
	// The server goroutine is reaped before the next test starts: one
	// outliving its test would read the h3ReleaseStreamBytes hook while
	// TestH3CreditsRequestBytesOnce swaps it (a data race under -race).
	t.Cleanup(func() { <-serverDone })
	go func() {
		defer close(serverDone)
		conn, err := acceptOne(t, serverPC, h3ServerTLS(t), params)
		if err != nil {
			serverUp <- err
			return
		}
		serverUp <- nil
		served <- ServeH3Conn(ctx, conn, cfg, handle)
	}()

	conn, err := quic.Dial(context.Background(), clientPC, serverPC.LocalAddr(), h3ClientTLS(), clientParams)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := <-serverUp; err != nil {
		t.Fatalf("the server: %v", err)
	}
	return conn, cancel, served
}

// RFC 9114 §6.2.1: "the receiver MUST NOT request that the sender close the
// control stream", and a control stream closed at any point is a connection
// error of type H3_CLOSED_CRITICAL_STREAM. The STOP_SENDING is the last
// thing the client sends: the server used to learn of it only at the next
// datagram carrying stream frames, so the connection stayed open until the
// client spoke again.
//
// The client stops the stream once it has seen it, as a client reading the
// server's SETTINGS does: RFC 9000 §19.5 makes a STOP_SENDING that reaches
// the server before ServeH3Conn opened stream 3 a STREAM_STATE_ERROR, which is
// what this test used to race into one run in thirty.
func TestH3StopSendingOnTheControlStreamClosesTheConnection(t *testing.T) {
	conn, _, _ := startH3(t, quic.DefaultParameters(), func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
		res := make([]Response, b.Len())
		for i := range res {
			res[i] = Response{Status: 200}
		}
		return sluice.Batch[Response]{Items: res}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	control, err := conn.AcceptStream(ctx)
	if err != nil {
		t.Fatalf("waiting for the server's control stream: %v", err)
	}
	if control.ID() != 3 {
		t.Fatalf("the server opened stream %d first, want its control stream 3", control.ID())
	}
	if err := control.StopSending(h3NoError); err != nil {
		t.Fatalf("STOP_SENDING on the server's control stream: %v", err)
	}

	select {
	case <-conn.Done():
	case <-ctx.Done():
		t.Fatal("the connection stayed open after STOP_SENDING on the control stream")
	}
	ce, ok := errors.AsType[*quic.ConnectionError](conn.Err())
	if !ok || !ce.Application {
		t.Fatalf("the connection ended with %v, want an application close", conn.Err())
	}
	if ce.Code != h3ClosedCriticalStream {
		t.Fatalf("closed with %#x, want H3_CLOSED_CRITICAL_STREAM (%#x)", ce.Code, h3ClosedCriticalStream)
	}
}

// The same STOP_SENDING, arriving before SETTINGS has gone out: the write
// of SETTINGS fails, and ServeH3Conn used to return that error with the
// connection left open — a client waiting on a server nobody runs, until
// the idle timeout. It is closed on the way out now, with the §6.2.1 code,
// like every other refusal at the top of ServeH3Conn.
func TestH3StopSendingBeforeSettingsClosesTheConnection(t *testing.T) {
	serverPC, clientPC := dgram.Pair()
	t.Cleanup(func() { _ = serverPC.Close(); _ = clientPC.Close() })
	serverUp := make(chan error, 1)
	stopped := make(chan struct{})
	served := make(chan error, 1)
	serverDone := make(chan struct{})
	t.Cleanup(func() { <-serverDone })
	go func() {
		defer close(serverDone)
		conn, err := acceptOne(t, serverPC, h3ServerTLS(t), quic.DefaultParameters())
		if err != nil {
			serverUp <- err
			return
		}
		// Opened before ServeH3Conn runs, so the client's STOP_SENDING finds
		// the stream — a STOP_SENDING for a stream this end never opened
		// would be a transport error instead (RFC 9000 §19.5).
		control, err := conn.Stream(3)
		if err != nil {
			serverUp <- err
			return
		}
		serverUp <- nil
		<-stopped
		for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(time.Millisecond) {
			if _, reset := control.WasReset(); reset {
				break
			}
			if time.Now().After(deadline) {
				served <- errors.New("the STOP_SENDING never reached the server")
				return
			}
		}
		served <- ServeH3Conn(t.Context(), conn, Config{}, func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
			return sluice.Batch[Response]{Items: make([]Response, b.Len())}
		})
	}()

	conn, err := quic.Dial(context.Background(), clientPC, serverPC.LocalAddr(), h3ClientTLS(), quic.DefaultParameters())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := <-serverUp; err != nil {
		t.Fatalf("the server: %v", err)
	}
	if err := h3StreamOf(t, conn, 3).StopSending(h3NoError); err != nil {
		t.Fatalf("STOP_SENDING on the server's control stream: %v", err)
	}
	close(stopped)

	select {
	case <-conn.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the connection stayed open after STOP_SENDING on the control stream before SETTINGS")
	}
	ce, ok := errors.AsType[*quic.ConnectionError](conn.Err())
	if !ok || !ce.Application {
		t.Fatalf("the connection ended with %v, want an application close", conn.Err())
	}
	if ce.Code != h3ClosedCriticalStream {
		t.Fatalf("closed with %#x, want H3_CLOSED_CRITICAL_STREAM (%#x)", ce.Code, h3ClosedCriticalStream)
	}
	if err := <-served; !errors.Is(err, ErrH3Protocol) {
		t.Fatalf("ServeH3Conn returned %v, want ErrH3Protocol", err)
	}
}

// askH3 sends one request on a fresh stream and returns the stream.
func askH3(t *testing.T, conn *quic.Conn, request []byte) *quic.Stream {
	t.Helper()
	s, err := conn.OpenStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Write(request)
	if err == nil {
		err = s.CloseWrite()
	}
	// A server may refuse a request before reading all of it, with
	// STOP_SENDING H3_NO_ERROR (RFC 9114 §4.1.1): when it lands before the
	// last byte or the FIN leaves, the write fails and the answer stands.
	if code, reset := s.WasReset(); err != nil && (!reset || code != h3NoError) {
		t.Fatal(err)
	}
	return s
}

// readH3Body reads a response stream to its end, under a bound, and returns
// its DATA payload and whether a HEADERS field named content-length was sent.
func readH3Body(t *testing.T, s *quic.Stream) (body string, contentLength bool) {
	t.Helper()
	type result struct {
		b   []byte
		err error
	}
	done := make(chan result, 1)
	go func() {
		var got []byte
		buf := make([]byte, 4096)
		for {
			n, err := s.Read(buf)
			got = append(got, buf[:n]...)
			if err != nil {
				done <- result{got, err}
				return
			}
		}
	}()
	var r result
	select {
	case r = <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the response stream never ended")
	}
	if !errors.Is(r.err, io.EOF) {
		t.Fatalf("the response stream ended with %v", r.err)
	}
	frames, _, _, err := parseH3Frames(nil, r.b, 1<<24)
	if err != nil {
		t.Fatalf("the response did not parse: %v", err)
	}
	d := newQPACKDecoder(nil, 0)
	for _, f := range frames {
		switch f.Type {
		case h3Data:
			body += string(f.Payload)
		case h3Headers:
			fields, err := d.Decode(nil, f.Payload)
			if err != nil {
				t.Fatal(err)
			}
			for _, h := range fields {
				contentLength = contentLength || string(h.Name) == "content-length"
			}
		}
	}
	return body, contentLength
}

// A streamed body over a real connection: every batch arrives as DATA, in
// order, an empty batch costs nothing, and the stream ends cleanly with no
// content-length in the head.
func TestH3StreamedBodyOverARealConnection(t *testing.T) {
	conn, _, _ := startH3(t, quic.DefaultParameters(), func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
		res := make([]Response, b.Len())
		for i := range res {
			res[i] = Response{Status: 200, Stream: func(yield func(sluice.Batch[[]byte]) bool) {
				for _, batch := range [][][]byte{{[]byte("row0;"), []byte("row1;")}, {}, {[]byte("row2;")}} {
					if !yield(sluice.Batch[[]byte]{Items: batch}) {
						return
					}
				}
			}}
		}
		return sluice.Batch[Response]{Items: res}
	})
	body, contentLength := readH3Body(t, askH3(t, conn, h3Request("GET", "/export")))
	if body != "row0;row1;row2;" {
		t.Errorf("the streamed body was %q", body)
	}
	if contentLength {
		t.Error("a streamed response stated a content-length")
	}
}

// A request that arrives while the handler is busy with an earlier batch on
// the same connection is not held behind it: each batch is its own call, as
// on HTTP/2. It used to wait — one responder per connection called the
// handler batch after batch — so a slow call delayed every stream that
// arrived during it.
func TestH3RequestDuringABusyCallIsNotHeldBehindIt(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan struct{})
	var once sync.Once
	conn, _, _ := startH3(t, quic.DefaultParameters(), func(ctx context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
		if string(b.Items[0].Target) == "/held" {
			once.Do(func() { close(entered) })
			<-release
		}
		return echoTarget(ctx, b)
	})
	// Registered after startH3's cleanups, so it runs before them: a held
	// call would otherwise keep the server goroutine they wait for.
	var released sync.Once
	unblock := func() { released.Do(func() { close(release) }) }
	t.Cleanup(unblock)

	held := askH3(t, conn, h3Request("GET", "/held"))
	<-entered
	if body, _ := readH3Body(t, askH3(t, conn, h3Request("GET", "/next"))); body != "/next" {
		t.Fatalf("the second stream answered %q while the first call was held, want %q", body, "/next")
	}
	unblock()
	if body, _ := readH3Body(t, held); body != "/held" {
		t.Errorf("the held stream answered %q, want %q", body, "/held")
	}
}

// A producer blocked in its own code — not in its yield — does not hold
// ServeH3Conn once the connection has ended. It learns at its next yield, which
// returns false. The cancellation drains the connection first (ADR 0017): a
// response in flight is waited for up to ShutdownGrace, and this producer
// never finishes, so the grace is what ends the connection — kept short here.
func TestServeH3ReturnsWhileAProducerIsBlocked(t *testing.T) {
	blocked := make(chan struct{})
	release := make(chan struct{})
	yielded := make(chan bool, 1)
	conn, cancel, served := startH3With(t, Config{ShutdownGrace: 200 * time.Millisecond}, quic.DefaultParameters(), quic.DefaultParameters(), func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
		res := make([]Response, b.Len())
		for i := range res {
			res[i] = Response{Status: 200, Stream: func(yield func(sluice.Batch[[]byte]) bool) {
				if !yield(sluice.Batch[[]byte]{Items: [][]byte{[]byte("first")}}) {
					return
				}
				close(blocked)
				<-release
				yielded <- yield(sluice.Batch[[]byte]{Items: [][]byte{[]byte("late")}})
			}}
		}
		return sluice.Batch[Response]{Items: res}
	})
	defer close(release)

	askH3(t, conn, h3Request("GET", "/feed"))
	select {
	case <-blocked:
	case <-time.After(10 * time.Second):
		t.Fatal("the producer never started")
	}
	cancel()
	select {
	case <-served:
	case <-time.After(5 * time.Second):
		t.Fatal("ServeH3Conn did not return after its connection ended: it waits on a blocked producer")
	}

	release <- struct{}{}
	select {
	case ok := <-yielded:
		if ok {
			t.Error("a yield after the connection ended returned true")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the released producer's yield never returned")
	}
}

// Request bodies are credited to the transport once. Under delivery-time
// crediting — the fallback ServeH3Conn keeps when the windows are too small for
// manual credit — the transport has already credited every byte, so this
// layer must release nothing: it used to release each body again, and
// MAX_DATA ran ahead of what was consumed without bound. Under manual credit,
// what this layer releases is exactly what the request streams delivered.
func TestH3CreditsRequestBytesOnce(t *testing.T) {
	var released atomic.Uint64
	orig := h3ReleaseStreamBytes
	h3ReleaseStreamBytes = func(c *quic.Conn, id, n uint64) {
		released.Add(n)
		orig(c, id, n)
	}
	t.Cleanup(func() { h3ReleaseStreamBytes = orig })

	for _, mode := range []struct {
		name   string
		params quic.TransportParameters
		manual bool
	}{
		{"delivery", quic.DefaultParameters(), false},
		{"manual", mustTransportParameters(Config{}), true},
	} {
		t.Run(mode.name, func(t *testing.T) {
			released.Store(0)
			conn, _, _ := startH3(t, mode.params, func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
				res := make([]Response, b.Len())
				for i := range res {
					res[i] = Response{Status: 200, Body: []byte("ok")}
				}
				return sluice.Batch[Response]{Items: res}
			})
			var sent uint64
			for range 8 {
				req := h3Request("POST", "/upload", [2]string{"content-length", "4096"})
				req = appendH3Frame(req, h3Data, make([]byte, 4096))
				sent += uint64(len(req))
				if body, _ := readH3Body(t, askH3(t, conn, req)); body != "ok" {
					t.Fatalf("the answer was %q", body)
				}
			}
			want := uint64(0)
			if mode.manual {
				want = sent
			}
			if got := released.Load(); got != want {
				t.Errorf("released %d bytes back to the transport for %d delivered, want %d", got, sent, want)
			}
		})
	}
}

// ServeH3Conn hands crediting to itself when the windows allow it
// (quic.CreditOnRelease): a byte this layer never releases is then a byte
// the peer never gets credit back for. With the release swallowed, uploads
// past the connection window the server announced stall; were the
// transport still crediting on delivery, all of them would go through.
func TestH3ServeCreditsOnRelease(t *testing.T) {
	orig := h3ReleaseStreamBytes
	h3ReleaseStreamBytes = func(*quic.Conn, uint64, uint64) {}
	t.Cleanup(func() { h3ReleaseStreamBytes = orig })

	got := make(chan int, 8)
	conn, _, _ := startH3(t, mustTransportParameters(Config{}), func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
		for _, r := range b.Items {
			got <- len(r.Body)
		}
		return sluice.Batch[Response]{Items: make([]Response, b.Len())}
	})
	// Eight uploads whose sum (4.8 MiB) exceeds the connection window (just
	// over 4 MiB), as in TestH3UploadLargerThanTheInitialWindows.
	const bodyLen = 600 << 10
	done := 0
	for range 8 {
		s, err := conn.OpenStream(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		stream := h3Request("POST", "/upload", [2]string{"content-length", "614400"})
		stream = appendH3Frame(stream, h3Data, make([]byte, bodyLen))
		wrote := make(chan error, 1)
		go func() {
			if _, err := s.Write(stream); err != nil {
				wrote <- err
				return
			}
			wrote <- s.CloseWrite()
		}()
		select {
		case err := <-wrote:
			if err != nil {
				t.Fatalf("upload %d: %v", done, err)
			}
		case <-time.After(2 * time.Second):
			// Parked on credit that never comes.
		}
		// Each body reaches the handler before the next upload starts, so
		// this layer retains nothing: what stalls is the transport's credit.
		select {
		case <-got:
			done++
			continue
		case <-time.After(time.Second):
		}
		break
	}
	if done == 8 {
		t.Fatal("every upload went through with no byte released: the transport credited on delivery")
	}
	if done == 0 {
		t.Fatal("not even the first upload, inside the initial windows, reached the handler")
	}
}

// A handler that never returns does not hold ServeH3Conn past the grace: the
// cancellation drains, the grace passes, the connection is closed under the
// handler, whose context is cancelled, and ServeH3Conn returns ErrShutdownForced,
// as Serve does (ADR 0017 §2). It used to wait for the responder after the
// close, and the responder was inside the handler; then it returned nil, the
// same as a drain that ended in time.
func TestServeH3ReturnsAtTheGraceUnderAStuckHandler(t *testing.T) {
	const grace = 200 * time.Millisecond
	entered := make(chan struct{})
	release := make(chan struct{})
	handlerErr := make(chan error, 1)
	conn, cancel, served := startH3With(t, Config{ShutdownGrace: grace}, quic.DefaultParameters(), quic.DefaultParameters(), func(ctx context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
		close(entered)
		select {
		case <-ctx.Done():
		case <-release:
		}
		handlerErr <- ctx.Err()
		<-release
		res := make([]Response, b.Len())
		for i := range res {
			res[i] = Response{Status: 200, Body: []byte("late")}
		}
		return sluice.Batch[Response]{Items: res}
	})
	defer close(release)

	askH3(t, conn, h3Request("GET", "/stuck"))
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the handler never ran")
	}
	cancelled := time.Now()
	cancel()
	select {
	case err := <-served:
		if !errors.Is(err, ErrShutdownForced) {
			t.Errorf("ServeH3Conn returned %v, wanted ErrShutdownForced: the grace cut a request in flight", err)
		}
		if took := time.Since(cancelled); took < grace {
			t.Errorf("ServeH3Conn returned %v after the cancellation, before the %v grace", took, grace)
		}
	case <-time.After(2 * grace):
		t.Fatal("ServeH3Conn did not return after the grace: it waits on a handler that has not returned")
	}
	select {
	case err := <-handlerErr:
		if err == nil {
			t.Error("the handler's context was not cancelled by the forced stop")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the handler's context was never cancelled")
	}
}

// RFC 9114 §4.2.2 and §4.1: a request over one of the configured bounds is
// answered on its own stream — 431 for its headers, 413 for its body — with a
// clean FIN after STOP_SENDING H3_NO_ERROR, and the connection survives with
// the request beside it. A client behind a proxy acts on a status; a bare
// H3_EXCESSIVE_LOAD tells it nothing.
func TestH3AnswersAStatusOnAnOversizedStream(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		request []byte
		status  string
	}{
		{"a field section over MaxHeaderBytes", Config{MaxHeaderBytes: 4096, MaxHeaders: 128}, func() []byte {
			// A small section that decodes large: each indexed field line
			// naming the static table's accept-encoding entry is one byte on
			// the wire and 64 in the list, so the compressed bound is
			// nowhere near — and the field count stays under its own bound.
			block := appendQPACK(nil, []Header{
				{Name: []byte(":method"), Value: []byte("GET")},
				{Name: []byte(":path"), Value: []byte("/x")},
				{Name: []byte(":scheme"), Value: []byte("https")},
				{Name: []byte(":authority"), Value: []byte("h")},
			})
			for range 4096/64 + 1 {
				block = append(block, 0xc0|31)
			}
			return appendH3Frame(nil, h3Headers, block)
		}(), "431"},
		{"more than MaxHeaders fields", Config{MaxHeaders: 8}, func() []byte {
			extra := make([][2]string, 12)
			for i := range extra {
				extra[i] = [2]string{fmt.Sprintf("x-%d", i), "v"}
			}
			return h3Request("GET", "/x", extra...)
		}(), "431"},
		{"a body over MaxBodyBytes", Config{MaxBodyBytes: 1024}, func() []byte {
			// Two DATA frames each within the bound, whose sum is not: the
			// body bound, not the frame-length one.
			req := h3Request("POST", "/x")
			req = appendH3Frame(req, h3Data, bytes.Repeat([]byte{'b'}, 1024))
			return appendH3Frame(req, h3Data, bytes.Repeat([]byte{'b'}, 1024))
		}(), "413"},
		{"one DATA frame declared over MaxBodyBytes", Config{MaxBodyBytes: 1024}, func() []byte {
			// The frame's declared length is the whole refusal: a few bytes
			// of its payload follow, the rest never does. This used to end
			// the stream with H3_EXCESSIVE_LOAD, where h1 and h2 answer 413.
			req := h3Request("POST", "/x")
			req = quic.AppendVarint(req, h3Data)
			req = quic.AppendVarint(req, 1<<20)
			return append(req, "bbbb"...)
		}(), "413"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var served atomic.Int32
			conn, _, _ := startH3With(t, tt.cfg, quic.DefaultParameters(), quic.DefaultParameters(),
				func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
					served.Add(int32(b.Len())) //nolint:gosec // G115: a test's batch size
					res := make([]Response, b.Len())
					for i := range res {
						res[i] = Response{Status: 200}
					}
					return sluice.Batch[Response]{Items: res}
				})
			refused := askH3Open(t, conn, tt.request)
			healthy := askH3(t, conn, h3Request("GET", "/y"))

			// readH3Fields reads to a clean EOF: a RESET_STREAM would end it
			// with an error instead.
			if got := readH3Fields(t, refused)[":status"]; len(got) != 1 || got[0] != tt.status {
				t.Errorf("the oversized request got :status %q, want %s", got, tt.status)
			}
			if code, reset := refused.WasReset(); !reset || code != h3NoError {
				t.Errorf("the oversized request's stream: stop-sending %v with %#x, want H3_NO_ERROR", reset, code)
			}
			if got := readH3Fields(t, healthy)[":status"]; len(got) != 1 || got[0] != "200" {
				t.Errorf("the request beside it got :status %q, want 200", got)
			}
			if got := served.Load(); got != 1 {
				t.Errorf("the handler saw %d requests, want the one healthy request", got)
			}
		})
	}
}

// Config.IdleTimeout reaches an HTTP/3 connection as the transport's
// max_idle_timeout, through TransportParameters and nowhere else: a zero
// field is the two minutes every protocol defaults to, a set field is
// carried as is. Before this, the field was read by HTTP/1.1 and HTTP/2 and
// silently ignored by HTTP/3, which kept the transport's own 30s whatever
// Config said. The default is written out rather than read from
// defaultIdleTimeout: it is the value a proxy in front is set against.
func TestTransportParametersCarryIdleTimeout(t *testing.T) {
	if got := mustTransportParameters(Config{}).MaxIdleTimeout; got != 2*time.Minute {
		t.Errorf("zero IdleTimeout → max_idle_timeout %v, want 2m0s", got)
	}
	if got := mustTransportParameters(Config{IdleTimeout: 7 * time.Second}).MaxIdleTimeout; got != 7*time.Second {
		t.Errorf("IdleTimeout 7s → max_idle_timeout %v, want 7s", got)
	}
}

// RFC 9110 §15.5: the status the transport answers an oversized request
// with carries a one-line text/plain explanation, the status text, which
// HTTP/3 carries nowhere else; a HEAD read before the refusal states the
// line's length and type and gets no DATA (§9.3.2).
func TestH3TransportErrorCarriesBody(t *testing.T) {
	conn, _, _ := startH3With(t, Config{MaxBodyBytes: 1024}, quic.DefaultParameters(), quic.DefaultParameters(),
		func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
			return sluice.Batch[Response]{Items: make([]Response, b.Len())}
		})
	over := func(method string) []byte {
		return appendH3Frame(h3Request(method, "/x"), h3Data, bytes.Repeat([]byte{'b'}, 2048))
	}
	post := askH3(t, conn, over("POST"))
	head := askH3(t, conn, over("HEAD"))

	fields, body := readH3FieldsAndBody(t, post)
	if fields[":status"] != "413" || fields["content-type"] != "text/plain; charset=utf-8" || fields["content-length"] != "22" {
		t.Errorf("POST: fields %v, want 413 as text/plain with a length of 22", fields)
	}
	if body != "413 Content Too Large\n" {
		t.Errorf("POST: body %q, want the status text", body)
	}
	fields, body = readH3FieldsAndBody(t, head)
	if fields[":status"] != "413" || fields["content-length"] != "22" || body != "" {
		t.Errorf("HEAD: fields %v, body %q; want 413 with the length stated and no DATA", fields, body)
	}
}

// readH3FieldsAndBody reads a response stream to its end: the last value of
// each field, and the DATA payloads joined.
// askH3Open sends a request and leaves its stream open: the FIN never
// goes out. For a request the server refuses on its head, that makes the
// refusal land before the request's end for certain, which is what earns it
// a STOP_SENDING (RFC 9114 §4.1.1) — a FIN that reached the server first,
// as a Write followed by a CloseWrite may be coalesced into under load,
// leaves it nothing to stop and no STOP_SENDING is sent (Stream.StopSending
// on a finished receive side). The response is read as usual; the server's
// RESET_STREAM for the write side ends what the client did not close.
func askH3Open(t *testing.T, conn *quic.Conn, request []byte) *quic.Stream {
	t.Helper()
	s, err := conn.OpenStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(request); err != nil {
		if code, reset := s.WasReset(); !reset || code != h3NoError {
			t.Fatal(err)
		}
	}
	return s
}

func readH3FieldsAndBody(t *testing.T, s *quic.Stream) (fields map[string]string, body string) {
	t.Helper()
	_ = s.SetReadDeadline(time.Now().Add(10 * time.Second))
	raw, err := io.ReadAll(s)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("the response stream ended with %v", err)
	}
	frames, _, _, err := parseH3Frames(nil, raw, 1<<24)
	if err != nil {
		t.Fatalf("the response did not parse: %v", err)
	}
	fields = map[string]string{}
	d := newQPACKDecoder(nil, 0)
	for _, f := range frames {
		switch f.Type {
		case h3Headers:
			decoded, err := d.Decode(nil, f.Payload)
			if err != nil {
				t.Fatal(err)
			}
			for _, h := range decoded {
				fields[string(h.Name)] = string(h.Value)
			}
		case h3Data:
			body += string(f.Payload)
		}
	}
	return fields, body
}

// heldCalls is a handler that holds every call for /held until unblock and
// records the most requests it ever had in flight at once; exceeded is
// closed the first time that count passes limit. Any other target is echoed
// at once.
type heldCalls struct {
	limit    int64
	release  chan struct{}
	exceeded chan struct{}
	once     sync.Once
	released sync.Once
	running  atomic.Int64
	peak     atomic.Int64
}

func newHeldCalls(limit int) *heldCalls {
	return &heldCalls{limit: int64(limit), release: make(chan struct{}), exceeded: make(chan struct{})}
}

func (h *heldCalls) handle(ctx context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
	if string(b.Items[0].Target) != "/held" {
		return echoTarget(ctx, b)
	}
	n := h.running.Add(int64(b.Len()))
	for p := h.peak.Load(); n > p && !h.peak.CompareAndSwap(p, n); p = h.peak.Load() {
	}
	if n > h.limit {
		h.once.Do(func() { close(h.exceeded) })
	}
	<-h.release
	h.running.Add(-int64(b.Len()))
	return echoTarget(ctx, b)
}

func (h *heldCalls) unblock() { h.released.Do(func() { close(h.release) }) }

// sendH3 opens a request stream and sends a complete GET on it, FIN
// included. A write that fails is not the test's failure: the server may
// have refused the stream, or closed the connection, before the bytes left —
// which is what the caller then reads. A connection already closed yields
// no stream.
func sendH3(t *testing.T, conn *quic.Conn, target string) *quic.Stream {
	t.Helper()
	s, err := conn.OpenStream(t.Context())
	if err != nil {
		return nil
	}
	if _, err := s.Write(h3Request("GET", target)); err == nil {
		_ = s.CloseWrite()
	}
	return s
}

// h3CancelBurst sends n complete GET requests for /held, each cancelled at
// once with STOP_SENDING, as RFC 9114 §4.1.1 has a client do once its request
// is out. It stops early on a connection the server has closed.
func h3CancelBurst(t *testing.T, conn *quic.Conn, n int) {
	t.Helper()
	for range n {
		s := sendH3(t, conn, "/held")
		if s == nil {
			return
		}
		_ = s.StopSending(h3RequestCancelled)
	}
}

// settledH3 reads a stream on a goroutine and reports how it ended: the
// error of the Read that failed. A response that ends cleanly is io.EOF.
func settledH3(s *quic.Stream) <-chan error {
	done := make(chan error, 1)
	go func() {
		var buf [4096]byte
		for {
			if _, err := s.Read(buf[:]); err != nil {
				done <- err
				return
			}
		}
	}()
	return done
}

// RFC 9114 §4.1.1 has a client cancel a request whose HEADERS and FIN are out
// with STOP_SENDING alone, and the transport gives the stream's slot back the
// moment both directions are done (RFC 9000 §3.2) — before the handler called
// for that request has returned. The calls in flight on one connection are
// bounded by this layer's own count, not by the transport's streams: past
// MaxConcurrentStreams unfinished requests, cancelled or not, a new stream is
// refused with H3_REQUEST_REJECTED. Before 2026-10-20 the limit was read on
// the streams still assembling, and sixteen cancelled requests ran sixteen
// handler calls on a limit of eight: the rapid-reset shape (CVE-2023-44487)
// on HTTP/3.
func TestH3CancelledRequestsDoNotOutrunTheLimit(t *testing.T) {
	const limit = 8
	h := newHeldCalls(limit)
	conn, _, _ := startH3With(t, Config{MaxConcurrentStreams: limit}, quic.DefaultParameters(), quic.DefaultParameters(), h.handle)
	t.Cleanup(h.unblock)

	h3CancelBurst(t, conn, 2*limit)
	probe := sendH3(t, conn, "/held")
	if probe == nil {
		t.Fatalf("the connection closed with %v after %d cancelled requests on a limit of %d", conn.Err(), 2*limit, limit)
	}
	select {
	case err := <-settledH3(probe):
		if code, reset := probe.WasReset(); !reset || code != h3RequestRejected {
			t.Fatalf("the stream over the limit ended with %v (reset %v, code %#x), want H3_REQUEST_REJECTED (%#x)",
				err, reset, code, h3RequestRejected)
		}
	case <-h.exceeded:
		t.Fatalf("%d requests in flight in handler calls on a limit of %d", h.peak.Load(), limit)
	case <-time.After(10 * time.Second):
		t.Fatalf("the stream over the limit was neither refused nor answered; %d requests in flight", h.running.Load())
	}
	h.unblock()
	if p := h.peak.Load(); p > limit {
		t.Fatalf("%d requests in flight in handler calls at once, want at most %d", p, limit)
	}
	select {
	case <-conn.Done():
		t.Fatalf("the connection closed with %v; a client under the allowance keeps it", conn.Err())
	default:
	}
}

// A connection that only cancels is closed: each cancellation of a request
// this layer still holds, and each refusal, charges a flat allowance of four
// times MaxConcurrentStreams plus two per request the connection let the
// server answer — the HTTP/2 rapid-reset bound, with H3_EXCESSIVE_LOAD
// (RFC 9114 §8.1, §10.5) as its code. Forty cancelled requests on a limit of
// eight are forty charges against a flat thirty-two.
func TestH3AConnectionThatOnlyCancelsIsClosed(t *testing.T) {
	const limit = 8
	h := newHeldCalls(limit)
	conn, _, _ := startH3With(t, Config{MaxConcurrentStreams: limit}, quic.DefaultParameters(), quic.DefaultParameters(), h.handle)
	t.Cleanup(h.unblock)

	h3CancelBurst(t, conn, 5*limit)
	select {
	case <-conn.Done():
	case <-time.After(10 * time.Second):
		t.Fatalf("the connection stayed open after %d cancelled requests on a limit of %d", 5*limit, limit)
	}
	ce, ok := errors.AsType[*quic.ConnectionError](conn.Err())
	if !ok || !ce.Application {
		t.Fatalf("the connection ended with %v, want an application close", conn.Err())
	}
	if ce.Code != h3ExcessiveLoad {
		t.Fatalf("closed with %#x, want H3_EXCESSIVE_LOAD (%#x)", ce.Code, h3ExcessiveLoad)
	}
	h.unblock()
	if p := h.peak.Load(); p > limit {
		t.Fatalf("%d requests in flight in handler calls at once, want at most %d", p, limit)
	}
}

// Answered requests earn cancellations: two each, on top of the flat
// allowance. On a limit of two the flat part is eight; twelve requests
// answered in full raise it to thirty-two, and sixteen cancellations — which
// would have closed the connection alone — leave it open, the stream over
// the limit refused as usual.
func TestH3AnsweredRequestsEarnCancellations(t *testing.T) {
	const limit = 2
	h := newHeldCalls(limit)
	conn, _, _ := startH3With(t, Config{MaxConcurrentStreams: limit}, quic.DefaultParameters(), quic.DefaultParameters(), h.handle)
	t.Cleanup(h.unblock)

	for range 12 {
		if body, _ := readH3Body(t, askH3(t, conn, h3Request("GET", "/ok"))); body != "/ok" {
			t.Fatalf("answered %q, want %q", body, "/ok")
		}
	}
	h3CancelBurst(t, conn, 16)
	probe := sendH3(t, conn, "/held")
	if probe == nil {
		t.Fatalf("the connection closed with %v after 16 cancellations on 12 answered requests", conn.Err())
	}
	select {
	case <-settledH3(probe):
		if code, reset := probe.WasReset(); !reset || code != h3RequestRejected {
			t.Fatalf("the stream over the limit ended with reset %v, code %#x, want H3_REQUEST_REJECTED (%#x)", reset, code, h3RequestRejected)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("the stream over the limit was neither refused nor answered; the connection: %v", conn.Err())
	}
	select {
	case <-conn.Done():
		t.Fatalf("the connection closed with %v; 12 answered requests allow 32 cancellations", conn.Err())
	default:
	}
}

// readH3Frame reads exactly one HTTP/3 frame from s — its two varints, then
// its payload — leaving whatever follows unread, under a bound: a server
// that waits for the body before answering an Expect: 100-continue request
// is what the bound catches.
func readH3Frame(t *testing.T, s *quic.Stream) h3Frame {
	t.Helper()
	_ = s.SetReadDeadline(time.Now().Add(5 * time.Second))
	defer func() { _ = s.SetReadDeadline(time.Time{}) }()
	var prefix []byte
	for {
		var b [1]byte
		if _, err := io.ReadFull(s, b[:]); err != nil {
			t.Fatalf("no frame arrived on the stream: %v", err)
		}
		prefix = append(prefix, b[0])
		typ, after, err := quic.Varint(prefix)
		if err != nil {
			continue
		}
		length, _, err := quic.Varint(after)
		if err != nil {
			continue
		}
		payload := make([]byte, length)
		if _, err := io.ReadFull(s, payload); err != nil {
			t.Fatalf("the frame's payload did not arrive: %v", err)
		}
		return h3Frame{Type: typ, Payload: payload}
	}
}

// h3Status decodes a HEADERS frame's :status, as the peer would.
func h3Status(t *testing.T, f h3Frame) string {
	t.Helper()
	if f.Type != h3Headers {
		t.Fatalf("frame type %#x, want HEADERS", f.Type)
	}
	fields, err := newQPACKDecoder(nil, 0).Decode(nil, f.Payload)
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range fields {
		if string(h.Name) == ":status" {
			return string(h.Value)
		}
	}
	t.Fatal("a HEADERS frame without :status")
	return ""
}

// An HTTP/3 request whose head carries Expect: 100-continue and whose
// stream is not yet closed gets an interim 100 on its stream as soon as the
// head is complete, before any of the body has arrived — RFC 9110 §10.1.1's
// MUST ("an immediate final status or an immediate 100", "MUST NOT wait for
// the content"), carried in HTTP/3 as a HEADERS frame ahead of the final
// one (RFC 9114 §4.1). The body then follows and the final response comes
// after the 100, never before it. Before this, the assembler completed a
// request at its FIN only and nothing was written before that: a conformant
// client waited out its own timer.
func TestH3AnswersExpectContinueWithAnInterim100(t *testing.T) {
	conn, _, _ := startH3(t, quic.DefaultParameters(), echoTarget)
	s, err := conn.OpenStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(h3Request("POST", "/x", [2]string{"content-length", "2"}, [2]string{"expect", "100-continue"})); err != nil {
		t.Fatal(err)
	}

	if got := h3Status(t, readH3Frame(t, s)); got != "100" {
		t.Fatalf("the first HEADERS on the stream carries :status %s, want 100 before the body", got)
	}

	if _, err := s.WriteFinal(appendH3Frame(nil, h3Data, []byte("hi"))); err != nil {
		t.Fatal(err)
	}
	fields, body := readH3FieldsAndBody(t, s)
	if fields[":status"] != "200" || body != "/x" {
		t.Fatalf("after the body: :status %q body %q, want 200 and /x", fields[":status"], body)
	}
}

// The twin: a head that carries Expect: 100-continue and the FIN with it has
// no content to wait for, so no 100 is sent (RFC 9110 §10.1.1: the server
// MAY omit the 100 "if the framing indicates that there is no content"; a
// 100 the client is not waiting for is noise ahead of the only response it
// reads). The stream carries one HEADERS frame, the final one. The FIN is
// on the head's own STREAM frame, as a client that has no body sends it; a
// FIN that arrives in a later datagram is not told apart from a body to
// come, and the 100 then precedes the final response, which the MAY allows.
func TestH3SendsNoInterim100WhenTheHeadEndsTheStream(t *testing.T) {
	conn, _, _ := startH3(t, quic.DefaultParameters(), echoTarget)
	s, err := conn.OpenStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.WriteFinal(h3Request("POST", "/x", [2]string{"expect", "100-continue"})); err != nil {
		t.Fatal(err)
	}
	if got := readH3Fields(t, s)[":status"]; len(got) != 1 || got[0] != "200" {
		t.Fatalf("the stream carried :status %q, want 200 once and no 100", got)
	}
}

// The 501 of a well-formed CONNECT on the wire (RFC 9114 §4.4, RFC 9110
// §15.6.2): a status with its one line of text and a clean FIN after
// STOP_SENDING H3_NO_ERROR, the connection and the request beside it
// unharmed, the handler never called.
func TestH3AnswersAWellFormedConnectWith501(t *testing.T) {
	var served atomic.Int32
	conn, _, _ := startH3With(t, Config{}, quic.DefaultParameters(), quic.DefaultParameters(),
		func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
			served.Add(int32(b.Len())) //nolint:gosec // G115: a test's batch size
			res := make([]Response, b.Len())
			for i := range res {
				res[i] = Response{Status: 200}
			}
			return sluice.Batch[Response]{Items: res}
		})
	connect := appendH3Frame(nil, h3Headers, appendQPACK(nil, []Header{
		{Name: []byte(":method"), Value: []byte("CONNECT")},
		{Name: []byte(":authority"), Value: []byte("h:443")},
	}))
	refused := askH3Open(t, conn, connect)
	healthy := askH3(t, conn, h3Request("GET", "/y"))

	fields, body := readH3FieldsAndBody(t, refused)
	if fields[":status"] != "501" {
		t.Errorf("the CONNECT got :status %q, want 501", fields[":status"])
	}
	if want := fmt.Sprintf("%d %s\n", 501, reason(501)); body != want {
		t.Errorf("the CONNECT's body = %q, want %q", body, want)
	}
	if code, reset := refused.WasReset(); !reset || code != h3NoError {
		t.Errorf("the CONNECT's stream: stop-sending %v with %#x, want H3_NO_ERROR", reset, code)
	}
	if got := readH3Fields(t, healthy)[":status"]; len(got) != 1 || got[0] != "200" {
		t.Errorf("the request beside it got :status %q, want 200", got)
	}
	if got := served.Load(); got != 1 {
		t.Errorf("the handler saw %d requests, want the one GET and never the CONNECT", got)
	}
}
