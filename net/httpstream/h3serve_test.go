package httpstream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/internal/dgram"
	"github.com/mlagarrigue/sluice/net/quic"
)

// startH3 serves one HTTP/3 connection over an in-memory datagram pair, with
// the server's transport parameters given, and returns the client end, a
// cancel for the server's context, and what ServeH3 returned once it has.
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
		buf := make([]byte, 2048)
		if err := serverPC.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
			serverUp <- err
			return
		}
		n, peer, err := serverPC.ReadFrom(buf)
		if err != nil {
			serverUp <- err
			return
		}
		conn, err := quic.Accept(serverPC, quic.Datagram{Peer: peer, Payload: buf[:n]}, h3ServerTLS(t), params)
		if err != nil {
			serverUp <- err
			return
		}
		serverUp <- nil
		served <- ServeH3(ctx, conn, cfg, handle)
	}()

	conn, err := quic.Dial(clientPC, serverPC.LocalAddr(), h3ClientTLS(), clientParams)
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
// the server before ServeH3 opened stream 3 a STREAM_STATE_ERROR, which is
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
// of SETTINGS fails, and ServeH3 used to return that error with the
// connection left open — a client waiting on a server nobody runs, until
// the idle timeout. It is closed on the way out now, with the §6.2.1 code,
// like every other refusal at the top of ServeH3.
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
		buf := make([]byte, 2048)
		if err := serverPC.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
			serverUp <- err
			return
		}
		n, peer, err := serverPC.ReadFrom(buf)
		if err != nil {
			serverUp <- err
			return
		}
		conn, err := quic.Accept(serverPC, quic.Datagram{Peer: peer, Payload: buf[:n]}, h3ServerTLS(t), quic.DefaultParameters())
		if err != nil {
			serverUp <- err
			return
		}
		// Opened before ServeH3 runs, so the client's STOP_SENDING finds
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
		served <- ServeH3(t.Context(), conn, Config{}, func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
			return sluice.Batch[Response]{Items: make([]Response, b.Len())}
		})
	}()

	conn, err := quic.Dial(clientPC, serverPC.LocalAddr(), h3ClientTLS(), quic.DefaultParameters())
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
		t.Fatalf("ServeH3 returned %v, want ErrH3Protocol", err)
	}
}

// askH3 sends one request on a fresh stream and returns the stream.
func askH3(t *testing.T, conn *quic.Conn, request []byte) *quic.Stream {
	t.Helper()
	s, err := conn.OpenStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(request); err != nil {
		t.Fatal(err)
	}
	if err := s.CloseWrite(); err != nil {
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
	frames, _, err := parseH3Frames(nil, r.b, 1<<24)
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

// A producer blocked in its own code — not in its yield — does not hold
// ServeH3 once the connection has ended. It learns at its next yield, which
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
		t.Fatal("ServeH3 did not return after its connection ended: it waits on a blocked producer")
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
// crediting — the fallback ServeH3 keeps when the windows are too small for
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

// A handler that never returns does not hold ServeH3 past the grace: the
// cancellation drains, the grace passes, the connection is closed under the
// handler, whose context is cancelled, and ServeH3 returns ErrShutdownForced,
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
			t.Errorf("ServeH3 returned %v, wanted ErrShutdownForced: the grace cut a request in flight", err)
		}
		if took := time.Since(cancelled); took < grace {
			t.Errorf("ServeH3 returned %v after the cancellation, before the %v grace", took, grace)
		}
	case <-time.After(2 * grace):
		t.Fatal("ServeH3 did not return after the grace: it waits on a handler that has not returned")
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
			refused := askH3(t, conn, tt.request)
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
// field is the 30s every protocol defaults to, a set field is carried as
// is. Before this, the field was read by HTTP/1.1 and HTTP/2 and silently
// ignored by HTTP/3, which kept the transport's own 30s whatever Config said.
func TestTransportParametersCarryIdleTimeout(t *testing.T) {
	if got := mustTransportParameters(Config{}).MaxIdleTimeout; got != defaultIdleTimeout {
		t.Errorf("zero IdleTimeout → max_idle_timeout %v, want %v", got, defaultIdleTimeout)
	}
	if got := mustTransportParameters(Config{IdleTimeout: 7 * time.Second}).MaxIdleTimeout; got != 7*time.Second {
		t.Errorf("IdleTimeout 7s → max_idle_timeout %v, want 7s", got)
	}
}
