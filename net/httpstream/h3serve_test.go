package httpstream

import (
	"context"
	"errors"
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
	serverPC, clientPC := dgram.Pair()
	t.Cleanup(func() { _ = serverPC.Close(); _ = clientPC.Close() })

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	serverUp := make(chan error, 1)
	served := make(chan error, 1)
	go func() {
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
		conn, err := quic.Accept(serverPC, peer, buf[:n], h3ServerTLS(t), params)
		if err != nil {
			serverUp <- err
			return
		}
		serverUp <- nil
		served <- ServeH3(ctx, conn, Config{}, handle)
	}()

	conn, err := quic.Dial(clientPC, serverPC.LocalAddr(), h3ClientTLS(), quic.DefaultParameters())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := <-serverUp; err != nil {
		t.Fatalf("the server: %v", err)
	}
	return conn, cancel, served
}

// askH3 sends one request on a fresh stream and returns the stream.
func askH3(t *testing.T, conn *quic.Conn, request []byte) *quic.Stream {
	t.Helper()
	s, err := conn.OpenStream()
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
	conn, _, _ := startH3(t, quic.DefaultParameters(), func(b sluice.Batch[Request]) sluice.Batch[Response] {
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
// returns false.
func TestServeH3ReturnsWhileAProducerIsBlocked(t *testing.T) {
	blocked := make(chan struct{})
	release := make(chan struct{})
	yielded := make(chan bool, 1)
	conn, cancel, served := startH3(t, quic.DefaultParameters(), func(b sluice.Batch[Request]) sluice.Batch[Response] {
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
		{"manual", Config{}.TransportParameters(), true},
	} {
		t.Run(mode.name, func(t *testing.T) {
			released.Store(0)
			conn, _, _ := startH3(t, mode.params, func(b sluice.Batch[Request]) sluice.Batch[Response] {
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
