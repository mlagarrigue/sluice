package httpstream

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/internal/dgram"
	"github.com/mlagarrigue/sluice/net/quic"
)

// The HTTP/3 side of TestH2PingAloneDoesNotKeepAnIdleConnection. QUIC's own
// idle timer restarts on every packet from the peer, PING included (RFC 9000
// §10.1), so a client that pinged once per period used to hold an HTTP/3
// connection — and its place under the listener's MaxConns and
// MaxConnsPerIP — with no request open, where HTTP/2 closed it. The
// connection now keeps an idle clock of its own that counts requests, not
// packets, and ends with GOAWAY then H3_NO_ERROR (or its grease) after
// between one and two IdleTimeouts without one.
func TestH3PingAloneDoesNotKeepAnIdleConnection(t *testing.T) {
	const idle = 300 * time.Millisecond
	serverPC, clientPC := dgram.Pair()
	defer func() { _ = serverPC.Close() }()
	defer func() { _ = clientPC.Close() }()

	served := make(chan error, 1)
	go func() {
		conn, err := acceptOne(t, serverPC, h3ServerTLS(t), quic.DefaultConfig())
		if err != nil {
			served <- err
			return
		}
		served <- ServeH3Conn(t.Context(), conn, Config{IdleTimeout: idle}, echoTarget)
	}()

	conn, err := quic.Dial(t.Context(), clientPC, serverPC.LocalAddr(), h3ClientTLS(), quic.DefaultConfig())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	// One request, answered in full: the connection owes nothing after it.
	s, err := conn.OpenStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.WriteFinal(h3Request("GET", "/once")); err != nil {
		t.Fatal(err)
	}
	if err := s.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(s); err != nil {
		t.Fatalf("reading the answer: %v", err)
	}

	// Then nothing but a QUIC PING every third of an IdleTimeout, each one
	// acknowledged, which is what restarts the transport's idle timer.
	start := time.Now()
	for time.Since(start) < 10*idle {
		select {
		case <-conn.Done():
		case <-time.After(idle / 3):
			if err := conn.Ping(t.Context()); err == nil {
				continue
			}
		}
		break
	}
	select {
	case <-conn.Done():
	case <-time.After(idle):
		t.Fatalf("a connection fed nothing but PINGs outlived %v of a %v IdleTimeout", 10*idle, idle)
	}
	if elapsed := time.Since(start); elapsed > 5*idle {
		t.Errorf("the idle connection was ended after %v of PINGs, want within one or two IdleTimeouts", elapsed)
	}
	// The close is the graceful one: an application code, H3_NO_ERROR or a
	// reserved one in its place, after a GOAWAY naming the stream after the
	// request.
	ce, ok := errors.AsType[*quic.ConnectionError](conn.Err())
	if !ok || !ce.Application || (ce.Code != h3NoError && !reserved(ce.Code)) {
		t.Errorf("the idle close was %v, want H3_NO_ERROR or its grease", conn.Err())
	}
	if ids := readH3GoAways(t, h3StreamOf(t, conn, 3), 1); ids[len(ids)-1] != 4 {
		t.Errorf("GOAWAY %v before the idle close, want the stream after the request, 4", ids)
	}
	select {
	case err := <-served:
		if err != nil {
			t.Errorf("ServeH3Conn returned %v for an idle close", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ServeH3Conn did not return after the idle close")
	}
}

// The idle clock does not run while this end owes an answer: a handler that
// takes four IdleTimeouts is answered, not cut, as over HTTP/2.
func TestH3IdleClockWaitsForTheAnswerInFlight(t *testing.T) {
	const idle = 200 * time.Millisecond
	serverPC, clientPC := dgram.Pair()
	defer func() { _ = serverPC.Close() }()
	defer func() { _ = clientPC.Close() }()
	go func() {
		conn, err := acceptOne(t, serverPC, h3ServerTLS(t), quic.DefaultConfig())
		if err != nil {
			return
		}
		_ = ServeH3Conn(t.Context(), conn, Config{IdleTimeout: idle}, func(ctx context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
			time.Sleep(4 * idle)
			return echoTarget(ctx, b)
		})
	}()
	conn, err := quic.Dial(t.Context(), clientPC, serverPC.LocalAddr(), h3ClientTLS(), quic.DefaultConfig())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	s, err := conn.OpenStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.WriteFinal(h3Request("GET", "/slow")); err != nil {
		t.Fatal(err)
	}
	if err := s.SetReadDeadline(time.Now().Add(10 * idle)); err != nil {
		t.Fatal(err)
	}
	reply, err := io.ReadAll(s)
	if err != nil {
		t.Fatalf("the answer owed for four IdleTimeouts was cut: %v", err)
	}
	frames, _, _, err := parseH3Frames(nil, reply, 1<<20)
	if err != nil {
		t.Fatalf("the answer did not parse: %v", err)
	}
	var body string
	for _, f := range frames {
		if f.Type == h3Data {
			body = string(f.Payload)
		}
	}
	if body != "/slow" {
		t.Errorf("the answer carried %q, want /slow", body)
	}
}
