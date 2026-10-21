package httpstream

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
)

// A PING is not use. A peer that sends nothing but PING, SETTINGS or
// WINDOW_UPDATE used to restart the idle clock with each one and hold a
// connection — its goroutines, buffer and stream slots — for 17 bytes per
// IdleTimeout. The clock now runs from the last stream activity.
func TestH2PingAloneDoesNotKeepAnIdleConnection(t *testing.T) {
	addr, _ := serveH2(t, Config{IdleTimeout: 300 * time.Millisecond}, echoTarget)
	c := dialH2(t, addr)
	c.request(1, "GET", "/once")
	c.flush()
	if got := c.bodies(1)[1]; got != "/once" {
		t.Fatalf("stream 1 answered %q", got)
	}

	start := time.Now()
	tmp := make([]byte, 4096)
	for !c.sawGoAway && time.Since(start) < 3*time.Second {
		c.frame(h2Frame{Type: framePing, Payload: []byte("keepaliv")})
		c.flush()
		_ = c.conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		n, err := c.conn.Read(tmp)
		c.in = append(c.in, tmp[:n]...)
		c.parse()
		if err != nil && !errors.Is(err, io.EOF) {
			var ne net.Error
			if !errors.As(err, &ne) || !ne.Timeout() {
				break
			}
		}
	}
	if !c.sawGoAway {
		t.Fatal("a connection fed nothing but PINGs outlived three seconds of a 300ms IdleTimeout")
	}
	if code := goAwayCode(c.goAway); code != errNoError {
		t.Errorf("the idle close carried GOAWAY code %#x, want NO_ERROR", code)
	}
	if elapsed := time.Since(start); elapsed > 1500*time.Millisecond {
		t.Errorf("the idle connection was ended after %v of PINGs, want within one or two IdleTimeouts", elapsed)
	}
}

// A connection that has not sent a byte is not resting between two requests:
// it has ReadTimeout to send its first one, as a request has to arrive, not
// the IdleTimeout a keep-alive connection may rest for.
func TestSilentConnectionHasReadTimeoutToSpeak(t *testing.T) {
	cfg := Config{ReadTimeout: 200 * time.Millisecond, IdleTimeout: 10 * time.Second}
	t.Run("nothing at all", func(t *testing.T) {
		addr, _ := serveTest(t, cfg, echoTarget)
		c := dial(t, addr)
		_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
		start := time.Now()
		n, err := c.Read(make([]byte, 64))
		if n > 0 || !errors.Is(err, io.EOF) {
			t.Fatalf("read %d bytes, %v after %v; want a clean close within ReadTimeout", n, err, time.Since(start))
		}
	})
	t.Run("an HTTP/2 preface and SETTINGS, then nothing", func(t *testing.T) {
		addr, _ := serveTest(t, cfg, echoTarget)
		c := dialH2(t, addr)
		start := time.Now()
		payload := c.waitGoAway()
		if elapsed := time.Since(start); elapsed > 2*time.Second {
			t.Errorf("the silent HTTP/2 connection was ended after %v, want within ReadTimeout", elapsed)
		}
		if code := goAwayCode(payload); code != errNoError {
			t.Errorf("GOAWAY code %#x, want NO_ERROR", code)
		}
	})
}

// RFC 9113 §6.8: "Endpoints SHOULD always send a GOAWAY frame before closing
// a connection so that the remote peer can know whether a stream has been
// partially processed or not." Under Serve, an HTTP/2 connection with no
// stream yet — preface and SETTINGS exchanged, the first request perhaps on
// the wire — used to be cut at once by the shutdown, with no GOAWAY; it is
// now told like a busy one and closes itself at the PING's acknowledgement,
// and Serve still returns well before the grace.
func TestServeShutdownTellsAnIdleH2Connection(t *testing.T) {
	addr, cancel, done := serveUntilCancelled(t, Config{ShutdownGrace: 5 * time.Second}, echoTarget)
	c := dialH2(t, addr)
	c.pump(func() bool { return len(c.settings) > 0 })

	start := time.Now()
	cancel()
	c.pump(func() bool { return len(c.goAways) >= 1 && len(c.pinged) >= 1 })
	if len(c.goAways) < 1 || len(c.pinged) < 1 {
		t.Fatalf("after the cancellation: %d GOAWAY and %d PING; the idle connection was cut without being told", len(c.goAways), len(c.pinged))
	}
	if last := goAwayLast(c.goAways[0]); last != maxStreamID {
		t.Errorf("first GOAWAY names %d, want 2^31-1", last)
	}
	c.frame(h2Frame{Type: framePing, Flags: flagAck, Payload: c.pinged[0]})
	c.flush()
	c.pump(func() bool { return len(c.goAways) >= 2 })
	if len(c.goAways) < 2 || goAwayLast(c.goAways[1]) != 0 {
		t.Fatalf("second GOAWAY missing or not naming stream 0: %d GOAWAY", len(c.goAways))
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Serve waited the grace for an idle HTTP/2 connection that acknowledged the PING")
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Errorf("Serve returned after %v, want one round trip", took)
	}
}

// RFC 9113 §5.4.1: an endpoint that encounters a connection error "SHOULD
// first send a GOAWAY frame". The frame used to wait for every handler in
// flight to return; it is queued at once now.
func TestH2ConnectionErrorIsToldBeforeHandlersReturn(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	addr, _ := serveTest(t, Config{}, func(ctx context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
		close(entered)
		<-release
		return echoTarget(ctx, b)
	})
	t.Cleanup(func() { close(release) })
	c := dialH2(t, addr)
	c.request(1, "GET", "/slow")
	c.flush()
	<-entered

	c.frame(h2Frame{Type: frameData, StreamID: 99, Payload: []byte("x")})
	c.flush()
	start := time.Now()
	c.pump(func() bool { return c.sawGoAway })
	if !c.sawGoAway {
		t.Fatal("no GOAWAY while the handler was still running")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("the GOAWAY arrived after %v, want at once", elapsed)
	}
	if code := goAwayCode(c.goAway); code != errProtocolError {
		t.Errorf("GOAWAY code %#x, want PROTOCOL_ERROR", code)
	}
}
