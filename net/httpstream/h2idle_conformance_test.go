package httpstream

import (
	"errors"
	"io"
	"net"
	"testing"
	"time"
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
