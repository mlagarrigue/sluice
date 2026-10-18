package quic_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/net/quic"
)

// idlePair is a handshaken pair whose server announces a 300 ms idle
// timeout; the effective timeout is the smaller announcement, so one side
// is enough to make silence fatal within the test's budget.
func idlePair(t *testing.T) (client, server *quic.Conn) {
	t.Helper()
	params := quic.DefaultParameters()
	params.MaxIdleTimeout = 300 * time.Millisecond
	client, server, _, _ = handshakePairParams(t, serverTLS(t), clientTLS(), params)
	return client, server
}

// With a keepalive armed under the idle timeout, a connection nobody uses
// for more than three idle timeouts is still there: a stream opened after
// the silence reaches the peer, and neither end saw an error.
func TestKeepAliveHoldsAnIdleConnection(t *testing.T) {
	client, server := idlePair(t)
	if err := client.SetKeepAlive(100 * time.Millisecond); err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Second)

	cs, err := client.OpenStream(t.Context())
	if err != nil {
		t.Fatalf("OpenStream after a second of silence: %v", err)
	}
	if _, err := cs.Write([]byte{'x'}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	ss, err := server.AcceptStream(ctx)
	if err != nil {
		t.Fatalf("AcceptStream after a second of keepalive: %v", err)
	}
	if ss.ID() != cs.ID() {
		t.Fatalf("accepted stream %d, the peer opened %d", ss.ID(), cs.ID())
	}
	if err := client.Err(); err != nil {
		t.Fatalf("client Err: %v", err)
	}
	if err := server.Err(); err != nil {
		t.Fatalf("server Err: %v", err)
	}
}

// The control: the same pair without a keepalive ends by the idle timeout
// well within a second, and AcceptStream reports the connection closed —
// not its own context — because the idle end is a quiet close, not an
// error (Err stays nil).
func TestIdleTimeoutEndsASilentConnection(t *testing.T) {
	_, server := idlePair(t)

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	s, err := server.AcceptStream(ctx)
	if err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("AcceptStream on a silent connection: stream %v, err %v; want the connection closed by idle", s, err)
	}
	if !strings.Contains(err.Error(), "the connection is closed") {
		t.Fatalf("AcceptStream on a silent connection: %v; want the connection closed", err)
	}
	if err := server.Err(); !errors.Is(err, quic.ErrIdleTimeout) {
		t.Fatalf("server Err after idle = %v, want ErrIdleTimeout", err)
	}
}

// KeepAlive reads the period in force: zero while off, the period asked
// for, half the negotiated idle timeout when the period asked for is
// longer. A negative period is refused and leaves the keepalive as it was.
func TestKeepAliveReportsTheEffectivePeriod(t *testing.T) {
	client, _ := idlePair(t)
	if got := client.KeepAlive(); got != 0 {
		t.Fatalf("KeepAlive before any SetKeepAlive = %v, want 0", got)
	}
	if err := client.SetKeepAlive(-time.Second); err == nil {
		t.Fatal("SetKeepAlive accepted a negative period")
	}
	if got := client.KeepAlive(); got != 0 {
		t.Fatalf("KeepAlive after a refused negative period = %v, want 0", got)
	}
	steps := []struct {
		period, want time.Duration
	}{
		{100 * time.Millisecond, 100 * time.Millisecond},
		{time.Second, 150 * time.Millisecond}, // half the 300 ms idle timeout
		{0, 0},
	}
	for _, s := range steps {
		if err := client.SetKeepAlive(s.period); err != nil {
			t.Fatalf("SetKeepAlive(%v): %v", s.period, err)
		}
		if got := client.KeepAlive(); got != s.want {
			t.Fatalf("KeepAlive after SetKeepAlive(%v) = %v, want %v", s.period, got, s.want)
		}
	}
	if err := client.SetKeepAlive(100 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := client.SetKeepAlive(-1); err == nil {
		t.Fatal("SetKeepAlive accepted a negative period")
	}
	if got := client.KeepAlive(); got != 100*time.Millisecond {
		t.Fatalf("KeepAlive after a refused negative period = %v, want the 100ms in force", got)
	}
}
