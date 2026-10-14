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
	client.SetKeepAlive(100 * time.Millisecond)

	time.Sleep(time.Second)

	cs, err := client.OpenStream()
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
	if err := server.Err(); err != nil {
		t.Fatalf("server Err after idle: %v; an idle end is not an error", err)
	}
}
