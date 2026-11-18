package httpstream

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/internal/dgram"
	"github.com/mlagarrigue/sluice/net/quic"
)

// mutedWrites drops every datagram its end sends once muted: a peer that
// stops acknowledging.
type mutedWrites struct {
	net.PacketConn
	muted atomic.Bool
}

func (m *mutedWrites) WriteTo(p []byte, a net.Addr) (int, error) {
	if m.muted.Load() {
		return len(p), nil
	}
	return m.PacketConn.WriteTo(p, a)
}

// drainIdle serves a connection that never sends a request, then cancels
// the server; with mute, the client acknowledges nothing from then on.
func drainIdle(t *testing.T, mute bool) error {
	t.Helper()
	serverPC, clientRaw := dgram.Pair()
	clientPC := &mutedWrites{PacketConn: clientRaw}
	t.Cleanup(func() { _ = serverPC.Close(); _ = clientPC.Close() })
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	served := make(chan error, 1)
	up := make(chan struct{})
	go func() {
		conn, err := acceptOne(t, serverPC, h3ServerTLS(t), quic.DefaultConfig())
		if err != nil {
			served <- err
			return
		}
		close(up)
		served <- ServeH3Conn(ctx, conn, Config{ShutdownGrace: 300 * time.Millisecond}, h3OKHandler)
	}()
	conn, err := quic.Dial(context.Background(), clientPC, serverPC.LocalAddr(), h3ClientTLS(), quic.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	<-up
	// The pair has no delay: 200 ms is many round trips, for SETTINGS and
	// the client's own unidirectional streams to cross.
	time.Sleep(200 * time.Millisecond)
	if mute {
		clientPC.muted.Store(true)
	}
	cancel()
	select {
	case err := <-served:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("ServeH3Conn did not return")
		return nil
	}
}

// ErrShutdownForced says the grace passed with requests in flight. A
// connection that never carried a request, whose client lets the drain's
// PING go unacknowledged, is closed at the grace with nothing in flight:
// ServeH3Conn returns nil for it, as for an idle client that acknowledges,
// and logs no warning about requests in flight.
func TestH3IdleDrainUnacknowledgedIsNotForced(t *testing.T) {
	if err := drainIdle(t, false); err != nil {
		t.Fatalf("witness: an idle, acknowledging connection drained with %v, want nil", err)
	}
	if err := drainIdle(t, true); err != nil {
		t.Fatalf("an idle connection whose client went silent drained with %v, want nil", err)
	}
}
