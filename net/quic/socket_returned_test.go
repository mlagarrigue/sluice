package quic_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/net/quic"
)

// A connection that owns its socket hands it back to the caller once Wait
// returns, as the godoc says. The jolts that end its read loop (Close's, and
// Wait's) leave an expired read deadline behind, and nothing cleared it: a
// NewListener on the same socket, whose read loop never sets one, or a read
// of the caller's own, failed with os.ErrDeadlineExceeded on every ReadFrom.
func TestASocketHandedBackHasNoDeadlineLeft(t *testing.T) {
	udp, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	l, err := quic.NewListener(udp, serverTLS(t), quic.DefaultParameters(), quic.ListenerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	own, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer own.Close()
	// A held closing period, cut short by a Wait whose ctx ends: the path
	// that leaves the socket with Wait's expired deadline.
	holding := quic.DefaultParameters()
	holding.HoldClosing = true
	client, err := quic.Dial(context.Background(), own, l.Addr(), clientTLS(), holding)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := l.Accept(); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_ = client.Wait(ctx) // ctx's error: the period was cut short, the socket freed

	peer, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	if _, err := peer.WriteTo([]byte("after"), own.LocalAddr()); err != nil {
		t.Fatal(err)
	}
	got := make(chan error, 1)
	go func() {
		buf := make([]byte, 64)
		n, _, err := own.ReadFrom(buf)
		if err == nil && string(buf[:n]) != "after" {
			err = net.UnknownNetworkError(string(buf[:n]))
		}
		got <- err
	}()
	select {
	case err := <-got:
		if err != nil {
			t.Fatalf("ReadFrom on the socket handed back: %v", err)
		}
	case <-time.After(5 * time.Second):
		_ = own.Close()
		t.Fatal("ReadFrom on the socket handed back never returned")
	}
}
