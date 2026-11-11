package quic_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/net/quic"
)

// Config.HoldClosing is what the godoc recommends to "a caller that keeps
// the socket open to dial again on it". A Dial under it whose handshake
// fails enters the held closing state, and no read loop was ever started to
// hold it and then leave it: the jolt's expired read deadline stayed on the
// caller's socket. The failed handshake now holds the state itself, as the
// read loop would: the caller's next ReadFrom on the socket reads the
// datagram sent to it, as TestASocketADialFailedOnHasNoDeadlineLeft has it
// without HoldClosing.
func TestASocketAHeldDialFailedOnHasNoDeadlineLeft(t *testing.T) {
	// A peer that reads nothing: the handshake never completes.
	silent, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer silent.Close()
	own, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer own.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	cfg := quic.DefaultConfig()
	cfg.HoldClosing = true
	if c, err := quic.Dial(ctx, own, silent.LocalAddr(), clientTLS(), cfg); err == nil {
		_ = c.Close()
		t.Fatal("precondition lost: Dial to a silent peer succeeded")
	}

	// Well past any closing period a handshake that never heard the peer
	// could hold (three probe timeouts from the initial RTT).
	time.Sleep(5 * time.Second)

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
			t.Fatalf("ReadFrom on the socket a failed Dial under HoldClosing left behind: %v", err)
		}
	case <-time.After(10 * time.Second):
		_ = own.Close()
		t.Fatal("ReadFrom on the socket a failed Dial under HoldClosing left behind never returned")
	}
}
