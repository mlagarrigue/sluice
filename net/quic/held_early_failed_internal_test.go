package quic

import (
	"context"
	"crypto/tls"
	"net"
	"testing"
	"time"
)

// A DialEarly that returns in 0-RTT and whose handshake then fails — the
// server forces a HelloRetryRequest and closes with decrypt_error, so the client enters the
// draining state — must still free its socket: Wait returns, and the
// caller's next ReadFrom on the socket reads, with HoldClosing as without:
// under it, no read loop held the closing state the failed handshake left,
// so Wait never returned and the socket kept an expired read deadline.
func TestAnEarlyDialWhoseHandshakeFailsFreesItsSocket(t *testing.T) {
	for _, hold := range []bool{false, true} {
		name := "default"
		if hold {
			name = "HoldClosing"
		}
		t.Run(name, func(t *testing.T) {
			serverPC, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
			if err != nil {
				t.Fatal(err)
			}
			serverCfg := ServerTLSForTest(t)
			serverCfg.CurvePreferences = []tls.CurveID{tls.CurveP256}
			l, err := NewListener(serverPC, serverCfg, DefaultConfig(), ListenerConfig{EarlyData: true})
			if err != nil {
				_ = serverPC.Close()
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = l.Close() })
			go echoEachStream(l)

			cache := &ticketLanded{ClientSessionCache: tls.NewLRUClientSessionCache(2), stored: make(chan struct{}, 1), forgetGroup: true}
			clientCfg := ClientTLSForTest()
			clientCfg.ClientSessionCache = cache
			clientCfg.CurvePreferences = []tls.CurveID{tls.CurveP256, tls.X25519}
			listen := func() *net.UDPConn {
				pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = pc.Close() })
				return pc
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			first, err := Dial(ctx, listen(), l.Addr(), clientCfg, DefaultConfig())
			if err != nil {
				t.Fatalf("first handshake: %v", err)
			}
			select {
			case <-cache.stored:
			case <-ctx.Done():
				t.Fatal("no session ticket reached the client")
			}
			_ = first.Close()

			clientCfg.CurvePreferences = []tls.CurveID{tls.X25519, tls.CurveP256}
			own := listen()
			held := &heldReads{PacketConn: own, release: make(chan struct{})}
			cfg := DefaultConfig()
			cfg.HoldClosing = hold
			c, err := DialEarly(ctx, held, l.Addr(), clientCfg, cfg)
			if err != nil {
				t.Fatalf("DialEarly: %v", err)
			}
			select {
			case <-c.HandshakeComplete():
				t.Fatal("precondition lost: DialEarly did not return in 0-RTT")
			default:
			}
			close(held.release)
			select {
			case <-c.Done():
			case <-c.HandshakeComplete():
				t.Fatal("precondition lost: the handshake completed")
			case <-ctx.Done():
				t.Fatal("precondition lost: the handshake neither failed nor completed")
			}

			// Wait under a context already ended: it must return.
			wctx, wcancel := context.WithCancel(context.Background())
			wcancel()
			waited := make(chan error, 1)
			go func() { waited <- c.Wait(wctx) }()
			select {
			case <-waited:
			case <-time.After(15 * time.Second):
				t.Errorf("Wait(ended ctx) never returned after a failed handshake (HoldClosing=%v)", hold)
			}

			// The caller's socket must read again.
			peer := listen()
			if _, err := peer.WriteTo([]byte("after"), own.LocalAddr()); err != nil {
				t.Fatal(err)
			}
			_ = own.SetWriteDeadline(time.Time{})
			got := make(chan error, 1)
			go func() {
				buf := make([]byte, 64)
				for {
					n, _, err := own.ReadFrom(buf)
					if err != nil || string(buf[:n]) == "after" {
						got <- err
						return
					}
				}
			}()
			select {
			case err := <-got:
				if err != nil {
					t.Errorf("ReadFrom on the socket left by the failed handshake (HoldClosing=%v): %v", hold, err)
				}
			case <-time.After(10 * time.Second):
				t.Errorf("ReadFrom on the socket left by the failed handshake never returned (HoldClosing=%v)", hold)
			}
		})
	}
}
