package quic

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ListenerConfig.MaxConnsPerIP: "only connections that proved the address
// are refused at the bound". The admission counts the address's
// connections less its unproven handshakes, and an Initial that comes back
// with a token is admitted over those: when such a handshake then
// completes, it proves the address past the bound, and is refused as an
// Initial at the bound is. Without that, one address ended with twice the
// bound in established connections.
//
// MaxConnsPerIP is 1. Client A's handshake goes through a relay that holds
// the server's datagrams; client B dials directly from the same address, is
// sent a Retry, comes back with its token and completes. Then the relay lets
// A's datagrams through and A's handshake completes, past the bound. The
// control has A complete first: B is then refused at admission.
func TestAHandshakeProvenPastTheAddressBoundIsRefused(t *testing.T) {
	run := func(t *testing.T, holdA bool) (accepted int, bErr error) {
		t.Helper()
		spc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
		if err != nil {
			t.Fatal(err)
		}
		l, err := NewListener(spc, ServerTLSForTest(t), DefaultConfig(), ListenerConfig{MaxConnsPerIP: 1})
		if err != nil {
			t.Fatal(err)
		}
		defer l.Close()
		var n atomic.Int32
		go func() {
			for {
				c, err := l.Accept()
				if err != nil {
					return
				}
				n.Add(1)
				t.Cleanup(func() { _ = c.Close() })
			}
		}()

		// The relay: A talks to rpc, rpc talks to the listener from its own
		// port on the same address. Server-to-client datagrams wait on
		// release while holdA.
		rpc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
		if err != nil {
			t.Fatal(err)
		}
		defer rpc.Close()
		apc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
		if err != nil {
			t.Fatal(err)
		}
		defer apc.Close()
		release := make(chan struct{})
		var mu sync.Mutex
		var held [][]byte
		released := false
		go func() {
			buf := make([]byte, 65536)
			for {
				k, from, err := rpc.ReadFromUDP(buf)
				if err != nil {
					return
				}
				d := append([]byte(nil), buf[:k]...)
				if from.Port == apc.LocalAddr().(*net.UDPAddr).Port {
					_, _ = rpc.WriteTo(d, spc.LocalAddr())
					continue
				}
				mu.Lock()
				if holdA && !released {
					held = append(held, d)
					mu.Unlock()
					continue
				}
				mu.Unlock()
				_, _ = rpc.WriteTo(d, apc.LocalAddr())
			}
		}()
		go func() {
			<-release
			mu.Lock()
			released = true
			q := held
			held = nil
			mu.Unlock()
			for _, d := range q {
				_, _ = rpc.WriteTo(d, apc.LocalAddr())
			}
		}()

		aDone := make(chan error, 1)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			c, err := Dial(ctx, apc, rpc.LocalAddr(), ClientTLSForTest(), DefaultConfig())
			if err == nil {
				t.Cleanup(func() { _ = c.Close() })
			}
			aDone <- err
		}()
		if !holdA {
			close(release)
			if err := <-aDone; err != nil {
				t.Fatalf("control: A did not complete: %v", err)
			}
		} else {
			// A's Initial is admitted, unproven, before B arrives.
			deadline := time.Now().Add(5 * time.Second)
			for {
				l.mu.Lock()
				k := len(l.unvalidated)
				l.mu.Unlock()
				if k == 1 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("A's handshake was never admitted unproven: the probe does not exercise the bound")
				}
				time.Sleep(time.Millisecond)
			}
		}

		bpc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
		if err != nil {
			t.Fatal(err)
		}
		defer bpc.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		b, bErr := Dial(ctx, bpc, spc.LocalAddr(), ClientTLSForTest(), DefaultConfig())
		if bErr == nil {
			t.Cleanup(func() { _ = b.Close() })
		}
		if holdA {
			close(release)
			<-aDone // refused by the server once its handshake completes, or just after
		}
		deadline := time.Now().Add(5 * time.Second)
		want := int32(1)
		if bErr == nil {
			want = 2
		}
		for n.Load() < want && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		l.mu.Lock()
		byIP := 0
		for _, v := range l.connsByIP {
			byIP += v
		}
		l.mu.Unlock()
		t.Logf("holdA=%v: B err=%v, %d accepted, connsByIP total %d", holdA, bErr, n.Load(), byIP)
		return int(n.Load()), bErr
	}

	t.Run("control, A completes first", func(t *testing.T) {
		accepted, bErr := run(t, false)
		if bErr == nil || accepted != 1 {
			t.Fatalf("control: B err=%v, %d accepted; want B refused and one connection — the control is void", bErr, accepted)
		}
	})
	t.Run("A held unproven while B comes back with a token", func(t *testing.T) {
		accepted, _ := run(t, true)
		if accepted > 1 {
			t.Errorf("%d connections established from one address under MaxConnsPerIP 1", accepted)
		}
	})
}
