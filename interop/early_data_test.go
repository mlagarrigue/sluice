package interop

import (
	"bytes"
	"context"
	"crypto/tls"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	quicgo "github.com/quic-go/quic-go"

	"github.com/mlagarrigue/sluice/net/quic"
)

// earlyServer runs a sluice listener that echoes the first stream of each
// connection, and reports for each accepted connection whether Accept
// handed it out before its handshake completed.
func earlyServer(t *testing.T, cfg *tls.Config, lcfg quic.ListenerConfig) (addr net.Addr, early <-chan bool) {
	t.Helper()
	pc := listenUDP(t)
	ln, err := quic.NewListener(pc, cfg, quic.DefaultParameters(), lcfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	accepted := make(chan bool, 8)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			select {
			case <-c.HandshakeComplete():
				accepted <- false
			default:
				accepted <- true
			}
			go func() {
				defer c.Close()
				st, err := c.AcceptStream(context.Background())
				if err != nil {
					return
				}
				_, _ = io.Copy(st, st)
				_ = st.CloseWrite()
				<-c.Done()
			}()
		}
	}()
	return pc.LocalAddr(), accepted
}

// stickyCache keeps the first ticket it is given and ignores every later
// Put, removals included: a client that presents one ticket again and again,
// which is what a replay of its first flight looks like to the server.
type stickyCache struct {
	mu sync.Mutex
	cs map[string]*tls.ClientSessionState
}

func (c *stickyCache) Get(key string) (*tls.ClientSessionState, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cs, ok := c.cs[key]
	return cs, ok
}

func (c *stickyCache) Put(key string, cs *tls.ClientSessionState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.cs[key]; !ok && cs != nil {
		c.cs[key] = cs
	}
}

// primeTicket runs one full connection to addr so that cache holds the
// ticket the server issues on it.
func primeTicket(t *testing.T, ctx context.Context, addr net.Addr, cfg *tls.Config, cache tls.ClientSessionCache) {
	t.Helper()
	c, err := quicgo.Dial(ctx, listenUDP(t), addr, cfg, &quicgo.Config{MaxIdleTimeout: timeout})
	if err != nil {
		t.Fatalf("first handshake: %v", err)
	}
	defer c.CloseWithError(0, "")
	for deadline := time.Now().Add(timeout); ; {
		if _, ok := cache.Get(cfg.ServerName); ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("sluice issued no session ticket quic-go stored")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// dialEarly resumes from the cached ticket with quic-go's DialEarly and, when
// send is true, writes the payload on a stream opened before the handshake
// completes — in 0-RTT when the server accepts it — and checks the echo.
func dialEarly(t *testing.T, ctx context.Context, addr net.Addr, cfg *tls.Config, send bool) quicgo.ConnectionState {
	t.Helper()
	c, err := quicgo.DialEarly(ctx, listenUDP(t), addr, cfg, &quicgo.Config{MaxIdleTimeout: timeout})
	if err != nil {
		t.Fatalf("DialEarly: %v", err)
	}
	defer c.CloseWithError(0, "")
	if send {
		st, err := c.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		want := []byte("early request")
		if _, err := st.Write(want); err != nil {
			t.Fatal(err)
		}
		if err := st.Close(); err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(st)
		if err != nil {
			t.Fatalf("reading the echo: %v", err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("echo = %q, want %q", got, want)
		}
	}
	select {
	case <-c.HandshakeComplete():
	case <-ctx.Done():
		t.Fatal("the resumed handshake never completed")
	}
	return c.ConnectionState()
}

// TestQuicGoZeroRTTAgainstSluiceServer: a listener with EarlyData issues a
// ticket that allows 0-RTT, quic-go resumes with DialEarly and writes its
// request before the handshake ends, sluice accepts the 0-RTT and hands the
// connection to Accept before the handshake completes, and the request
// written in 0-RTT is answered.
func TestQuicGoZeroRTTAgainstSluiceServer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	addr, early := earlyServer(t, serverTLS(t), quic.ListenerConfig{EarlyData: true})

	cfg := clientTLS()
	cfg.ClientSessionCache = tls.NewLRUClientSessionCache(1)
	primeTicket(t, ctx, addr, cfg, cfg.ClientSessionCache)
	if <-early {
		t.Fatal("the first connection was accepted before its handshake completed, with no 0-RTT to read")
	}

	st := dialEarly(t, ctx, addr, cfg, true)
	if !st.TLS.DidResume || !st.Used0RTT {
		t.Fatalf("DidResume = %v, Used0RTT = %v; want both", st.TLS.DidResume, st.Used0RTT)
	}
	if !<-early {
		t.Error("the 0-RTT connection was accepted only once its handshake completed")
	}
}

// A ticket's 0-RTT is accepted once (RFC 8446 §8): presented a second time —
// a replay of the client's first flight, as far as the server can tell —
// it resumes the session in 1-RTT.
func TestSluiceAcceptsATicketsZeroRTTOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	addr, _ := earlyServer(t, serverTLS(t), quic.ListenerConfig{EarlyData: true})

	cfg := clientTLS()
	cache := &stickyCache{cs: map[string]*tls.ClientSessionState{}}
	cfg.ClientSessionCache = cache
	primeTicket(t, ctx, addr, cfg, cache)

	if st := dialEarly(t, ctx, addr, cfg, false); !st.Used0RTT {
		t.Fatal("the ticket's first 0-RTT was refused")
	}
	st := dialEarly(t, ctx, addr, cfg, false)
	if !st.TLS.DidResume {
		t.Error("the second presentation did not resume the session")
	}
	if st.Used0RTT {
		t.Error("the same ticket had its 0-RTT accepted twice")
	}
}

// A ticket is good for 0-RTT only at the listener that issued it (ADR 0019):
// another instance holding the same ticket keys resumes the session without
// it, whether it serves 0-RTT itself or only tickets — where, before, a
// ticket marked for early data had crypto/tls accept 0-RTT that net/quic
// then never read.
func TestZeroRTTStaysWithTheInstanceThatIssuedTheTicket(t *testing.T) {
	for _, lcfg := range []quic.ListenerConfig{{EarlyData: true}, {SessionTickets: true}} {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		key := [32]byte{1, 2, 3}
		cfgA, cfgB := serverTLS(t), serverTLS(t)
		cfgA.SetSessionTicketKeys([][32]byte{key})
		cfgB.SetSessionTicketKeys([][32]byte{key})
		addrA, _ := earlyServer(t, cfgA, quic.ListenerConfig{EarlyData: true})
		addrB, _ := earlyServer(t, cfgB, lcfg)

		cfg := clientTLS()
		cfg.ClientSessionCache = tls.NewLRUClientSessionCache(1)
		primeTicket(t, ctx, addrA, cfg, cfg.ClientSessionCache)
		st := dialEarly(t, ctx, addrB, cfg, false)
		if !st.TLS.DidResume {
			t.Errorf("EarlyData %v: the other instance did not resume the session its keys open", lcfg.EarlyData)
		}
		if st.Used0RTT {
			t.Errorf("EarlyData %v: the other instance accepted 0-RTT on a ticket it did not issue", lcfg.EarlyData)
		}
		cancel()
	}
}
