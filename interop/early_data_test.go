package interop

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	quicgo "github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/net/httpstream"
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
				// Written the moment Accept returns, as an HTTP/3 server
				// writes its SETTINGS: a 0-RTT connection is handed out
				// before its handshake completes, and must already be able
				// to write in 1-RTT.
				if u, err := c.OpenUniStream(context.Background()); err == nil {
					_, _ = u.Write([]byte("first write"))
				}
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

// A ticket's 0-RTT is accepted once (RFC 9846 §8): presented again — a
// replay of the client's first flight, as far as the server can tell — it
// resumes the session in 1-RTT, the fourth time as the second. Until
// 2026-10-29 a refused presentation's end gave back the entry the admitted
// one held, and the third presentation had its 0-RTT accepted again.
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
	for i := 2; i <= 4; i++ {
		st := dialEarly(t, ctx, addr, cfg, false)
		if !st.TLS.DidResume {
			t.Errorf("presentation %d did not resume the session", i)
		}
		if st.Used0RTT {
			t.Errorf("presentation %d of the same ticket had its 0-RTT accepted again", i)
		}
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

// lateFinishPC delays every datagram the client writes once it has heard
// from the server, by delay: what the client sends in its first flight —
// ClientHello and 0-RTT — arrives at once, and its Finished arrives late,
// which holds the server's handshake open long enough to see what happens
// to a 0-RTT request before it completes.
type lateFinishPC struct {
	net.PacketConn // the interface only: a *net.UDPConn would let quic-go write around WriteTo
	delay          time.Duration
	heard          atomic.Bool
}

func (p *lateFinishPC) ReadFrom(b []byte) (int, net.Addr, error) {
	n, addr, err := p.PacketConn.ReadFrom(b)
	if err == nil {
		p.heard.Store(true)
	}
	return n, addr, err
}

func (p *lateFinishPC) WriteTo(b []byte, addr net.Addr) (int, error) {
	if !p.heard.Load() {
		return p.PacketConn.WriteTo(b, addr)
	}
	late := append([]byte(nil), b...)
	time.AfterFunc(p.delay, func() { _, _ = p.PacketConn.WriteTo(late, addr) })
	return len(b), nil
}

// TestHTTP3EarlyRequestWaitsForTheHandshake: quic-go sends a GET in 0-RTT
// and holds back its Finished. Without Config.EarlyData the handler is
// called only once the handshake has completed, since the request could be
// a replay; with a predicate that allows GET it is called before.
func TestHTTP3EarlyRequestWaitsForTheHandshake(t *testing.T) {
	for _, tc := range []struct {
		name          string
		early         func(*httpstream.Request) bool
		wantCompleted bool
	}{
		{"no predicate", nil, true},
		{"GET allowed", func(r *httpstream.Request) bool { return string(r.Method) == "GET" }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			cfg := httpstream.Config{IdleTimeout: timeout, ReadTimeout: timeout, WriteTimeout: timeout, EarlyData: tc.early}
			params, err := cfg.TransportParameters()
			if err != nil {
				t.Fatal(err)
			}
			pc := listenUDP(t)
			ln, err := quic.NewListener(pc, serverTLS(t), params, quic.ListenerConfig{EarlyData: true})
			if err != nil {
				t.Fatal(err)
			}
			defer ln.Close()
			type call struct{ early, completed bool }
			calls := make(chan call, 4)
			go func() {
				for {
					c, err := ln.Accept()
					if err != nil {
						return
					}
					var acceptedEarly bool
					select {
					case <-c.HandshakeComplete():
					default:
						acceptedEarly = true
					}
					handle := func(_ context.Context, b sluice.Batch[httpstream.Request]) sluice.Batch[httpstream.Response] {
						completed := false
						select {
						case <-c.HandshakeComplete():
							completed = true
						default:
						}
						calls <- call{acceptedEarly, completed}
						out := make([]httpstream.Response, b.Len())
						for i := range out {
							out[i] = httpstream.Response{Status: 200, Body: []byte("ok")}
						}
						return sluice.Batch[httpstream.Response]{Items: out}
					}
					go func() { _ = httpstream.ServeH3Conn(ctx, c, cfg, handle) }()
				}
			}()

			tlsCfg := clientTLS()
			tlsCfg.NextProtos = []string{"h3"}
			tlsCfg.ClientSessionCache = tls.NewLRUClientSessionCache(1)
			get := func(method string, pc net.PacketConn) {
				t.Helper()
				tr := &http3.Transport{
					TLSClientConfig: tlsCfg,
					QUICConfig:      &quicgo.Config{MaxIdleTimeout: timeout},
					Dial: func(ctx context.Context, _ string, tc *tls.Config, qc *quicgo.Config) (*quicgo.Conn, error) {
						return quicgo.DialEarly(ctx, pc, ln.Addr(), tc, qc)
					},
				}
				defer tr.Close()
				req, err := http.NewRequestWithContext(ctx, method, "https://"+ln.Addr().String()+"/x", http.NoBody)
				if err != nil {
					t.Fatal(err)
				}
				resp, err := tr.RoundTrip(req)
				if err != nil {
					t.Fatalf("%s: %v", method, err)
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					t.Fatalf("%s: %s", method, resp.Status)
				}
			}

			get(http.MethodGet, listenUDP(t))
			<-calls
			for deadline := time.Now().Add(timeout); ; {
				if _, ok := tlsCfg.ClientSessionCache.Get(tlsCfg.ServerName); ok {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("no session ticket reached quic-go")
				}
				time.Sleep(5 * time.Millisecond)
			}

			get(http3.MethodGet0RTT, &lateFinishPC{PacketConn: listenUDP(t), delay: 300 * time.Millisecond})
			got := <-calls
			if !got.early {
				t.Fatal("the resumed connection was not accepted in 0-RTT; the test proves nothing")
			}
			if got.completed != tc.wantCompleted {
				t.Errorf("the handler ran with the handshake completed = %v, want %v", got.completed, tc.wantCompleted)
			}
		})
	}
}

// sluiceDialEarly resumes from cfg's cached ticket with DialEarly, opens a
// stream at once and writes payload on it, and returns the connection, the
// stream, and whether DialEarly returned before the handshake completed.
func sluiceDialEarly(t *testing.T, ctx context.Context, addr net.Addr, cfg *tls.Config, payload string) (*quic.Conn, *quic.Stream, bool) {
	t.Helper()
	c, err := quic.DialEarly(ctx, listenUDP(t), addr, cfg, quic.DefaultParameters())
	if err != nil {
		t.Fatalf("DialEarly: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	early := false
	select {
	case <-c.HandshakeComplete():
	default:
		early = true
	}
	st, err := c.OpenStream(ctx)
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	if _, err := st.WriteFinal([]byte(payload)); err != nil {
		t.Fatalf("writing in 0-RTT: %v", err)
	}
	return c, st, early
}

// sluicePrime runs one full sluice connection so that cfg's cache holds the
// ticket the server issues, stored with the server's parameters.
func sluicePrime(t *testing.T, ctx context.Context, addr net.Addr, cfg *tls.Config) {
	t.Helper()
	c, err := quic.Dial(ctx, listenUDP(t), addr, cfg, quic.DefaultParameters())
	if err != nil {
		t.Fatalf("first handshake: %v", err)
	}
	defer c.Close()
	for deadline := time.Now().Add(timeout); ; {
		if _, ok := cfg.ClientSessionCache.Get(cfg.ServerName); ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("no session ticket reached the client")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestSluiceZeroRTTAgainstSluiceServer: DialEarly returns before the
// handshake ends, the request written then leaves in 0-RTT, the listener
// accepts it early, and the echo comes back.
func TestSluiceZeroRTTAgainstSluiceServer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	addr, accepted := earlyServer(t, serverTLS(t), quic.ListenerConfig{EarlyData: true})
	cfg := clientTLS()
	cfg.ClientSessionCache = tls.NewLRUClientSessionCache(1)
	sluicePrime(t, ctx, addr, cfg)
	<-accepted

	_, st, early := sluiceDialEarly(t, ctx, addr, cfg, "early request")
	if !early {
		t.Fatal("DialEarly waited for the handshake; no 0-RTT was attempted")
	}
	got, err := io.ReadAll(st)
	if err != nil {
		t.Fatalf("reading the echo: %v", err)
	}
	if string(got) != "early request" {
		t.Fatalf("echo = %q", got)
	}
	if !<-accepted {
		t.Error("the server did not accept the connection's 0-RTT")
	}
}

// TestSluiceZeroRTTAgainstQuicGoServer: the same against quic-go listening
// with Allow0RTT, which reports whether it used 0-RTT.
func TestSluiceZeroRTTAgainstQuicGoServer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	ln, err := quicgo.ListenEarly(listenUDP(t), serverTLS(t), &quicgo.Config{MaxIdleTimeout: timeout, Allow0RTT: true})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	used := make(chan bool, 4)
	go func() {
		for {
			c, err := ln.Accept(ctx)
			if err != nil {
				return
			}
			go func() {
				st, err := c.AcceptStream(ctx)
				if err != nil {
					return
				}
				_, _ = io.Copy(st, st)
				_ = st.Close()
				<-c.HandshakeComplete()
				used <- c.ConnectionState().Used0RTT
			}()
		}
	}()
	cfg := clientTLS()
	cfg.ClientSessionCache = tls.NewLRUClientSessionCache(1)
	sluicePrime(t, ctx, ln.Addr(), cfg)

	_, st, early := sluiceDialEarly(t, ctx, ln.Addr(), cfg, "early request")
	if !early {
		t.Fatal("DialEarly waited for the handshake; no 0-RTT was attempted")
	}
	got, err := io.ReadAll(st)
	if err != nil {
		t.Fatalf("reading the echo: %v", err)
	}
	if string(got) != "early request" {
		t.Fatalf("echo = %q", got)
	}
	if !<-used {
		t.Error("quic-go did not see the request in 0-RTT")
	}
}

// When the server refuses 0-RTT — here an instance that did not issue the
// ticket — the stream opened in 0-RTT fails with ErrEarlyDataRejected, and
// the connection carries a stream opened afterwards (RFC 9001 §4.6.2: the
// streams start over).
func TestSluiceZeroRTTRefused(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	key := [32]byte{4, 5, 6}
	cfgA, cfgB := serverTLS(t), serverTLS(t)
	cfgA.SetSessionTicketKeys([][32]byte{key})
	cfgB.SetSessionTicketKeys([][32]byte{key})
	addrA, _ := earlyServer(t, cfgA, quic.ListenerConfig{EarlyData: true})
	addrB, _ := earlyServer(t, cfgB, quic.ListenerConfig{EarlyData: true})
	cfg := clientTLS()
	cfg.ClientSessionCache = tls.NewLRUClientSessionCache(1)
	sluicePrime(t, ctx, addrA, cfg)

	c, st, early := sluiceDialEarly(t, ctx, addrB, cfg, "refused")
	if !early {
		t.Fatal("DialEarly waited for the handshake; no 0-RTT was attempted")
	}
	if _, err := io.ReadAll(st); !errors.Is(err, quic.ErrEarlyDataRejected) {
		t.Fatalf("the 0-RTT stream read %v, want ErrEarlyDataRejected", err)
	}
	if _, err := st.Write([]byte("more")); !errors.Is(err, quic.ErrEarlyDataRejected) {
		t.Errorf("a write on the refused stream returned %v, want ErrEarlyDataRejected", err)
	}
	again, err := c.OpenStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if again.ID() != st.ID() {
		t.Errorf("the stream after the refusal is %d, want %d: the streams start over", again.ID(), st.ID())
	}
	if _, err := again.WriteFinal([]byte("in 1-RTT")); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(again)
	if err != nil || string(got) != "in 1-RTT" {
		t.Fatalf("echo in 1-RTT = %q, %v", got, err)
	}
}

// Dial resumes without offering 0-RTT, so its ticket costs the server no
// admission: the same ticket presented by DialEarly afterwards still has its
// 0-RTT accepted.
func TestDialDoesNotSpendTheTicketsZeroRTT(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	addr, accepted := earlyServer(t, serverTLS(t), quic.ListenerConfig{EarlyData: true})
	cfg := clientTLS()
	cache := &stickyCache{cs: map[string]*tls.ClientSessionState{}}
	cfg.ClientSessionCache = cache
	sluicePrime(t, ctx, addr, cfg)
	<-accepted

	c, err := quic.Dial(ctx, listenUDP(t), addr, cfg, quic.DefaultParameters())
	if err != nil {
		t.Fatal(err)
	}
	if !c.ConnectionState().DidResume {
		t.Fatal("Dial did not resume")
	}
	_ = c.Close()

	_, st, early := sluiceDialEarly(t, ctx, addr, cfg, "after Dial")
	if !early {
		t.Fatal("DialEarly waited for the handshake")
	}
	// A refused 0-RTT fails the stream (ErrEarlyDataRejected): the echo is
	// the proof that the server accepted it.
	if _, err := io.ReadAll(st); err != nil {
		t.Fatalf("the ticket's 0-RTT was spent by Dial: %v", err)
	}
}

// A full ticket register refuses 0-RTT in the TLS handshake, as ADR 0019
// says: with room for one ticket, the second ticket's resumption is accepted
// without early_data — DidResume, not Used0RTT. Until 2026-10-27 the
// register's capacity was checked after crypto/tls had accepted the 0-RTT:
// the client saw early_data in the EncryptedExtensions (Used0RTT), the
// server kept no early key, and the 0-RTT packets were lost to a probe
// timeout.
func TestAFullTicketRegisterRefusesZeroRTTInTLS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	addr, _ := earlyServer(t, serverTLS(t), quic.ListenerConfig{EarlyData: true, MaxEarlyDataTickets: 1})

	first := clientTLS()
	first.ClientSessionCache = &stickyCache{cs: map[string]*tls.ClientSessionState{}}
	primeTicket(t, ctx, addr, first, first.ClientSessionCache)
	if st := dialEarly(t, ctx, addr, first, true); !st.Used0RTT {
		t.Fatal("the first ticket's 0-RTT was refused; the case under test is not reached")
	}

	// The register is full: the second ticket resumes the session, in
	// 1-RTT, and the refusal is in the handshake, not a silent drop.
	second := clientTLS()
	second.ClientSessionCache = &stickyCache{cs: map[string]*tls.ClientSessionState{}}
	primeTicket(t, ctx, addr, second, second.ClientSessionCache)
	st := dialEarly(t, ctx, addr, second, false) // a refusal invalidates quic-go's early streams: nothing is sent
	if !st.TLS.DidResume {
		t.Error("the second ticket did not resume the session")
	}
	if st.Used0RTT {
		t.Error("the second ticket had its 0-RTT accepted in TLS although the register was full: the server keeps no early key for it")
	}
}
