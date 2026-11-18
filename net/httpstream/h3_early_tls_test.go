package httpstream

import (
	"context"
	"crypto/tls"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/net/quic"
)

// earlyTicketCache keeps the first ticket a full handshake leaves, so the
// next connection resumes with it.
type earlyTicketCache struct {
	mu     sync.Mutex
	cs     *tls.ClientSessionState
	frozen bool
	stored chan struct{}
}

func (c *earlyTicketCache) Get(string) (*tls.ClientSessionState, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cs, c.cs != nil
}

func (c *earlyTicketCache) Put(_ string, cs *tls.ClientSessionState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.frozen || cs == nil {
		return
	}
	c.cs = cs
	select {
	case c.stored <- struct{}{}:
	default:
	}
}

// heldPacketConn delivers nothing until release is closed: the client's
// handshake stays open while it sends in 0-RTT.
type heldPacketConn struct {
	net.PacketConn
	release chan struct{}
}

func (h *heldPacketConn) ReadFrom(b []byte) (int, net.Addr, error) {
	<-h.release
	return h.PacketConn.ReadFrom(b)
}

type earlyTLSSeen struct{ reqComplete, connComplete, resumed bool }

// A connection accepted in 0-RTT is served before its handshake completes:
// a request handled in early data carries the state of then, and one sent
// after the handshake carries HandshakeComplete, as the connection reports.
// serveH3 copied the state once, at the start, and every request of the
// connection carried that unfinished copy. The first connection, a full
// handshake, is the witness; the second resumes with 0-RTT, its handshake
// held open while a request is sent, then released before a second one.
func TestH3RequestTLSCompletesWithTheHandshake(t *testing.T) {
	serverPC, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	l, err := quic.NewListener(serverPC, h3ServerTLS(t), quic.DefaultConfig(), quic.ListenerConfig{EarlyData: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })

	var mu sync.Mutex
	var seen []earlyTLSSeen
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go func() {
				handler := func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
					out := make([]Response, b.Len())
					for i, r := range b.Items {
						st := conn.ConnectionState()
						mu.Lock()
						seen = append(seen, earlyTLSSeen{r.TLS.HandshakeComplete, st.HandshakeComplete, r.TLS.DidResume})
						mu.Unlock()
						out[i] = Response{Status: 200}
					}
					return sluice.Batch[Response]{Items: out}
				}
				_ = ServeH3Conn(t.Context(), conn, Config{BodyDelivery: BodyWhole, EarlyData: func(*Request) bool { return true }}, handler)
			}()
		}
	}()

	listen := func() net.PacketConn {
		pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = pc.Close() })
		return pc
	}
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Second)
	defer cancel()
	cache := &earlyTicketCache{stored: make(chan struct{}, 1)}
	cfg := h3ClientTLS()
	cfg.ClientSessionCache = cache

	// First connection: full handshake, a ticket, and the witness request.
	first, err := quic.Dial(ctx, listen(), l.Addr(), cfg, quic.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	askH3(t, first, h3Request("GET", "/first"))
	select {
	case <-cache.stored:
	case <-ctx.Done():
		t.Fatal("no ticket")
	}
	waitSeen := func(n int) {
		for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
			mu.Lock()
			got := len(seen)
			mu.Unlock()
			if got >= n {
				return
			}
		}
		t.Fatalf("the handler ran %d times, want %d", len(seen), n)
	}
	waitSeen(1)
	_ = first.Close()
	cache.mu.Lock()
	cache.frozen = true
	cache.mu.Unlock()

	// Second connection: resumed with 0-RTT, its handshake held open while the
	// request is sent, then released; a second request leaves afterwards.
	held := &heldPacketConn{PacketConn: listen(), release: make(chan struct{})}
	c, err := quic.DialEarly(ctx, held, l.Addr(), cfg, quic.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	select {
	case <-c.HandshakeComplete():
		t.Fatal("precondition lost: no 0-RTT")
	default:
	}
	askH3(t, c, h3Request("GET", "/early"))
	waitSeen(2) // allowed in early data: handled before the handshake completes
	close(held.release)
	select {
	case <-c.HandshakeComplete():
	case <-ctx.Done():
		t.Fatal("handshake did not complete")
	}
	askH3(t, c, h3Request("GET", "/late"))
	waitSeen(3)

	mu.Lock()
	defer mu.Unlock()
	t.Logf("seen (Request.TLS complete, Conn state complete, DidResume): %+v", seen)
	if !seen[0].reqComplete {
		t.Fatalf("witness: a 1-RTT connection's request carries an unfinished TLS state")
	}
	if seen[1].reqComplete || !seen[1].resumed {
		t.Fatalf("precondition: the early request carries HandshakeComplete=%v, DidResume=%v; want false and true", seen[1].reqComplete, seen[1].resumed)
	}
	if !seen[2].reqComplete {
		t.Fatalf("a request sent after the handshake completed carries Request.TLS.HandshakeComplete=false (the connection reports %v)", seen[2].connComplete)
	}
}
