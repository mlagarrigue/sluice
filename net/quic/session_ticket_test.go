package quic_test

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/net/quic"
)

// ticketCache is a client session cache that says when a ticket lands, so a
// test waits for the server's NewSessionTicket rather than for a guess.
type ticketCache struct {
	tls.ClientSessionCache
	stored chan struct{}
}

func newTicketCache() *ticketCache {
	return &ticketCache{ClientSessionCache: tls.NewLRUClientSessionCache(4), stored: make(chan struct{}, 4)}
}

func (c *ticketCache) Put(key string, cs *tls.ClientSessionState) {
	c.ClientSessionCache.Put(key, cs)
	if cs != nil {
		c.stored <- struct{}{}
	}
}

// echoListener serves every connection l accepts with an echo per stream.
func echoListener(t *testing.T, l *quic.Listener) {
	t.Helper()
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				for {
					s, err := c.AcceptStream(context.Background())
					if err != nil {
						return
					}
					buf := make([]byte, 64)
					n, _ := s.Read(buf)
					_, _ = s.Write(buf[:n])
					_ = s.CloseWrite()
				}
			}()
		}
	}()
}

// dialCached dials l's address with cache as the client's session cache and
// proves the connection carries a stream.
func dialCached(t *testing.T, addr net.Addr, cache tls.ClientSessionCache) *quic.Conn {
	t.Helper()
	cfg := clientTLS()
	cfg.ClientSessionCache = cache
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pc.Close() })
	conn, err := quic.Dial(context.Background(), pc, addr, cfg, quic.DefaultParameters())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := echoOnce(t, conn, "ping"); err != nil {
		t.Fatal(err)
	}
	return conn
}

// A listener asked for session tickets sends one after the handshake (RFC
// 9001 §4.5), and the client that presents it on its next connection
// resumes: the handshake completes as a PSK resumption.
func TestSessionTicketsResumeTheSession(t *testing.T) {
	l, addr := newUDPListener(t, quic.ListenerConfig{SessionTickets: true})
	echoListener(t, l)
	cache := newTicketCache()

	first := dialCached(t, addr, cache)
	if first.ConnectionState().DidResume {
		t.Fatal("the first connection resumed, with nothing to resume")
	}
	select {
	case <-cache.stored:
	case <-time.After(10 * time.Second):
		t.Fatal("no session ticket reached the client")
	}
	_ = first.Close()

	second := dialCached(t, addr, cache)
	if !second.ConnectionState().DidResume {
		t.Error("the second connection did not resume the session its ticket names")
	}
	select {
	case <-cache.stored:
	case <-time.After(10 * time.Second):
		t.Error("the resumed connection brought no ticket for the next one")
	}
}

// Off by default: a listener not asked for tickets sends none, so the
// client's cache stays empty even after a round trip on a stream, which
// arrives after the ticket would have.
func TestNoSessionTicketByDefault(t *testing.T) {
	l, addr := newUDPListener(t, quic.ListenerConfig{})
	echoListener(t, l)
	cache := newTicketCache()

	dialCached(t, addr, cache)
	select {
	case <-cache.stored:
		t.Error("a ticket arrived from a listener that was not asked for one")
	default:
	}
}

// A Config that disables tickets would have crypto/tls send none, silently:
// asking the listener for them at the same time is refused up front.
func TestNewListenerRefusesTicketsTheConfigDisables(t *testing.T) {
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	cfg := serverTLS(t)
	cfg.SessionTicketsDisabled = true
	_, err = quic.NewListener(pc, cfg, quic.DefaultParameters(), quic.ListenerConfig{SessionTickets: true})
	if !errors.Is(err, quic.ErrQUIC) {
		t.Fatalf("NewListener = %v, want ErrQUIC naming SessionTicketsDisabled", err)
	}
}
