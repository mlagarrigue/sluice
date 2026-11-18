package quic

import (
	"context"
	"crypto/tls"
	"net"
	"sync"
	"testing"
	"time"
)

// blockingCache is a ClientSessionCache whose Get waits for release.
type blockingCache struct {
	release chan struct{}
	entered chan struct{}
	once    sync.Once
}

func (c *blockingCache) Get(string) (*tls.ClientSessionState, bool) {
	c.once.Do(func() { close(c.entered) })
	<-c.release
	return nil, false
}

func (c *blockingCache) Put(string, *tls.ClientSessionState) {}

type dialFunc func(context.Context, net.PacketConn, net.Addr, *tls.Config, Config) (*Conn, error)

// dialWithCache dials an address that never answers, under a context of
// timeout, and hands over what the Dial returned.
func dialWithCache(t *testing.T, dial dialFunc, cache tls.ClientSessionCache, timeout time.Duration) <-chan error {
	t.Helper()
	srv, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pc.Close() })
	cfg := &tls.Config{NextProtos: []string{"t"}, MinVersion: tls.VersionTLS13, ServerName: "example.com", ClientSessionCache: cache}
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		_, err := dial(ctx, pc, srv.LocalAddr(), cfg, DefaultConfig())
		done <- err
	}()
	return done
}

// A cache whose Get blocks holds the Dials on it, as crypto/tls's would, and
// no other: taking a ticket was one step under a lock for the whole package,
// and a Dial on another cache waited behind the blocked Get for as long as it
// stayed blocked, past its own context. Dial and DialEarly both take it.
func TestADialBlockedInItsCacheHoldsNoOtherDial(t *testing.T) {
	for _, tc := range []struct {
		name string
		dial dialFunc
	}{{"Dial", Dial}, {"DialEarly", DialEarly}} {
		t.Run(tc.name, func(t *testing.T) {
			bc := &blockingCache{release: make(chan struct{}), entered: make(chan struct{})}
			blocked := dialWithCache(t, tc.dial, bc, 300*time.Millisecond)
			t.Cleanup(func() {
				close(bc.release)
				<-blocked
			})
			select {
			case <-bc.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("the blocking cache was never asked")
			}
			other := dialWithCache(t, Dial, mapSessionCache{}, 300*time.Millisecond)
			select {
			case <-other:
			case <-time.After(3 * time.Second):
				t.Fatal("a Dial on another cache is still held 3s past its 300ms context, behind the blocked Get")
			}
		})
	}
}
