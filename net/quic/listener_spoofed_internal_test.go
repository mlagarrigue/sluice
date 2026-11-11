package quic

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"
)

// blackHole is the socket a forged Initial "arrived on": everything the
// listener answers through it vanishes, as the answers to a spoofed source
// address reach someone who never asked for them.
type blackHole struct {
	once sync.Once
	done chan struct{}
}

func newBlackHole() *blackHole { return &blackHole{done: make(chan struct{})} }

func (b *blackHole) ReadFrom([]byte) (int, net.Addr, error) {
	<-b.done
	return 0, nil, net.ErrClosed
}
func (b *blackHole) WriteTo(p []byte, _ net.Addr) (int, error) { return len(p), nil }
func (b *blackHole) Close() error                              { b.once.Do(func() { close(b.done) }); return nil }

func (b *blackHole) LocalAddr() net.Addr              { return &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9} }
func (b *blackHole) SetDeadline(time.Time) error      { return nil }
func (b *blackHole) SetReadDeadline(time.Time) error  { return nil }
func (b *blackHole) SetWriteDeadline(time.Time) error { return nil }

// The same, with the forged Initial handed to dispatch from a source
// address nobody behind it reads (127.0.0.1, port 1) and every answer
// swallowed: the sender receives nothing at that address, which is what a
// spoofed source is. A real client at 127.0.0.1 is admitted under
// MaxConnsPerIP 1. Under AlwaysRetry, the control, the forged Initial gets
// a (lost) Retry and is never counted.
func TestASpoofedInitialDoesNotTakeAnAddressSlot(t *testing.T) {
	for _, tc := range []struct {
		name        string
		alwaysRetry bool
	}{
		{"direct", false},
		{"control-always-retry", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
			if err != nil {
				t.Fatal(err)
			}
			l, err := NewListener(spc, ServerTLSForTest(t), DefaultConfig(), ListenerConfig{MaxConnsPerIP: 1, AlwaysRetry: tc.alwaysRetry})
			if err != nil {
				t.Fatal(err)
			}
			defer l.Close()
			go func() {
				for {
					c, err := l.Accept()
					if err != nil {
						return
					}
					t.Cleanup(func() { _ = c.Close() })
				}
			}()

			bh := newBlackHole()
			defer bh.Close()
			forged := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}
			l.dispatch(sealedClientInitial(t, []byte("dcid-spoofed"), []byte("scid-spoofed")), forged, bh, 0)

			l.mu.Lock()
			counted := l.connsByIP[l.connKey(forged)]
			l.mu.Unlock()
			t.Logf("count for 127.0.0.1 after the forged Initial: %d", counted)
			if tc.alwaysRetry && counted != 0 {
				t.Fatalf("control: under AlwaysRetry the forged Initial was counted (%d)", counted)
			}

			pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
			if err != nil {
				t.Fatal(err)
			}
			defer pc.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			c, err := Dial(ctx, pc, spc.LocalAddr(), ClientTLSForTest(), DefaultConfig())
			if err != nil {
				t.Fatalf("a client at 127.0.0.1 was refused while a spoofed, never-answered Initial held the address's slot: %v", err)
			}
			_ = c.Close()
		})
	}
}
