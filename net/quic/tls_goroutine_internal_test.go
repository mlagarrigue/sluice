package quic

import (
	"context"
	"crypto/tls"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"
)

// tlsHandshakeGoroutines counts the goroutines crypto/tls runs a QUIC
// handshake on, waiting up to two seconds for the count to fall to at most
// want: a goroutine told to stop still has to be scheduled to return.
func tlsHandshakeGoroutines(want int) int {
	buf := make([]byte, 1<<20)
	deadline := time.Now().Add(2 * time.Second)
	for {
		n := runtime.Stack(buf, true)
		got := strings.Count(string(buf[:n]), "crypto/tls.(*Conn).HandshakeContext")
		if got <= want || time.Now().After(deadline) {
			return got
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A Dial that gives up — no server ever answers — ends the handshake
// goroutine crypto/tls started for it. It used to stay parked for the life
// of the process, one per attempt, with the TLS state it holds.
func TestAbandonedDialEndsTheTLSHandshake(t *testing.T) {
	mute, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer mute.Close()
	before := tlsHandshakeGoroutines(0)
	const attempts = 3
	for range attempts {
		pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		_, err = Dial(ctx, pc, mute.LocalAddr(), ClientTLSForTest(), DefaultParameters())
		cancel()
		_ = pc.Close()
		if err == nil {
			t.Fatal("a Dial to a silent address succeeded")
		}
	}
	if got := tlsHandshakeGoroutines(before); got > before {
		t.Fatalf("%d TLS handshake goroutines after %d abandoned Dials, want %d", got, attempts, before)
	}
}

// The server side of the same: a client that sends one Initial and never
// another holds a pending connection until HandshakeTimeout, and nothing
// after it — before, every such client left a goroutine behind, an
// unbounded cost for the price of one datagram each.
func TestAbandonedListenerHandshakeEndsTLS(t *testing.T) {
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	l, err := NewListener(pc, &tls.Config{MinVersion: tls.VersionTLS13, NextProtos: []string{"h3"}}, DefaultParameters(),
		ListenerConfig{HandshakeTimeout: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	before := tlsHandshakeGoroutines(0)
	from := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 5555}
	const attempts = 3
	for i := range attempts {
		l.dispatch(sealedClientInitial(t, []byte{'o', 'd', 'c', 'i', 'd', '-', 'a', byte('0' + i)}, []byte("cscid-bb")), from, l.pc, 0)
	}
	l.mu.Lock()
	pending := len(l.pendingConns)
	l.mu.Unlock()
	if pending != attempts {
		t.Fatalf("%d pending connections, want %d: the Initials did not start handshakes", pending, attempts)
	}
	deadline := time.Now().Add(5 * time.Second)
	for pending > 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
		l.mu.Lock()
		pending = len(l.pendingConns)
		l.mu.Unlock()
	}
	if pending > 0 {
		t.Fatalf("%d handshakes still pending past their timeout", pending)
	}
	if got := tlsHandshakeGoroutines(before); got > before {
		t.Fatalf("%d TLS handshake goroutines after %d abandoned handshakes, want %d", got, attempts, before)
	}
}
