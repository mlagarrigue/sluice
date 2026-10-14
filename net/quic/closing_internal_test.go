package quic

import (
	"testing"
	"time"
)

func routed(l *Listener) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.keysOf)
}

// A closed connection leaves the listener's table when its closing state
// ends (three probe timeouts, RFC 9000 §10.2), not before — while it is in
// the table its identifiers still route, so dispatch can repeat the close —
// and not never: the MaxConns slot it holds comes back.
func TestClosingKeepsTheConnectionRoutedUntilTheStateEnds(t *testing.T) {
	l, _ := newInternalUDPListener(t, "udp", "127.0.0.1:0", ListenerConfig{})
	client := dialInternal(t, loopbackSocket(t), l.Addr())
	server, err := l.Accept()
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	if n := routed(l); n != 1 {
		t.Fatalf("routed connections right after Close = %d, want 1: forgotten before its closing state ended", n)
	}
	select {
	case <-server.gone:
	case <-time.After(10 * time.Second):
		t.Fatal("the closing state never ended")
	}
	deadline := time.Now().Add(5 * time.Second)
	for routed(l) != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("routed connections after the closing state = %d, want 0", routed(l))
		}
		time.Sleep(time.Millisecond)
	}
	_ = client
}
