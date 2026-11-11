package quic

import (
	"context"
	"net"
	"testing"
	"time"
)

// ListenerConfig.MaxConnsPerIP bounds the
// connections of one source address "so that one peer cannot hold the whole
// MaxConns on its own". The count is taken when the first Initial arrives,
// from the datagram's source address, before anything has shown that the
// sender receives at that address (RFC 9000 §8.1): one Initial that decrypts
// under its own identifier's keys — which needs no secret — and is never
// followed up holds the address's slot until the handshake times out.
//
// A client that does answer from that address is admitted, past a Retry,
// while the only other connection counted there has not validated its
// address; it was refused with CONNECTION_REFUSED.
func TestAnUnvalidatedInitialDoesNotTakeAnAddressSlot(t *testing.T) {
	spc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	l, err := NewListener(spc, ServerTLSForTest(t), DefaultConfig(), ListenerConfig{MaxConnsPerIP: 1})
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

	// One Initial from the address, never followed up.
	mute, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer mute.Close()
	if _, err := mute.WriteTo(sealedClientInitial(t, []byte("dcid-mute"), []byte("scid-mute")), spc.LocalAddr()); err != nil {
		t.Fatal(err)
	}
	counted, unvalidated := false, false
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline) && !counted; time.Sleep(time.Millisecond) {
		l.mu.Lock()
		for c := range l.keysOf {
			counted = true
			c.mu.Lock()
			unvalidated = c.amplActive
			c.mu.Unlock()
		}
		l.mu.Unlock()
	}
	if !counted || !unvalidated {
		t.Fatalf("precondition lost: the Initial is counted %v, its address unvalidated %v", counted, unvalidated)
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
		t.Fatalf("a client that answers from the address was refused while one unvalidated Initial held its slot: %v", err)
	}
	_ = c.Close()
}
