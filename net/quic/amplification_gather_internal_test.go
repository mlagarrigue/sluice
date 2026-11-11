package quic

import (
	"context"
	"crypto/tls"
	"net"
	"testing"
	"time"
)

// RFC 9000 §8.1: "Prior to validating the client address, servers MUST NOT
// send more than three times as many bytes as the number of bytes they have
// received." The budget counted what was sealed, and while a datagram is
// gathered the last packet built waits unsealed until its successor
// arrives: a first flight longer than the budget went one packet past it.
// On the wire, with a certificate chain far past the budget: a relay
// forwards the client's first flight only, then counts every byte the
// server sends back to its address.
func TestFirstFlightStaysWithinTheAmplificationBudget(t *testing.T) {
	cfg := ServerTLSForTest(t)
	chain := cfg.Certificates[0]
	der := chain.Certificate[0]
	for total := len(der); total < 12_000; total += len(der) {
		chain.Certificate = append(chain.Certificate, der)
	}
	cfg.Certificates = []tls.Certificate{chain}

	lpc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	l, err := NewListener(lpc, cfg, DefaultConfig(), ListenerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })

	relay, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = relay.Close() })
	cpc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cpc.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _, _ = Dial(ctx, cpc, relay.LocalAddr(), ClientTLSForTest(), DefaultConfig()) }()

	// The client's first flight may span two datagrams (a post-quantum key
	// share makes the ClientHello longer than one): what the client sends in
	// the first 200 ms is forwarded and counted — well before its first
	// probe timeout — and nothing after.
	buf := make([]byte, 65536)
	received, sent, datagrams := 0, 0, 0
	window := time.Now().Add(200 * time.Millisecond)
	for {
		_ = relay.SetReadDeadline(time.Now().Add(1500 * time.Millisecond))
		m, from, err := relay.ReadFrom(buf)
		if err != nil {
			break
		}
		switch {
		case from.String() == lpc.LocalAddr().String():
			sent += m
			datagrams++
		case time.Now().Before(window):
			received += m
			if _, err := relay.WriteTo(buf[:m], lpc.LocalAddr()); err != nil {
				t.Fatal(err)
			}
		}
	}
	if sent == 0 {
		t.Fatal("precondition lost: the server sent nothing")
	}
	if sent > 3*received {
		t.Errorf("the server sent %d bytes in %d datagrams to an unvalidated address that sent %d: more than three times",
			sent, datagrams, received)
	}
}
