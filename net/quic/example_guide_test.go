package quic_test

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/mlagarrigue/sluice/net/quic"
)

// The configuration to start from: modest transport parameters, which a
// field set on the copy overrides, and Validate before it is handed over.
func ExampleDefaultConfig() {
	cfg := quic.DefaultConfig()
	cfg.MaxIdleTimeout = 30 * time.Second
	fmt.Println(cfg.Validate())
	// Output: <nil>
}

// A listener reads one UDP socket for every connection it accepts. See the
// package example for a whole exchange.
func ExampleNewListener() {
	serverTLS, _ := selfSigned()
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		fmt.Println(err)
		return
	}
	ln, err := quic.NewListener(pc, serverTLS, quic.DefaultConfig(), quic.ListenerConfig{})
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() { _ = ln.Close() }()
	fmt.Println(ln.DiscardedPackets(), ln.ReadErrors())
	// Output: 0 0
}

// Dial runs the handshake over a socket of the caller's and returns once it
// is complete, or ctx ends it. Compiled, not run: it needs a server.
func ExampleDial() {
	_, clientTLS := selfSigned()
	pc, err := net.ListenUDP("udp", nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() { _ = pc.Close() }()
	server, err := net.ResolveUDPAddr("udp", "quic.internal:443")
	if err != nil {
		fmt.Println(err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := quic.Dial(ctx, pc, server, clientTLS, quic.DefaultConfig())
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() { _ = conn.Close() }()
	fmt.Println(conn.Stats().SmoothedRTT)
}
