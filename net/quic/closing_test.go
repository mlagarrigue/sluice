package quic_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/net/quic"
)

// RFC 9000 §10.2.1: after sending its CONNECTION_CLOSE, an endpoint stays
// in the closing state and answers any packet it still attributes to the
// connection with the close again. Here the server's close is lost on the
// way out; the client, unaware, sends on, and must hear the application
// code — not the stateless reset a listener that had already forgotten the
// connection would answer with, and not its idle timeout.
func TestClosingRepeatsALostConnectionClose(t *testing.T) {
	udp, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	server := &hookedConn{PacketConn: udp}
	l, err := quic.NewListener(server, serverTLS(t), quic.DefaultParameters(), quic.ListenerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	client := dialUDP(t, l.Addr(), quic.DefaultParameters())
	accepted, err := l.Accept()
	if err != nil {
		t.Fatal(err)
	}
	// Let the handshake's tail (HANDSHAKE_DONE, the last acknowledgements)
	// leave before the filter arms, so that the first 1-RTT datagram it
	// sees is the close.
	time.Sleep(50 * time.Millisecond)
	dropped := 0
	server.mu.Lock()
	server.filterWrite = func(b []byte) [][]byte {
		if dropped == 0 && b[0]&0x80 == 0 {
			dropped++
			return nil
		}
		return [][]byte{b}
	}
	server.mu.Unlock()

	if err := accepted.CloseWithError(0x100); err != nil {
		t.Fatal(err)
	}
	server.mu.Lock()
	lost := dropped
	server.mu.Unlock()
	if lost != 1 {
		t.Fatalf("the close was not the datagram dropped (dropped %d)", lost)
	}

	// The client has heard nothing and talks on: this packet is what the
	// closing state answers.
	s, err := client.OpenStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write([]byte("still here")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-client.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("the client never heard the repeated close")
	}
	ce, ok := errors.AsType[*quic.ConnectionError](client.Err())
	if !ok || !ce.Application || ce.Code != 0x100 {
		t.Fatalf("client Err = %v, want a ConnectionError with application code 0x100", client.Err())
	}
}
