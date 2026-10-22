package quic_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/net/quic"
)

// A handshake the server's TLS refused — no ALPN in common — reaches the
// dialer as the peer's CONNECTION_CLOSE carrying the alert in RFC 9001
// §4.8's crypto range: 0x0100 + no_application_protocol (120), a
// ConnectionError a caller reads by its code instead of its text.
func TestDialReportsTheRefusedHandshakeByItsCode(t *testing.T) {
	serverPC, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer serverPC.Close()
	clientPC, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer clientPC.Close()
	l, err := quic.NewListener(serverPC, serverTLS(t), quic.DefaultParameters(), quic.ListenerConfig{})
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
			_ = c.Close()
		}
	}()

	cfg := clientTLS()
	cfg.NextProtos = []string{"not-what-the-server-speaks"}
	_, err = quic.Dial(context.Background(), clientPC, serverPC.LocalAddr(), cfg, quic.DefaultParameters())
	ce, ok := errors.AsType[*quic.ConnectionError](err)
	if !ok || ce.Application || ce.Code != 0x0178 {
		t.Fatalf("Dial with no ALPN in common = %v, want a ConnectionError with transport code 0x0178 (no_application_protocol)", err)
	}
}

// The peer's plain Close — CONNECTION_CLOSE with NO_ERROR, transport
// flavour — is reported too, as a ConnectionError with code 0 and
// Application unset: the transport does not decide that an end was clean,
// the caller does.
func TestErrReportsThePeersTransportClose(t *testing.T) {
	client, server, _, _ := handshakePair(t, serverTLS(t), clientTLS())
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-client.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("the client never saw the peer's close")
	}
	ce, ok := errors.AsType[*quic.ConnectionError](client.Err())
	if !ok || ce.Application || ce.Code != 0 || ce.Reason != "" {
		t.Fatalf("client Err = %v, want a ConnectionError with transport code 0", client.Err())
	}
	if err := server.Err(); err != nil {
		t.Errorf("server Err = %v after its own Close, want nil", err)
	}
}
