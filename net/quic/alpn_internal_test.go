package quic

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"testing"
	"time"
)

// RFC 9001 §8.1: "endpoints MUST use ALPN", and a server that finds no
// protocol in common MUST close with no_application_protocol. NewListener
// refuses a static Config without NextProtos; one returned by
// GetConfigForClient was not looked at, crypto/tls negotiated the empty
// protocol without complaint against a client that offered none, and the
// server finished its handshake without ALPN. It now ends it with
// CONNECTION_CLOSE 0x0178 (TLS alert 120 in the §4.8 range).
func TestAServerWithoutALPNClosesWithNoApplicationProtocol(t *testing.T) {
	srvCfg := ServerTLSForTest(t)
	bare := srvCfg.Clone()
	bare.NextProtos = nil
	srvCfg.GetConfigForClient = func(*tls.ClientHelloInfo) (*tls.Config, error) { return bare, nil }
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	l, err := NewListener(pc, srvCfg, DefaultParameters(), ListenerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })

	clientPC := loopbackSocket(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := newClientConn(ctx, clientPC, l.Addr(), ClientTLSForTest(), DefaultParameters())
	if err != nil {
		t.Fatal(err)
	}
	// A client that offers no protocol: newClientConn refuses one, so its
	// TLS state is replaced after the check — crypto/tls's client accepts
	// a server that selects nothing when it offered nothing.
	cliCfg := ClientTLSForTest()
	cliCfg.NextProtos = nil
	c.tls = tls.QUICClient(&tls.QUICConfig{TLSConfig: clientTLSConfig(cliCfg), EnableSessionEvents: true})
	// The client's handshake ends at the server's Finished, the server's
	// at the client's: the server's refusal reaches a client that may
	// already count itself connected, and closes it.
	if err = c.handshake(ctx); err == nil {
		t.Cleanup(func() { _ = c.Close() })
		select {
		case <-c.Done():
			err = c.Err()
		case <-ctx.Done():
			t.Fatalf("the connection stayed open without ALPN (negotiated %q)", c.ConnectionState().NegotiatedProtocol)
		}
	}
	ce, ok := errors.AsType[*ConnectionError](err)
	if !ok || ce.Application || ce.Code != 0x0178 {
		t.Errorf("the connection ended with %v, want the server's CONNECTION_CLOSE 0x0178 (no_application_protocol)", err)
	}
	if n := queued(l); n != 0 {
		t.Errorf("the listener queued %d connections without ALPN for Accept", n)
	}
}
