package quic_test

import (
	"crypto/tls"
	"net"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/net/quic"
)

// handshakePair runs one handshake over real loopback UDP and returns both
// ends: the client Dial produced and the connection the listener accepted.
func handshakePair(t *testing.T, serverCfg, clientCfg *tls.Config) (client, server *quic.Conn, l *quic.Listener, clientPC *net.UDPConn) {
	t.Helper()
	return handshakePairParams(t, serverCfg, clientCfg, quic.DefaultParameters())
}

// handshakePairParams is handshakePair with the server's transport
// parameters chosen by the test: a small initial window, typically.
func handshakePairParams(t *testing.T, serverCfg, clientCfg *tls.Config, serverParams quic.TransportParameters) (client, server *quic.Conn, l *quic.Listener, clientPC *net.UDPConn) {
	t.Helper()
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	l, err = quic.NewListener(pc, serverCfg, serverParams, quic.ListenerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	accepted := make(chan *quic.Conn, 1)
	go func() {
		c, err := l.Accept()
		if err != nil {
			close(accepted)
			return
		}
		accepted <- c
	}()
	clientPC, err = net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = clientPC.Close() })
	client, err = quic.Dial(clientPC, l.Addr(), clientCfg, quic.DefaultParameters())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	select {
	case server = <-accepted:
		if server == nil {
			t.Fatal("Accept failed")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the handshake never completed")
	}
	t.Cleanup(func() { _ = server.Close() })
	return client, server, l, clientPC
}

// waitFor polls cond until it holds or the test's patience runs out.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("waiting for %s: still not the case after 10s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Both ends see the same negotiated protocol, which is what a server reads
// before it speaks h3 to a client that asked for it.
func TestConnectionStateReportsNegotiatedALPN(t *testing.T) {
	// A host name, not the IP literal clientTLS carries: RFC 6066 §3 keeps
	// IP literals out of server_name, so crypto/tls sends none for one.
	clientCfg := clientTLS()
	clientCfg.ServerName = "sluice.test"
	client, server, _, _ := handshakePair(t, serverTLS(t), clientCfg)
	for side, c := range map[string]*quic.Conn{"client": client, "server": server} {
		st := c.ConnectionState()
		if st.NegotiatedProtocol != "h3" {
			t.Errorf("%s: NegotiatedProtocol = %q, want h3", side, st.NegotiatedProtocol)
		}
		if st.Version != tls.VersionTLS13 {
			t.Errorf("%s: Version = %#x, want TLS 1.3", side, st.Version)
		}
		if !st.HandshakeComplete {
			t.Errorf("%s: HandshakeComplete is false after Dial/Accept returned", side)
		}
	}
	if sni := server.ConnectionState().ServerName; sni != "sluice.test" {
		t.Errorf("server: ServerName = %q, want the client's", sni)
	}
	if n := len(client.ConnectionState().PeerCertificates); n != 1 {
		t.Errorf("client: %d peer certificates, want the server's one", n)
	}
}

// No protocol in common is a failed handshake, not a connection with an
// empty NegotiatedProtocol to read: RFC 9001 §8.1 makes ALPN mandatory.
func TestNoCommonALPNFailsTheHandshake(t *testing.T) {
	serverCfg := serverTLS(t)
	serverCfg.NextProtos = []string{"h3-29"}
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	l, err := quic.NewListener(pc, serverCfg, quic.DefaultParameters(), quic.ListenerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	clientPC, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer clientPC.Close()
	if c, err := quic.Dial(clientPC, l.Addr(), clientTLS(), quic.DefaultParameters()); err == nil {
		_ = c.Close()
		t.Fatalf("Dial succeeded with no ALPN in common; negotiated %q", c.ConnectionState().NegotiatedProtocol)
	}
}

// Each end's RemoteAddr is the other's LocalAddr, and the listener's Addr
// is the socket it was given — the three addresses a log line names.
func TestAddressesMatchAcrossTheConnection(t *testing.T) {
	client, server, l, clientPC := handshakePair(t, serverTLS(t), clientTLS())
	if got, want := l.Addr().String(), server.LocalAddr().String(); got != want {
		t.Errorf("Listener.Addr = %s, the accepted connection's LocalAddr = %s", got, want)
	}
	if got, want := client.RemoteAddr().String(), server.LocalAddr().String(); got != want {
		t.Errorf("client RemoteAddr = %s, server LocalAddr = %s", got, want)
	}
	if got, want := server.RemoteAddr().String(), client.LocalAddr().String(); got != want {
		t.Errorf("server RemoteAddr = %s, client LocalAddr = %s", got, want)
	}
	if got, want := client.LocalAddr().String(), clientPC.LocalAddr().String(); got != want {
		t.Errorf("client LocalAddr = %s, its socket's = %s", got, want)
	}
}

// After a client Rebind the client's LocalAddr is the new socket's at once,
// and the server's RemoteAddr follows as soon as it sees the move.
func TestRemoteAddrFollowsRebind(t *testing.T) {
	client, server, _, _ := handshakePair(t, serverTLS(t), clientTLS())
	go echoOneStream(t, server)
	second, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	// The client confirms on HANDSHAKE_DONE, which may still be in flight
	// when Dial returns; Rebind refuses until then.
	var rebindErr error
	waitFor(t, "the client to confirm the handshake and rebind", func() bool {
		rebindErr = client.Rebind(second)
		return rebindErr == nil
	})
	if got, want := client.LocalAddr().String(), second.LocalAddr().String(); got != want {
		t.Errorf("client LocalAddr after Rebind = %s, want %s", got, want)
	}
	s, err := client.OpenStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write([]byte("from the second socket")); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the server to follow the client", func() bool {
		return server.RemoteAddr().String() == second.LocalAddr().String()
	})
}
