package quic

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"testing"
)

// clientHello12 is a TLS 1.2 ClientHello: legacy_version 0x0303, no
// supported_versions extension, so the versions it offers are 1.2 and
// below (RFC 9846 §4.2.1).
func clientHello12() []byte {
	body := []byte{0x03, 0x03}
	body = append(body, make([]byte, 32)...)    // random
	body = append(body, 0x00)                   // legacy_session_id
	body = append(body, 0x00, 0x02, 0xc0, 0x2f) // one cipher suite
	body = append(body, 0x01, 0x00)             // null compression
	return append([]byte{0x01, 0x00, byte(len(body) >> 8), byte(len(body))}, body...)
}

// RFC 9846 Appendix E.2 (EID 9040): a server that negotiates TLS 1.3 only
// answers a ClientHello offering no TLS 1.3 with protocol_version. NewListener
// refuses a Config below TLS 1.3, but what GetConfigForClient returns was
// not seen: one without MinVersion negotiated TLS 1.2 and failed inside
// crypto/tls's QUIC path, closing INTERNAL_ERROR. The Listener holds the
// returned Config to TLS 1.3 now (serverTLSConfig), so the client gets
// protocol_version, 0x0146 on the wire.
func TestAConfigForClientIsHeldToTLS13(t *testing.T) {
	base := ServerTLSForTest(t)
	bare := base.Clone()
	bare.MinVersion = 0
	base.GetConfigForClient = func(*tls.ClientHelloInfo) (*tls.Config, error) { return bare, nil }

	q := tls.QUICServer(&tls.QUICConfig{TLSConfig: serverTLSConfig(base)})
	if err := q.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	err := q.HandleData(tls.QUICEncryptionLevelInitial, clientHello12())
	var alert tls.AlertError
	if !errors.As(err, &alert) || alert != 70 {
		t.Fatalf("a TLS 1.2 ClientHello under a Config without MinVersion ended with %v; want protocol_version (70)", err)
	}
	if bare.MinVersion != 0 {
		t.Error("the Config GetConfigForClient returned was changed; it is the caller's")
	}

	// The Listener's handshakes go through the same copy.
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	l, err := NewListener(pc, base, DefaultConfig(), ListenerConfig{})
	if err != nil {
		_ = pc.Close()
		t.Fatal(err)
	}
	defer l.Close()
	got, err := l.cfg.GetConfigForClient(&tls.ClientHelloInfo{})
	if err != nil || got.MinVersion != tls.VersionTLS13 {
		t.Fatalf("the Listener's GetConfigForClient returned MinVersion %#x, %v; want TLS 1.3", got.MinVersion, err)
	}
}
