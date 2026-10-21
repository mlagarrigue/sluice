package quic

import (
	"context"
	"crypto/tls"
	"strings"
	"testing"
	"time"
)

// RFC 9001 §4.2: QUIC uses TLS 1.3 only, and crypto/tls refuses a Config
// whose MinVersion is below it when the handshake starts — the zero value
// of a hand-built Config included. Every constructor refuses such a Config
// up front, with an error that names the field, before anything is sent: a
// Listener that let it through would close every handshake with a
// CONNECTION_CLOSE that says nothing of the cause.
func TestConstructorsRefuseAConfigBelowTLS13(t *testing.T) {
	srv := &tls.Config{Certificates: ServerTLSForTest(t).Certificates, NextProtos: []string{"h3"}}
	cli := ClientTLSForTest().Clone()
	cli.MinVersion = 0
	cli.NextProtos = []string{"h3"}
	pc := loopbackSocket(t)

	if l, err := NewListener(pc, srv, DefaultParameters(), ListenerConfig{}); err == nil {
		_ = l.Close()
		t.Error("NewListener accepted a Config with MinVersion == 0")
	} else if !strings.Contains(err.Error(), "MinVersion") {
		t.Errorf("NewListener error does not name MinVersion: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cpc := loopbackSocket(t)
	if c, err := DialContext(ctx, cpc, pc.LocalAddr(), cli, DefaultParameters()); err == nil {
		_ = c.Close()
		t.Error("DialContext accepted a Config with MinVersion == 0")
	} else if !strings.Contains(err.Error(), "MinVersion") {
		t.Errorf("DialContext error does not name MinVersion: %v", err)
	}

	if c, err := Accept(pc, Datagram{}, srv, DefaultParameters()); err == nil {
		_ = c.Close()
		t.Error("Accept accepted a Config with MinVersion == 0")
	} else if !strings.Contains(err.Error(), "MinVersion") {
		t.Errorf("Accept error does not name MinVersion: %v", err)
	}

	// TLS 1.2 named explicitly is refused the same way: the check is on the
	// minimum, not on the zero value.
	srv12 := srv.Clone()
	srv12.MinVersion = tls.VersionTLS12
	if _, err := NewListener(pc, srv12, DefaultParameters(), ListenerConfig{}); err == nil || !strings.Contains(err.Error(), "MinVersion") {
		t.Errorf("NewListener with MinVersion TLS 1.2: %v", err)
	}
}
