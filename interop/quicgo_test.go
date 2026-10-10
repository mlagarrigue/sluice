// Package interop confronts sluice's protocol implementations with peers
// that share none of their code.
//
// Every test in net/quic checks the transport against itself, against the
// published RFC 9001 vectors and against a harness that drops, reorders and
// forges datagrams. None of that catches an assumption both ends of the
// harness share. quic-go is the first peer that shares nothing: a handshake
// that completes, a stream that echoes and a close that is acknowledged
// here are what the package documentation means by interoperating.
//
// This is its own module on purpose: quic-go must never enter the root
// module's dependency graph (ADR 0002), and a nested module is excluded from
// the root's `./...`. CI runs it as the interop-quic job.
package interop

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"math/big"
	"net"
	"testing"
	"time"

	quicgo "github.com/quic-go/quic-go"

	"github.com/mlagarrigue/sluice/net/quic"
)

const (
	alpn    = "h3"
	timeout = 10 * time.Second
)

// serverTLS mints a self-signed certificate for 127.0.0.1 and configures
// TLS 1.3 with ALPN, which QUIC makes mandatory (RFC 9001 §8.1). No cipher
// suite is forced: the two implementations must agree on one by themselves.
func serverTLS(t testing.TB) *tls.Config {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
		MinVersion:   tls.VersionTLS13,
		NextProtos:   []string{alpn},
	}
}

func clientTLS() *tls.Config {
	return &tls.Config{
		InsecureSkipVerify: true, //nolint:gosec // G402: a certificate this test minted
		MinVersion:         tls.VersionTLS13,
		NextProtos:         []string{alpn},
		ServerName:         "127.0.0.1",
	}
}

func listenUDP(t testing.TB) *net.UDPConn {
	t.Helper()
	pc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pc.Close() })
	return pc
}

// payload is larger than one datagram so the echo crosses packet
// boundaries, and larger than the initial stream flow-control window of
// quic.DefaultParameters so both sides must grant credit along the way.
func payload() []byte {
	p := make([]byte, 256<<10)
	for i := range p {
		p[i] = byte(i * 7)
	}
	return p
}

// TestSluiceClientAgainstQuicGoServer: quic-go listens, sluice dials, opens
// a stream, sends payload, half-closes, reads the echo until EOF, closes the
// connection. quic-go must then see the connection end with sluice's
// CONNECTION_CLOSE rather than with its own idle timeout.
func TestSluiceClientAgainstQuicGoServer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ln, err := quicgo.Listen(listenUDP(t), serverTLS(t), &quicgo.Config{MaxIdleTimeout: timeout})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	serverDone := make(chan error, 1)
	go func() {
		serverDone <- func() error {
			c, err := ln.Accept(ctx)
			if err != nil {
				return err
			}
			st, err := c.AcceptStream(ctx)
			if err != nil {
				return err
			}
			if _, err := io.Copy(st, st); err != nil {
				return err
			}
			if err := st.Close(); err != nil {
				return err
			}
			// The client closes the connection; quic.Conn.Close sends a
			// transport-level CONNECTION_CLOSE with NO_ERROR (RFC 9000 §10.2),
			// which quic-go reports as a remote TransportError on the
			// connection context.
			<-c.Context().Done()
			var closeErr *quicgo.TransportError
			if err := context.Cause(c.Context()); !errors.As(err, &closeErr) ||
				!closeErr.Remote || closeErr.ErrorCode != quicgo.NoError {
				return errors.New("quic-go did not see the client's CONNECTION_CLOSE: " + err.Error())
			}
			return nil
		}()
	}()

	client, err := quic.DialContext(ctx, listenUDP(t), ln.Addr(), clientTLS(), quic.DefaultParameters())
	if err != nil {
		t.Fatalf("sluice could not complete a handshake with quic-go: %v", err)
	}
	st, err := client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	want := payload()
	echoed := make(chan error, 1)
	go func() {
		if _, err := st.Write(want); err != nil {
			echoed <- err
			return
		}
		echoed <- st.CloseWrite()
	}()
	got, err := io.ReadAll(st)
	if err != nil {
		t.Fatalf("reading the echo: %v", err)
	}
	if err := <-echoed; err != nil {
		t.Fatalf("writing the payload: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("echo differs: got %d bytes, want %d", len(got), len(want))
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatalf("quic-go server: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("the quic-go server never saw the connection end")
	}
}

// TestQuicGoClientAgainstSluiceServer is the mirror: sluice listens on a
// shared socket, quic-go dials, opens a stream, sends payload, closes it,
// reads the echo until EOF, closes the connection with an application
// error sluice must surface through PeerClosed.
func TestQuicGoClientAgainstSluiceServer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	pc := listenUDP(t)
	ln, err := quic.NewListener(pc, serverTLS(t), quic.DefaultParameters(), quic.ListenerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	const closeCode = 0x42
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- func() error {
			c, err := ln.Accept()
			if err != nil {
				return err
			}
			st, err := c.AcceptStream()
			if err != nil {
				return err
			}
			if _, err := io.Copy(st, st); err != nil {
				return err
			}
			if err := st.CloseWrite(); err != nil {
				return err
			}
			<-c.Done()
			code, _, ok := c.PeerClosed()
			if !ok || code != closeCode {
				return errors.New("sluice did not see quic-go's CONNECTION_CLOSE: " + c.Err().Error())
			}
			return nil
		}()
	}()

	client, err := quicgo.Dial(ctx, listenUDP(t), pc.LocalAddr(), clientTLS(), &quicgo.Config{MaxIdleTimeout: timeout})
	if err != nil {
		t.Fatalf("quic-go could not complete a handshake with sluice: %v", err)
	}
	st, err := client.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := payload()
	echoed := make(chan error, 1)
	go func() {
		if _, err := st.Write(want); err != nil {
			echoed <- err
			return
		}
		echoed <- st.Close()
	}()
	got, err := io.ReadAll(st)
	if err != nil {
		t.Fatalf("reading the echo: %v", err)
	}
	if err := <-echoed; err != nil {
		t.Fatalf("writing the payload: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("echo differs: got %d bytes, want %d", len(got), len(want))
	}
	if err := client.CloseWithError(closeCode, "done"); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatalf("sluice server: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("the sluice server never saw the connection end")
	}
}
