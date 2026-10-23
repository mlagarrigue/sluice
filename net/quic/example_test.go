package quic_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"math/big"
	"net"
	"time"

	"github.com/mlagarrigue/sluice/net/quic"
)

// A server and a client on the loopback: the client opens a stream, sends a
// word and its end, and reads back what the server made of it. A real
// deployment loads its certificate; this one mints a self-signed one so the
// example runs on its own.
func Example() {
	serverTLS, clientTLS := selfSigned()

	spc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		panic(err)
	}
	ln, err := quic.NewListener(spc, serverTLS, quic.DefaultParameters(), quic.ListenerConfig{})
	if err != nil {
		panic(err)
	}
	defer func() { _ = ln.Close() }()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		s, err := conn.AcceptStream(context.Background())
		if err != nil {
			return
		}
		word, _ := io.ReadAll(s) // up to the client's end of stream
		_, _ = s.WriteFinal(bytes.ToUpper(word))
	}()

	cpc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		panic(err)
	}
	defer func() { _ = cpc.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := quic.Dial(ctx, cpc, ln.Addr(), clientTLS, quic.DefaultParameters())
	if err != nil {
		panic(err)
	}
	defer func() { _ = conn.Close() }()

	s, err := conn.OpenStream(ctx)
	if err != nil {
		panic(err)
	}
	if _, err := s.WriteFinal([]byte("hello")); err != nil { // the data and the end together
		panic(err)
	}
	answer, err := io.ReadAll(s)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(answer))
	// Output: HELLO
}

// selfSigned mints a certificate for 127.0.0.1 and the client configuration
// that trusts it. QUIC needs TLS 1.3 and an application protocol both ends
// agree on.
func selfSigned() (server, client *tls.Config) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		panic(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		panic(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		panic(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(cert)
	server = &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
		MinVersion:   tls.VersionTLS13,
		NextProtos:   []string{"echo"},
	}
	client = &tls.Config{
		RootCAs:    roots,
		ServerName: "127.0.0.1",
		MinVersion: tls.VersionTLS13,
		NextProtos: []string{"echo"},
	}
	return server, client
}
