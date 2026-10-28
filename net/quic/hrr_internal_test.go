package quic

import (
	"context"
	"crypto/tls"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/internal/dgram"
)

// RFC 9846 §4.2.4: a server that takes none of the client's key shares
// answers with a HelloRetryRequest, and the client sends a second
// ClientHello — one more round trip at the Initial level, with CRYPTO
// offsets that run on past the first hello. crypto/tls does the TLS; what
// this test exercises is the transport carrying it: the second hello
// framed, acknowledged, coalesced and delivered like the first. The wire
// is the judge: the client's Initial-only datagrams before its first
// Handshake packet count the hellos it sent.
func TestHandshakeSurvivesAHelloRetryRequest(t *testing.T) {
	hellos := func(t *testing.T, clientCurves []tls.CurveID) (int, tls.CurveID) {
		t.Helper()
		serverPC, clientPC := dgram.Pair()
		defer serverPC.Close()
		defer clientPC.Close()
		clientTap := newTap(clientPC)
		serverCfg := ServerTLSForTest(t)
		serverCfg.CurvePreferences = []tls.CurveID{tls.CurveP256}

		done := make(chan error, 1)
		go func() {
			buf := make([]byte, 2048)
			_ = serverPC.SetReadDeadline(time.Now().Add(10 * time.Second))
			n, peer, err := serverPC.ReadFrom(buf)
			if err != nil {
				done <- err
				return
			}
			conn, err := Accept(serverPC, datagram{Peer: peer, Payload: buf[:n]}, serverCfg, DefaultParameters())
			if err != nil {
				done <- err
				return
			}
			defer conn.Close()
			done <- nil
		}()
		clientCfg := ClientTLSForTest()
		clientCfg.CurvePreferences = clientCurves
		client, err := Dial(context.Background(), clientTap, serverPC.LocalAddr(), clientCfg, DefaultParameters())
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		waitFor(t, "the client to confirm", func() bool {
			client.mu.Lock()
			defer client.mu.Unlock()
			return client.handshakeConfirmed
		})
		initialOnly := 0
		for _, w := range clientTap.recorded() {
			types := packetTypes(t, w.data)
			if hasTypes(types, packetHandshake) {
				break
			}
			if hasTypes(types, packetInitial) {
				initialOnly++
			}
		}
		return initialOnly, client.ConnectionState().CurveID
	}

	// Only the first group gets a key share; the server takes P-256 alone.
	n, curve := hellos(t, []tls.CurveID{tls.X25519, tls.CurveP256})
	if curve != tls.CurveP256 {
		t.Errorf("negotiated %v, want P-256: the server's choice was not forced", curve)
	}
	if n < 2 {
		t.Errorf("%d Initial-only client datagrams before the Handshake flight, want at least 2 (ClientHello, then ACK + second ClientHello): no HelloRetryRequest happened", n)
	}
	// The control: a key share the server accepts at once, one hello.
	n, curve = hellos(t, []tls.CurveID{tls.CurveP256})
	if curve != tls.CurveP256 {
		t.Errorf("control negotiated %v, want P-256", curve)
	}
	if n != 1 {
		t.Errorf("control: %d Initial-only client datagrams before the Handshake flight, want exactly 1 without a HelloRetryRequest", n)
	}
}
