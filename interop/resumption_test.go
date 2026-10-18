package interop

import (
	"bytes"
	"context"
	"crypto/tls"
	"io"
	"testing"
	"time"

	quicgo "github.com/quic-go/quic-go"

	"github.com/mlagarrigue/sluice/net/quic"
)

// TestResumptionAgainstQuicGoServer dials a quic-go server twice from one
// tls.ClientSessionCache. The server offers 0-RTT (Allow0RTT), so the
// ticket it issues on the first connection marks early data, and on the
// second crypto/tls attempts a PSK resumption and hands sluice the 0-RTT
// secret (RFC 9001 §4.6.1). sluice sends no 0-RTT packet; the secret must
// go nowhere — not into the 1-RTT space, where it sealed packets the server
// could not open. What is observable from here: the second handshake
// completes as a resumption, a stream echoes, and nothing is lost on
// loopback. That last check does not catch the old behaviour by itself —
// nothing left the application space before the 1-RTT secret replaced the
// early one, so this test passed before the fix too; what it proves is
// that a resumption with 0-RTT on offer works against a real server. That
// no key reaches the application space is proven in net/quic
// (TestEarlySecretInstallsNoApplicationKeys): the space is not visible
// from this module.
func TestResumptionAgainstQuicGoServer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ln, err := quicgo.Listen(listenUDP(t), serverTLS(t), &quicgo.Config{MaxIdleTimeout: timeout, Allow0RTT: true})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept(ctx)
			if err != nil {
				return
			}
			go func() {
				st, err := c.AcceptStream(ctx)
				if err != nil {
					return
				}
				_, _ = io.Copy(st, st)
				_ = st.Close()
			}()
		}
	}()

	cfg := clientTLS()
	cache := tls.NewLRUClientSessionCache(1)
	cfg.ClientSessionCache = cache
	want := payload()

	echo := func(client *quic.Conn) {
		t.Helper()
		st, err := client.OpenStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		written := make(chan error, 1)
		go func() {
			if _, err := st.Write(want); err != nil {
				written <- err
				return
			}
			written <- st.CloseWrite()
		}()
		got, err := io.ReadAll(st)
		if err != nil {
			t.Fatalf("reading the echo: %v", err)
		}
		if err := <-written; err != nil {
			t.Fatalf("writing the payload: %v", err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("echo differs: got %d bytes, want %d", len(got), len(want))
		}
	}

	first, err := quic.DialContext(ctx, listenUDP(t), ln.Addr(), cfg, quic.DefaultParameters())
	if err != nil {
		t.Fatalf("first handshake: %v", err)
	}
	if first.ConnectionState().DidResume {
		t.Fatal("the first connection resumed; the cache was not empty")
	}
	echo(first)
	// The ticket rides a 1-RTT CRYPTO frame after the handshake; the cache
	// says when it has been received and stored.
	for deadline := time.Now().Add(timeout); ; {
		if _, ok := cache.Get(cfg.ServerName); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("quic-go issued no session ticket the client stored")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second, err := quic.DialContext(ctx, listenUDP(t), ln.Addr(), cfg, quic.DefaultParameters())
	if err != nil {
		t.Fatalf("second handshake, from the session cache: %v", err)
	}
	defer second.Close()
	if !second.ConnectionState().DidResume {
		t.Fatal("the second connection did not resume; the 0-RTT secret was never offered")
	}
	echo(second)
	if got := second.Stats().LostPackets; got != 0 {
		t.Errorf("LostPackets = %d on loopback; a packet left under a key the server does not hold", got)
	}
}
