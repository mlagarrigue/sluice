package interop

import (
	"bytes"
	"context"
	"io"
	"testing"

	quicgo "github.com/quic-go/quic-go"

	"github.com/mlagarrigue/sluice/net/quic"
)

// TestQuicGoClientAgainstSluiceServerWithPreferredAddress: sluice listens on
// two sockets and announces the second as its preferred_address (RFC 9000
// §9.6). quic-go does not migrate to a preferred address, but it pools the
// identifier the announcement carries as sequence 1 (§5.1.1) and switches
// to it the moment its handshake completes — in the Handshake packets that
// carry its Finished, still on the original path. A server that only
// recognises that identifier at the preferred address, or only in short
// headers, never completes the handshake: this is what the interop runner's
// connectionmigration case showed, as an INTERNAL_ERROR after the server's
// handshake deadline. The echo must complete over the original address.
func TestQuicGoClientAgainstSluiceServerWithPreferredAddress(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	pc := listenUDP(t)
	preferred := listenUDP(t)
	ln, err := quic.NewListener(pc, serverTLS(t), quic.DefaultParameters(), quic.ListenerConfig{PreferredAddress: preferred})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

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
			return st.CloseWrite()
		}()
	}()

	client, err := quicgo.Dial(ctx, listenUDP(t), pc.LocalAddr(), clientTLS(), &quicgo.Config{MaxIdleTimeout: timeout})
	if err != nil {
		t.Fatalf("quic-go could not complete a handshake with sluice: %v", err)
	}
	defer client.CloseWithError(0, "done")
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
		t.Fatalf("reading the echo under the preferred identifier: %v", err)
	}
	if err := <-echoed; err != nil {
		t.Fatalf("writing the payload: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("echo differs: got %d bytes, want %d", len(got), len(want))
	}
	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatalf("sluice server: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("the sluice server never finished the echo")
	}
}
