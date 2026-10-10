package interop

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	quicgo "github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/net/httpstream"
	"github.com/mlagarrigue/sluice/net/quic"
)

// h3Handle mirrors net/httpstream/internal/interopserver: every request is
// answered with its own target, and /stream with a body of three batches
// pulled one at a time, so a streamed response crosses a wire a client that
// shares no code reads.
func h3Handle(b sluice.Batch[httpstream.Request]) sluice.Batch[httpstream.Response] {
	out := make([]httpstream.Response, 0, b.Len())
	for _, r := range b.Items {
		if string(r.Target) == "/stream" {
			out = append(out, httpstream.Response{Status: 200, Stream: h3Streamed})
			continue
		}
		out = append(out, httpstream.Response{
			Status: 200,
			Body:   append([]byte("you asked for "), r.Target...),
		})
	}
	return sluice.Batch[httpstream.Response]{Items: out}
}

func h3Streamed(yield func(sluice.Batch[[]byte]) bool) {
	for i := 1; i <= 3; i++ {
		line := fmt.Appendf(nil, "batch %d\n", i)
		if !yield(sluice.Batch[[]byte]{Items: [][]byte{line}}) {
			return
		}
	}
}

const h3Stream = "batch 1\nbatch 2\nbatch 3\n"

// startH3Server runs ServeH3 behind a quic.Listener and returns the URL
// prefix to reach it. Every accepted connection is served until the test
// ends; what ServeH3 returned is reported through served.
func startH3Server(t *testing.T) (base string, served <-chan error) {
	t.Helper()
	pc := listenUDP(t)
	cfg := httpstream.Config{IdleTimeout: timeout, ReadTimeout: timeout, WriteTimeout: timeout}
	ln, err := quic.NewListener(pc, serverTLS(t), cfg.TransportParameters(), quic.ListenerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	out := make(chan error, 16)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { out <- httpstream.ServeH3(ctx, c, cfg, h3Handle) }()
		}
	}()
	return "https://" + pc.LocalAddr().String(), out
}

// h3Client is quic-go's HTTP/3 client over a 1-RTT handshake: sluice speaks
// no 0-RTT, and the default dialer would try early data first.
func h3Client(t *testing.T) *http.Client {
	t.Helper()
	tr := &http3.Transport{
		TLSClientConfig: clientTLS(),
		QUICConfig:      &quicgo.Config{MaxIdleTimeout: timeout},
		Dial: func(ctx context.Context, addr string, tlsCfg *tls.Config, cfg *quicgo.Config) (*quicgo.Conn, error) {
			return quicgo.DialAddr(ctx, addr, tlsCfg, cfg)
		},
	}
	t.Cleanup(func() { _ = tr.Close() })
	return &http.Client{Transport: tr, Timeout: timeout}
}

func h3Get(t *testing.T, client *http.Client, url string) string {
	t.Helper()
	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("GET %s: reading the body: %v", url, err)
	}
	if resp.StatusCode != http.StatusOK || resp.ProtoMajor != 3 {
		t.Fatalf("GET %s: %s over %s", url, resp.Status, resp.Proto)
	}
	return string(body)
}

// TestQuicGoHTTP3ClientAgainstServeH3: a third-party HTTP/3 client gets a
// plain response, a streamed one, and several requests on one connection —
// sequentially, then two streamed bodies at once.
func TestQuicGoHTTP3ClientAgainstServeH3(t *testing.T) {
	base, served := startH3Server(t)
	client := h3Client(t)

	if got := h3Get(t, client, base+"/hello"); got != "you asked for /hello" {
		t.Fatalf("plain response: got %q", got)
	}
	if got := h3Get(t, client, base+"/stream"); got != h3Stream {
		t.Fatalf("streamed response: got %q", got)
	}
	for i := range 3 {
		target := fmt.Sprintf("/again/%d", i)
		if got := h3Get(t, client, base+target); got != "you asked for "+target {
			t.Fatalf("request %d on the same connection: got %q", i, got)
		}
	}

	var wg sync.WaitGroup
	got := make([]string, 2)
	for i := range got {
		wg.Go(func() { got[i] = h3Get(t, client, base+"/stream") })
	}
	wg.Wait()
	for i, g := range got {
		if g != h3Stream {
			t.Fatalf("parallel streamed response %d: got %q", i, g)
		}
	}

	// Closing the client's connections must end ServeH3 cleanly, not by
	// the idle timeout.
	client.Transport.(*http3.Transport).CloseIdleConnections()
	select {
	case err := <-served:
		if err != nil {
			t.Logf("ServeH3 returned: %v", err)
		}
	case <-time.After(timeout):
		t.Fatal("ServeH3 did not return after the client closed")
	}
}
