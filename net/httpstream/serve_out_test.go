package httpstream

import (
	"bytes"
	"context"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
)

// TestOutputBufferReturnsAboveTheRetainBound pipelines requests whose
// buffered answers, written together, outgrow outRetainMax, and reads the
// capacity serveConn keeps once each batch is written: never above the
// bound, on the all-buffered path and on the path where a streamed response
// follows the buffered ones. Without retainOut's bound the connection kept
// the largest batch's output until it closed, a cost MaxConns did not count.
func TestOutputBufferReturnsAboveTheRetainBound(t *testing.T) {
	big := bytes.Repeat([]byte("x"), 2*outRetainMax)
	cases := []struct {
		name    string
		targets []string
	}{
		{"buffered", []string{"/big", "/big", "/big", "/big"}},
		{"streamed", []string{"/big", "/big", "/stream"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var kept []int
			hook := func(c int) {
				mu.Lock()
				kept = append(kept, c)
				mu.Unlock()
			}
			outRetainHook.Store(&hook)
			t.Cleanup(func() { outRetainHook.Store(nil) })

			server, client := net.Pipe()
			defer func() { _ = client.Close() }()
			if err := client.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
				t.Fatal(err)
			}
			done := make(chan struct{})
			go func() {
				defer close(done)
				defer func() { _ = server.Close() }()
				serveConn(t.Context(), &prefixConn{Conn: server}, nil, Config{}.mustResolve(), func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
					out := make([]Response, 0, b.Len())
					for _, r := range b.Items {
						if string(r.Target) == "/stream" {
							out = append(out, Response{Status: 200, Stream: func(yield func(sluice.Batch[[]byte]) bool) {
								yield(sluice.Batch[[]byte]{Items: [][]byte{[]byte("streamed")}})
							}})
							continue
						}
						out = append(out, Response{Status: 200, Body: big})
					}
					return sluice.Batch[Response]{Items: out}
				})
			}()

			var reqs strings.Builder
			for i, target := range tc.targets {
				reqs.WriteString("GET " + target + " HTTP/1.1\r\nHost: h\r\n")
				if i == len(tc.targets)-1 {
					reqs.WriteString("Connection: close\r\n")
				}
				reqs.WriteString("\r\n")
			}
			go func() { _, _ = io.WriteString(client, reqs.String()) }()
			n, err := io.Copy(io.Discard, client)
			if err != nil {
				t.Fatal(err)
			}
			<-done

			bigs := int64(strings.Count(strings.Join(tc.targets, " "), "/big"))
			if n < bigs*int64(len(big)) {
				t.Fatalf("read %d bytes, want at least %d: the large answers did not go out", n, bigs*int64(len(big)))
			}
			mu.Lock()
			defer mu.Unlock()
			if len(kept) == 0 {
				t.Fatal("no batch was written")
			}
			for i, c := range kept {
				if c > outRetainMax {
					t.Errorf("after batch %d the connection keeps a %d-byte output buffer, above outRetainMax (%d)", i, c, outRetainMax)
				}
			}
		})
	}
}
