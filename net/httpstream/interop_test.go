package httpstream

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
)

// interopServer starts a real listener behind Serve and returns a
// base URL an ordinary net/http.Client can dial. This is the one test in the
// package that talks to a stdlib client rather than a raw socket: the rest of
// the suite hand-writes bytes because it is checking framing this package
// owns, but a client nobody wrote by hand is the check that what goes out the
// wire is still an HTTP/1.1 response net/http itself will parse.
func interopServer(t *testing.T, cfg Config, h Handler) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = Serve(ctx, ln, cfg, h)
	}()
	t.Cleanup(func() { cancel(); <-done })
	return "http://" + ln.Addr().String()
}

// A client that reuses its transport keeps one connection across several
// requests — the ordinary keep-alive case Serve exists to answer
// efficiently, checked here from the far side with a client this package did
// not write.
func TestInteropKeepAliveReusesConnection(t *testing.T) {
	base := interopServer(t, Config{}, func(b sluice.Batch[Request]) sluice.Batch[Response] {
		out := make([]Response, 0, b.Len())
		for _, r := range b.Items {
			out = append(out, Response{Status: 200, Body: append([]byte(nil), r.Target...)})
		}
		return sluice.Batch[Response]{Items: out}
	})

	client := &http.Client{Timeout: 5 * time.Second}
	defer client.CloseIdleConnections()

	for i := range 3 {
		var reused bool
		trace := &httptrace.ClientTrace{
			GotConn: func(info httptrace.GotConnInfo) { reused = info.Reused },
		}
		req, err := http.NewRequestWithContext(
			httptrace.WithClientTrace(t.Context(), trace),
			http.MethodGet, fmt.Sprintf("%s/r%d", base, i), nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			t.Fatalf("request %d: reading body: %v", i, err)
		}
		if resp.StatusCode != 200 {
			t.Fatalf("request %d: status = %d", i, resp.StatusCode)
		}
		if want := fmt.Sprintf("/r%d", i); string(body) != want {
			t.Fatalf("request %d: body = %q, want %q", i, body, want)
		}
		// The first request opens the connection; every one after it must
		// reuse it, or keep-alive is not actually keeping anything alive.
		if i > 0 && !reused {
			t.Errorf("request %d: dialed a new connection instead of reusing the keep-alive one", i)
		}
	}
}

// A streamed response has no Content-Length by construction — its length is
// not known when the headers go out — so a stdlib client must fall back to
// chunked framing and still read the whole body back in order.
func TestInteropReadsChunkedStreamedResponse(t *testing.T) {
	base := interopServer(t, Config{}, func(b sluice.Batch[Request]) sluice.Batch[Response] {
		return sluice.Batch[Response]{Items: []Response{{
			Status: 200,
			Stream: func(yield func(sluice.Batch[[]byte]) bool) {
				for i := range 5 {
					if !yield(sluice.Batch[[]byte]{Items: [][]byte{fmt.Appendf(nil, "chunk%d-", i)}}) {
						return
					}
				}
			},
		}}}
	})

	client := &http.Client{Timeout: 5 * time.Second}
	defer client.CloseIdleConnections()

	resp, err := client.Get(base + "/stream")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if resp.ContentLength >= 0 {
		t.Errorf("ContentLength = %d, want unknown (-1): a streamed response cannot know its length in advance", resp.ContentLength)
	}
	if len(resp.TransferEncoding) == 0 || resp.TransferEncoding[0] != "chunked" {
		t.Errorf("TransferEncoding = %v, want [chunked]", resp.TransferEncoding)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading the streamed body: %v", err)
	}
	want := "chunk0-chunk1-chunk2-chunk3-chunk4-"
	if !bytes.Equal(body, []byte(want)) {
		t.Errorf("body = %q, want %q", body, want)
	}
}

// net/http's Transport sends Expect: 100-continue itself, ahead of the body,
// when ExpectContinueTimeout is set and the request carries one; it then
// waits for the interim response before writing the body. This exercises the
// server's 100-continue handling from a client this package never had to
// special-case for it.
func TestInteropExpectContinue(t *testing.T) {
	base := interopServer(t, Config{ReadTimeout: 2 * time.Second}, func(b sluice.Batch[Request]) sluice.Batch[Response] {
		out := make([]Response, 0, b.Len())
		for _, r := range b.Items {
			out = append(out, Response{Status: 200, Body: append([]byte(nil), r.Body...)})
		}
		return sluice.Batch[Response]{Items: out}
	})

	client := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			ExpectContinueTimeout: 2 * time.Second,
		},
	}
	defer client.CloseIdleConnections()

	req, err := http.NewRequest(http.MethodPost, base+"/upload", bytes.NewReader([]byte("payload")))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Expect", "100-continue")
	req.ContentLength = int64(len("payload"))

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("request with Expect: 100-continue: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200 — the interim 100 Continue either never arrived or the body was mishandled", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "payload" {
		t.Errorf("body = %q, want %q", body, "payload")
	}
}
