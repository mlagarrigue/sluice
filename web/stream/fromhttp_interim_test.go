package stream_test

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/textproto"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/net/httpstream"
	"github.com/mlagarrigue/sluice/web/stream"
)

// A net/http handler's WriteHeader(103) reaches the client under FromHTTP
// as it does under net/http: a 103 carrying the Link set so far, then the
// final response, over HTTP/1.1 and HTTP/2.
func TestFromHTTPSendsEarlyHints(t *testing.T) {
	const link = "</app.css>; rel=preload; as=style"
	h := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Link", link)
		w.WriteHeader(http.StatusEarlyHints)
		w.Header().Del("Link")
		_, _ = io.WriteString(w, "page")
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = httpstream.Serve(ctx, ln, httpstream.Config{}, stream.FromHTTP(h))
	}()
	defer func() { cancel(); <-done }()

	for _, h2 := range []bool{false, true} {
		var protocols http.Protocols
		protocols.SetHTTP1(!h2)
		protocols.SetUnencryptedHTTP2(h2)
		client := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{Protocols: &protocols}}
		var codes []int
		var hinted string
		trace := &httptrace.ClientTrace{Got1xxResponse: func(code int, hdr textproto.MIMEHeader) error {
			codes, hinted = append(codes, code), hdr.Get("Link")
			return nil
		}}
		req, err := http.NewRequestWithContext(httptrace.WithClientTrace(context.Background(), trace), "GET", "http://"+ln.Addr().String()+"/", nil)
		if err != nil {
			t.Fatal(err)
		}
		res, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(res.Body)
		_ = res.Body.Close()
		client.CloseIdleConnections()
		if len(codes) != 1 || codes[0] != 103 || hinted != link {
			t.Errorf("HTTP/2 %v: interim %v with Link %q, want one 103 with %q", h2, codes, hinted, link)
		}
		if res.StatusCode != 200 || string(body) != "page" || res.Header.Get("Link") != "" {
			t.Errorf("HTTP/2 %v: final %d %q, Link %q: want 200 page without the hint's Link", h2, res.StatusCode, body, res.Header.Get("Link"))
		}
	}
}
