package stream_test

import (
	"bufio"
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
	"net/http"
	"net/netip"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/net/httpstream"
	"github.com/mlagarrigue/sluice/web"
	"github.com/mlagarrigue/sluice/web/stream"
)

// answer runs h through FromHTTP on one batch of requests.
func answer(t *testing.T, h http.Handler, reqs ...httpstream.Request) []httpstream.Response {
	t.Helper()
	out := stream.FromHTTP(h)(context.Background(), sluice.Batch[httpstream.Request]{Items: reqs})
	if out.Len() != len(reqs) {
		t.Fatalf("%d responses for %d requests", out.Len(), len(reqs))
	}
	return out.Items
}

func get(target string, headers ...string) httpstream.Request {
	r := httpstream.Request{Method: []byte("GET"), Target: []byte(target)}
	for i := 0; i+1 < len(headers); i += 2 {
		r.Headers = append(r.Headers, httpstream.Header{Name: []byte(headers[i]), Value: []byte(headers[i+1])})
	}
	return r
}

// pulled is what a streamed response's producer yields, chunk by chunk.
func pulled(r httpstream.Response) []string {
	var chunks []string
	r.Stream(func(b sluice.Batch[[]byte]) bool {
		for _, c := range b.Items {
			chunks = append(chunks, string(c))
		}
		return true
	})
	return chunks
}

// values are the values of every field named name, in order.
func values(fields []httpstream.Header, name string) []string {
	var vs []string
	for _, f := range fields {
		if strings.EqualFold(string(f.Name), name) {
			vs = append(vs, string(f.Value))
		}
	}
	return vs
}

// A ServeMux with method patterns answers a batch through FromHTTP as it
// answers net/http: each request its own route, the mux's own 404, the web
// helpers unchanged.
func TestFromHTTPServesAServeMux(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /orders/{id}", func(w http.ResponseWriter, r *http.Request) {
		id, _, ok := web.PathInt64(r, "id")
		if !ok {
			_ = web.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "bad key"})
			return
		}
		_ = web.WriteJSON(w, http.StatusOK, map[string]int64{"id": id})
	})
	mux.HandleFunc("POST /orders", func(w http.ResponseWriter, r *http.Request) {
		var req struct{ Key int64 }
		if _, ok := web.DecodeJSON(r, 1<<10, &req); !ok {
			_ = web.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "bad body"})
			return
		}
		_ = web.WriteJSON(w, http.StatusOK, map[string]int64{"key": req.Key})
	})
	post := httpstream.Request{
		Method: []byte("POST"), Target: []byte("/orders"), Body: []byte(`{"key":9}`),
		Headers: []httpstream.Header{{Name: []byte("content-type"), Value: []byte("application/json")}},
	}
	out := answer(t, mux, get("/orders/7"), get("/orders/x"), get("/nowhere"), post)

	for i, want := range []struct {
		status int
		body   string
		ctype  string
	}{
		{200, `{"id":7}`, "application/json"},
		{400, `{"error":"bad key"}`, "application/json"},
		{404, "404 page not found", "text/plain; charset=utf-8"},
		{200, `{"key":9}`, "application/json"},
	} {
		r := out[i]
		if r.Status != want.status || strings.TrimSpace(string(r.Body)) != want.body {
			t.Errorf("response %d = %d %q, want %d %q", i, r.Status, r.Body, want.status, want.body)
		}
		if got := values(r.Headers, "content-type"); len(got) != 1 || got[0] != want.ctype {
			t.Errorf("response %d Content-Type = %q, want %q", i, got, want.ctype)
		}
		if r.Stream != nil || r.Trailers != nil {
			t.Errorf("response %d: a buffered answer came with a Stream or Trailers", i)
		}
	}
}

// The *http.Request is the one net/http would build from the same request.
func TestFromHTTPBuildsTheRequest(t *testing.T) {
	state := &tls.ConnectionState{NegotiatedProtocol: "h3"}
	var got *http.Request
	var body string
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r
		b, _ := io.ReadAll(r.Body)
		body = string(b)
	})
	req := httpstream.Request{
		Method: []byte("PUT"),
		Target: []byte("/a%20b/c?x=1&y=two"),
		Headers: []httpstream.Header{
			{Name: []byte("host"), Value: []byte("example.test")},
			{Name: []byte("x-trace-id"), Value: []byte("t1")},
			{Name: []byte("accept"), Value: []byte("a")},
			{Name: []byte("accept"), Value: []byte("b")},
		},
		Body:       []byte("payload"),
		RemoteAddr: netip.MustParseAddrPort("192.0.2.1:4433"),
		TLS:        state,
	}
	answer(t, h, req)

	switch {
	case got.Method != "PUT", got.URL.Path != "/a b/c", got.URL.RawQuery != "x=1&y=two",
		got.URL.Query().Get("y") != "two", got.RequestURI != "/a%20b/c?x=1&y=two":
		t.Errorf("method or target: %s %q %q %q", got.Method, got.URL.Path, got.URL.RawQuery, got.RequestURI)
	case got.Host != "example.test" || got.Header.Get("Host") != "":
		t.Errorf("Host = %q, Header Host = %q; want it moved to Request.Host", got.Host, got.Header.Get("Host"))
	case got.Header.Get("X-Trace-Id") != "t1" || strings.Join(got.Header.Values("Accept"), ",") != "a,b":
		t.Errorf("header = %v", got.Header)
	case got.Proto != "HTTP/3.0" || got.ProtoMajor != 3 || got.ProtoMinor != 0:
		t.Errorf("Proto = %s", got.Proto)
	case got.RemoteAddr != "192.0.2.1:4433" || got.TLS != state:
		t.Errorf("RemoteAddr %q, TLS %p", got.RemoteAddr, got.TLS)
	case got.ContentLength != 7 || body != "payload":
		t.Errorf("ContentLength %d, body %q", got.ContentLength, body)
	case got.Context().Err() == nil:
		t.Error("the request's context outlived its handler")
	}
}

// One goroutine per request: two handlers of one batch that wait for each
// other both finish, which a batch run in sequence never does.
func TestFromHTTPRunsTheBatchConcurrently(t *testing.T) {
	var ready sync.WaitGroup
	ready.Add(2)
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ready.Done()
		ready.Wait()
		_, _ = io.WriteString(w, r.URL.Path) //nolint:gosec // G705: an echo the test alone reads
	})
	done := make(chan []httpstream.Response)
	go func() { done <- answer(t, h, get("/a"), get("/b")) }()
	select {
	case out := <-done:
		if string(out[0].Body) != "/a" || string(out[1].Body) != "/b" {
			t.Errorf("bodies %q %q: an answer at the wrong position", out[0].Body, out[1].Body)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the batch's handlers ran one after the other")
	}
}

// What the transport refuses on a response is dropped before it, the length
// and the Content-Type are the writer's and the sniffer's, and the fields go
// out sorted, as net/http writes them.
func TestFromHTTPResponseFields(t *testing.T) {
	cases := []struct {
		name  string
		set   func(http.Header)
		body  string
		want  map[string][]string // nil value: absent
		order []string
	}{
		{
			name: "forbidden and malformed fields dropped",
			set: func(h http.Header) {
				h.Set("Content-Length", "5")
				h.Set("Connection", "close")
				h.Set("Transfer-Encoding", "chunked") //nolint:gosec // G113: set to be dropped, which is what is under test
				h.Set("Keep-Alive", "timeout=5")
				h["Bad Name"] = []string{"x"}
				h.Set("X-Split", "a\r\nInjected: yes")
				h["X-Nul"] = []string{"a\x00b"}
				h.Set("Date", "yesterday")
				h.Set("Content-Type", "text/plain")
			},
			body: "hello",
			want: map[string][]string{
				"content-length": nil, "connection": nil, "transfer-encoding": nil, "keep-alive": nil,
				"bad name": nil, "x-nul": nil, "date": nil,
				"x-split": {"a  Injected: yes"}, "content-type": {"text/plain"},
			},
			order: []string{"Content-Type", "X-Split"},
		},
		{
			name: "a valid Date kept",
			set:  func(h http.Header) { h.Set("Date", "Sun, 06 Nov 1994 08:49:37 GMT") },
			want: map[string][]string{"date": {"Sun, 06 Nov 1994 08:49:37 GMT"}},
		},
		{
			name: "Content-Type sniffed",
			body: "<html><body>hi</body></html>",
			want: map[string][]string{"content-type": {"text/html; charset=utf-8"}},
		},
		{
			name: "no sniffing under a Content-Encoding",
			set:  func(h http.Header) { h.Set("Content-Encoding", "gzip") },
			body: "\x1f\x8b\x08",
			want: map[string][]string{"content-type": nil, "content-encoding": {"gzip"}},
		},
		{
			name: "a Content-Type asked away",
			set:  func(h http.Header) { h["Content-Type"] = nil },
			body: "<html></html>",
			want: map[string][]string{"content-type": nil},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if tc.set != nil {
					tc.set(w.Header())
				}
				_, _ = io.WriteString(w, tc.body)
			})
			r := answer(t, h, get("/"))[0]
			for name, want := range tc.want {
				if got := values(r.Headers, name); strings.Join(got, "|") != strings.Join(want, "|") || (want == nil) != (got == nil) {
					t.Errorf("%s = %q, want %q", name, got, want)
				}
			}
			var names []string
			for _, f := range r.Headers {
				names = append(names, string(f.Name))
			}
			if tc.order != nil && strings.Join(names, ",") != strings.Join(tc.order, ",") {
				t.Errorf("fields %v, want %v", names, tc.order)
			}
			if string(r.Body) != tc.body {
				t.Errorf("body %q, want %q", r.Body, tc.body)
			}
		})
	}
}

// A Flush sends the head and streams the rest; the Content-Length the
// handler set frames it.
func TestFromHTTPFlushStreams(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "3")
		_, _ = io.WriteString(w, "a")
		w.(http.Flusher).Flush()
		_, _ = io.WriteString(w, "b")
		if err := http.NewResponseController(w).Flush(); err != nil {
			t.Errorf("ResponseController.Flush: %v", err)
		}
		_, _ = io.WriteString(w, "c")
	})
	r := answer(t, h, get("/"))[0]
	if r.Stream == nil || r.Body != nil || r.ContentLength != 3 {
		t.Fatalf("Stream %v, Body %q, ContentLength %d: want a stream of 3 bytes", r.Stream != nil, r.Body, r.ContentLength)
	}
	if got := strings.Join(pulled(r), "|"); got != "a|b|c" {
		t.Errorf("chunks %q, want a|b|c", got)
	}
	if values(r.Headers, "content-length") != nil {
		t.Error("Content-Length left in the fields")
	}
}

// Past the threshold an answer streams without a Flush, so it is never held
// whole.
func TestFromHTTPStreamsALargeAnswer(t *testing.T) {
	const size = 200 << 10
	h := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		chunk := strings.Repeat("x", 1<<10)
		for range size >> 10 {
			_, _ = io.WriteString(w, chunk)
		}
	})
	r := answer(t, h, get("/"))[0]
	if r.Stream == nil {
		t.Fatal("a 200 KiB answer was buffered whole")
	}
	chunks := pulled(r)
	if n := len(strings.Join(chunks, "")); n != size || len(chunks) < 3 {
		t.Errorf("%d bytes in %d chunks, want %d in several", n, len(chunks), size)
	}
}

// An answer without a body is decided at the Flush and what follows is
// written to nowhere, as net/http discards a HEAD's body.
func TestFromHTTPAnswerWithoutABodyAfterAFlush(t *testing.T) {
	for _, tc := range []struct {
		method string
		status int
	}{{"HEAD", 200}, {"GET", 204}, {"GET", 304}} {
		t.Run(fmt.Sprint(tc.method, tc.status), func(t *testing.T) {
			returned := make(chan error, 1)
			h := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				w.(http.Flusher).Flush()
				_, err := io.WriteString(w, strings.Repeat("x", 100<<10))
				returned <- err
			})
			r := answer(t, h, httpstream.Request{Method: []byte(tc.method), Target: []byte("/")})[0]
			if r.Status != tc.status || r.Stream != nil || r.Body != nil {
				t.Errorf("%d, Stream %v, Body %d bytes", r.Status, r.Stream != nil, len(r.Body))
			}
			select {
			case err := <-returned:
				if err != nil {
					t.Errorf("a write after the head: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("the handler is still waiting to write a body nobody reads")
			}
		})
	}
}

// Trailers announced in a Trailer field, or set under TrailerPrefix, go out
// after the body; a response without any asks for none, which would change
// HTTP/1.1's framing.
func TestFromHTTPTrailers(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/plain" {
			_, _ = io.WriteString(w, "body")
			return
		}
		w.Header().Set("Trailer", "X-Sum")
		_, _ = io.WriteString(w, "body")
		w.Header().Set("X-Sum", "42")
		w.Header().Set(http.TrailerPrefix+"X-Late", "yes")
	})
	out := answer(t, h, get("/with"), get("/plain"))
	if out[1].Trailers != nil {
		t.Error("a response without trailers asked for them")
	}
	if out[0].Trailers == nil {
		t.Fatal("announced trailers were not asked for")
	}
	if got := values(out[0].Headers, "trailer"); len(got) != 1 || got[0] != "X-Sum" {
		t.Errorf("Trailer field %q", got)
	}
	tr := out[0].Trailers()
	if values(tr, "x-sum")[0] != "42" || values(tr, "x-late")[0] != "yes" || values(out[0].Headers, "x-sum") != nil {
		t.Errorf("trailers %q, head %q", tr, out[0].Headers)
	}
}

// A streamed answer asks the transport for trailers only when they can
// come: announced, whatever the length, or set later under TrailerPrefix on
// an answer whose length is unknown. Asked for on an answer whose handler
// set a Content-Length, they would change its HTTP/1.1 framing to chunked.
func TestFromHTTPStreamedTrailers(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/framed":
			w.Header().Set("Content-Length", "2")
		case "/announced":
			w.Header().Set("Content-Length", "2")
			w.Header().Set("Trailer", "X-Sum")
		case "/late":
		}
		_, _ = io.WriteString(w, "a")
		w.(http.Flusher).Flush()
		_, _ = io.WriteString(w, "b")
		if r.URL.Path == "/late" {
			w.Header().Set(http.TrailerPrefix+"X-Late", "yes")
		}
	})
	out := answer(t, h, get("/framed"), get("/announced"), get("/late"))
	for i, want := range []bool{false, true, true} {
		if out[i].Stream == nil {
			t.Fatalf("response %d is not streamed", i)
		}
		if got := out[i].Trailers != nil; got != want {
			t.Errorf("response %d: Trailers asked for = %v, want %v", i, got, want)
		}
		pulled(out[i])
	}
}

// A name announced in Trailer that is not a token is left out of the
// Trailer field, as it is left out of the trailers that follow the body: the
// transport refuses a field value with a control byte or a CR or LF, so the
// name failed the whole response. The token names beside it stay announced.
func TestATrailerNameThatIsNotATokenIsDropped(t *testing.T) {
	for _, bad := range []string{"X-A\x01B", "X-A\r\nSet-Cookie: x", "Bad Name"} {
		h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Trailer", "X-Sum, "+bad)
			_, _ = io.WriteString(w, "body")
			w.Header().Set("X-Sum", "42")
		})
		reqs := []httpstream.Request{get("/")}
		out := answer(t, h, reqs...)
		if got := values(out[0].Headers, "trailer"); len(got) != 1 || got[0] != "X-Sum" {
			t.Errorf("Trailer %q: the field says %q, want only X-Sum", bad, got)
		}
		if _, err := httpstream.WriteBatch(nil, httpstream.Config{}, reqs, out); err != nil {
			t.Errorf("Trailer %q fails the whole response: %v", bad, err)
		}
	}
}

// A panic fails its own response, through the transport that logs it; the
// others of the batch are answered.
func TestFromHTTPPanics(t *testing.T) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/boom":
			panic("boom")
		case "/abort":
			panic(http.ErrAbortHandler)
		case "/late":
			_, _ = io.WriteString(w, "head")
			w.(http.Flusher).Flush()
			panic("late boom")
		}
		_, _ = io.WriteString(w, "fine")
	})
	out := answer(t, h, get("/boom"), get("/abort"), get("/late"), get("/ok"))

	raised := func(r httpstream.Response) (chunks []string, p any) {
		defer func() { p = recover() }()
		r.Stream(func(b sluice.Batch[[]byte]) bool {
			for _, c := range b.Items {
				chunks = append(chunks, string(c))
			}
			return true
		})
		return chunks, nil
	}
	if _, p := raised(out[0]); p == nil || !strings.Contains(fmt.Sprint(p), "boom") ||
		!strings.Contains(fmt.Sprint(p), "fromhttp_test.go") {
		t.Errorf("the early panic re-raised as %v; want its value and the handler's stack", p)
	}
	if chunks, p := raised(out[1]); p != nil || len(chunks) != 0 || out[1].ContentLength != 1 {
		t.Errorf("abort: %q, panic %v, ContentLength %d; want an empty stream short of its length", chunks, p, out[1].ContentLength)
	}
	if chunks, p := raised(out[2]); strings.Join(chunks, "") != "head" || !strings.Contains(fmt.Sprint(p), "late boom") {
		t.Errorf("late panic: %q then %v", chunks, p)
	}
	if string(out[3].Body) != "fine" {
		t.Errorf("the batch's other answer = %q", out[3].Body)
	}
}

// A client that stops reading stops the handler: its writes fail and its
// context ends.
func TestFromHTTPClientGone(t *testing.T) {
	stopped := make(chan error, 1)
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rc := http.NewResponseController(w)
		for {
			_, _ = io.WriteString(w, "tick")
			if err := rc.Flush(); err != nil {
				<-r.Context().Done()
				stopped <- err
				return
			}
		}
	})
	r := answer(t, h, get("/events"))[0]
	n := 0
	r.Stream(func(sluice.Batch[[]byte]) bool { n++; return n < 3 })
	select {
	case err := <-stopped:
		if err == nil {
			t.Error("Flush succeeded for a client gone")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the handler kept writing to a client that left")
	}
}

// A stream the transport never pulls — a head it refused, a connection gone
// first — does not hold its handler for good.
func TestFromHTTPUnpulledStreamReleasesItsHandler(t *testing.T) {
	stopped := make(chan struct{})
	h := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		defer close(stopped)
		rc := http.NewResponseController(w)
		for range 1000 {
			_, _ = io.WriteString(w, "tick")
			if rc.Flush() != nil {
				return
			}
		}
	})
	if r := answer(t, h, get("/events"))[0]; r.Stream == nil {
		t.Fatal("no stream")
	}
	deadline := time.After(10 * time.Second)
	for {
		runtime.GC()
		select {
		case <-stopped:
			return
		case <-deadline:
			t.Fatal("the handler of a dropped stream still waits")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// The body is the transport's only while the batch is answered: a read after
// the flush that ended it fails rather than reading a reused buffer.
func TestFromHTTPBodyReadAfterTheFlushFails(t *testing.T) {
	proceed := make(chan struct{})
	readErr := make(chan error, 1)
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.(http.Flusher).Flush()
		<-proceed
		_, err := io.ReadAll(r.Body)
		readErr <- err
	})
	r := answer(t, h, httpstream.Request{Method: []byte("POST"), Target: []byte("/"), Body: []byte("late")})[0]
	close(proceed)
	go pulled(r)
	if err := <-readErr; err == nil {
		t.Error("the body read after the batch was answered")
	}
}

// End to end over a real transport and net/http's client: a body sent in
// pieces reaches the handler through Request.Stream, and a flushed answer
// reaches the client before the handler returns.
func TestFromHTTPOverTheTransport(t *testing.T) {
	release := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("POST /upload", func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		fmt.Fprintf(w, "%d bytes, ContentLength %d", len(b), r.ContentLength) //nolint:gosec // G705: an echo the test alone reads
	})
	mux.HandleFunc("GET /events", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: first\n\n")
		w.(http.Flusher).Flush()
		<-release
		_, _ = io.WriteString(w, "data: second\n\n")
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &httpstream.Server{Handler: stream.FromHTTP(mux)}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	base := "http://" + ln.Addr().String()

	pr, pw := io.Pipe()
	go func() {
		for range 64 {
			_, _ = pw.Write([]byte(strings.Repeat("y", 1<<10)))
		}
		_ = pw.Close()
	}()
	resp, err := http.Post(base+"/upload", "application/octet-stream", pr) // chunked: no length
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if string(b) != "65536 bytes, ContentLength -1" {
		t.Errorf("upload answered %q", b)
	}

	resp, err = http.Get(base + "/events")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	br := bufio.NewReader(resp.Body)
	first, err := br.ReadString('\n')
	if err != nil || first != "data: first\n" {
		t.Fatalf("first event %q, %v: the flush did not reach the client", first, err)
	}
	close(release)
	rest, _ := io.ReadAll(br)
	if string(rest) != "\ndata: second\n\n" {
		t.Errorf("rest %q", rest)
	}
}

// ServeH3 serves until its context ends, then drains and closes the closers;
// a listener that fails still closes them.
func TestServeH3Lifecycle(t *testing.T) {
	cert := selfSignedH3(t)
	closed := &closeCounter{}
	srv := &http.Server{
		Addr:              "127.0.0.1:0",
		ReadHeaderTimeout: time.Second,
		Handler:           http.NotFoundHandler(),
		TLSConfig:         &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- stream.ServeH3(ctx, srv, time.Second, "", "", closed) }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("ServeH3 = %v, want nil after a normal stop", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ServeH3 did not return once its context ended")
	}
	if closed.n != 1 {
		t.Errorf("closer closed %d times, want 1", closed.n)
	}

	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pc.Close() }()
	srv.Addr = pc.LocalAddr().String() // taken
	if err := stream.ServeH3(context.Background(), srv, time.Second, "", "", closed); err == nil {
		t.Error("ServeH3 on a taken address returned nil")
	}
	if closed.n != 2 {
		t.Errorf("after a failed listen, the closer was closed %d times in all, want 2", closed.n)
	}
	if err := stream.ServeH3(context.Background(), nil, 0, "", ""); err == nil {
		t.Error("ServeH3 without a server returned nil")
	}
}

type closeCounter struct{ n int }

func (c *closeCounter) Shutdown(context.Context) error { c.n++; return nil }

func selfSignedH3(t *testing.T) tls.Certificate {
	t.Helper()
	return certWithUsage(t, x509.KeyUsageDigitalSignature)
}

// certWithUsage is a self-signed certificate for 127.0.0.1 with the given
// Key Usage.
func certWithUsage(t *testing.T, usage x509.KeyUsage) tls.Certificate {
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
		KeyUsage:     usage,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}
