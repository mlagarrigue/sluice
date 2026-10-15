package httpstream

import (
	"bufio"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/net/quic"
)

// lengthHandler answers with a streamed body of "ab" then "cde" under a
// declared ContentLength: /exact declares the 5 bytes it yields, /short 6,
// /long 4 — the second batch would cross it — and /after 5 then yields a
// surplus "f" once the length is out. /trailers declares 5 and has
// trailers, which HTTP/1.1 carries only after a chunked body. /pulled
// records whether its producer ran, for the HEAD answer that must not pull
// it.
func lengthHandler(pulled *bool) Handler {
	return func(b sluice.Batch[Request]) sluice.Batch[Response] {
		out := make([]Response, len(b.Items))
		for i := range b.Items {
			chunks := []string{"ab", "cde"}
			length := int64(5)
			var trailers func() []Header
			switch string(b.Items[i].Target) {
			case "/short":
				length = 6
			case "/long":
				length = 4
			case "/after":
				chunks = append(chunks, "f")
			case "/trailers":
				trailers = func() []Header { return []Header{{Name: []byte("X-Sum"), Value: []byte("5")}} }
			}
			out[i] = Response{
				ContentLength: length,
				Trailers:      trailers,
				Stream: func(yield func(sluice.Batch[[]byte]) bool) {
					if pulled != nil {
						*pulled = true
					}
					for _, c := range chunks {
						if !yield(sluice.Batch[[]byte]{Items: [][]byte{[]byte(c)}}) {
							return
						}
					}
				},
			}
		}
		return sluice.Batch[Response]{Items: out}
	}
}

// checkClientLength asks net/http's client for each case and checks what it
// read: the declared length and the whole body when the producer kept to
// it, a failed read — never a clean end — when it did not.
func checkClientLength(t *testing.T, client *http.Client, base, proto string) {
	t.Helper()
	resp, err := client.Get(base + "/exact")
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatalf("/exact: reading the body: %v", err)
	}
	if resp.Proto != proto {
		t.Fatalf("answered over %s, want %s", resp.Proto, proto)
	}
	if resp.ContentLength != 5 || string(body) != "abcde" {
		t.Errorf("/exact: Content-Length %d and body %q, want 5 and abcde", resp.ContentLength, body)
	}
	if len(resp.TransferEncoding) != 0 {
		t.Errorf("/exact: Transfer-Encoding %v beside a Content-Length", resp.TransferEncoding)
	}
	for _, path := range []string{"/short", "/long"} {
		resp, err := client.Get(base + path)
		if err != nil {
			continue // a failure before the head is a failure too
		}
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err == nil {
			t.Errorf("%s: the body %q read as whole", path, body)
		}
		if strings.Contains(string(body), "cde") && path == "/long" {
			t.Errorf("/long: the batch past the declared length reached the client: %q", body)
		}
	}
}

func TestResponseContentLengthOverHTTP1(t *testing.T) {
	base := interopServer(t, Config{}, lengthHandler(nil))
	client := &http.Client{Timeout: 5 * time.Second}
	defer client.CloseIdleConnections()
	checkClientLength(t, client, base, "HTTP/1.1")

	// Trailers keep the body chunked, with no Content-Length beside the
	// Transfer-Encoding.
	resp, err := client.Get(base + "/trailers")
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil || string(body) != "abcde" {
		t.Fatalf("/trailers: body %q, %v", body, err)
	}
	if resp.ContentLength != -1 || len(resp.TransferEncoding) != 1 || resp.Trailer.Get("X-Sum") != "5" {
		t.Errorf("/trailers: Content-Length %d, Transfer-Encoding %v, trailers %v; want chunked with X-Sum",
			resp.ContentLength, resp.TransferEncoding, resp.Trailer)
	}
}

// rawHTTP1 sends request on a fresh connection and reads until the server
// closes it, failing if it does not within the deadline.
func rawHTTP1(t *testing.T, base, request string) string {
	t.Helper()
	conn, err := net.Dial("tcp", strings.TrimPrefix(base, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.WriteString(conn, request); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(conn)
	if err != nil {
		t.Fatalf("the connection did not close: %v (read %q)", err, got)
	}
	return string(got)
}

// Once the declared length is out, a surplus closes the connection without
// a byte of it written, and nothing is answered after.
func TestResponseContentLengthSurplusAfterTheBodyCloses(t *testing.T) {
	base := interopServer(t, Config{}, lengthHandler(nil))
	got := rawHTTP1(t, base, "GET /after HTTP/1.1\r\nHost: h\r\n\r\nGET /exact HTTP/1.1\r\nHost: h\r\n\r\n")
	if !strings.Contains(got, "Content-Length: 5\r\n") || !strings.HasSuffix(got, "\r\n\r\nabcde") {
		t.Errorf("the answer was %q, want the declared 5 bytes and the close", got)
	}
}

// An HTTP/1.0 client gets the declared length rather than a body to EOF,
// so the connection it asked to keep stays open.
func TestResponseContentLengthOverHTTP10(t *testing.T) {
	base := interopServer(t, Config{}, lengthHandler(nil))
	conn, err := net.Dial("tcp", strings.TrimPrefix(base, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	r := bufio.NewReader(conn)
	for range 2 {
		if _, err := io.WriteString(conn, "GET /exact HTTP/1.0\r\nConnection: keep-alive\r\n\r\n"); err != nil {
			t.Fatal(err)
		}
		resp, err := http.ReadResponse(r, nil)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil || resp.ContentLength != 5 || string(body) != "abcde" || resp.Close {
			t.Fatalf("Content-Length %d, body %q, close %v, %v; want 5, abcde, kept open",
				resp.ContentLength, body, resp.Close, err)
		}
	}
}

// A HEAD answer states the declared length and never pulls the producer.
func TestResponseContentLengthHead(t *testing.T) {
	pulled := false
	base := interopServer(t, Config{}, lengthHandler(&pulled))
	got := rawHTTP1(t, base, "HEAD /exact HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n")
	if !strings.Contains(got, "Content-Length: 5\r\n") || strings.Contains(got, "Transfer-Encoding") ||
		!strings.HasSuffix(got, "\r\n\r\n") {
		t.Errorf("the HEAD answer was %q, want Content-Length: 5 and no body", got)
	}
	if pulled {
		t.Error("the producer was pulled for a HEAD answer")
	}
}

func TestResponseContentLengthOverHTTP2(t *testing.T) {
	base := interopServer(t, Config{}, lengthHandler(nil))
	var protocols http.Protocols
	protocols.SetUnencryptedHTTP2(true)
	client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{Protocols: &protocols}}
	defer client.CloseIdleConnections()
	checkClientLength(t, client, base, "HTTP/2.0")
	// The connection serves on after the resets.
	resp, err := client.Get(base + "/trailers")
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil || string(body) != "abcde" || resp.ContentLength != 5 || resp.Trailer.Get("X-Sum") != "5" {
		t.Errorf("/trailers: body %q, Content-Length %d, trailers %v, %v; want both", body, resp.ContentLength, resp.Trailer, err)
	}
}

func TestResponseContentLengthOverHTTP3(t *testing.T) {
	conn, _, _ := startH3(t, quic.DefaultParameters(), lengthHandler(nil))
	s := askH3(t, conn, h3Request("GET", "/exact"))
	_ = s.SetReadDeadline(time.Now().Add(10 * time.Second))
	raw, err := io.ReadAll(s)
	if err != nil {
		t.Fatalf("/exact: the response stream ended with %v", err)
	}
	frames, _, err := parseH3Frames(nil, raw, 1<<24)
	if err != nil || len(frames) == 0 {
		t.Fatalf("/exact: %d frames, %v", len(frames), err)
	}
	head, err := newQPACKDecoder(nil, 0).Decode(nil, frames[0].Payload)
	if err != nil {
		t.Fatal(err)
	}
	length := ""
	for _, h := range head {
		if string(h.Name) == "content-length" {
			length = string(h.Value)
		}
	}
	var body []byte
	for _, f := range frames[1:] {
		body = append(body, f.Payload...)
	}
	if length != "5" || string(body) != "abcde" {
		t.Errorf("/exact: content-length %q and body %q, want 5 and abcde", length, body)
	}

	for _, path := range []string{"/short", "/long", "/after"} {
		s := askH3(t, conn, h3Request("GET", path))
		_ = s.SetReadDeadline(time.Now().Add(10 * time.Second))
		raw, err := io.ReadAll(s)
		if err == nil || errors.Is(err, io.EOF) {
			t.Errorf("%s: the stream ended cleanly, as if its body were whole", path)
		}
		if code, reset := s.WasReset(); !reset || code != h3InternalError {
			t.Errorf("%s: reset %v with %#x, want H3_INTERNAL_ERROR", path, reset, code)
		}
		if path == "/long" && strings.Contains(string(raw), "cde") {
			t.Errorf("/long: the batch past the declared length reached the client: %q", raw)
		}
	}
}

// The declared length describes a Stream and nothing else.
func TestResponseContentLengthRefused(t *testing.T) {
	stream := func(func(sluice.Batch[[]byte]) bool) {}
	for name, r := range map[string]Response{
		"negative":       {ContentLength: -1, Stream: stream},
		"beside a Body":  {ContentLength: 5, Body: []byte("abcde")},
		"with no Stream": {ContentLength: 5},
	} {
		if _, err := WriteBatch(nil, Config{}, nil, []Response{r}); !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: WriteBatch returned %v, want ErrMalformed", name, err)
		}
	}
}

// net/http's HTTP/2 client checks the length itself, so the server's own
// answer is read raw: a producer that misses its length gets RST_STREAM,
// never END_STREAM, and the batch past the length never leaves.
func TestResponseContentLengthResetsTheHTTP2Stream(t *testing.T) {
	addr, _ := serveH2(t, H2Config{}, lengthHandler(nil))
	c := dialH2(t, addr)
	for i, path := range []string{"/short", "/long", "/after"} {
		id := uint32(2*i + 1)
		c.request(id, "GET", path)
		c.flush()
		if code := c.waitReset(id); code != 0x2 {
			t.Errorf("%s: reset with %#x, want INTERNAL_ERROR", path, code)
		}
		if c.ended[id] {
			t.Errorf("%s: the stream ended, as if its body were whole", path)
		}
		if path == "/long" && strings.Contains(c.got[id], "cde") {
			t.Errorf("/long: the batch past the declared length was sent: %q", c.got[id])
		}
	}
	c.request(7, "GET", "/exact")
	c.flush()
	c.pump(func() bool { return c.ended[7] || c.sawGoAway })
	if c.got[7] != "abcde" {
		t.Errorf("after the resets the connection answered %q, want abcde", c.got[7])
	}
}
