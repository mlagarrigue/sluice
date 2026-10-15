package httpstream

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/net/quic"
)

// trailerHandler answers /stream with a streamed body whose trailers report
// what the producer counted, the gRPC shape, and anything else with a Body
// and the same trailers. /refused sets a trailer the writer must refuse.
func trailerHandler(b sluice.Batch[Request]) sluice.Batch[Response] {
	out := make([]Response, len(b.Items))
	for i := range b.Items {
		announce := []Header{{Name: []byte("Trailer"), Value: []byte("Grpc-Status, X-Sum")}}
		switch string(b.Items[i].Target) {
		case "/stream":
			sum := 0
			out[i] = Response{
				Headers: announce,
				Stream: func(yield func(sluice.Batch[[]byte]) bool) {
					for _, c := range []string{"ab", "cde"} {
						sum += len(c)
						if !yield(sluice.Batch[[]byte]{Items: [][]byte{[]byte(c)}}) {
							return
						}
					}
				},
				Trailers: func() []Header {
					return []Header{
						{Name: []byte("Grpc-Status"), Value: []byte("0")},
						{Name: []byte("X-Sum"), Value: strconv.AppendInt(nil, int64(sum), 10)},
					}
				},
			}
		case "/refused":
			out[i] = Response{
				Body:     []byte("abcde"),
				Trailers: func() []Header { return []Header{{Name: []byte("Content-Length"), Value: []byte("5")}} },
			}
		default:
			out[i] = Response{
				Headers: announce,
				Body:    []byte("abcde"),
				Trailers: func() []Header {
					return []Header{
						{Name: []byte("Grpc-Status"), Value: []byte("0")},
						{Name: []byte("X-Sum"), Value: []byte("5")},
					}
				},
			}
		}
	}
	return sluice.Batch[Response]{Items: out}
}

// checkClientTrailers asks net/http's client, over the protocols it is set
// to, for a buffered and a streamed answer and checks it read the body and
// then the trailers the handler set.
func checkClientTrailers(t *testing.T, client *http.Client, base, proto string) {
	t.Helper()
	for _, path := range []string{"/body", "/stream"} {
		resp, err := client.Get(base + path)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			t.Fatalf("%s: reading the body: %v", path, err)
		}
		if resp.Proto != proto {
			t.Fatalf("%s: answered over %s, want %s", path, resp.Proto, proto)
		}
		if string(body) != "abcde" {
			t.Errorf("%s: body %q, want abcde", path, body)
		}
		if got := resp.Trailer.Get("Grpc-Status"); got != "0" {
			t.Errorf("%s over %s: trailer Grpc-Status %q, want 0 (trailers %v)", path, resp.Proto, got, resp.Trailer)
		}
		if got := resp.Trailer.Get("X-Sum"); got != "5" {
			t.Errorf("%s over %s: trailer X-Sum %q, want 5", path, resp.Proto, got)
		}
	}
}

func TestResponseTrailersReachAnHTTP1Client(t *testing.T) {
	base := interopServer(t, Config{}, trailerHandler)
	client := &http.Client{Timeout: 5 * time.Second}
	defer client.CloseIdleConnections()
	checkClientTrailers(t, client, base, "HTTP/1.1")
}

func TestResponseTrailersReachAnHTTP2Client(t *testing.T) {
	base := interopServer(t, Config{}, trailerHandler)
	var protocols http.Protocols
	protocols.SetUnencryptedHTTP2(true)
	transport := &http.Transport{Protocols: &protocols}
	client := &http.Client{Timeout: 5 * time.Second, Transport: transport}
	defer client.CloseIdleConnections()
	checkClientTrailers(t, client, base, "HTTP/2.0")
}

// HTTP/3 has no client in the standard library: the stream is read raw and
// its frames checked in order — the head, the DATA, then a HEADERS frame
// holding the trailers and nothing after it.
func TestResponseTrailersReachAnHTTP3Client(t *testing.T) {
	conn, _, _ := startH3(t, quic.DefaultParameters(), trailerHandler)
	for _, path := range []string{"/body", "/stream"} {
		s := askH3(t, conn, h3Request("POST", path))
		_ = s.SetReadDeadline(time.Now().Add(10 * time.Second))
		raw, err := io.ReadAll(s)
		if err != nil {
			t.Fatalf("%s: the response stream ended with %v", path, err)
		}
		frames, _, err := parseH3Frames(nil, raw, 1<<24)
		if err != nil {
			t.Fatalf("%s: the response did not parse: %v", path, err)
		}
		if len(frames) < 3 || frames[0].Type != h3Headers || frames[len(frames)-1].Type != h3Headers {
			t.Fatalf("%s: %d frames, want HEADERS, DATA…, HEADERS", path, len(frames))
		}
		var body []byte
		for _, f := range frames[1 : len(frames)-1] {
			if f.Type != h3Data {
				t.Fatalf("%s: frame type %#x between the head and the trailers", path, f.Type)
			}
			body = append(body, f.Payload...)
		}
		if string(body) != "abcde" {
			t.Errorf("%s: body %q, want abcde", path, body)
		}
		trailers, err := newQPACKDecoder(nil, 0).Decode(nil, frames[len(frames)-1].Payload)
		if err != nil {
			t.Fatal(err)
		}
		got := map[string]string{}
		for _, h := range trailers {
			got[string(h.Name)] = string(h.Value)
		}
		if len(got) != 2 || got["grpc-status"] != "0" || got["x-sum"] != "5" {
			t.Errorf("%s: trailers %v, want grpc-status 0 and x-sum 5, lowercase", path, got)
		}
	}
}

// A trailer that frames the message is refused on every protocol. With the
// body in hand nothing is written yet, so the client gets the 500 a refused
// header gets.
func TestResponseTrailerRefusedBeforeTheBodyIsA500(t *testing.T) {
	base := interopServer(t, Config{}, trailerHandler)
	var h2 http.Protocols
	h2.SetUnencryptedHTTP2(true)
	for _, transport := range []*http.Transport{{}, {Protocols: &h2}} {
		client := &http.Client{Timeout: 5 * time.Second, Transport: transport}
		resp, err := client.Get(base + "/refused")
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != 500 {
			t.Errorf("%s: status %d, want 500", resp.Proto, resp.StatusCode)
		}
		client.CloseIdleConnections()
	}

	conn, _, _ := startH3(t, quic.DefaultParameters(), trailerHandler)
	if f := readH3Fields(t, askH3(t, conn, h3Request("GET", "/refused"))); f[":status"][0] != "500" {
		t.Errorf("HTTP/3: status %v, want 500", f[":status"])
	}
}

// What WriteBatch writes for trailers: a Body sent as one chunk with the
// trailers after the last, a HEAD answer and an HTTP/1.0 client with no
// trailers and the Content-Length they had before, and the Trailer header
// refused on a response that has none.
func TestWriteBatchResponseTrailers(t *testing.T) {
	trailers := func() []Header { return []Header{{Name: []byte("X-Sum"), Value: []byte("5")}} }
	res := Response{Body: []byte("abcde"), Trailers: trailers}
	out, err := WriteBatch(nil, Config{}, []Request{{Method: []byte("GET"), KeepAlive: true}}, []Response{res})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(out, []byte("Transfer-Encoding: chunked\r\n\r\n5\r\nabcde\r\n0\r\nX-Sum: 5\r\n\r\n")) ||
		bytes.Contains(out, []byte("Content-Length")) {
		t.Errorf("GET with trailers:\n%s", out)
	}

	called := false
	res.Trailers = func() []Header { called = true; return trailers() }
	for name, req := range map[string]Request{
		"HEAD":     {Method: []byte("HEAD"), KeepAlive: true},
		"HTTP/1.0": {Method: []byte("GET"), http10: true},
	} {
		out, err := WriteBatch(nil, Config{}, []Request{req}, []Response{res})
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Contains(out, []byte("Content-Length: 5\r\n")) || bytes.Contains(out, []byte("chunked")) {
			t.Errorf("%s:\n%s", name, out)
		}
	}
	if called {
		t.Error("Trailers was called for a response that cannot carry them")
	}

	announce := []Header{{Name: []byte("Trailer"), Value: []byte("X-Sum")}}
	if _, err := WriteBatch(nil, Config{}, nil, []Response{{Headers: announce}}); !errors.Is(err, ErrMalformed) {
		t.Errorf("Trailer without Trailers: %v, want ErrMalformed", err)
	}
	if _, err := WriteBatch(nil, Config{}, nil, []Response{{Headers: announce, Trailers: trailers}}); err != nil {
		t.Errorf("Trailer with Trailers: %v", err)
	}
	for _, name := range []string{"Transfer-Encoding", "trailer", "TE", "Connection", "X\r\nInjected"} {
		bad := Response{Trailers: func() []Header { return []Header{{Name: []byte(name), Value: []byte("x")}} }}
		if _, err := WriteBatch(nil, Config{}, nil, []Response{bad}); !errors.Is(err, ErrMalformed) {
			t.Errorf("trailer %q: %v, want ErrMalformed", strings.TrimSpace(name), err)
		}
	}
	panics := Response{Trailers: func() []Header { panic("boom") }}
	if _, err := WriteBatch(nil, Config{}, nil, []Response{panics}); err == nil {
		t.Error("a panicking Trailers was not refused")
	}
}
