package httpstream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"sync"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/internal/varint"
	"github.com/mlagarrigue/sluice/net/quic"
)

// h3StreamServer serves HTTP/3 with bodies streaming, on the parameters
// Config.TransportParameters gives such a server, and returns the client
// side with the server's stream window.
func h3StreamServer(t *testing.T, cfg Config, handle Handler) (*quic.Conn, int) {
	t.Helper()
	params, err := cfg.TransportParameters()
	if err != nil {
		t.Fatal(err)
	}
	conn, _, served := startH3With(t, cfg, params, quic.DefaultParameters(), handle)
	// A failed test says how the server ended, when it has: a client error
	// such as "the connection is closed" names nothing on its own.
	t.Cleanup(func() {
		select {
		case err := <-served:
			if err != nil && t.Failed() {
				t.Logf("the server ended with: %v", err)
			}
		default:
		}
	})
	return conn, int(params.InitialMaxStreamDataBidiRemote) //nolint:gosec // G115: a window in a test
}

// h3DataHead is a DATA frame's type and length, for a payload written apart.
func h3DataHead(length int) []byte {
	return varint.Append(varint.Append(nil, h3Data), uint64(length)) //nolint:gosec // G115: positive
}

// Two uploads at once, each four times the stream window: one as a single
// DATA frame larger than the window — it can only arrive if its payload is
// taken as it comes — and one as many small frames. Both arrive whole through
// Request.Stream, which needs the stream credit given back as the handler
// consumes.
func TestH3StreamedUploadsInParallel(t *testing.T) {
	cfg := Config{MaxBodyBytes: 16 << 10, MaxStreamedBodyBytes: 8 << 20}
	conn, window := h3StreamServer(t, cfg, streamedHandler)
	size := 4 * window
	bodies := [][]byte{pattern(size), bytes.Repeat([]byte("z"), size)}
	got := make([]string, len(bodies))
	var wg sync.WaitGroup
	for i, body := range bodies {
		s, err := conn.OpenStream(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		wg.Go(func() {
			req := h3Request("POST", "/up")
			if i == 0 {
				req = append(req, h3DataHead(len(body))...)
				req = append(req, body...)
			} else {
				for p := body; len(p) > 0; p = p[min(len(p), 16<<10):] {
					req = appendH3Frame(req, h3Data, p[:min(len(p), 16<<10)])
				}
			}
			if _, err := s.WriteFinal(req); err != nil {
				got[i] = err.Error()
				return
			}
			got[i], _ = readH3Body(t, s)
		})
	}
	wg.Wait()
	for i, body := range bodies {
		want := fmt.Sprintf("streamed %d %08x", len(body), crc32.ChecksumIEEE(body))
		if got[i] != want {
			t.Errorf("upload %d: %q, want %q", i, got[i], want)
		}
	}
}

// The stream's credit goes back as the handler consumes, not as the bytes
// arrive: with the handler not reading, the client cannot write past the
// stream window, and it can once the handler reads.
func TestH3StreamedCreditFollowsConsumption(t *testing.T) {
	cfg := Config{MaxBodyBytes: 16 << 10, MaxStreamedBodyBytes: 8 << 20}
	release := make(chan struct{})
	read := make(chan int, 1)
	conn, window := h3StreamServer(t, cfg, func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
		<-release
		body, _ := b.Items[0].ReadAll(nil, 8<<20)
		read <- len(body)
		return sluice.Batch[Response]{Items: []Response{{Status: 204}}}
	})
	s, err := conn.OpenStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	size := 2 * window
	wrote := make(chan error, 1)
	go func() {
		req := append(h3Request("POST", "/up"), h3DataHead(size)...)
		_, err := s.WriteFinal(append(req, pattern(size)...))
		wrote <- err
	}()
	select {
	case err := <-wrote:
		close(release)
		t.Fatalf("twice the stream window written before the handler read anything (%v)", err)
	case <-time.After(300 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-wrote:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the write never finished once the handler read")
	}
	if n := <-read; n != size {
		t.Errorf("the handler read %d bytes, want %d", n, size)
	}
}

// A body whole in the datagram that brought its head is still delivered in
// Body with streaming on; one whose FIN comes later streams.
func TestH3StreamingKeepsSmallBodiesWhole(t *testing.T) {
	conn, _ := h3StreamServer(t, Config{MaxBodyBytes: 16 << 10, MaxStreamedBodyBytes: 8 << 20}, streamedHandler)
	s, err := conn.OpenStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	// The FIN rides with the bytes: one STREAM frame, one delivery.
	if _, err := s.WriteFinal(appendH3Frame(h3Request("POST", "/"), h3Data, []byte("small"))); err != nil {
		t.Fatal(err)
	}
	body, _ := readH3Body(t, s)
	if want := fmt.Sprintf("whole 5 %08x", crc32.ChecksumIEEE([]byte("small"))); body != want {
		t.Errorf("answer %q, want %q", body, want)
	}
}

// A body the handler leaves unread: the response arrives whole, then the
// client is told to stop sending (STOP_SENDING, H3_NO_ERROR, RFC 9114 §4.1).
func TestH3UnreadStreamedBodyIsStoppedAfterTheResponse(t *testing.T) {
	conn, _ := h3StreamServer(t, Config{MaxBodyBytes: 1024, MaxStreamedBodyBytes: 1 << 20},
		func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
			return sluice.Batch[Response]{Items: []Response{{Status: 200, Body: []byte("not read")}}}
		})
	s := askH3Open(t, conn, appendH3Frame(h3Request("POST", "/"), h3Data, pattern(100)))
	if body, _ := readH3Body(t, s); body != "not read" {
		t.Fatalf("answer %q", body)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := s.Write(appendH3Frame(nil, h3Data, pattern(100))); err != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if code, reset := s.WasReset(); !reset || code != h3NoError {
		t.Errorf("the request side: reset %v with %#x, want STOP_SENDING H3_NO_ERROR", reset, code)
	}
}

// A client that resets its stream midway: the handler reading the body
// learns it from Err.
func TestH3StreamedBodyResetByTheClient(t *testing.T) {
	first := make(chan struct{})
	done := make(chan error, 1)
	conn, _ := h3StreamServer(t, Config{MaxBodyBytes: 1024, MaxStreamedBodyBytes: 1 << 20},
		func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
			src := b.Items[0].Stream()
			n := 0
			for range src.Stream() {
				n++
				if n == 1 {
					close(first)
				}
			}
			done <- src.Err()
			return sluice.Batch[Response]{Items: []Response{{Status: 200}}}
		})
	s := askH3Open(t, conn, appendH3Frame(h3Request("POST", "/"), h3Data, pattern(100)))
	select {
	case <-first:
	case <-time.After(5 * time.Second):
		t.Fatal("the handler got no piece of the body")
	}
	if err := s.Reset(h3RequestCancelled); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, ErrMalformed) {
			t.Errorf("Err after the client's reset: %v, want ErrMalformed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the handler is still waiting for a body its client reset")
	}
}

// A content-length past every bound is refused with 413 at the head; a
// streamed body past MaxStreamedBodyBytes ends its stream with ErrTooLarge.
func TestH3StreamedBodyBounds(t *testing.T) {
	conn, _ := h3StreamServer(t, Config{MaxBodyBytes: 1024, MaxStreamedBodyBytes: 4096}, streamedHandler)
	s := askH3(t, conn, h3Request("POST", "/", [2]string{"content-length", "5000"}))
	fields, _ := readH3FieldsAndBody(t, s)
	if fields[":status"] != "413" {
		t.Errorf("content-length 5000 against 4096: status %q, want 413", fields[":status"])
	}
	s = askH3Open(t, conn, append(append(h3Request("POST", "/"), h3DataHead(6000)...), pattern(6000)...))
	fields, body := readH3FieldsAndBody(t, s)
	if fields[":status"] != "400" || !bytes.Contains([]byte(body), []byte("exceeds a configured bound")) {
		t.Errorf("a 6000-byte body against 4096: %q %q, want the handler's ErrTooLarge", fields[":status"], body)
	}
}
