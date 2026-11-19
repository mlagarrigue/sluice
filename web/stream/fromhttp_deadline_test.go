package stream_test

import (
	"errors"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/net/httpstream"
)

// An answer cut short ends unterminated: what the producer yields before it
// stops, and the panic it stops with, if any.
func cut(r httpstream.Response) (chunks []string, stopped any) {
	defer func() { stopped = recover() }()
	r.Stream(func(b sluice.Batch[[]byte]) bool {
		for _, c := range b.Items {
			chunks = append(chunks, string(c))
		}
		return true
	})
	return chunks, nil
}

// A write deadline set through http.ResponseController holds under FromHTTP
// as it does under net/http: SetWriteDeadline answers nil rather than
// http.ErrNotSupported, a write past it fails with os.ErrDeadlineExceeded,
// and the answer ends unterminated — net/http closes the connection under a
// write that misses its deadline, so the client never takes a cut answer
// for a whole one.
func TestFromHTTPWriteDeadlineInThePast(t *testing.T) {
	// The handler's goroutine outlives answer once the answer streams: what
	// it saw comes back on a channel, not in variables read beside it.
	type seen struct{ set, wrote error }
	saw := make(chan seen, 1)
	h := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		set := http.NewResponseController(w).SetWriteDeadline(time.Now().Add(-time.Second))
		_, wrote := io.WriteString(w, "late")
		saw <- seen{set, wrote}
	})
	out := answer(t, h, get("/"))
	got := <-saw
	set, wrote := got.set, got.wrote
	if set != nil {
		t.Fatalf("SetWriteDeadline = %v, want nil", set)
	}
	if !errors.Is(wrote, os.ErrDeadlineExceeded) {
		t.Fatalf("a write past the deadline: %v, want os.ErrDeadlineExceeded", wrote)
	}
	r := out[0]
	if r.Stream == nil || r.ContentLength != 1 {
		t.Fatalf("answer %+v: want one byte promised and none sent, an unterminated end", r)
	}
	if chunks, _ := cut(r); len(chunks) != 0 {
		t.Errorf("the cut answer carried %q", chunks)
	}
}

// A deadline that passes while the transport is behind ends the wait: the
// handler is not held past it by a client that stopped reading, and its
// next write fails.
func TestFromHTTPWriteDeadlineEndsAWaitForTheTransport(t *testing.T) {
	wrote := make(chan error, 1)
	h := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
		_, _ = io.WriteString(w, "a")
		w.(http.Flusher).Flush() // nobody pulls: the hand-over waits
		_, err := io.WriteString(w, "b")
		wrote <- err
	})
	out := answer(t, h, get("/"))
	select {
	case err := <-wrote:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("the write after the deadline: %v, want os.ErrDeadlineExceeded", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the handler is still waiting for the transport 5 s after its 100 ms deadline")
	}
	if _, stopped := cut(out[0]); stopped == nil {
		t.Error("the streamed answer ended as if whole after its deadline passed")
	}
}

// A read deadline set through http.ResponseController holds as well: a read
// of the body begun past it fails with os.ErrDeadlineExceeded.
func TestFromHTTPReadDeadlineInThePast(t *testing.T) {
	var set, read error
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		set = http.NewResponseController(w).SetReadDeadline(time.Now().Add(-time.Second))
		_, read = io.ReadAll(r.Body)
	})
	req := httpstream.Request{Method: []byte("POST"), Target: []byte("/"), Body: []byte("payload")}
	answer(t, h, req)
	if set != nil {
		t.Fatalf("SetReadDeadline = %v, want nil", set)
	}
	if !errors.Is(read, os.ErrDeadlineExceeded) {
		t.Fatalf("a read past the deadline: %v, want os.ErrDeadlineExceeded", read)
	}
}
