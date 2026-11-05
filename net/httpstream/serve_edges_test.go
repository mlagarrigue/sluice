package httpstream

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
)

func TestFirstMethod(t *testing.T) {
	if got := firstMethod([]Request{{Method: []byte("HEAD")}, {Method: []byte("GET")}}); string(got) != "HEAD" {
		t.Errorf("firstMethod = %q, want HEAD", got)
	}
	if got := firstMethod(nil); got != nil {
		t.Errorf("firstMethod(nil) = %q", got)
	}
}

// A write that timed out after ProgressBytes or more goes on; one that
// moved less is the error.
func TestWriteBoundGoesOnAfterProgress(t *testing.T) {
	b := writeBound{timeout: time.Second, progress: 4}
	noDeadline := func(time.Time) error { return nil }
	for moved, ok := range map[int]bool{4: true, 3: false} {
		var wrote []byte
		calls := 0
		write := func(p []byte) (int, error) {
			calls++
			if calls == 1 {
				wrote = append(wrote, p[:moved]...)
				return moved, os.ErrDeadlineExceeded
			}
			wrote = append(wrote, p...)
			return len(p), nil
		}
		err := b.write(noDeadline, write, []byte("abcdefgh"))
		if ok && (err != nil || string(wrote) != "abcdefgh") || !ok && !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Errorf("%d bytes before the deadline: %q, %v", moved, wrote, err)
		}
	}
}

// An address whose last connection leaves is forgotten.
func TestConnSetForgetsAnAddressWithNoConnections(t *testing.T) {
	s := newConnSet(Config{}.mustResolve())
	c := &addrConn{remote: &net.TCPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}}
	if s.add(c) == nil {
		t.Fatal("add refused the connection")
	}
	s.remove(c)
	if len(s.byIP) != 0 {
		t.Errorf("%d addresses counted after their last connection left", len(s.byIP))
	}
}

// The watch reads no more than its limit, and ends there by itself.
func TestPeerWatchStopsAtItsLimit(t *testing.T) {
	client, server := net.Pipe()
	defer func() { _ = client.Close(); _ = server.Close() }()
	go func() { _, _ = client.Write(make([]byte, 116)) }()
	w := watchPeer(&prefixConn{Conn: server}, 16)
	select {
	case <-w.done:
	case <-time.After(2 * time.Second):
		t.Fatal("the watch did not end at its limit")
	}
	if len(w.got) != 16 {
		t.Errorf("the watch read %d bytes, want its limit of 16", len(w.got))
	}
	w.stop()
}

// A batch context asked for Done after its batch was written follows its
// parent; one abandoned first is done already; with nothing of its own,
// Err is the parent's.
func TestBatchContextAfterItsBatch(t *testing.T) {
	parent, cancel := context.WithCancel(t.Context())
	x := &batchCtx{Context: parent, live: 1}
	x.written()
	done := x.Done()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Error("Done after the batch was written does not follow the parent")
	}

	x = &batchCtx{Context: t.Context(), live: 1}
	x.leave()
	x.written()
	select {
	case <-x.Done():
	case <-time.After(time.Second):
		t.Error("an abandoned batch's Done is not done")
	}

	parent, cancel = context.WithCancel(t.Context())
	x = &batchCtx{Context: parent, live: 1}
	cancel()
	if err := x.Err(); !errors.Is(err, context.Canceled) {
		t.Errorf("Err = %v, want the parent's", err)
	}
}

// exchange writes raw to a server and reads until it closes.
func exchange(t *testing.T, addr, raw string) string {
	t.Helper()
	c := dial(t, addr)
	defer func() { _ = c.Close() }()
	if _, err := io.WriteString(c, raw); err != nil {
		t.Fatal(err)
	}
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	out, _ := io.ReadAll(c)
	return string(out)
}

func streamOf(parts ...string) sluice.Stream[[]byte] {
	return func(yield func(sluice.Batch[[]byte]) bool) {
		for _, p := range parts {
			if !yield(sluice.Batch[[]byte]{Items: [][]byte{[]byte(p)}}) {
				return
			}
		}
	}
}

// A batch with a streamed response in it, written response by response:
// the close goes on the last and only there, a HEAD's buffered answer and a
// HEAD's 500 carry no body, a streamed 204 no framing, and a streamed body
// short of its declared length ends the connection.
func TestAStreamedBatchAtItsEdges(t *testing.T) {
	answers := map[string]Response{
		"/s":     {Status: 200, Stream: streamOf("zz")},
		"/h":     {Status: 200, Body: []byte("abc")},
		"/bad":   {Status: 200, Body: []byte("x"), Stream: streamOf("y")},
		"/nc":    {Status: 204, Stream: streamOf()},
		"/short": {Status: 200, ContentLength: 5, Stream: streamOf("abc")},
		"/next":  {Status: 200, Body: []byte("next")},
	}
	addr, _ := serveTest(t, Config{}, func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
		out := make([]Response, b.Len())
		for i, r := range b.Items {
			out[i] = answers[string(r.Target)]
		}
		return sluice.Batch[Response]{Items: out}
	})
	get := func(target string, extra ...string) string {
		return string(req(append([]string{"GET " + target + " HTTP/1.1", "Host: h"}, extra...)...))
	}
	head := func(target string, extra ...string) string {
		return string(req(append([]string{"HEAD " + target + " HTTP/1.1", "Host: h"}, extra...)...))
	}

	out := exchange(t, addr, get("/s")+get("/s", "Connection: close"))
	second := strings.LastIndex(out, "HTTP/1.1 200")
	if at := strings.Index(out, "Connection: close"); strings.Count(out, "Connection: close") != 1 || at < second {
		t.Errorf("the close is not on the last response alone:\n%s", out)
	}

	out = exchange(t, addr, head("/h")+get("/s", "Connection: close"))
	if strings.Contains(out, "abc") || strings.Count(out, "HTTP/1.1 200") != 2 {
		t.Errorf("a HEAD's buffered answer in a streamed batch:\n%s", out)
	}

	out = exchange(t, addr, head("/bad", "Connection: close"))
	if !strings.Contains(out, " 500 ") || strings.Contains(out, string(transportError(500).Body)) {
		t.Errorf("a HEAD's 500 in a streamed batch:\n%s", out)
	}

	out = exchange(t, addr, get("/nc", "Connection: close"))
	if !strings.Contains(out, " 204 ") || strings.Contains(strings.ToLower(out), "transfer-encoding") {
		t.Errorf("a streamed 204:\n%s", out)
	}

	out = exchange(t, addr, get("/short")+get("/next"))
	if strings.Count(out, "HTTP/1.1 ") != 1 || strings.Contains(out, "next") {
		t.Errorf("a streamed body short of its length did not end the connection:\n%s", out)
	}
}
