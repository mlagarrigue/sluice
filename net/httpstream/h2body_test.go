package httpstream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
)

// streamedHandler answers each request with how its body arrived and what it
// held: "streamed <length> <crc32>" or "whole <length> <crc32>", or the
// error that ended it.
func streamedHandler(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
	out := make([]Response, b.Len())
	for i := range b.Items {
		r := &b.Items[i]
		form := "whole"
		if r.Body == nil && r.body != nil {
			form = "streamed"
		}
		body, err := r.ReadAll(nil, 8<<20)
		if err != nil {
			out[i] = Response{Status: 400, Body: []byte(err.Error())}
			continue
		}
		out[i] = Response{Status: 200, Body: fmt.Appendf(nil, "%s %d %08x", form, len(body), crc32.ChecksumIEEE(body))}
	}
	return sluice.Batch[Response]{Items: out}
}

// h2StreamCfg streams bodies past a small whole-body bound, at the default
// stream window.
func h2StreamCfg() Config {
	return Config{MaxBodyBytes: 16 << 10, MaxStreamedBodyBytes: 8 << 20}
}

// Two uploads at once, each sixteen times the stream window and far past
// MaxBodyBytes, from a client this package did not write: each arrives whole
// through Request.Stream, which can only happen if the window of each stream
// is granted back as its handler consumes — the client may never have more
// than one window in flight on a stream.
func TestH2StreamedUploadsInParallel(t *testing.T) {
	base := interopServer(t, h2StreamCfg(), streamedHandler)
	var protocols http.Protocols
	protocols.SetUnencryptedHTTP2(true)
	client := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{Protocols: &protocols}}
	defer client.CloseIdleConnections()

	const size = 16 * initialWindow
	bodies := [][]byte{pattern(size), bytes.Repeat([]byte("z"), size)}
	got := make([]string, len(bodies))
	errs := make([]error, len(bodies))
	var wg sync.WaitGroup
	for i, body := range bodies {
		wg.Go(func() {
			// The second has no length, as an upload produced on the fly.
			var r io.Reader = bytes.NewReader(body)
			if i == 1 {
				r = io.MultiReader(r)
			}
			res, err := client.Post(base+"/up", "application/octet-stream", r)
			if err != nil {
				errs[i] = err
				return
			}
			defer func() { _ = res.Body.Close() }()
			b, err := io.ReadAll(res.Body)
			if res.ProtoMajor != 2 {
				err = errors.Join(err, fmt.Errorf("served over %s", res.Proto))
			}
			got[i], errs[i] = string(b), err
		})
	}
	wg.Wait()
	for i, body := range bodies {
		want := fmt.Sprintf("streamed %d %08x", len(body), crc32.ChecksumIEEE(body))
		if errs[i] != nil || got[i] != want {
			t.Errorf("upload %d: %q, %v; want %q", i, got[i], errs[i], want)
		}
	}
}

// A body whole in the frames that brought its head, under MaxBodyBytes, is
// still delivered in Body with streaming on, and its requests batch.
func TestH2StreamingKeepsSmallBodiesWhole(t *testing.T) {
	addr, sizes := serveH2(t, h2StreamCfg(), streamedHandler)
	c := dialH2(t, addr)
	for id := uint32(1); id <= 3; id += 2 {
		c.frame(h2Frame{Type: frameHeaders, Flags: flagEndHeaders, StreamID: id, Payload: headBlock("POST", "/")})
		c.data(id, []byte("small"), true)
	}
	c.flush()
	got := c.bodies(2)
	want := fmt.Sprintf("whole 5 %08x", crc32.ChecksumIEEE([]byte("small")))
	if got[1] != want || got[3] != want {
		t.Errorf("answers %q and %q, want %q", got[1], got[3], want)
	}
	if s := sizes(); len(s) != 1 || s[0] != 2 {
		t.Errorf("batches %v, want the two requests in one", s)
	}
}

// The stream's window goes back as the handler consumes, not as the bytes
// arrive: a handler that has not read leaves the peer with what it sent
// against, which is the backpressure.
func TestH2StreamedWindowFollowsConsumption(t *testing.T) {
	release := make(chan struct{})
	read := make(chan string, 1)
	addr, _ := serveH2(t, h2StreamCfg(), func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
		<-release
		body, err := b.Items[0].ReadAll(nil, 1<<20)
		read <- fmt.Sprint(len(body), err)
		return sluice.Batch[Response]{Items: []Response{{Status: 204}}}
	})
	c := dialH2(t, addr)
	c.frame(h2Frame{Type: frameHeaders, Flags: flagEndHeaders, StreamID: 1, Payload: headBlock("POST", "/")})
	c.data(1, pattern(initialWindow-535), false)
	// A PING behind the body: its acknowledgement follows whatever the
	// reader queued while it read the DATA before it.
	c.frame(h2Frame{Type: framePing, Payload: make([]byte, 8)})
	c.flush()
	c.pump(func() bool { return c.pings > 0 })
	if c.pings == 0 {
		t.Fatal("no PING acknowledgement")
	}
	if n := c.windows[1]; n != 0 {
		t.Fatalf("%d WINDOW_UPDATE on the stream before its handler read anything", n)
	}
	close(release)
	c.waitWindowUpdate(1)
	c.data(1, pattern(535), true)
	c.flush()
	c.bodies(1)
	if got := <-read; got != fmt.Sprint(initialWindow, nil) {
		t.Errorf("the handler read %s, want %d bytes", got, initialWindow)
	}
}

// A body the handler leaves unread: the response goes out whole, then
// RST_STREAM(NO_ERROR) tells the client to stop sending (RFC 9113 §8.1), and
// the connection carries on.
func TestH2UnreadStreamedBodyIsResetAfterTheResponse(t *testing.T) {
	addr, _ := serveH2(t, h2StreamCfg(), func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
		out := make([]Response, b.Len())
		for i := range out {
			out[i] = Response{Status: 200, Body: []byte("not read")}
		}
		return sluice.Batch[Response]{Items: out}
	})
	c := dialH2(t, addr)
	c.frame(h2Frame{Type: frameHeaders, Flags: flagEndHeaders, StreamID: 1, Payload: headBlock("POST", "/")})
	c.data(1, pattern(1000), false)
	c.flush()
	if code := c.waitReset(1); code != errNoError {
		t.Fatalf("stream reset with %#x, want NO_ERROR", code)
	}
	if c.resetEarly[1] || c.got[1] != "not read" {
		t.Fatalf("the reset came before the response's end (answer %q)", c.got[1])
	}
	// Late DATA from a client that had not yet read the reset is tolerated,
	// and the next request is served.
	c.data(1, pattern(1000), true)
	c.request(3, "GET", "/next")
	c.flush()
	if got := c.bodies(2)[3]; got != "not read" {
		t.Errorf("the request behind it answered %q", got)
	}
	if c.sawGoAway {
		t.Errorf("the connection ended: GOAWAY %x", c.goAway)
	}
}

// A client that resets its stream midway: the handler reading the body
// learns it from Err, as a broken HTTP/1.1 body says it.
func TestH2StreamedBodyResetByTheClient(t *testing.T) {
	first := make(chan struct{})
	done := make(chan error, 1)
	addr, _ := serveH2(t, h2StreamCfg(), func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
		src := b.Items[0].Stream()
		n := 0
		for batch := range src.Stream() {
			if n == 0 {
				close(first)
			}
			for _, p := range batch.Items {
				n += len(p)
			}
		}
		done <- src.Err()
		return sluice.Batch[Response]{Items: []Response{{Status: 200}}}
	})
	c := dialH2(t, addr)
	c.frame(h2Frame{Type: frameHeaders, Flags: flagEndHeaders, StreamID: 1, Payload: headBlock("POST", "/")})
	c.data(1, pattern(1000), false)
	c.flush()
	select {
	case <-first:
	case <-time.After(5 * time.Second):
		t.Fatal("the handler got no piece of the body")
	}
	c.frame(h2Frame{Type: frameRSTStream, StreamID: 1, Payload: windowUpdatePayload(errCancel)})
	c.flush()
	select {
	case err := <-done:
		if !errors.Is(err, ErrMalformed) {
			t.Errorf("Err after the client's reset: %v, want ErrMalformed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the handler is still waiting for a body its client reset")
	}
}

// A body that grows past MaxStreamedBodyBytes midway ends its stream with
// ErrTooLarge; the handler answers, and the stream is reset after it.
func TestH2StreamedBodyPastItsBound(t *testing.T) {
	cfg := Config{MaxBodyBytes: 1024, MaxStreamedBodyBytes: 4096}
	addr, _ := serveH2(t, cfg, streamedHandler)
	c := dialH2(t, addr)
	c.frame(h2Frame{Type: frameHeaders, Flags: flagEndHeaders, StreamID: 1, Payload: headBlock("POST", "/")})
	c.data(1, pattern(2000), false)
	c.data(1, pattern(3000), false)
	c.flush()
	if code := c.waitReset(1); code != errNoError || c.resetEarly[1] {
		t.Fatalf("reset %#x, early %v: want NO_ERROR after the response", code, c.resetEarly[1])
	}
	if h2Status(c.heads[1]) != 400 || !bytes.Contains([]byte(c.got[1]), []byte("exceeds a configured bound")) {
		t.Errorf("answer %d %q, want the handler's ErrTooLarge", h2Status(c.heads[1]), c.got[1])
	}
}

// A content-length past every bound is refused with 413 at the head, before
// the 100 Continue the client is waiting for; one within the bound gets its
// 100 when the handler first reads, and the body after it.
func TestH2StreamedBodyAndContinue(t *testing.T) {
	addr, _ := serveH2(t, Config{MaxBodyBytes: 1024, MaxStreamedBodyBytes: 4096}, streamedHandler)
	c := dialH2(t, addr)
	expect := [2]string{"expect", "100-continue"}
	c.frame(h2Frame{
		Type: frameHeaders, Flags: flagEndHeaders, StreamID: 1,
		Payload: headBlock("POST", "/", expect, [2]string{"content-length", "5000"}),
	})
	c.frame(h2Frame{
		Type: frameHeaders, Flags: flagEndHeaders, StreamID: 3,
		Payload: headBlock("POST", "/", expect, [2]string{"content-length", "3000"}),
	})
	c.flush()
	c.pump(func() bool { return c.ended[1] && len(c.heads[3]) > 0 })
	if got := h2Status(c.heads[1]); got != 413 {
		t.Errorf("content-length 5000 against 4096: status %d first, want 413", got)
	}
	if got := h2Status(c.heads[3]); got != 100 {
		t.Fatalf("content-length 3000: status %d first, want 100", got)
	}
	body := pattern(3000)
	c.data(3, body, true)
	c.flush()
	want := fmt.Sprintf("streamed 3000 %08x", crc32.ChecksumIEEE(body))
	if got := c.bodies(2)[3]; got != want {
		t.Errorf("the body after the 100: %q, want %q", got, want)
	}
}

// A client that stops sending midway: the handler's wait for the next bytes
// has ReadTimeout, and Err says the deadline passed.
func TestH2StreamedBodyReadTimeout(t *testing.T) {
	cfg := h2StreamCfg()
	cfg.ReadTimeout = 200 * time.Millisecond
	done := make(chan error, 1)
	addr, _ := serveH2(t, cfg, func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
		_, err := b.Items[0].ReadAll(nil, 1<<20)
		done <- err
		return sluice.Batch[Response]{Items: []Response{{Status: 408}}}
	})
	c := dialH2(t, addr)
	c.frame(h2Frame{Type: frameHeaders, Flags: flagEndHeaders, StreamID: 1, Payload: headBlock("POST", "/")})
	c.data(1, pattern(100), false)
	c.flush()
	select {
	case err := <-done:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Errorf("Err of a body that stopped: %v, want os.ErrDeadlineExceeded", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the handler is still waiting past ReadTimeout")
	}
}

// A streamed body is bounded by its window, not by the aggregate bound of
// the bodies assembled whole: an upload streaming beside unfinished whole
// bodies that fill that bound is not reset for their sake.
func TestH2StreamedBodyBesideTheAggregateBound(t *testing.T) {
	release := make(chan struct{})
	addr, _ := serveH2(t, Config{MaxBodyBytes: 256, MaxStreamedBodyBytes: 1 << 20}, func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
		out := make([]Response, b.Len())
		for i := range b.Items {
			<-release
			body, err := b.Items[i].ReadAll(nil, 1<<20)
			out[i] = Response{Status: 200, Body: fmt.Append(nil, len(body), err)}
		}
		return sluice.Batch[Response]{Items: out}
	})
	c := dialH2(t, addr)
	c.frame(h2Frame{Type: frameHeaders, Flags: flagEndHeaders, StreamID: 1, Payload: headBlock("POST", "/")})
	c.data(1, pattern(10), false)
	c.flush()
	c.frame(h2Frame{Type: framePing, Payload: make([]byte, 8)})
	c.flush()
	c.pump(func() bool { return c.pings > 0 }) // stream 1 is handed over
	// Four unfinished bodies whose heads arrive with them, each under
	// MaxBodyBytes and together filling the 4×256 aggregate bound, all in
	// one read so that they are one frame batch.
	for id := uint32(3); id <= 9; id += 2 {
		c.frame(h2Frame{Type: frameHeaders, Flags: flagEndHeaders, StreamID: id, Payload: headBlock("POST", "/")})
		c.data(id, pattern(256), false)
	}
	c.data(1, pattern(100), true)
	c.flush()
	close(release)
	if got := c.bodies(1)[1]; got != "110 <nil>" {
		t.Errorf("the streamed upload answered %q (reset %#x)", got, c.resets[1])
	}
}
