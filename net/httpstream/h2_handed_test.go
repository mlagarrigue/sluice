package httpstream

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
)

// A trailer section over MaxHeaders on a stream whose request reached the
// handler and whose answer has ended — END_STREAM, then RST_STREAM(NO_ERROR)
// for the body it did not wait for — draws nothing: the stream is closed on
// this side, and RFC 9113 §5.1 allows no frame on it. It drew a 431 refusal,
// HEADERS, DATA and a second RST_STREAM. The control, a trailer section
// within the bound, is as quiet.
func TestH2TrailersOverTheBoundOnAnAnsweredStreamDrawNothing(t *testing.T) {
	for _, tc := range []struct {
		name   string
		fields int
	}{
		{"within the bound", 3},
		{"over the bound", 65},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addr, _ := serveH2(t, Config{}, func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
				out := make([]Response, b.Len())
				for i := range out {
					out[i] = Response{Status: 200, Body: []byte("ok")}
				}
				return sluice.Batch[Response]{Items: out}
			})
			c := dialH2(t, addr)
			c.frame(h2Frame{Type: frameHeaders, Flags: flagEndHeaders, StreamID: 1, Payload: headBlock("POST", "/x")})
			c.frame(h2Frame{Type: frameData, StreamID: 1, Payload: []byte("abc")})
			c.flush()
			c.pump(func() bool { return c.ended[1] && c.resetCount[1] >= 1 })
			if c.got[1] != "ok" || c.resets[1] != 0 || c.resetCount[1] != 1 {
				t.Fatalf("setup: body %q, code %#x, %d RST_STREAM; want the answer, then RST_STREAM(NO_ERROR)", c.got[1], c.resets[1], c.resetCount[1])
			}
			var trailers []byte
			for i := range tc.fields {
				trailers = literal(trailers, fmt.Sprintf("t%d", i), "v")
			}
			c.frame(h2Frame{Type: frameHeaders, Flags: flagEndHeaders | flagEndStream, StreamID: 1, Payload: trailers})
			c.frame(h2Frame{Type: framePing, Payload: []byte("pingping")})
			c.flush()
			c.pump(func() bool { return c.pings > 0 || c.sawGoAway })
			if c.sawGoAway {
				t.Fatalf("GOAWAY %x", c.goAway)
			}
			if c.got[1] != "ok" || c.resetCount[1] != 1 {
				t.Errorf("after the trailers: body %q, %d RST_STREAM; want \"ok\" and the one RST_STREAM", c.got[1], c.resetCount[1])
			}
		})
	}
}

// The same trailer section while the handler's streamed answer is still
// running cuts the answer with one RST_STREAM: no second head, no refusal's
// text inside the body the client is reading. It drew HEADERS 431 — a
// trailer with a pseudo-field to the client — and the 431 line appended to
// the body. Within the bound, the answer runs to its end.
func TestH2TrailersOverTheBoundOnARunningAnswerCutIt(t *testing.T) {
	for _, tc := range []struct {
		name   string
		fields int
		cut    bool
	}{
		{"within the bound", 1, false},
		{"over the bound", 300, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			release := make(chan struct{})
			started := make(chan struct{})
			addr, _ := serveH2(t, Config{BodyDelivery: BodyStreamed}, func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
				out := make([]Response, 0, b.Len())
				for range b.Items {
					out = append(out, Response{Status: 200, Stream: func(yield func(sluice.Batch[[]byte]) bool) {
						if !yield(sluice.Batch[[]byte]{Items: [][]byte{[]byte("first")}}) {
							return
						}
						close(started)
						<-release
						yield(sluice.Batch[[]byte]{Items: [][]byte{[]byte("-second")}})
					}})
				}
				return sluice.Batch[Response]{Items: out}
			})
			c := dialH2(t, addr)
			c.frame(h2Frame{Type: frameHeaders, Flags: flagEndHeaders, StreamID: 1, Payload: headBlock("POST", "/up")})
			c.flush()
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("setup: the streamed answer never started")
			}
			c.pump(func() bool { return c.got[1] == "first" })

			var trailers []byte
			for range tc.fields {
				trailers = literal(trailers, "x-trailer", "v")
			}
			c.frame(h2Frame{Type: frameHeaders, Flags: flagEndHeaders | flagEndStream, StreamID: 1, Payload: trailers})
			c.frame(h2Frame{Type: framePing, Payload: []byte("pingping")})
			c.flush()
			c.pump(func() bool { return c.pings > 0 || c.sawGoAway })
			close(release)
			if !tc.cut {
				c.pump(func() bool { return c.ended[1] || c.sawGoAway })
				// A RST_STREAM(NO_ERROR) may follow the whole answer (RFC 9113
				// §8.1); an error code may not.
				if c.got[1] != "first-second" || !c.ended[1] || c.resets[1] != 0 {
					t.Fatalf("body %q, ended %v, RST_STREAM code %#x; want the whole answer and no error", c.got[1], c.ended[1], c.resets[1])
				}
				return
			}
			c.pump(func() bool { return c.resetCount[1] > 0 || c.sawGoAway })
			// A PING after the release: whatever the answer still had to
			// send has been written by the time its ACK comes back.
			c.frame(h2Frame{Type: framePing, Payload: []byte("pingpong")})
			c.flush()
			c.pump(func() bool { return c.pings > 1 || c.sawGoAway })
			if c.sawGoAway {
				t.Fatalf("GOAWAY %x", c.goAway)
			}
			if c.got[1] != "first" || c.ended[1] || c.resetCount[1] != 1 {
				t.Errorf("body %q, ended %v, %d RST_STREAM; want \"first\", the answer cut by one RST_STREAM", c.got[1], c.ended[1], c.resetCount[1])
			}
		})
	}
}

// A client that cancels requests already answered — the RST_STREAM crossing
// the END_STREAM, which §5.1 lets a closed stream receive for a short while —
// takes back no credit: the answer was written, the request completed. It
// took one back while the closed-stream ring still said half-closed, and
// every such cancellation cost a reset and two of allowance: GOAWAY at the
// seventeenth with MaxFramesPerBatch 4. The client here reads each answer
// before it resets, so that the answer is out whatever the scheduling; the
// server cannot tell that from a reset sent before the END_STREAM arrived.
func TestCancellingAnsweredRequestsIsNotRapidReset(t *testing.T) {
	addr, _ := serveH2(t, Config{MaxFramesPerBatch: 4}, echoTarget)
	c := dialH2(t, addr)
	var code [4]byte
	be32put(code[:], errCancel)
	for i := range 60 {
		id := uint32(2*i + 1)
		c.request(id, "GET", "/a")
		c.flush()
		c.pump(func() bool { return c.ended[id] || c.sawGoAway })
		if c.sawGoAway {
			t.Fatalf("GOAWAY after %d answered requests, each cancelled once answered", i)
		}
		c.frame(h2Frame{Type: frameRSTStream, StreamID: id, Payload: code[:]})
	}
	c.frame(h2Frame{Type: framePing, Payload: []byte("pingping")})
	c.flush()
	c.pump(func() bool { return c.pings > 0 || c.sawGoAway })
	if c.sawGoAway {
		t.Fatalf("GOAWAY %x after 60 answered requests, each cancelled once answered", c.goAway)
	}
}
