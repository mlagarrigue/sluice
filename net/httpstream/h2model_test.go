package httpstream

import (
	"bytes"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
)

// The acceptance criteria for the connection model: a streamed response is no
// longer bounded by the credit granted before it started, one stream's failure
// costs one stream, and a connection leaves nothing behind.

// drain reads a streamed response, granting window as it goes and pinging as
// it goes, and returns what arrived.
//
// It is the client half of the criterion. A real peer grants credit
// incrementally — that is what flow control is — and a server that can only
// spend what it was given in advance cannot answer past 65535 bytes.
func (c *h2client) drain(id uint32, grant uint32, timeout time.Duration) string {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for !c.ended[id] && !c.sawGoAway && time.Now().Before(deadline) {
		before := len(c.got[id])
		c.frame(h2Frame{
			Type: frameWindowUpdate, StreamID: 0,
			Payload: windowUpdatePayload(grant),
		})
		c.frame(h2Frame{
			Type: frameWindowUpdate, StreamID: id,
			Payload: windowUpdatePayload(grant),
		})
		// PING rides along on every round: a peer that uses it for liveness
		// kills a connection that goes quiet, and the whole point of taking
		// the handler off the read goroutine is that it no longer does.
		c.frame(h2Frame{Type: framePing, Payload: []byte("liveness")})
		c.flush()
		c.pump(func() bool {
			return c.ended[id] || c.sawGoAway || len(c.got[id]) >= before+int(grant)
		})
	}
	return c.got[id]
}

// A streamed response of several times the initial window completes against a
// client that grants credit as it reads, and the connection answers PING while
// it runs.
//
// Both halves used to be impossible at once, and for the same reason: the
// goroutine writing the body was the goroutine that would have read the
// WINDOW_UPDATE and answered the PING. Past 65535 bytes the server gave up and
// took the connection — and every other request on it — with it.
func TestH2StreamedResponsePastTheInitialWindow(t *testing.T) {
	const chunk, chunks = 16 << 10, 20 // 320 KiB, near five times 65535
	addr, _ := serveTest(t, Config{}, func(b sluice.Batch[Request]) sluice.Batch[Response] {
		out := make([]Response, 0, b.Len())
		for range b.Items {
			out = append(out, Response{Status: 200, Stream: func(yield func(sluice.Batch[[]byte]) bool) {
				for i := range chunks {
					c := bytes.Repeat([]byte{byte('a' + i%26)}, chunk)
					if !yield(sluice.Batch[[]byte]{Items: [][]byte{c}}) {
						return
					}
				}
			}})
		}
		return sluice.Batch[Response]{Items: out}
	})
	c := dialH2(t, addr)

	c.request(1, "GET", "/export")
	c.flush()

	body := c.drain(1, initialWindow, 20*time.Second)
	if c.sawGoAway {
		t.Fatalf("the connection ended with GOAWAY after %d bytes", len(body))
	}
	if len(body) != chunk*chunks {
		t.Fatalf("the streamed response carried %d bytes, want %d", len(body), chunk*chunks)
	}
	if !c.ended[1] {
		t.Error("the stream never ended")
	}
	if c.pings == 0 {
		t.Error("no PING was answered while the response ran")
	}
}

// One malformed request among many costs that one. It is what a multiplexed
// protocol has a per-stream error channel for, and what a server with only
// GOAWAY cannot express: before the split, a client that sent one bad request
// among a hundred lost the ninety-nine healthy ones too.
func TestH2OneMalformedRequestAmongManyCostsOne(t *testing.T) {
	const n = 10
	const bad = 4
	addr, _ := serveTest(t, Config{}, echoTarget)
	c := dialH2(t, addr)

	for i := range n {
		id := uint32(2*i + 1) //nolint:gosec // G115: the loop is bounded by n
		if i == bad {
			// An uppercase field name, which RFC 9113 §8.2.1 makes malformed
			// — a name normalised rather than refused is a header that means
			// one thing to this hop and another to the next.
			c.frame(h2Frame{
				Type:     frameHeaders,
				Flags:    flagEndHeaders | flagEndStream,
				StreamID: id,
				Payload:  headBlock("GET", "/bad", [2]string{"X-Uppercase", "1"}),
			})
			continue
		}
		c.request(id, "GET", fmt.Sprintf("/r%d", i))
	}
	c.flush()

	if code := c.waitReset(2*bad + 1); code != 0x1 {
		t.Errorf("the malformed stream was reset with %#x, want PROTOCOL_ERROR (0x1)", code)
	}
	got := c.bodies(n - 1)
	for i := range n {
		id := uint32(2*i + 1) //nolint:gosec // G115: the loop is bounded by n
		if i == bad {
			if _, answered := got[id]; answered {
				t.Errorf("the malformed request on stream %d was answered", id)
			}
			continue
		}
		if want := fmt.Sprintf("/r%d", i); got[id] != want {
			t.Errorf("stream %d answered %q, want %q", id, got[id], want)
		}
	}
	if c.sawGoAway {
		t.Error("the connection ended over one malformed request")
	}
}

// RFC 9113 §5.1 asks a receiver to tolerate frames that were already in flight
// when a stream closed. The trap is the accounting: the peer debited its
// connection window for that DATA whatever this end thinks of the stream, so a
// server that drops it without counting stalls the connection a few resets
// later, with nothing in the logs to say why.
func TestH2CountsDataOnAStreamTheClientReset(t *testing.T) {
	addr, _ := serveTest(t, Config{}, echoTarget)
	c := dialH2(t, addr)

	c.frame(h2Frame{
		Type: frameHeaders, Flags: flagEndHeaders,
		StreamID: 1, Payload: headBlock("POST", "/upload"),
	})
	c.frame(h2Frame{
		Type: frameRSTStream, StreamID: 1,
		Payload: []byte{0, 0, 0, 0x8}, // CANCEL
	})
	// More than half the connection window, arriving after the reset, in
	// frames no larger than the server's advertised SETTINGS_MAX_FRAME_SIZE.
	payload := bytes.Repeat([]byte{'z'}, 60<<10)
	for range 10 {
		c.data(1, payload, false)
	}
	c.request(3, "GET", "/after")
	c.flush()

	if got := c.bodies(1)[3]; got != "/after" {
		t.Fatalf("the connection answered %q after tolerated frames, want %q", got, "/after")
	}
	if c.sawGoAway {
		t.Fatal("frames in flight when a stream was reset ended the connection")
	}
	if c.windows[0] == 0 {
		t.Error("600 KiB of tolerated DATA replenished nothing: the connection window is leaking")
	}
}

// §6.9.1 wants FLOW_CONTROL_ERROR from a peer that sends more than it was
// granted. It is conformance rather than safety here — a body is bounded by
// MaxBodyBytes whatever the window says — and it is a stream error, which it
// could not be before there was a per-stream error path.
func TestH2RefusesDataPastTheStreamWindow(t *testing.T) {
	// A large MaxFrameSize, configured deliberately: at the 16384 default a
	// client cannot overshoot the stream window at all, because the server
	// replenishes it long before a conformant frame could reach the edge.
	addr, _ := serveH2(t, H2Config{MaxFrameSize: 1 << 20}, echoTarget)
	c := dialH2(t, addr)

	c.frame(h2Frame{
		Type: frameHeaders, Flags: flagEndHeaders,
		StreamID: 1, Payload: headBlock("POST", "/upload"),
	})
	// One frame past the 65535 a stream starts with. The connection's own
	// window is larger, so this is the stream's check and nothing else.
	c.frame(h2Frame{
		Type: frameData, StreamID: 1,
		Payload: bytes.Repeat([]byte{'z'}, initialWindow+1),
	})
	c.request(3, "GET", "/after")
	c.flush()

	if code := c.waitReset(1); code != 0x3 {
		t.Errorf("stream 1 was reset with %#x, want FLOW_CONTROL_ERROR (0x3)", code)
	}
	if got := c.bodies(1)[3]; got != "/after" {
		t.Errorf("stream 3 answered %q; a flow-control violation took its neighbour", got)
	}
}

// A trailer section ends the message. RFC 9113 §8.1 makes a pseudo-header in
// one malformed, and DATA after one is a body with no stated end — bounded
// only by MaxBodyBytes, which is not the same as bounded by the peer's word.
func TestH2RefusesMalformedTrailers(t *testing.T) {
	t.Run("a pseudo-header in a trailer section", func(t *testing.T) {
		addr, _ := serveTest(t, Config{}, echoTarget)
		c := dialH2(t, addr)

		c.frame(h2Frame{
			Type: frameHeaders, Flags: flagEndHeaders,
			StreamID: 1, Payload: headBlock("POST", "/t"),
		})
		c.frame(h2Frame{Type: frameData, StreamID: 1, Payload: []byte("hi")})
		c.frame(h2Frame{
			Type: frameHeaders, Flags: flagEndHeaders | flagEndStream,
			StreamID: 1, Payload: literal(nil, ":method", "GET"),
		})
		c.request(3, "GET", "/after")
		c.flush()

		if code := c.waitReset(1); code != 0x1 {
			t.Errorf("stream 1 was reset with %#x, want PROTOCOL_ERROR (0x1)", code)
		}
		if got := c.bodies(1)[3]; got != "/after" {
			t.Errorf("stream 3 answered %q; the malformed trailer took its neighbour", got)
		}
	})

	t.Run("DATA after a trailer section", func(t *testing.T) {
		addr, _ := serveTest(t, Config{}, echoTarget)
		c := dialH2(t, addr)

		c.frame(h2Frame{
			Type: frameHeaders, Flags: flagEndHeaders,
			StreamID: 1, Payload: headBlock("POST", "/t"),
		})
		c.frame(h2Frame{Type: frameData, StreamID: 1, Payload: []byte("hi")})
		// Trailers without END_STREAM, then more body: the case a spot check
		// cannot catch, because it is the stream's state that is wrong and not
		// the frame's.
		c.frame(h2Frame{
			Type: frameHeaders, Flags: flagEndHeaders,
			StreamID: 1, Payload: literal(nil, "x-checksum", "1"),
		})
		c.frame(h2Frame{Type: frameData, StreamID: 1, Payload: []byte("more")})
		c.request(3, "GET", "/after")
		c.flush()

		if code := c.waitReset(1); code != 0x1 {
			t.Errorf("stream 1 was reset with %#x, want PROTOCOL_ERROR (0x1)", code)
		}
		if got := c.bodies(1)[3]; got != "/after" {
			t.Errorf("stream 3 answered %q; the refusal took its neighbour", got)
		}
	})
}

// A connection's goroutines are the connection's: they end when it does.
//
// Three per connection is a design decision (see ServeH2), and a design
// decision about goroutines is only worth anything if they are all joined —
// otherwise it is three leaks per connection, which a server discovers under
// load and not before.
//
// The server is driven through serveH2, whose per-connection goroutine is the
// test's own: once ServeH2 has returned, no goroutine should be running — or
// have been created by — a function of this package. That is checked by
// name rather than against a goroutine count, so there is no baseline to
// guess and no settling to wait for.
func TestH2ConnectionsLeaveNoGoroutines(t *testing.T) {
	const chunk, chunks = 4 << 10, 8
	addr, _ := serveH2(t, H2Config{}, func(b sluice.Batch[Request]) sluice.Batch[Response] {
		out := make([]Response, 0, b.Len())
		for _, r := range b.Items {
			if string(r.Target) == "/export" {
				out = append(out, Response{Status: 200, Stream: func(yield func(sluice.Batch[[]byte]) bool) {
					for range chunks {
						if !yield(sluice.Batch[[]byte]{Items: [][]byte{bytes.Repeat([]byte{'x'}, chunk)}}) {
							return
						}
					}
				}})
				continue
			}
			out = append(out, Response{Status: 200, Body: append([]byte(nil), r.Target...)})
		}
		return sluice.Batch[Response]{Items: out}
	})

	round := func() {
		c := dialH2(t, addr)
		defer func() { _ = c.conn.Close() }()
		for i := range 6 {
			c.request(uint32(2*i+1), "GET", fmt.Sprintf("/r%d", i)) //nolint:gosec // G115: bounded loop
		}
		c.request(13, "GET", "/export")
		c.flush()
		if got := c.drain(13, initialWindow, 10*time.Second); len(got) != chunk*chunks {
			t.Fatalf("the streamed response carried %d bytes, want %d", len(got), chunk*chunks)
		}
		if got := c.bodies(7)[1]; got != "/r0" {
			t.Fatalf("stream 1 answered %q while a body streamed beside it", got)
		}
	}
	for range 5 {
		round()
	}

	// A bounded wait for the effect, not a settling heuristic: the server
	// sees each close a moment after the client makes it, and a joined
	// goroutine is still listed while its deferred close runs.
	var left []string
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		if left = packageGoroutines(); len(left) == 0 {
			return
		}
	}
	t.Errorf("%d goroutines of this package outlived their connections:\n%s", len(left), strings.Join(left, "\n\n"))
}

// packageGoroutines returns the stacks of the goroutines that are running a
// function of this module's production code, or were created by one. A
// frame is attributed by the file that follows it in the dump, so a test
// helper's own goroutines — a listener's accept loop, say — do not count.
func packageGoroutines() []string {
	buf := make([]byte, 1<<20)
	buf = buf[:runtime.Stack(buf, true)]
	var out []string
	for g := range strings.SplitSeq(string(buf), "\n\n") {
		lines := strings.Split(g, "\n")
		for i, l := range lines {
			// A function line starts with the import path; the file line
			// below it is indented, and may well contain the module's name
			// too when the checkout lives in a directory of that name.
			if !strings.HasPrefix(l, "github.com/mlagarrigue/sluice/") || i+1 >= len(lines) {
				continue
			}
			if !strings.Contains(lines[i+1], "_test.go:") {
				out = append(out, g)
				break
			}
		}
	}
	return out
}
