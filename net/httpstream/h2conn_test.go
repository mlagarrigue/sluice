package httpstream

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
)

// h2client is the smallest client that can exercise this server: it encodes
// requests as HPACK literals — which a conformant decoder must accept — and
// reads frames back.
type h2client struct {
	t    *testing.T
	conn net.Conn
	buf  []byte // what has been staged for the next flush

	// in is what has arrived and not yet been parsed, and it is kept on the
	// client rather than in one read call: a test that reads twice on one
	// connection would otherwise drop whatever sat half-parsed between them.
	in         []byte
	got        map[uint32]string
	heads      map[uint32][]byte
	ended      map[uint32]bool
	windows    map[uint32]int
	resets     map[uint32]uint32
	resetCount map[uint32]int
	settings   map[uint16]uint32
	pings      int
	goAway     []byte
	sawGoAway  bool
	// maxData is the largest DATA payload seen, for the tests about the
	// size frames leave at.
	maxData int
}

func dialH2(t *testing.T, addr string) *h2client {
	t.Helper()
	c := dial(t, addr)
	if _, err := c.Write([]byte(Preface)); err != nil {
		t.Fatal(err)
	}
	// An empty SETTINGS, which is what a client must send after the preface.
	f, _ := appendH2Frame(nil, h2Frame{Type: frameSettings})
	if _, err := c.Write(f); err != nil {
		t.Fatal(err)
	}
	return &h2client{
		t: t, conn: c,
		got:        map[uint32]string{},
		heads:      map[uint32][]byte{},
		ended:      map[uint32]bool{},
		windows:    map[uint32]int{},
		resets:     map[uint32]uint32{},
		resetCount: map[uint32]int{},
		settings:   map[uint16]uint32{},
	}
}

// frame stages one raw frame, for the tests that need a sequence no helper
// covers.
func (c *h2client) frame(f h2Frame) {
	c.t.Helper()
	b, err := appendH2Frame(nil, f)
	if err != nil {
		c.t.Fatal(err)
	}
	c.buf = append(c.buf, b...)
}

// headBlock encodes a request's field section, without framing it.
func headBlock(method, path string, extra ...[2]string) []byte {
	var block []byte
	block = literal(block, ":method", method)
	block = literal(block, ":path", path)
	block = literal(block, ":scheme", "http")
	for _, kv := range extra {
		block = literal(block, kv[0], kv[1])
	}
	return block
}

// initialWindow is what the protocol starts every window at, spelled out
// here because it is the peer's half of the contract, not this package's.
const initialWindow = 65535

// pump reads until done holds, folding whole frames into what the client has
// seen.
func (c *h2client) pump(done func() bool) {
	c.t.Helper()
	tmp := make([]byte, 4096)
	deadline := time.Now().Add(5 * time.Second)
	for !done() && time.Now().Before(deadline) {
		if err := c.conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
			c.t.Fatal(err)
		}
		n, err := c.conn.Read(tmp)
		c.in = append(c.in, tmp[:n]...)
		c.parse()
		if err != nil && n == 0 {
			return
		}
	}
}

func (c *h2client) parse() {
	for len(c.in) >= 9 {
		length := int(c.in[0])<<16 | int(c.in[1])<<8 | int(c.in[2])
		if len(c.in) < 9+length {
			return
		}
		typ, flags := c.in[3], c.in[4]
		id := binary.BigEndian.Uint32(c.in[5:9]) & 0x7fffffff
		payload := c.in[9 : 9+length]
		switch typ {
		case frameData:
			c.got[id] += string(payload)
			c.maxData = max(c.maxData, len(payload))
			if flags&flagEndStream != 0 {
				c.ended[id] = true
			}
		case frameHeaders:
			if _, seen := c.got[id]; !seen {
				c.got[id] = "" // a bodiless answer still answers
			}
			if _, seen := c.heads[id]; !seen {
				c.heads[id] = append([]byte(nil), payload...)
			}
			if flags&flagEndStream != 0 {
				c.ended[id] = true
			}
		case frameWindowUpdate:
			c.windows[id]++
		case frameRSTStream:
			c.resets[id] = binary.BigEndian.Uint32(payload)
			c.resetCount[id]++
		case frameSettings:
			if flags&flagAck == 0 {
				for p := payload; len(p) >= 6; p = p[6:] {
					c.settings[uint16(p[0])<<8|uint16(p[1])] = binary.BigEndian.Uint32(p[2:6])
				}
			}
		case framePing:
			if flags&flagAck != 0 {
				c.pings++
			}
		case frameGoAway:
			c.goAway = append([]byte(nil), payload...)
			c.sawGoAway = true
		}
		c.in = c.in[9+length:]
	}
}

// waitGoAway reads until the connection is ended and returns the GOAWAY
// payload.
func (c *h2client) waitGoAway() []byte {
	c.t.Helper()
	c.pump(func() bool { return c.sawGoAway })
	if !c.sawGoAway {
		c.t.Fatal("the connection did not end with GOAWAY")
	}
	return c.goAway
}

// goAwayCode is the error code out of a GOAWAY payload (RFC 9113 §6.8).
func goAwayCode(payload []byte) uint32 {
	if len(payload) < 8 {
		return ^uint32(0)
	}
	return binary.BigEndian.Uint32(payload[4:8])
}

// waitReset reads until the server resets one stream, and returns the error
// code it carried. A reset rather than a GOAWAY is the whole point of the
// per-stream error path: the connection is expected to survive it, so this
// fails if the connection ends instead.
func (c *h2client) waitReset(id uint32) uint32 {
	c.t.Helper()
	c.pump(func() bool { _, seen := c.resets[id]; return seen || c.sawGoAway })
	code, seen := c.resets[id]
	if !seen {
		c.t.Fatalf("stream %d was not reset", id)
	}
	return code
}

// h2Status reads the :status out of a response's head block.
//
// It decodes the encoder in this package rather than an arbitrary one, which
// is all it needs to: a status the static table holds is one indexed byte, and
// anything else is a literal with the name taken from index 8.
func h2Status(block []byte) int {
	if len(block) == 0 {
		return 0
	}
	if idx := int(block[0]); block[0]&0x80 != 0 && idx >= 0x88 && idx <= 0x8e {
		return []int{200, 204, 206, 304, 400, 404, 500}[idx-0x88]
	}
	if len(block) >= 2 && block[0] == 0x08 {
		n := int(block[1])
		if len(block) >= 2+n {
			v, err := strconv.Atoi(string(block[2 : 2+n]))
			if err != nil {
				return 0
			}
			return v
		}
	}
	return 0
}

// windowUpdatePayload is a WINDOW_UPDATE's four-byte increment.
func windowUpdatePayload(n uint32) []byte {
	return []byte{byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n)}
}

// waitWindowUpdate reads until the server grants room on one stream, which is
// the only thing that lets an upload past the initial window continue.
func (c *h2client) waitWindowUpdate(id uint32) {
	c.t.Helper()
	c.pump(func() bool { return c.windows[id] > 0 || c.sawGoAway })
	if c.windows[id] == 0 {
		c.t.Fatalf("no WINDOW_UPDATE arrived for stream %d; the upload is stalled", id)
	}
}

// literal encodes one field as HPACK literal-without-indexing, name included.
func literal(dst []byte, name, value string) []byte {
	dst = append(dst, 0x00, byte(len(name)))
	dst = append(dst, name...)
	dst = append(dst, byte(len(value)))
	return append(dst, value...)
}

func (c *h2client) request(id uint32, method, path string, extra ...[2]string) {
	c.t.Helper()
	c.frame(h2Frame{
		Type:     frameHeaders,
		Flags:    flagEndHeaders | flagEndStream,
		StreamID: id,
		Payload:  headBlock(method, path, append([][2]string{{":authority", "h"}}, extra...)...),
	})
}

// data stages a body as DATA frames no larger than 16384 bytes — the
// SETTINGS_MAX_FRAME_SIZE this server advertises, which a conformant client
// honours. END_STREAM, when asked for, rides on the last frame.
func (c *h2client) data(id uint32, payload []byte, endStream bool) {
	c.t.Helper()
	const maxFrame = 16384
	for {
		n := min(len(payload), maxFrame)
		chunk, rest := payload[:n], payload[n:]
		var flags byte
		if endStream && len(rest) == 0 {
			flags = flagEndStream
		}
		c.frame(h2Frame{Type: frameData, Flags: flags, StreamID: id, Payload: chunk})
		if len(rest) == 0 {
			return
		}
		payload = rest
	}
}

// admitted stages a request with stage and sends it, retrying on the next
// stream for as long as the server answers REFUSED_STREAM, and returns the
// stream it was admitted on.
//
// A slot frees when the responder finishes with its batch, which is just
// after the client has read the answer — so a request sent the moment an
// answer arrives may still meet the limit. Retrying a refused stream is what
// RFC 9113 §8.7 allows a client, and the admission itself is the event this
// waits on, where a sleep would only guess at it.
func (c *h2client) admitted(id uint32, stage func(id uint32)) uint32 {
	c.t.Helper()
	for range 1000 {
		stage(id)
		c.flush()
		c.pump(func() bool { _, reset := c.resets[id]; return c.ended[id] || reset || c.sawGoAway })
		if code, reset := c.resets[id]; !reset || code != 0x7 {
			return id
		}
		id += 2
	}
	c.t.Fatal("the server refused 1000 requests in a row: no slot ever freed")
	return 0
}

func (c *h2client) flush() {
	c.t.Helper()
	if _, err := c.conn.Write(c.buf); err != nil {
		c.t.Fatal(err)
	}
	c.buf = c.buf[:0]
}

// bodies reads until want streams have *ended* in total, and returns what
// each carried.
//
// Ended rather than merely seen: a streamed response arrives as several DATA
// frames, so stopping at the first one reads a prefix and calls it the
// answer. The END_STREAM flag is the only thing that says a body is whole.
//
// It also stops on GOAWAY, so a test that expects a refusal does not wait out
// its own deadline.
func (c *h2client) bodies(want int) map[uint32]string {
	c.t.Helper()
	c.pump(func() bool { return len(c.ended) >= want || c.sawGoAway })
	return c.got
}

// serveH2 starts a prior-knowledge HTTP/2 server with its own [H2Config] —
// for the tests whose subject is a knob [Serve]'s Config does not carry, such
// as MaxConcurrentStreams. The handler is wrapped exactly as serveTest wraps
// it, so batch sizes are read through the returned accessor rather than
// through a shared slice the handler goroutine is still appending to.
func serveH2(t *testing.T, cfg H2Config, h Handler) (addr string, sizes func() []int) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var seen []int

	ctx, cancel := context.WithCancel(t.Context())
	var wg sync.WaitGroup
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Go(func() {
				defer func() { _ = c.Close() }()
				_ = ServeH2(ctx, c, cfg, func(b sluice.Batch[Request]) sluice.Batch[Response] {
					mu.Lock()
					seen = append(seen, b.Len())
					mu.Unlock()
					return h(b)
				})
			})
		}
	}()
	t.Cleanup(func() {
		cancel()
		_ = ln.Close()
		wg.Wait()
	})

	return ln.Addr().String(), func() []int {
		mu.Lock()
		defer mu.Unlock()
		return append([]int(nil), seen...)
	}
}

// End to end over HTTP/2: a request in, the handler's answer out, on the
// stream it was asked on.
func TestH2ServesARequest(t *testing.T) {
	addr, _ := serveTest(t, Config{}, echoTarget)
	c := dialH2(t, addr)

	c.request(1, "GET", "/orders/7")
	c.flush()

	bodies := c.bodies(1)
	if got := bodies[1]; got != "/orders/7" {
		t.Errorf("stream 1 answered %q, want %q", got, "/orders/7")
	}
}

// The thesis at HTTP/2, and the reason this protocol suits the model better
// than HTTP/1.1: multiplexing means several requests are in flight by design,
// so one read completes several and they reach the handler together — with no
// pipelining, no waiting, and nothing unusual asked of the client.
func TestH2MultiplexedRequestsArriveAsOneBatch(t *testing.T) {
	addr, sizes := serveTest(t, Config{}, echoTarget)
	c := dialH2(t, addr)

	const n = 6
	for i := range n {
		c.request(uint32(2*i+1), "GET", fmt.Sprintf("/r%d", i))
	}
	c.flush() // all six on the wire before any answer is read

	bodies := c.bodies(n)
	for i := range n {
		id := uint32(2*i + 1)
		if want := fmt.Sprintf("/r%d", i); bodies[id] != want {
			t.Errorf("stream %d answered %q, want %q", id, bodies[id], want)
		}
	}
	// Read through serveTest's mutex-guarded accessor: the handler goroutine
	// is what appends, so a bare shared slice here is a data race.
	got := sizes()
	if len(got) == 0 || got[0] < 2 {
		t.Errorf("batch sizes %v: six multiplexed requests were served one at a time", got)
	}
	t.Logf("batch sizes seen: %v", got)
}

// One Serve, two protocols. The client that speaks HTTP/1.1 and the one that
// speaks HTTP/2 reach the same handler, because prior-knowledge HTTP/2 opens
// with a preface no HTTP/1.1 request can begin with.
func TestServeSpeaksBothProtocols(t *testing.T) {
	addr, _ := serveTest(t, Config{}, echoTarget)

	t.Run("HTTP/1.1", func(t *testing.T) {
		c := dial(t, addr)
		fmt.Fprint(c, "GET /one HTTP/1.1\r\nHost: h\r\n\r\n")
		_, body := readResponse(t, newReader(c))
		if body != "/one" {
			t.Errorf("body = %q", body)
		}
	})

	t.Run("HTTP/2", func(t *testing.T) {
		c := dialH2(t, addr)
		c.request(1, "GET", "/two")
		c.flush()
		if got := c.bodies(1)[1]; got != "/two" {
			t.Errorf("body = %q", got)
		}
	})
}

// The refusals RFC 9113 §8.3 requires, which exist because an HTTP/2 request
// that reaches an HTTP/1.1 hop unchecked is how a smuggled request is written.
func TestH2RefusesMalformedRequests(t *testing.T) {
	tests := []struct {
		name  string
		block func([]byte) []byte
	}{
		{"a connection-specific field", func(b []byte) []byte { return literal(b, "connection", "keep-alive") }},
		{"Transfer-Encoding", func(b []byte) []byte { return literal(b, "transfer-encoding", "chunked") }},
		{"an uppercase field name", func(b []byte) []byte { return literal(b, "X-Bad", "v") }},
		{"TE that is not trailers", func(b []byte) []byte { return literal(b, "te", "gzip") }},
		{"an unknown pseudo-header", func(b []byte) []byte { return literal(b, ":oops", "v") }},
		// §8.2.1's octet rules. HPACK can carry any byte, so these arrive
		// from conformantly-encoded blocks and only this layer can refuse
		// them — the h1 parser they would smuggle past never sees them.
		{"a header name with a space", func(b []byte) []byte { return literal(b, "x y", "v") }},
		{"a header name with a colon", func(b []byte) []byte { return literal(b, "x:y", "v") }},
		{"a header value with CRLF", func(b []byte) []byte { return literal(b, "x-a", "a\r\nb") }},
		{"a header value with NUL", func(b []byte) []byte { return literal(b, "x-a", "a\x00b") }},
		{"a repeated content-length", func(b []byte) []byte {
			return literal(literal(b, "content-length", "0"), "content-length", "0")
		}},
		{"a content-length that is not a number", func(b []byte) []byte { return literal(b, "content-length", "4x") }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr, sizes := serveTest(t, Config{}, echoTarget)
			c := dialH2(t, addr)

			var block []byte
			block = literal(block, ":method", "GET")
			block = literal(block, ":path", "/x")
			block = literal(block, ":scheme", "http")
			block = tt.block(block)
			f, _ := appendH2Frame(nil, h2Frame{
				Type: frameHeaders, Flags: flagEndHeaders | flagEndStream,
				StreamID: 1, Payload: block,
			})
			if _, err := c.conn.Write(f); err != nil {
				t.Fatal(err)
			}

			// The stream is reset with PROTOCOL_ERROR rather than answered
			// (§8.1.1 asks for a stream error, not a connection error: one
			// malformed request must not take the healthy ones with it),
			// and the request never reaches the handler.
			if code := c.waitReset(1); code != 0x1 {
				t.Errorf("reset code = %#x, want 0x1 PROTOCOL_ERROR", code)
			}
			if c.sawGoAway {
				t.Error("one malformed request ended the whole connection")
			}
			if got := sizes(); len(got) > 0 {
				t.Errorf("a malformed request reached the handler: %v", got)
			}
		})
	}
}

// The same streamed response over HTTP/2, where it costs nothing: the
// protocol multiplexes, so an answer that takes a while does not hold the
// others up — which is exactly what it does over HTTP/1.1, where order is
// the only correlation there is.
func TestH2StreamedResponse(t *testing.T) {
	addr, _ := serveTest(t, Config{}, func(b sluice.Batch[Request]) sluice.Batch[Response] {
		out := make([]Response, 0, b.Len())
		for range b.Items {
			out = append(out, Response{
				Status: 200,
				Stream: func(yield func(sluice.Batch[[]byte]) bool) {
					for i := range 3 {
						if !yield(sluice.Batch[[]byte]{Items: [][]byte{fmt.Appendf(nil, "p%d", i)}}) {
							return
						}
					}
				},
			})
		}
		return sluice.Batch[Response]{Items: out}
	})

	c := dialH2(t, addr)
	c.request(1, "GET", "/streamed")
	c.flush()

	if got := c.bodies(1)[1]; got != "p0p1p2" {
		t.Errorf("stream 1 carried %q, want the three parts", got)
	}
}

// B1. Completion order and identifier order are not the same order the moment
// a client interleaves, which a multiplexing client does by design: stream 3
// opens first and finishes last. Pairing the handler's answers with a list of
// finished streams sorted by identifier then frames each answer onto the
// wrong stream, and one caller reads another caller's response.
func TestH2InterleavedCompletionAnswersItsOwnStream(t *testing.T) {
	addr, sizes := serveTest(t, Config{}, echoTarget)
	c := dialH2(t, addr)

	// Stream 3 opens first and stays open; stream 5 opens and completes; then
	// stream 3's body arrives. Completion order is [5, 3], identifier order
	// is [3, 5].
	c.frame(h2Frame{
		Type: frameHeaders, Flags: flagEndHeaders,
		StreamID: 3, Payload: headBlock("POST", "/three"),
	})
	c.request(5, "GET", "/five")
	c.frame(h2Frame{
		Type: frameData, Flags: flagEndStream,
		StreamID: 3, Payload: []byte("x"),
	})
	c.flush()

	bodies := c.bodies(2)
	if bodies[3] != "/three" {
		t.Errorf("stream 3 answered %q, want %q", bodies[3], "/three")
	}
	if bodies[5] != "/five" {
		t.Errorf("stream 5 answered %q, want %q", bodies[5], "/five")
	}
	if got := sizes(); len(got) == 0 || got[0] < 2 {
		t.Errorf("batch sizes %v: the two streams did not complete in one batch, "+
			"so the pairing this test exists for was never exercised", got)
	}
}

// B2. A peer may split a header block at any byte — RFC 7541 gives no
// alignment to split on — so the split lands inside a name, an integer or a
// Huffman string as often as not. Decoding per frame refuses those, which is
// a conformant client whose connection dies for nothing.
//
// The split here is five bytes in, inside ":method"'s own name, which is as
// mid-field as a split gets.
func TestH2HeaderBlockSplitInsideAFieldIsAccepted(t *testing.T) {
	addr, _ := serveTest(t, Config{}, echoTarget)
	c := dialH2(t, addr)

	block := headBlock("GET", "/split", [2]string{":authority", "h"})
	head, rest := block[:5], block[5:]
	// END_STREAM rides on HEADERS; the CONTINUATION that closes the block
	// cannot carry it, which is the other half of what this exercises.
	c.frame(h2Frame{
		Type: frameHeaders, Flags: flagEndStream,
		StreamID: 1, Payload: head,
	})
	c.frame(h2Frame{
		Type: frameContinuation, Flags: flagEndHeaders,
		StreamID: 1, Payload: rest,
	})
	c.flush()

	if got := c.bodies(1)[1]; got != "/split" {
		t.Errorf("stream 1 answered %q, want %q", got, "/split")
	}
}

// B3. Flow control is two windows. A client's stream starts with 65535 bytes
// of room and nothing but a stream-level WINDOW_UPDATE tops it up, so a
// server that replenishes only the connection leaves every upload past 64 KiB
// stalled until the read timeout kills it — for a body size the configuration
// says is allowed.
//
// The client here honours its window, which is what makes the test fail
// before the fix instead of passing by ignoring the protocol.
func TestH2UploadPastTheInitialStreamWindow(t *testing.T) {
	addr, _ := serveTest(t, Config{}, func(b sluice.Batch[Request]) sluice.Batch[Response] {
		out := make([]Response, 0, b.Len())
		for _, r := range b.Items {
			out = append(out, Response{Status: 200, Body: fmt.Append(nil, len(r.Body))})
		}
		return sluice.Batch[Response]{Items: out}
	})
	c := dialH2(t, addr)

	c.frame(h2Frame{
		Type: frameHeaders, Flags: flagEndHeaders,
		StreamID: 1, Payload: headBlock("POST", "/upload"),
	})
	// Split at 16384, since that is the frame size this server advertises —
	// one 65535-byte frame would be a client ignoring SETTINGS.
	c.data(1, bytes.Repeat([]byte{'z'}, initialWindow), false)
	c.flush()

	// The whole stream window is spent. Nothing more may go out until the
	// server grants room on this stream.
	c.waitWindowUpdate(1)

	c.frame(h2Frame{
		Type: frameData, Flags: flagEndStream,
		StreamID: 1, Payload: []byte{'z'},
	})
	c.flush()

	if got, want := c.bodies(1)[1], strconv.Itoa(initialWindow+1); got != want {
		t.Errorf("the handler saw a %s-byte body, want %s", got, want)
	}
}

// SETTINGS_INITIAL_WINDOW_SIZE is the one setting that reaches backwards: it
// adjusts streams that are already open. The adjustment is a delta against
// the *previous* value, and measuring it against the protocol's default
// instead misadjusts every open stream by the whole of the first change —
// visible only on a second SETTINGS, which is why it survives a first read.
func TestH2SecondInitialWindowSizeAdjustsFromThePreviousValue(t *testing.T) {
	const body = 1500
	addr, _ := serveTest(t, Config{}, func(b sluice.Batch[Request]) sluice.Batch[Response] {
		out := make([]Response, 0, b.Len())
		for range b.Items {
			out = append(out, Response{Status: 200, Body: bytes.Repeat([]byte{'y'}, body)})
		}
		return sluice.Batch[Response]{Items: out}
	})
	c := dialH2(t, addr)

	// 1000, then a stream opened at 1000, then 2000: the stream must end at
	// 2000, which fits the answer. Against the default the delta would be
	// 2000-65535 and the stream would go deeply negative.
	c.frame(h2Frame{Type: frameSettings, Payload: settingsPayload([2]uint32{settingInitialWindowSize, 1000})})
	c.frame(h2Frame{
		Type: frameHeaders, Flags: flagEndHeaders,
		StreamID: 1, Payload: headBlock("POST", "/a"),
	})
	c.frame(h2Frame{Type: frameSettings, Payload: settingsPayload([2]uint32{settingInitialWindowSize, 2000})})
	c.frame(h2Frame{Type: frameData, Flags: flagEndStream, StreamID: 1})
	c.flush()

	if got := len(c.bodies(1)[1]); got != body {
		t.Errorf("stream 1 carried %d bytes, want %d", got, body)
	}
}

// B4, and the other half of the setting: a new stream starts at the window the
// peer advertised, and a response larger than it **waits** for credit.
//
// This asserted the opposite until the write side was split off: a response
// past the window was a FLOW_CONTROL_ERROR and a GOAWAY, which took every
// other request on the connection with it. Waiting was impossible then,
// because the goroutine that would read the WINDOW_UPDATE was the one blocked
// writing the response.
func TestH2WaitsForTheWindowInsteadOfRefusing(t *testing.T) {
	const body = 200
	addr, _ := serveTest(t, Config{}, func(b sluice.Batch[Request]) sluice.Batch[Response] {
		out := make([]Response, 0, b.Len())
		for range b.Items {
			out = append(out, Response{Status: 200, Body: bytes.Repeat([]byte{'y'}, body)})
		}
		return sluice.Batch[Response]{Items: out}
	})
	c := dialH2(t, addr)

	// A stream window of 100 with the connection's 65535 left alone: only the
	// stream-level window can hold a 200-byte answer back.
	c.frame(h2Frame{Type: frameSettings, Payload: settingsPayload([2]uint32{settingInitialWindowSize, 100})})
	c.request(1, "GET", "/a")
	c.flush()

	c.pump(func() bool { return len(c.got[1]) >= 100 || c.sawGoAway })
	if c.sawGoAway {
		t.Fatal("the connection ended instead of waiting for the window")
	}
	if got := len(c.got[1]); got != 100 {
		t.Fatalf("the server wrote %d bytes against a window of 100", got)
	}
	if c.ended[1] {
		t.Fatal("the stream ended before the peer granted the rest of the window")
	}

	// While the response is stalled the connection is not deaf: PING is
	// answered, which is what a peer that uses it for liveness is waiting for.
	c.frame(h2Frame{Type: framePing, Payload: []byte("liveness")})
	c.frame(h2Frame{
		Type: frameWindowUpdate, StreamID: 1,
		Payload: windowUpdatePayload(100),
	})
	c.flush()

	if got := c.bodies(1)[1]; len(got) != body {
		t.Errorf("stream 1 carried %d bytes once the window was raised, want %d", len(got), body)
	}
	if c.pings == 0 {
		t.Error("PING went unanswered while the response waited for credit")
	}
}

// RFC 9113 §6.9.1 caps a flow-control window at 2^31-1: an increment that is
// legal on its own but pushes the window past the cap is a
// FLOW_CONTROL_ERROR. Left unchecked the sum wraps an int32 into a negative
// window, which reads as a peer that granted nothing.
func TestH2RefusesAWindowUpdateThatOverflows(t *testing.T) {
	addr, _ := serveTest(t, Config{}, echoTarget)
	c := dialH2(t, addr)

	c.frame(h2Frame{Type: frameWindowUpdate, StreamID: 0, Payload: []byte{0x7f, 0xff, 0xff, 0xff}})
	c.flush()

	if code := goAwayCode(c.waitGoAway()); code != 0x3 {
		t.Errorf("GOAWAY carried code %#x, want FLOW_CONTROL_ERROR (0x3)", code)
	}
}

// Trailers are legal — this server allows "TE: trailers" itself — and a
// second header block after the body must not be read as a second request.
func TestH2TrailersDoNotEndTheConnection(t *testing.T) {
	addr, _ := serveTest(t, Config{}, func(b sluice.Batch[Request]) sluice.Batch[Response] {
		out := make([]Response, 0, b.Len())
		for _, r := range b.Items {
			out = append(out, Response{Status: 200, Body: append([]byte(nil), r.Body...)})
		}
		return sluice.Batch[Response]{Items: out}
	})
	c := dialH2(t, addr)

	c.frame(h2Frame{
		Type: frameHeaders, Flags: flagEndHeaders,
		StreamID: 1, Payload: headBlock("POST", "/t", [2]string{"te", "trailers"}),
	})
	c.frame(h2Frame{Type: frameData, StreamID: 1, Payload: []byte("hi")})
	c.frame(h2Frame{
		Type: frameHeaders, Flags: flagEndHeaders | flagEndStream,
		StreamID: 1, Payload: literal(nil, "x-checksum", "1"),
	})
	c.flush()

	if got := c.bodies(1)[1]; got != "hi" {
		t.Errorf("stream 1 answered %q, want %q", got, "hi")
	}
}

// RFC 9113 §5.1: DATA on a half-closed(remote) stream is STREAM_CLOSED.
// Accepted silently, it lets a body keep growing past the bound it was
// already measured against.
func TestH2RefusesDataAfterEndStream(t *testing.T) {
	addr, _ := serveTest(t, Config{}, echoTarget)
	c := dialH2(t, addr)

	c.request(1, "GET", "/a")
	c.frame(h2Frame{Type: frameData, StreamID: 1, Payload: []byte("more")})
	c.request(3, "GET", "/b")
	c.flush()

	if code := c.waitReset(1); code != 0x5 {
		t.Errorf("stream 1 was reset with %#x, want STREAM_CLOSED (0x5)", code)
	}
	// The neighbour survives, which is the whole reason the refusal is a
	// stream error rather than a connection one.
	if got := c.bodies(2)[3]; got != "/b" {
		t.Errorf("stream 3 answered %q; one stream's refusal took its neighbour", got)
	}
	if c.sawGoAway {
		t.Error("the connection ended over one stream's failure")
	}
}

// The authority reaches the handler under one name whichever protocol carried
// it. Without that, host-based routing is impossible over HTTP/2 — a client
// sends :authority and no Host — and the three protocols disagree about what
// a request even looks like.
func TestAuthorityReachesTheHandlerAsHost(t *testing.T) {
	handler := func(b sluice.Batch[Request]) sluice.Batch[Response] {
		out := make([]Response, 0, b.Len())
		for _, r := range b.Items {
			out = append(out, Response{Status: 200, Body: append([]byte(nil), r.Get("host")...)})
		}
		return sluice.Batch[Response]{Items: out}
	}
	addr, _ := serveTest(t, Config{}, handler)

	t.Run("HTTP/1.1", func(t *testing.T) {
		c := dial(t, addr)
		fmt.Fprint(c, "GET /a HTTP/1.1\r\nHost: orders.test\r\n\r\n")
		_, body := readResponse(t, newReader(c))
		if body != "orders.test" {
			t.Errorf("the handler read host %q", body)
		}
	})

	t.Run("HTTP/2", func(t *testing.T) {
		c := dialH2(t, addr)
		c.frame(h2Frame{
			Type: frameHeaders, Flags: flagEndHeaders | flagEndStream,
			StreamID: 1, Payload: headBlock("GET", "/a", [2]string{":authority", "orders.test"}),
		})
		c.flush()
		if got := c.bodies(1)[1]; got != "orders.test" {
			t.Errorf("the handler read host %q", got)
		}
	})
}

// Two hops reading a different host out of one request is how a request is
// routed somewhere it was not addressed, so the disagreement is refused
// rather than resolved in favour of either.
func TestH2RefusesHostDisagreeingWithAuthority(t *testing.T) {
	addr, sizes := serveTest(t, Config{}, echoTarget)
	c := dialH2(t, addr)

	c.frame(h2Frame{
		Type: frameHeaders, Flags: flagEndHeaders | flagEndStream,
		StreamID: 1, Payload: headBlock("GET", "/a",
			[2]string{":authority", "a.test"}, [2]string{"host", "b.test"}),
	})
	c.request(3, "GET", "/b")
	c.flush()

	if code := c.waitReset(1); code != 0x1 {
		t.Errorf("stream 1 was reset with %#x, want PROTOCOL_ERROR (0x1)", code)
	}
	if got := c.bodies(1)[3]; got != "/b" {
		t.Errorf("stream 3 answered %q; a malformed neighbour took it down", got)
	}
	for _, n := range sizes() {
		if n != 1 {
			t.Errorf("the handler saw a batch of %d; the malformed request reached it", n)
		}
	}
}

// The rapid-reset bound is a rate, not a lifetime count. A long-lived
// connection that serves requests may go on cancelling — 257 legitimate
// cancellations over an afternoon are not an attack — while one that only
// cancels runs out.
func TestH2RapidResetBoundIsARateNotALifetimeCount(t *testing.T) {
	// cancel opens a stream whose request is still arriving — HEADERS
	// without END_STREAM — and resets it, which is what a client abandoning
	// an upload does.
	cancel := func(c *h2client, id uint32) {
		c.frame(h2Frame{
			Type: frameHeaders, Flags: flagEndHeaders, StreamID: id,
			Payload: headBlock("POST", "/upload", [2]string{":authority", "h"}),
		})
		c.frame(h2Frame{
			Type: frameRSTStream, StreamID: id,
			Payload: []byte{0, 0, 0, 0x8}, // CANCEL
		})
	}

	t.Run("cancellations against completed requests are allowed", func(t *testing.T) {
		addr, _ := serveTest(t, Config{}, echoTarget)
		c := dialH2(t, addr)

		// 200 requests and 400 cancellations, well past the flat bound of
		// four batches' worth of frames. Sent in groups so the concurrency
		// limit is not what ends the connection instead.
		const groups, perGroup = 20, 10
		id := uint32(1)
		for g := range groups {
			for range perGroup {
				c.request(id, "GET", "/a")
				cancel(c, id+2)
				cancel(c, id+4)
				id += 6
			}
			c.flush()
			if got := len(c.bodies((g + 1) * perGroup)); got < (g+1)*perGroup {
				t.Fatalf("after %d cancellations the connection answered %d of %d requests",
					(g+1)*perGroup*2, got, (g+1)*perGroup)
			}
		}
		if c.sawGoAway {
			t.Errorf("the connection ended with code %#x", goAwayCode(c.goAway))
		}
	})

	t.Run("cancellations without requests are refused", func(t *testing.T) {
		addr, _ := serveTest(t, Config{}, echoTarget)
		c := dialH2(t, addr)

		for i := range uint32(400) {
			cancel(c, 2*i+1)
		}
		c.flush()

		if code := goAwayCode(c.waitGoAway()); code != 0xb {
			t.Errorf("GOAWAY carried code %#x, want ENHANCE_YOUR_CALM (0xb)", code)
		}
	})

	// CVE-2023-44487 itself: HEADERS with END_STREAM, then RST_STREAM at
	// once, repeated. Each HEADERS completes a request, so a bound that
	// credits completed requests without asking whether they were answered
	// earned two more resets per pair and never tripped — the handler ran
	// for all thousand cancelled requests.
	t.Run("cancelling every request at once is refused", func(t *testing.T) {
		addr, sizes := serveTest(t, Config{}, echoTarget)
		c := dialH2(t, addr)

		const pairs = 1000
		for i := range uint32(pairs) {
			id := 2*i + 1
			c.request(id, "GET", "/a")
			c.frame(h2Frame{
				Type: frameRSTStream, StreamID: id,
				Payload: []byte{0, 0, 0, 0x8},
			})
		}
		c.flush()

		if code := goAwayCode(c.waitGoAway()); code != 0xb {
			t.Errorf("GOAWAY carried code %#x, want ENHANCE_YOUR_CALM (0xb)", code)
		}
		handled := 0
		for _, n := range sizes() {
			handled += n
		}
		// The flat allowance is 4×MaxFramesPerBatch = 256 resets; what the
		// handler saw is bounded by that, plus whatever shared the batch
		// the bound tripped in, which is dropped.
		if handled > 4*64+1 {
			t.Errorf("the handler ran for %d of %d cancelled requests", handled, pairs)
		}
	})

	// A client that cancels a request it has already sent in full — a
	// browser navigating away — at a sane rate keeps its connection.
	t.Run("cancelling some sent requests is allowed", func(t *testing.T) {
		addr, _ := serveTest(t, Config{}, echoTarget)
		c := dialH2(t, addr)

		// 600 requests, one in three cancelled straight after it is sent:
		// 200 resets against 400 answered requests.
		id, kept := uint32(1), 0
		for g := range 20 {
			for i := range 30 {
				c.request(id, "GET", "/a")
				if i%3 == 0 {
					c.frame(h2Frame{
						Type: frameRSTStream, StreamID: id,
						Payload: []byte{0, 0, 0, 0x8},
					})
				} else {
					kept++
				}
				id += 2
			}
			c.flush()
			// A cancelled request may still be answered; only the kept
			// ones are owed.
			c.pump(func() bool {
				n := 0
				for sid := range c.ended {
					if (sid-1)/2%3 != 0 {
						n++
					}
				}
				return n >= kept || c.sawGoAway
			})
			if c.sawGoAway {
				t.Fatalf("group %d: the connection ended with code %#x", g, goAwayCode(c.goAway))
			}
		}
	})
}

// A handler bug must not become a malformed :status on the wire. HTTP/1.1
// refused this from the first commit; HTTP/2 rendered it with an unsigned
// helper that turns a negative code into an empty field.
func TestH2RefusesAStatusThatIsNotOne(t *testing.T) {
	addr, _ := serveTest(t, Config{}, func(b sluice.Batch[Request]) sluice.Batch[Response] {
		out := make([]Response, 0, b.Len())
		for range b.Items {
			out = append(out, Response{Status: 700})
		}
		return sluice.Batch[Response]{Items: out}
	})
	c := dialH2(t, addr)

	c.request(1, "GET", "/a")
	c.flush()

	// The caller's bug is answered as one, on its own stream: a 500 rather
	// than a status no version of HTTP carries, and rather than a GOAWAY that
	// would have cost every other request on the connection.
	c.bodies(1)
	if got := h2Status(c.heads[1]); got != 500 {
		t.Errorf("stream 1 was answered %d, want 500", got)
	}
	if bytes.Contains(c.heads[1], []byte("700")) {
		t.Error("a status of 700 was written to the wire")
	}
	if c.sawGoAway {
		t.Error("a handler bug ended the connection")
	}
}

// A handler panic is the caller's bug and reaches one stream, not the
// connection. It used to be a GOAWAY: one panic on one request cost every
// other request in flight on that connection.
func TestH2HandlerPanicAnswersOneStream(t *testing.T) {
	addr, _ := serveTest(t, Config{}, func(b sluice.Batch[Request]) sluice.Batch[Response] {
		for _, r := range b.Items {
			if string(r.Target) == "/boom" {
				panic("SECRET-/var/run/keys")
			}
		}
		out := make([]Response, 0, b.Len())
		for _, r := range b.Items {
			out = append(out, Response{Status: 200, Body: append([]byte(nil), r.Target...)})
		}
		return sluice.Batch[Response]{Items: out}
	})
	c := dialH2(t, addr)

	c.request(1, "GET", "/boom")
	c.flush()
	c.bodies(1)
	if got := h2Status(c.heads[1]); got != 500 {
		t.Errorf("the panicking request was answered %d, want 500", got)
	}
	if bytes.Contains(c.heads[1], []byte("SECRET")) {
		t.Error("the panic value was written to the wire")
	}
	if c.sawGoAway {
		t.Fatal("a handler panic ended the connection")
	}

	// And the connection still serves.
	c.request(3, "GET", "/ok")
	c.flush()
	if got := c.bodies(2)[3]; got != "/ok" {
		t.Errorf("after the panic the connection answered %q, want %q", got, "/ok")
	}
}

// GOAWAY's trailing field is free-form and goes to whoever dialled the
// socket. What used to go there was the wrapped error text, and a handler
// panic value travels in one of those.
func TestH2GoAwayCarriesNoInternalText(t *testing.T) {
	addr, _ := serveTest(t, Config{}, echoTarget)
	c := dialH2(t, addr)

	// A connection error, which is what GOAWAY is for now that a stream's own
	// failure is a RST_STREAM: DATA on a stream nobody opened.
	c.frame(h2Frame{Type: frameData, StreamID: 99, Payload: []byte("/etc/shadow")})
	c.flush()

	payload := c.waitGoAway()
	if len(payload) != 8 {
		t.Errorf("GOAWAY carried %d bytes of debug data: %q", len(payload)-8, payload[8:])
	}
	if bytes.Contains(payload, []byte("stream")) {
		t.Error("the wrapped error text was sent to the peer")
	}
}

// hpackEncInt encodes RFC 7541 §5.1's prefixed integer: the low prefixBits
// bits of the first octet carry v directly if it fits, otherwise the prefix
// is filled and continuation octets carry the rest seven bits at a time. It
// exists so these tests can build HPACK instructions the package's own
// literal helper does not need to.
func hpackEncInt(prefixBits uint8, top byte, v uint64) []byte {
	mask := uint64(1)<<prefixBits - 1
	if v < mask {
		return []byte{top | byte(v)}
	}
	out := []byte{top | byte(mask)}
	v -= mask
	for v >= 128 {
		out = append(out, byte(v%128+128))
		v /= 128
	}
	return append(out, byte(v))
}

// hpackTableSizeUpdate encodes a Dynamic Table Size Update (RFC 7541 §6.3).
func hpackTableSizeUpdate(size uint64) []byte {
	return hpackEncInt(5, 0x20, size)
}

// hpackIncIndexed encodes a literal header field with incremental indexing
// and a literal name (RFC 7541 §6.2.1): the only representation that adds an
// entry to the dynamic table, which is what these tests need to exercise it.
func hpackIncIndexed(name, value string) []byte {
	b := hpackEncInt(6, 0x40, 0) // index 0: the name follows as a literal
	b = append(b, byte(len(name)))
	b = append(b, name...)
	b = append(b, byte(len(value)))
	return append(b, value...)
}

// hpackIndexed encodes an indexed header field (RFC 7541 §6.1): idx into the
// shared static+dynamic index space.
func hpackIndexed(idx uint64) []byte {
	return hpackEncInt(7, 0x80, idx)
}

// staticTableLen is RFC 7541 Appendix A's 61 entries, which is where the
// dynamic table's own index space starts (index staticTableLen+1 is the most
// recently added dynamic entry).
const staticTableLen = 61

// B5. RFC 7541 §6.3 allows a peer to shrink its dynamic table size to zero
// and later restore it to a nonzero size, and the sequence is legal on its
// own: "shrink to 0, then restore" is exactly the case the decoder's
// advertised/maxSize split exists to keep from being refused (see the
// comment on HPACKDecoder.advertised in hpack.go). This drives that sequence
// end to end — an entry added while the table is nonzero, evicted by the
// shrink, and a fresh entry added and *read back by index* once the table is
// restored — which is what proves the table's bookkeeping, not just its
// ceiling, survived the round trip.
func TestH2HPACKTableShrinkToZeroThenRestore(t *testing.T) {
	addr, _ := serveTest(t, Config{}, func(b sluice.Batch[Request]) sluice.Batch[Response] {
		out := make([]Response, 0, b.Len())
		for _, r := range b.Items {
			out = append(out, Response{Status: 200, Body: append([]byte(nil), r.Get("x-custom")...)})
		}
		return sluice.Batch[Response]{Items: out}
	})
	c := dialH2(t, addr)

	// Stream 1: shrink the table to 0 first. Any entry indexed-in here would
	// be evicted on arrival (RFC 7541 §4.4), so this block carries none.
	var block1 []byte
	block1 = append(block1, hpackTableSizeUpdate(0)...)
	block1 = append(block1, headBlock("GET", "/a")...)
	c.frame(h2Frame{
		Type: frameHeaders, Flags: flagEndHeaders | flagEndStream,
		StreamID: 1, Payload: block1,
	})

	// Stream 3: restore the table to a nonzero size and add an entry to it.
	var block3 []byte
	block3 = append(block3, hpackTableSizeUpdate(100)...)
	block3 = append(block3, headBlock("GET", "/b")...)
	block3 = append(block3, hpackIncIndexed("x-custom", "v1")...)
	c.frame(h2Frame{
		Type: frameHeaders, Flags: flagEndHeaders | flagEndStream,
		StreamID: 3, Payload: block3,
	})

	// Stream 5: read the entry back purely by index. This only decodes to
	// the right field if the table's contents, not just its size, survived
	// the shrink and the restore.
	var block5 []byte
	block5 = append(block5, headBlock("GET", "/c")...)
	block5 = append(block5, hpackIndexed(staticTableLen+1)...)
	c.frame(h2Frame{
		Type: frameHeaders, Flags: flagEndHeaders | flagEndStream,
		StreamID: 5, Payload: block5,
	})
	c.flush()

	bodies := c.bodies(3)
	if got := bodies[5]; got != "v1" {
		t.Errorf("stream 5 read back %q via the dynamic table, want %q", got, "v1")
	}
	if c.sawGoAway {
		t.Fatal("a legal shrink-then-restore sequence ended the connection")
	}
}

// B6. h2conn.go strips PADDED and PRIORITY off a HEADERS frame before HPACK
// ever sees it. The normal case: both flags set, a real header block between
// the priority fields and the padding.
func TestH2PaddedPriorityHeadersAreStrippedCorrectly(t *testing.T) {
	addr, _ := serveTest(t, Config{}, echoTarget)
	c := dialH2(t, addr)

	block := headBlock("GET", "/pp")
	payload := []byte{4}                          // pad length
	payload = append(payload, make([]byte, 5)...) // E + stream dependency + weight
	payload = append(payload, block...)
	payload = append(payload, make([]byte, 4)...) // the padding itself
	c.frame(h2Frame{
		Type: frameHeaders,
		Flags: flagEndHeaders | flagEndStream |
			flagPadded | flagPriority,
		StreamID: 1, Payload: payload,
	})
	c.flush()

	if got := c.bodies(1)[1]; got != "/pp" {
		t.Errorf("stream 1 answered %q, want %q", got, "/pp")
	}
}

// B6, the edge case: the declared pad length consumes the whole of what is
// left once the pad-length octet itself is removed, so nothing remains for
// even the 5 priority bytes PRIORITY promised — let alone a header block.
// RFC 9113 §6.2 makes this malformed (there is nowhere for the priority
// fields to be), and the stripping code has to catch it explicitly rather
// than slice five bytes off an empty block.
func TestH2PaddedPriorityHeadersPadConsumesWholeBlock(t *testing.T) {
	addr, _ := serveTest(t, Config{}, echoTarget)
	c := dialH2(t, addr)

	// pad length 5, and 5 bytes of padding: after the pad-length octet is
	// dropped, pad (5) equals what remains (5), so the stripped block is
	// empty before PRIORITY's 5-byte slice is even attempted.
	payload := append([]byte{5}, make([]byte, 5)...)
	c.frame(h2Frame{
		Type: frameHeaders,
		Flags: flagEndHeaders | flagEndStream |
			flagPadded | flagPriority,
		StreamID: 1, Payload: payload,
	})
	c.flush()

	if code := goAwayCode(c.waitGoAway()); code != 0x1 {
		t.Errorf("GOAWAY carried code %#x, want PROTOCOL_ERROR (0x1)", code)
	}
}

// RFC 9113 §6.5.2 bounds SETTINGS_MAX_FRAME_SIZE to [2^14, 2^24-1], and §4.2
// obliges every endpoint to receive 16384-byte frames. A configuration whose
// body bound is small must therefore not shrink the advertised frame size
// with it: a conformant client treats the illegal setting as a connection
// error, and refuses 16384-byte frames the server was obliged to take.
func TestH2AdvertisedMaxFrameSizeKeepsTheProtocolFloor(t *testing.T) {
	addr, _ := serveTest(t, Config{MaxBodyBytes: 1000}, echoTarget)
	c := dialH2(t, addr)

	c.pump(func() bool { _, ok := c.settings[0x5]; return ok })
	got, ok := c.settings[0x5]
	if !ok {
		t.Fatal("the server advertised no SETTINGS_MAX_FRAME_SIZE")
	}
	if got < 16384 || got > 1<<24-1 {
		t.Fatalf("SETTINGS_MAX_FRAME_SIZE = %d, outside the legal [16384, 2^24-1]", got)
	}

	// And a full 16384-byte frame is accepted as framing: it fails the body
	// bound as a stream error, never the frame parser as a connection error.
	c.frame(h2Frame{
		Type: frameHeaders, Flags: flagEndHeaders,
		StreamID: 1, Payload: headBlock("POST", "/u"),
	})
	c.frame(h2Frame{
		Type: frameData, Flags: flagEndStream,
		StreamID: 1, Payload: bytes.Repeat([]byte{'z'}, 16384),
	})
	c.flush()

	if code := c.waitReset(1); code != 0xb {
		t.Errorf("a 16 KiB frame against a 1000-byte body bound reset with %#x, want ENHANCE_YOUR_CALM (0xb)", code)
	}
	if c.sawGoAway {
		t.Error("a frame at the protocol's mandatory size ended the connection")
	}
}

// RFC 9113 §6.8: a peer's GOAWAY means "finish what you have, open nothing
// new" — not "stop reading". The WINDOW_UPDATE an in-flight response is
// waiting for arrives on the read side, so a reader that stops at the GOAWAY
// stalls every response larger than its remaining send window until
// WriteTimeout resets it.
func TestH2ServesInFlightResponsesAfterPeerGoAway(t *testing.T) {
	const body = 200
	addr, _ := serveTest(t, Config{}, func(b sluice.Batch[Request]) sluice.Batch[Response] {
		out := make([]Response, 0, b.Len())
		for range b.Items {
			out = append(out, Response{Status: 200, Body: bytes.Repeat([]byte{'y'}, body)})
		}
		return sluice.Batch[Response]{Items: out}
	})
	c := dialH2(t, addr)

	// A stream window of 100, so only a later WINDOW_UPDATE can complete the
	// 200-byte answer — the update this test sends after its GOAWAY.
	c.frame(h2Frame{Type: frameSettings, Payload: settingsPayload([2]uint32{settingInitialWindowSize, 100})})
	c.request(1, "GET", "/a")
	c.frame(h2Frame{Type: frameGoAway, Payload: make([]byte, 8)}) // last-stream 0, NO_ERROR
	// A stream opened after the GOAWAY is refused, not served and not fatal.
	c.request(3, "GET", "/b")
	c.flush()

	c.pump(func() bool { return len(c.got[1]) >= 100 || c.sawGoAway })
	if got := len(c.got[1]); got != 100 {
		t.Fatalf("the server wrote %d bytes against a window of 100", got)
	}
	if code := c.waitReset(3); code != 0x7 {
		t.Errorf("a stream opened after GOAWAY was reset with %#x, want REFUSED_STREAM (0x7)", code)
	}

	c.frame(h2Frame{
		Type: frameWindowUpdate, StreamID: 1,
		Payload: windowUpdatePayload(100),
	})
	c.flush()

	if got := c.bodies(1)[1]; len(got) != body {
		t.Errorf("after its own GOAWAY the client received %d bytes, want %d: "+
			"the reader stopped and the WINDOW_UPDATE was never read", len(got), body)
	}
	if !c.ended[1] {
		t.Error("the in-flight response never ended")
	}
}

// The aggregate assembly bound. Each stream alone stays under MaxBodyBytes,
// but windows are replenished as DATA is buffered, so without a connection-
// level cap a peer can park MaxConcurrentStreams × MaxBodyBytes by never
// finishing any upload.
func TestH2BoundsBuffersAcrossUnfinishedUploads(t *testing.T) {
	// MaxBodyBytes 1000 makes the aggregate bound 4000: four 900-byte
	// unfinished uploads fit, the fifth does not.
	addr, _ := serveTest(t, Config{MaxBodyBytes: 1000}, echoTarget)
	c := dialH2(t, addr)

	for _, id := range []uint32{1, 3, 5, 7, 9} {
		c.frame(h2Frame{
			Type: frameHeaders, Flags: flagEndHeaders,
			StreamID: id, Payload: headBlock("POST", "/u"),
		})
		c.data(id, bytes.Repeat([]byte{'z'}, 900), false)
	}
	c.flush()

	if code := c.waitReset(9); code != 0xb {
		t.Errorf("the upload past the aggregate bound was reset with %#x, want ENHANCE_YOUR_CALM (0xb)", code)
	}
	if c.sawGoAway {
		t.Fatal("the aggregate bound ended the connection instead of one stream")
	}

	// The streams under the bound are unharmed: the first one completes.
	c.data(1, []byte("!"), true)
	c.flush()
	if got := c.bodies(1)[1]; got != "/u" {
		t.Errorf("stream 1 answered %q after a neighbour hit the aggregate bound, want %q", got, "/u")
	}
}

// The assembly deadline. The frame-level read clock restarts on every frame
// consumed, so a peer that buffered most of a body could hold the memory
// forever for the price of one PING per interval; ReadTimeout bounds the
// request's whole assembly instead, as it does on the HTTP/1.1 side.
func TestH2AssemblyDeadlineEndsAStalledUpload(t *testing.T) {
	addr, _ := serveTest(t, Config{ReadTimeout: 300 * time.Millisecond}, echoTarget)
	c := dialH2(t, addr)

	c.frame(h2Frame{
		Type: frameHeaders, Flags: flagEndHeaders,
		StreamID: 1, Payload: headBlock("POST", "/stalled"),
	})
	c.data(1, []byte("a body that never finishes"), false)
	c.flush()

	// PING keeps the frame-level clock alive — that is the attack — so only
	// the per-request deadline can end the stream.
	deadline := time.Now().Add(5 * time.Second)
	tmp := make([]byte, 4096)
	for time.Now().Before(deadline) {
		if _, seen := c.resets[1]; seen || c.sawGoAway {
			break
		}
		c.frame(h2Frame{Type: framePing, Payload: []byte("healthy?")})
		c.flush()
		if err := c.conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
			t.Fatal(err)
		}
		n, _ := c.conn.Read(tmp)
		c.in = append(c.in, tmp[:n]...)
		c.parse()
	}

	code, seen := c.resets[1]
	if !seen {
		t.Fatal("the stalled upload was never reset: one PING per interval holds its memory forever")
	}
	if code != 0x8 {
		t.Errorf("the stalled stream was reset with %#x, want CANCEL (0x8)", code)
	}
	if c.sawGoAway {
		t.Fatal("a stalled stream ended the whole connection")
	}

	// The connection still serves: the deadline is the stream's, not its
	// neighbours'.
	c.request(3, "GET", "/after")
	c.flush()
	if got := c.bodies(1)[3]; got != "/after" {
		t.Errorf("after the stalled stream was reset the connection answered %q, want %q", got, "/after")
	}
}

// SETTINGS entries are overwrites, so a frame repeating one identifier means
// its last value — which is also what keeps a frame full of repeated
// INITIAL_WINDOW_SIZE entries from buying a stream-window walk per entry.
func TestH2RepeatedSettingsEntriesMeanTheLastValue(t *testing.T) {
	t.Run("the last duplicate wins", func(t *testing.T) {
		const body = 300
		addr, _ := serveTest(t, Config{}, func(b sluice.Batch[Request]) sluice.Batch[Response] {
			out := make([]Response, 0, b.Len())
			for range b.Items {
				out = append(out, Response{Status: 200, Body: bytes.Repeat([]byte{'y'}, body)})
			}
			return sluice.Batch[Response]{Items: out}
		})
		c := dialH2(t, addr)

		// 1000 then 200 in one frame: the stream must open at 200.
		c.frame(h2Frame{Type: frameSettings, Payload: settingsPayload(
			[2]uint32{settingInitialWindowSize, 1000},
			[2]uint32{settingInitialWindowSize, 200},
		)})
		c.request(1, "GET", "/a")
		c.flush()

		c.pump(func() bool { return len(c.got[1]) >= 200 || c.sawGoAway })
		if c.sawGoAway {
			t.Fatal("a SETTINGS frame with a repeated identifier ended the connection")
		}
		if got := len(c.got[1]); got != 200 {
			t.Fatalf("the server wrote %d bytes; the stream window is not the last duplicate's 200", got)
		}
		if c.ended[1] {
			t.Fatal("the stream ended against a window of 200")
		}

		c.frame(h2Frame{Type: frameWindowUpdate, StreamID: 1, Payload: windowUpdatePayload(100)})
		c.flush()
		if got := c.bodies(1)[1]; len(got) != body {
			t.Errorf("stream 1 carried %d bytes once the window was raised, want %d", len(got), body)
		}
	})

	t.Run("an invalid entry is refused wherever it sits", func(t *testing.T) {
		// Last-wins must not become validate-last-only: the invalid first
		// entry is a connection error even with a valid duplicate behind it.
		addr, _ := serveTest(t, Config{}, echoTarget)
		c := dialH2(t, addr)

		c.frame(h2Frame{Type: frameSettings, Payload: settingsPayload(
			[2]uint32{0x2, 2}, // ENABLE_PUSH may only be 0 or 1
			[2]uint32{0x2, 0},
		)})
		c.flush()

		if code := goAwayCode(c.waitGoAway()); code != 0x1 {
			t.Errorf("GOAWAY carried code %#x, want PROTOCOL_ERROR (0x1)", code)
		}
	})
}

// RFC 9113 §4.3: every field block must reach the shared HPACK decoder, even
// a refused stream's. An incremental-indexing literal in a refused block that
// is never decoded desynchronises the dynamic table, and every later block on
// every stream decodes wrong — one refusal silently corrupting a connection
// that was promised it survives refusals. One RST answers the whole block,
// not one per CONTINUATION fragment.
func TestH2RefusedStreamBlockStillFeedsHPACK(t *testing.T) {
	release := make(chan struct{})
	addr, _ := serveH2(t, H2Config{MaxConcurrentStreams: 1},
		func(b sluice.Batch[Request]) sluice.Batch[Response] {
			out := make([]Response, 0, b.Len())
			for _, r := range b.Items {
				if string(r.Target) == "/hold" {
					<-release
				}
				out = append(out, Response{Status: 200, Body: append([]byte(nil), r.Get("x-k")...)})
			}
			return sluice.Batch[Response]{Items: out}
		})
	c := dialH2(t, addr)

	// Stream 1 occupies the only slot until release.
	c.request(1, "GET", "/hold")

	// Stream 3 is over the limit. Its block adds x-k to the dynamic table,
	// and is split across a CONTINUATION so a refusal answered per fragment
	// would show as two RSTs.
	block := headBlock("GET", "/refused")
	block = append(block, hpackIncIndexed("x-k", "v1")...)
	c.frame(h2Frame{
		Type: frameHeaders, StreamID: 3, Payload: block[:5],
	})
	c.frame(h2Frame{
		Type: frameContinuation, Flags: flagEndHeaders,
		StreamID: 3, Payload: block[5:],
	})
	c.flush()

	if code := c.waitReset(3); code != 0x7 {
		t.Fatalf("the over-limit stream was reset with %#x, want REFUSED_STREAM (0x7)", code)
	}
	close(release)
	c.bodies(1) // stream 1 completes, freeing the slot

	// The proof: a later block referencing the refused block's entry purely
	// by index only decodes if that block reached the decoder.
	var probe []byte
	probe = append(probe, headBlock("GET", "/probe")...)
	probe = append(probe, hpackIndexed(staticTableLen+1)...)
	id := c.admitted(5, func(id uint32) {
		c.frame(h2Frame{
			Type: frameHeaders, Flags: flagEndHeaders | flagEndStream,
			StreamID: id, Payload: probe,
		})
	})

	if got := c.bodies(2)[id]; got != "v1" {
		t.Errorf("stream %d read back %q via the dynamic table, want %q: the refused block never fed HPACK", id, got, "v1")
	}
	if c.sawGoAway {
		t.Fatal("the connection died after a refused stream: the HPACK table desynchronised")
	}
	if n := c.resetCount[3]; n != 1 {
		t.Errorf("the refused stream was reset %d times, want exactly one RST for the whole block", n)
	}
}

// RFC 9113 §5.1, the other half of the refusal defect: a client POSTing at
// the server's own advertised concurrency limit already has DATA on the wire
// behind its HEADERS. If the refusal does not advance lastID, that DATA reads
// as a frame on an idle stream — a connection error that kills every healthy
// request a conformant client had in flight.
func TestH2DataBehindARefusedStreamIsTolerated(t *testing.T) {
	release := make(chan struct{})
	addr, _ := serveH2(t, H2Config{MaxConcurrentStreams: 1},
		func(b sluice.Batch[Request]) sluice.Batch[Response] {
			out := make([]Response, 0, b.Len())
			for _, r := range b.Items {
				if string(r.Target) == "/hold" {
					<-release
				}
				out = append(out, Response{Status: 200, Body: append([]byte(nil), r.Target...)})
			}
			return sluice.Batch[Response]{Items: out}
		})
	c := dialH2(t, addr)

	c.request(1, "GET", "/hold")
	// A whole POST behind the refusal: HEADERS, body, END_STREAM.
	c.frame(h2Frame{
		Type: frameHeaders, Flags: flagEndHeaders,
		StreamID: 3, Payload: headBlock("POST", "/refused"),
	})
	c.data(3, []byte("already in flight"), true)
	c.flush()

	if code := c.waitReset(3); code != 0x7 {
		t.Errorf("the over-limit stream was reset with %#x, want REFUSED_STREAM (0x7)", code)
	}
	if c.sawGoAway {
		t.Fatal("DATA behind a refused stream was read as an idle-stream frame and killed the connection")
	}

	close(release)
	if got := c.bodies(1)[1]; got != "/hold" {
		t.Fatalf("stream 1 answered %q, want %q", got, "/hold")
	}

	id := c.admitted(5, func(id uint32) { c.request(id, "GET", "/after") })
	if got := c.bodies(2)[id]; got != "/after" {
		t.Errorf("after the refusal the connection answered %q, want %q", got, "/after")
	}
}

// RFC 9113 §8.1.1: content-length must equal the DATA sum, or the request is
// malformed. Unchecked, the field travels to the next hop stating a length
// this server never verified — the request-integrity half of the smuggling
// family.
func TestH2ChecksContentLengthAgainstTheBody(t *testing.T) {
	post := func(c *h2client, id uint32, declared string, body []byte) {
		c.frame(h2Frame{
			Type: frameHeaders, Flags: flagEndHeaders,
			StreamID: id, Payload: headBlock("POST", "/cl", [2]string{"content-length", declared}),
		})
		c.data(id, body, true)
	}

	t.Run("a short body is refused", func(t *testing.T) {
		addr, sizes := serveTest(t, Config{}, echoTarget)
		c := dialH2(t, addr)

		post(c, 1, "5", []byte("hi"))
		c.request(3, "GET", "/after")
		c.flush()

		if code := c.waitReset(1); code != 0x1 {
			t.Errorf("stream 1 was reset with %#x, want PROTOCOL_ERROR (0x1)", code)
		}
		if got := c.bodies(1)[3]; got != "/after" {
			t.Errorf("stream 3 answered %q; the length mismatch took its neighbour", got)
		}
		for _, n := range sizes() {
			if n != 1 {
				t.Errorf("the handler saw a batch of %d; the mismatched request reached it", n)
			}
		}
	})

	t.Run("END_STREAM on HEADERS with a nonzero length is refused", func(t *testing.T) {
		addr, _ := serveTest(t, Config{}, echoTarget)
		c := dialH2(t, addr)

		c.frame(h2Frame{
			Type: frameHeaders, Flags: flagEndHeaders | flagEndStream,
			StreamID: 1, Payload: headBlock("POST", "/cl", [2]string{"content-length", "3"}),
		})
		c.flush()

		if code := c.waitReset(1); code != 0x1 {
			t.Errorf("stream 1 was reset with %#x, want PROTOCOL_ERROR (0x1)", code)
		}
		if c.sawGoAway {
			t.Error("the mismatch ended the connection instead of the stream")
		}
	})

	t.Run("a matching length is served", func(t *testing.T) {
		addr, _ := serveTest(t, Config{}, echoTarget)
		c := dialH2(t, addr)

		post(c, 1, "2", []byte("hi"))
		c.flush()
		if got := c.bodies(1)[1]; got != "/cl" {
			t.Errorf("a request with a correct content-length answered %q, want %q", got, "/cl")
		}
	})
}

// RFC 9113 §8.2.1 and §10.3: pseudo-header values compose the request line
// of a downstream HTTP/1.1 hop, so they get the request line's own octet
// rules. HPACK can carry any byte; the h1 parser never tolerated these.
func TestH2RefusesInvalidPseudoHeaderValues(t *testing.T) {
	tests := []struct {
		name                            string
		method, path, scheme, authority string
	}{
		{"a method with a space", "GE T", "/x", "http", "h"},
		{"a path with a space", "GET", "/a b", "http", "h"},
		{"a path with a control byte", "GET", "/a\x01b", "http", "h"},
		{"a scheme that is not a token", "GET", "/x", "ht tp", "h"},
		{"an authority with a control byte", "GET", "/x", "http", "h\x7fst"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr, sizes := serveTest(t, Config{}, echoTarget)
			c := dialH2(t, addr)

			var block []byte
			block = literal(block, ":method", tt.method)
			block = literal(block, ":path", tt.path)
			block = literal(block, ":scheme", tt.scheme)
			block = literal(block, ":authority", tt.authority)
			c.frame(h2Frame{
				Type: frameHeaders, Flags: flagEndHeaders | flagEndStream,
				StreamID: 1, Payload: block,
			})
			c.flush()

			if code := c.waitReset(1); code != 0x1 {
				t.Errorf("reset code = %#x, want 0x1 PROTOCOL_ERROR", code)
			}
			if c.sawGoAway {
				t.Error("one malformed request ended the whole connection")
			}
			if got := sizes(); len(got) > 0 {
				t.Errorf("a malformed request reached the handler: %v", got)
			}
		})
	}
}

// Trailers take the same octet rules as the head's fields: they reach
// whatever reads the request last, which makes them just as good a smuggling
// carrier (§8.2.1 does not exempt them).
func TestH2RefusesInvalidTrailerFields(t *testing.T) {
	tests := []struct {
		name    string
		trailer []byte
	}{
		{"a value with a bare LF", literal(nil, "x-sum", "a\nb")},
		{"a value with NUL", literal(nil, "x-sum", "a\x00b")},
		{"an uppercase name", literal(nil, "X-Sum", "1")},
		{"a name with a space", literal(nil, "x sum", "1")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr, _ := serveTest(t, Config{}, echoTarget)
			c := dialH2(t, addr)

			c.frame(h2Frame{
				Type: frameHeaders, Flags: flagEndHeaders,
				StreamID: 1, Payload: headBlock("POST", "/t", [2]string{"te", "trailers"}),
			})
			c.frame(h2Frame{Type: frameData, StreamID: 1, Payload: []byte("hi")})
			c.frame(h2Frame{
				Type: frameHeaders, Flags: flagEndHeaders | flagEndStream,
				StreamID: 1, Payload: tt.trailer,
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
	}
}

// RFC 9113 §8.2.2's output half: a response generated with a connection-
// specific field is malformed, and a conformant client MUST reject it — so a
// handler's HTTP/1.1 habit is answered as the caller's bug, a 500 on its own
// stream, rather than written to the wire for the client's connection to die
// on.
func TestH2RefusesConnectionSpecificResponseFields(t *testing.T) {
	for _, field := range []string{"connection", "transfer-encoding", "keep-alive", "upgrade", "content-length"} {
		t.Run(field, func(t *testing.T) {
			addr, _ := serveTest(t, Config{}, func(b sluice.Batch[Request]) sluice.Batch[Response] {
				out := make([]Response, 0, b.Len())
				for range b.Items {
					out = append(out, Response{Status: 200, Headers: []Header{
						{Name: []byte(field), Value: []byte("x")},
					}})
				}
				return sluice.Batch[Response]{Items: out}
			})
			c := dialH2(t, addr)

			c.request(1, "GET", "/a")
			c.flush()

			c.bodies(1)
			if got := h2Status(c.heads[1]); got != 500 {
				t.Errorf("stream 1 was answered %d, want 500", got)
			}
			if bytes.Contains(c.heads[1], []byte(field)) {
				t.Errorf("the %s field was written to the wire", field)
			}
			if c.sawGoAway {
				t.Error("a handler bug ended the connection")
			}
		})
	}
}

// RFC 9113 §6.3: a PRIORITY frame of the wrong length is a *stream* error of
// type FRAME_SIZE_ERROR — the one frame whose length fault does not end the
// connection, because nothing connection-wide is in doubt: PRIORITY carries
// no HPACK and no flow-control state.
func TestH2PriorityOfTheWrongLengthResetsOnlyItsStream(t *testing.T) {
	addr, _ := serveTest(t, Config{}, echoTarget)
	c := dialH2(t, addr)

	// A stream whose request is still arriving: open, so there is a stream
	// to reset. On an idle one there is not (see the test below).
	c.frame(h2Frame{
		Type: frameHeaders, Flags: flagEndHeaders, StreamID: 1,
		Payload: headBlock("POST", "/upload", [2]string{":authority", "h"}),
	})
	c.frame(h2Frame{Type: framePriority, StreamID: 1, Payload: make([]byte, 4)})
	c.flush()

	if code := c.waitReset(1); code != 0x6 {
		t.Errorf("RST_STREAM carried code %#x, want FRAME_SIZE_ERROR (0x6)", code)
	}

	// The connection survives it: a request behind the bad frame is served.
	c.request(3, "GET", "/after")
	c.flush()
	if got := c.bodies(1)[3]; got != "/after" {
		t.Errorf("after the reset the request got %q, want %q", got, "/after")
	}
}

// RFC 9113 §5.1: an idle stream may receive HEADERS and PRIORITY, and nothing
// else; anything else is a connection error. RST_STREAM may never name an
// idle stream either, so the stream-error answers a malformed frame earns on
// an open stream are connection errors here — the server used to send
// RST_STREAM for a stream that never existed.
func TestH2FramesOnAnIdleStreamEndTheConnection(t *testing.T) {
	for _, tc := range []struct {
		name string
		f    h2Frame
		code uint32
	}{
		{"PRIORITY of the wrong length", h2Frame{Type: framePriority, StreamID: 5, Payload: make([]byte, 4)}, 0x6},
		{"WINDOW_UPDATE of zero", h2Frame{Type: frameWindowUpdate, StreamID: 5, Payload: windowUpdatePayload(0)}, 0x1},
		{"WINDOW_UPDATE", h2Frame{Type: frameWindowUpdate, StreamID: 5, Payload: windowUpdatePayload(10)}, 0x1},
		{"WINDOW_UPDATE on an even stream", h2Frame{Type: frameWindowUpdate, StreamID: 2, Payload: windowUpdatePayload(10)}, 0x1},
		{"RST_STREAM", h2Frame{Type: frameRSTStream, StreamID: 5, Payload: []byte{0, 0, 0, 0x8}}, 0x1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addr, _ := serveTest(t, Config{}, echoTarget)
			c := dialH2(t, addr)

			c.request(1, "GET", "/a") // stream 1 is used; 5 and 2 stay idle
			c.frame(tc.f)
			c.flush()

			if code := goAwayCode(c.waitGoAway()); code != tc.code {
				t.Errorf("GOAWAY carried code %#x, want %#x", code, tc.code)
			}
			if n := c.resetCount[tc.f.StreamID]; n != 0 {
				t.Errorf("the server sent %d RST_STREAM for idle stream %d", n, tc.f.StreamID)
			}
		})
	}

	t.Run("a well-formed PRIORITY is allowed", func(t *testing.T) {
		addr, _ := serveTest(t, Config{}, echoTarget)
		c := dialH2(t, addr)
		c.frame(h2Frame{Type: framePriority, StreamID: 5, Payload: make([]byte, 5)})
		c.request(7, "GET", "/a")
		c.flush()
		if got := c.bodies(1)[7]; got != "/a" || c.sawGoAway {
			t.Errorf("after a PRIORITY on an idle stream: %q, GOAWAY %v", got, c.sawGoAway)
		}
	})
}

// A header block on a stream this end has already closed is decoded — the
// HPACK table needs it — and dropped, whatever it holds. Refusing it for
// carrying too many fields sent a second RST_STREAM for a stream already
// gone, and charged a second reset to the rapid-reset allowance.
func TestH2OversizedBlockOnAClosedStreamIsNotResetAgain(t *testing.T) {
	addr, _ := serveTest(t, Config{MaxHeaders: 4}, echoTarget)
	c := dialH2(t, addr)

	// Open stream 1, then have the client reset it.
	c.frame(h2Frame{
		Type: frameHeaders, Flags: flagEndHeaders, StreamID: 1,
		Payload: headBlock("POST", "/upload", [2]string{":authority", "h"}),
	})
	c.frame(h2Frame{Type: frameRSTStream, StreamID: 1, Payload: []byte{0, 0, 0, 0x8}})
	// A trailer section already in flight, over MaxHeaders.
	var many [][2]string
	for i := range 8 {
		many = append(many, [2]string{"x-t" + strconv.Itoa(i), "v"})
	}
	var block []byte
	for _, kv := range many {
		block = literal(block, kv[0], kv[1])
	}
	c.frame(h2Frame{
		Type: frameHeaders, Flags: flagEndHeaders | flagEndStream, StreamID: 1,
		Payload: block,
	})
	c.request(3, "GET", "/after")
	c.flush()

	if got := c.bodies(1)[3]; got != "/after" {
		t.Fatalf("the request behind the block got %q", got)
	}
	if n := c.resetCount[1]; n != 0 {
		t.Errorf("the server reset closed stream 1 %d times, want 0", n)
	}
}

// RFC 9113 §6.9: a WINDOW_UPDATE with a zero increment on a stream is a
// stream error, PROTOCOL_ERROR; only the connection-level one (stream 0)
// ends the connection. Before the split, one zero on one stream cost a
// client every other request in flight.
func TestH2ZeroWindowUpdateOnAStreamResetsOnlyThatStream(t *testing.T) {
	addr, _ := serveTest(t, Config{}, echoTarget)
	c := dialH2(t, addr)

	c.request(1, "GET", "/one")
	c.flush()
	if got := c.bodies(1)[1]; got != "/one" {
		t.Fatalf("request 1 got %q", got)
	}

	c.frame(h2Frame{Type: frameWindowUpdate, StreamID: 1, Payload: windowUpdatePayload(0)})
	c.flush()
	if code := c.waitReset(1); code != 0x1 {
		t.Errorf("RST_STREAM carried code %#x, want PROTOCOL_ERROR (0x1)", code)
	}

	c.request(3, "GET", "/two")
	c.flush()
	if got := c.bodies(2)[3]; got != "/two" {
		t.Errorf("after the reset the request got %q, want %q", got, "/two")
	}

	// The connection-level zero stays a connection error.
	c.frame(h2Frame{Type: frameWindowUpdate, StreamID: 0, Payload: windowUpdatePayload(0)})
	c.flush()
	if code := goAwayCode(c.waitGoAway()); code != 0x1 {
		t.Errorf("GOAWAY carried code %#x, want PROTOCOL_ERROR (0x1)", code)
	}
}

// The error-code fidelity RFC 9113 §4.2 and §6 mandate: a frame over the
// advertised maximum, and a fixed-length control frame of the wrong length,
// are FRAME_SIZE_ERROR on the GOAWAY — not the generic PROTOCOL_ERROR §5.4
// would excuse.
func TestH2FrameSizeFaultsCarryFrameSizeError(t *testing.T) {
	tests := []struct {
		name  string
		frame h2Frame
	}{
		{"RST_STREAM of the wrong length", h2Frame{Type: frameRSTStream, StreamID: 1, Payload: make([]byte, 3)}},
		{"SETTINGS not a multiple of 6", h2Frame{Type: frameSettings, Payload: make([]byte, 7)}},
		{"SETTINGS acknowledgement with a payload", h2Frame{Type: frameSettings, Flags: flagAck, Payload: make([]byte, 6)}},
		{"PING of the wrong length", h2Frame{Type: framePing, Payload: make([]byte, 4)}},
		{"GOAWAY too short", h2Frame{Type: frameGoAway, Payload: make([]byte, 4)}},
		{"WINDOW_UPDATE of the wrong length", h2Frame{Type: frameWindowUpdate, Payload: make([]byte, 8)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr, _ := serveTest(t, Config{}, echoTarget)
			c := dialH2(t, addr)
			c.frame(tt.frame)
			c.flush()
			if code := goAwayCode(c.waitGoAway()); code != 0x6 {
				t.Errorf("GOAWAY carried code %#x, want FRAME_SIZE_ERROR (0x6)", code)
			}
		})
	}

	t.Run("a frame over the advertised maximum", func(t *testing.T) {
		addr, _ := serveTest(t, Config{}, echoTarget)
		c := dialH2(t, addr)
		c.flush()
		// Written raw: AppendFrame will not frame a length the server must
		// refuse. Nine header bytes declaring 16385 are enough — the refusal
		// reads the length, never the payload.
		hdr := []byte{0x00, 0x40, 0x01, framePing, 0, 0, 0, 0, 0}
		if _, err := c.conn.Write(hdr); err != nil {
			t.Fatal(err)
		}
		if code := goAwayCode(c.waitGoAway()); code != 0x6 {
			t.Errorf("GOAWAY carried code %#x, want FRAME_SIZE_ERROR (0x6)", code)
		}
	})
}

// DATA on an even-numbered stream is a connection error: even identifiers
// are the server's to open (§5.1.1) and this server opens none, so such a
// frame is not a straggler for a forgotten stream — tolerating it let a peer
// have traffic window-accounted on streams that never carried a request.
func TestH2DataOnAnEvenStreamIsAConnectionError(t *testing.T) {
	addr, _ := serveTest(t, Config{}, echoTarget)
	c := dialH2(t, addr)

	// Advance lastID past the even id, so the refusal is about parity rather
	// than about an identifier above everything seen.
	c.request(5, "GET", "/first")
	c.flush()
	if got := c.bodies(1)[5]; got != "/first" {
		t.Fatalf("request on stream 5 got %q", got)
	}

	c.frame(h2Frame{Type: frameData, StreamID: 2, Payload: []byte("x")})
	c.flush()
	if code := goAwayCode(c.waitGoAway()); code != 0x1 {
		t.Errorf("GOAWAY carried code %#x, want PROTOCOL_ERROR (0x1)", code)
	}
}

// §6.10 defines no PADDED flag on CONTINUATION, and §4.1 says an undefined
// flag is ignored. Ignored has to mean "the bit changes nothing": the whole
// payload is header block fragment, and no pad length is carved out of it.
// A server that honoured the bit would hand its HPACK decoder a different
// byte stream than a conformant intermediary saw — a dynamic-table desync.
func TestH2ContinuationPaddedFlagDoesNotCarveUpTheBlock(t *testing.T) {
	addr, _ := serveTest(t, Config{}, echoTarget)
	c := dialH2(t, addr)

	block := headBlock("GET", "/padded", [2]string{":authority", "h"})
	split := len(block) / 2
	c.frame(h2Frame{
		Type: frameHeaders, Flags: flagEndStream,
		StreamID: 1, Payload: block[:split],
	})
	c.frame(h2Frame{
		Type: frameContinuation, Flags: flagEndHeaders | flagPadded,
		StreamID: 1, Payload: block[split:],
	})
	c.flush()

	// Served normally: had the first payload byte been read as a pad length,
	// the block would have misdecoded and the request never answered.
	if got := c.bodies(1)[1]; got != "/padded" {
		t.Errorf("the padded-flag CONTINUATION got %q, want %q", got, "/padded")
	}
}

// H2Config.MaxHeaders bounds how many fields one block may decode to, the
// same bound the HTTP/1.1 parser applies — the byte bound alone admits
// hundreds of near-empty fields that cost a scan on every Request.Get. The
// refusal costs the one stream, not the connection.
func TestH2RefusesABlockOfTooManyFields(t *testing.T) {
	addr, _ := serveTest(t, Config{MaxHeaders: 8}, echoTarget)
	c := dialH2(t, addr)

	extra := make([][2]string, 10)
	for i := range extra {
		extra[i] = [2]string{fmt.Sprintf("x-f%d", i), "v"}
	}
	c.request(1, "GET", "/crowded", extra...)
	c.flush()
	if code := c.waitReset(1); code != 0xb {
		t.Errorf("RST_STREAM carried code %#x, want ENHANCE_YOUR_CALM (0xb)", code)
	}

	// The connection survives, and a modest request is served.
	c.request(3, "GET", "/modest")
	c.flush()
	if got := c.bodies(1)[3]; got != "/modest" {
		t.Errorf("after the reset the request got %q, want %q", got, "/modest")
	}
}

// A connection that has served a request is still an idle connection once the
// answer is out. The idle clock used to be disarmed while an answer was owed
// and never re-armed when it was paid, so every keep-alive connection that
// served one request escaped IdleTimeout for good. And an idle close is the
// graceful one: GOAWAY(NO_ERROR), not the INTERNAL_ERROR a failure carries.
func TestH2IdleTimeoutRunsAgainAfterAnAnsweredRequest(t *testing.T) {
	addr, _ := serveH2(t, H2Config{IdleTimeout: 200 * time.Millisecond}, echoTarget)
	c := dialH2(t, addr)

	c.request(1, "GET", "/once")
	c.flush()
	if got := c.bodies(1)[1]; got != "/once" {
		t.Fatalf("stream 1 answered %q", got)
	}

	start := time.Now()
	payload := c.waitGoAway()
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("the idle connection was ended after %v, want within 2s", elapsed)
	}
	if code := goAwayCode(payload); code != 0 {
		t.Errorf("an idle close carried GOAWAY code %#x, want NO_ERROR", code)
	}
	// And the socket is closed behind it, not merely announced as closing.
	if err := c.conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var one [1]byte
	for {
		if _, err := c.conn.Read(one[:]); err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				t.Fatal("the server announced GOAWAY but kept the connection open")
			}
			break
		}
	}
}

// An answered request's body belongs to the handler, and once the handler is
// done nothing should hold it. The stream state outlives the request in the
// closed-stream ring, and used to keep a copy of it there — body and all — so
// a live connection held MaxConcurrentStreams × MaxBodyBytes for nothing.
func TestH2AnsweredBodiesAreNotRetained(t *testing.T) {
	addr, _ := serveH2(t, H2Config{}, func(b sluice.Batch[Request]) sluice.Batch[Response] {
		out := make([]Response, b.Len())
		for i := range out {
			out[i] = Response{Status: 204}
		}
		return sluice.Batch[Response]{Items: out}
	})
	c := dialH2(t, addr)
	body := bytes.Repeat([]byte("x"), 60000) // under the 65535 initial stream window

	post := func(id uint32) {
		c.frame(h2Frame{
			Type:     frameHeaders,
			Flags:    flagEndHeaders,
			StreamID: id,
			Payload:  headBlock("POST", "/upload", [2]string{":authority", "h"}),
		})
		c.data(id, body, true)
		c.flush()
		c.pump(func() bool { return c.ended[id] || c.sawGoAway })
		if !c.ended[id] {
			t.Fatalf("stream %d was not answered", id)
		}
		// The client's own bookkeeping is not what is measured.
		delete(c.got, id)
		delete(c.heads, id)
	}

	post(1) // warms the connection's buffers before the baseline
	heap := func() uint64 {
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return m.HeapAlloc
	}
	before := heap()
	const n = 100
	for i := range uint32(n) {
		post(3 + 2*i)
	}
	after := heap()
	if c.sawGoAway {
		t.Fatal("the connection ended mid-test")
	}

	growth := int64(after) - int64(before)
	t.Logf("heap grew %d bytes over %d answered %d-byte POSTs", growth, n, len(body))
	if growth > 2<<20 {
		t.Errorf("the heap grew %d bytes over %d answered uploads (%d bytes of bodies): answered bodies are retained",
			growth, n, n*len(body))
	}
}

// A streamed response's producer runs after the handler has returned, on the
// responder goroutine, out of reach of the handler's own recover. A panic
// there used to end the process; it costs its own stream, and the connection
// keeps serving.
func TestH2StreamProducerPanicResetsOneStream(t *testing.T) {
	addr, _ := serveH2(t, H2Config{}, panickingProducer)
	c := dialH2(t, addr)

	c.request(1, "GET", "/boom")
	c.flush()
	if code := c.waitReset(1); code != 0x2 {
		t.Errorf("the panicking stream was reset with %#x, want INTERNAL_ERROR", code)
	}
	if c.sawGoAway {
		t.Fatal("a producer panic ended the connection")
	}

	c.request(3, "GET", "/ok")
	c.flush()
	c.pump(func() bool { return c.ended[3] || c.sawGoAway })
	if got := c.got[3]; got != "/ok" {
		t.Errorf("after the panic the connection answered %q, want %q", got, "/ok")
	}
}

// panickingProducer answers /boom with a stream that yields one chunk and
// then panics, and anything else with its own target.
func panickingProducer(b sluice.Batch[Request]) sluice.Batch[Response] {
	out := make([]Response, 0, b.Len())
	for _, r := range b.Items {
		if string(r.Target) != "/boom" {
			out = append(out, Response{Status: 200, Body: append([]byte(nil), r.Target...)})
			continue
		}
		out = append(out, Response{Status: 200, Stream: func(yield func(sluice.Batch[[]byte]) bool) {
			if !yield(sluice.Batch[[]byte]{Items: [][]byte{[]byte("first")}}) {
				return
			}
			panic("producer bug")
		}})
	}
	return sluice.Batch[Response]{Items: out}
}

// A peer may advertise SETTINGS_MAX_FRAME_SIZE up to 16 MiB, and that is a
// ceiling it accepts rather than a size this end must use. Frames leave at
// this end's own MaxFrameSize at most: before the cap, a 4 MiB body under
// open windows left as one 4 MiB frame, and the writer's round buffer — kept
// for the connection's life — grew to match.
func TestH2FramesLeaveAtOurMaxFrameSizeNotThePeers(t *testing.T) {
	const bodyLen = 4 << 20
	addr, _ := serveH2(t, H2Config{}, func(b sluice.Batch[Request]) sluice.Batch[Response] {
		res := make([]Response, b.Len())
		for i := range res {
			res[i] = Response{Status: 200, Body: bytes.Repeat([]byte("x"), bodyLen)}
		}
		return sluice.Batch[Response]{Items: res}
	})
	c := dialH2(t, addr)
	defer func() { _ = c.conn.Close() }()
	const settingMaxFrameSize = 0x5
	c.frame(h2Frame{Type: frameSettings, Payload: settingsPayload(
		[2]uint32{settingMaxFrameSize, 1<<24 - 1},
		[2]uint32{settingInitialWindowSize, 1<<31 - 1},
	)})
	c.frame(h2Frame{Type: frameWindowUpdate, Payload: windowUpdatePayload(1<<31 - 1 - initialWindow)})
	c.request(1, "GET", "/large")
	c.flush()

	got := c.bodies(1)
	if len(got[1]) != bodyLen {
		t.Fatalf("the body arrived with %d bytes, want %d (goaway %v)", len(got[1]), bodyLen, c.sawGoAway)
	}
	if c.maxData > 16384 {
		t.Errorf("a DATA frame carried %d bytes: frames follow the peer's 16 MiB, not this end's 16384", c.maxData)
	}
}

// Response heads are framed on the responder and written later, so the
// frame size they are cut at must hold whatever the peer's
// SETTINGS_MAX_FRAME_SIZE becomes in between. Only the protocol floor does:
// framed at a raised value snapshotted per batch, a peer that lowered it again
// before the write received a HEADERS over its limit.
func TestH2HeaderBlocksAreFramedAtTheProtocolFloor(t *testing.T) {
	big := bytes.Repeat([]byte("v"), 20000)
	addr, _ := serveH2(t, H2Config{MaxFrameSize: 1 << 20}, func(b sluice.Batch[Request]) sluice.Batch[Response] {
		res := make([]Response, b.Len())
		for i := range res {
			res[i] = Response{Status: 200, Headers: []Header{{Name: []byte("x-big"), Value: big}}, Body: []byte("ok")}
		}
		return sluice.Batch[Response]{Items: res}
	})
	c := dialH2(t, addr)
	defer func() { _ = c.conn.Close() }()
	const settingMaxFrameSize = 0x5
	c.frame(h2Frame{Type: frameSettings, Payload: settingsPayload([2]uint32{settingMaxFrameSize, 1 << 20})})
	c.request(1, "GET", "/")
	c.flush()

	if got := c.bodies(1)[1]; got != "ok" {
		t.Fatalf("the response body is %q (goaway %v)", got, c.sawGoAway)
	}
	if n := len(c.heads[1]); n > 16384 {
		t.Errorf("the HEADERS frame carried %d bytes, over the 16384 floor", n)
	}
}

// A peer that lowers SETTINGS_HEADER_TABLE_SIZE is owed a dynamic table size
// update at the start of the next header block (RFC 7541 §4.2, RFC 9113
// §4.3.1) — the smallest size seen since the last block, then the final one.
// The encoder never indexes, so nothing it says depends on the table; but a
// decoder that saw the setting shrink expects the update, and nghttp2 ends
// the connection with COMPRESSION_ERROR when it is missing.
func TestH2HeaderTableSizeChangeOpensTheNextBlock(t *testing.T) {
	addr, _ := serveH2(t, H2Config{}, func(b sluice.Batch[Request]) sluice.Batch[Response] {
		res := make([]Response, b.Len())
		for i := range res {
			res[i] = Response{Status: 200, Body: []byte("ok")}
		}
		return sluice.Batch[Response]{Items: res}
	})
	c := dialH2(t, addr)
	defer func() { _ = c.conn.Close() }()
	const settingHeaderTableSize = 0x1
	tableSize := func(v uint32) {
		c.frame(h2Frame{Type: frameSettings, Payload: settingsPayload([2]uint32{settingHeaderTableSize, v})})
	}
	// decodes checks a block against this package's own decoder, which is
	// the peer's half of the contract played by a conformant decoder.
	decodes := func(id uint32) {
		t.Helper()
		fields, err := newHPACKDecoder(4096, 0).Decode(nil, c.heads[id])
		if err != nil {
			t.Fatalf("stream %d: the block does not decode: %v", id, err)
		}
		if len(fields) == 0 || string(fields[0].Name) != ":status" || string(fields[0].Value) != "200" {
			t.Errorf("stream %d: decoded %q", id, fields)
		}
	}

	// Shrunk to zero: the next block opens with an update to zero.
	tableSize(0)
	c.request(1, "GET", "/")
	c.flush()
	c.bodies(1)
	if want := hpackTableSizeUpdate(0); !bytes.HasPrefix(c.heads[1], want) {
		t.Errorf("after a shrink to 0 the block opens % x, want % x", c.heads[1][:min(len(c.heads[1]), 4)], want)
	}
	decodes(1)

	// Owed once: the block after carries no update.
	c.request(3, "GET", "/")
	c.flush()
	c.bodies(2)
	if len(c.heads[3]) == 0 || c.heads[3][0]&0xe0 == 0x20 {
		t.Errorf("a block with no change since the last one opens % x", c.heads[3][:min(len(c.heads[3]), 4)])
	}
	decodes(3)

	// Several changes between two blocks: the smallest, then the final one.
	tableSize(4096)
	tableSize(1000)
	tableSize(2000)
	c.request(5, "GET", "/")
	c.flush()
	c.bodies(3)
	want := append(hpackTableSizeUpdate(1000), hpackTableSizeUpdate(2000)...)
	if !bytes.HasPrefix(c.heads[5], want) {
		t.Errorf("after 4096, 1000, 2000 the block opens % x, want % x", c.heads[5][:min(len(c.heads[5]), len(want))], want)
	}
	decodes(5)
	if c.sawGoAway {
		t.Errorf("the connection ended: GOAWAY %x", c.goAway)
	}
}

// A peer that grants no window for a response's payload has its stream reset
// once WriteTimeout passes with nothing written: CANCEL, since this end is the
// one that stopped waiting, and on that stream alone.
func TestH2StalledResponseIsResetAfterWriteTimeout(t *testing.T) {
	addr, _ := serveH2(t, H2Config{WriteTimeout: 300 * time.Millisecond},
		func(b sluice.Batch[Request]) sluice.Batch[Response] {
			res := make([]Response, b.Len())
			for i, r := range b.Items {
				res[i] = Response{Status: 200}
				if string(r.Target) == "/stalled" {
					res[i].Body = []byte("a payload the window never admits")
				}
			}
			return sluice.Batch[Response]{Items: res}
		})
	c := dialH2(t, addr)
	defer func() { _ = c.conn.Close() }()

	c.frame(h2Frame{Type: frameSettings, Payload: settingsPayload([2]uint32{settingInitialWindowSize, 0})})
	c.request(1, "GET", "/stalled")
	c.flush()

	if code := c.waitReset(1); code != 0x8 {
		t.Errorf("the stalled stream was reset with %#x, want CANCEL (0x8)", code)
	}
	if len(c.got[1]) != 0 {
		t.Errorf("%d payload bytes went out against a window of 0", len(c.got[1]))
	}

	// The connection outlives the stream: a bodiless answer needs no window.
	c.request(3, "GET", "/after")
	c.flush()
	c.pump(func() bool { return c.ended[3] || c.sawGoAway })
	if !c.ended[3] || c.sawGoAway {
		t.Errorf("after the reset the next request ended=%v, goaway=%v", c.ended[3], c.sawGoAway)
	}
}

// A producer of cadence ticks on a stream the peer resets learns it on its
// next batch: an empty batch is never handed to the writer, so the writer's
// refusal of a reset stream could not reach it.
func TestH2StreamedEmptyBatchesStopOnReset(t *testing.T) {
	started, stopped, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	addr, _ := serveH2(t, H2Config{}, func(b sluice.Batch[Request]) sluice.Batch[Response] {
		return sluice.Batch[Response]{Items: []Response{{
			Status: 200, Stream: cadenceProducer(started, stopped, release),
		}}}
	})
	t.Cleanup(func() { close(release) }) // runs before serveH2 waits

	c := dialH2(t, addr)
	c.request(1, "GET", "/feed")
	c.flush()
	c.pump(func() bool { _, seen := c.heads[1]; return seen })
	<-started
	c.frame(h2Frame{Type: frameRSTStream, StreamID: 1, Payload: []byte{0, 0, 0, 0x8}}) // CANCEL
	c.flush()

	waitStopped(t, stopped)
}

// The same producer on a connection the peer closes: the reader's end is
// what tells it, since nothing is ever written that could fail.
func TestH2StreamedEmptyBatchesStopWhenTheConnectionCloses(t *testing.T) {
	started, stopped, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	addr, _ := serveH2(t, H2Config{}, func(b sluice.Batch[Request]) sluice.Batch[Response] {
		return sluice.Batch[Response]{Items: []Response{{
			Status: 200, Stream: cadenceProducer(started, stopped, release),
		}}}
	})
	t.Cleanup(func() { close(release) })

	c := dialH2(t, addr)
	c.request(1, "GET", "/feed")
	c.flush()
	c.pump(func() bool { _, seen := c.heads[1]; return seen })
	<-started
	_ = c.conn.Close()

	waitStopped(t, stopped)
}

// Config.MaxConcurrentStreams reaches an HTTP/2 connection [Serve] accepts,
// not only HTTP/3 ones: it used to be dropped on the way to ServeH2, so the
// h2 side always advertised its own default whatever the caller set. Zero
// still means HTTP/2's own default of 100.
func TestServeAppliesMaxConcurrentStreamsToHTTP2(t *testing.T) {
	for _, tc := range []struct{ set, want uint32 }{{3, 3}, {0, 100}} {
		addr, _ := serveTest(t, Config{MaxConcurrentStreams: int(tc.set)}, echoTarget)
		c := dialH2(t, addr)
		c.flush()
		const settingMaxConcurrentStreams = 0x3
		c.pump(func() bool { _, ok := c.settings[settingMaxConcurrentStreams]; return ok })
		if got := c.settings[settingMaxConcurrentStreams]; got != tc.want {
			t.Errorf("Config.MaxConcurrentStreams %d: SETTINGS_MAX_CONCURRENT_STREAMS %d, want %d", tc.set, got, tc.want)
		}
	}
}

// RFC 9113 §6.5.2: SETTINGS_MAX_HEADER_LIST_SIZE is advisory, but a response
// over the peer's stated limit is one it will likely refuse. The server
// answers 500 instead, as it does any response it will not put on the wire,
// and a response under the limit is unaffected.
func TestH2HonoursThePeersMaxHeaderListSize(t *testing.T) {
	addr, _ := serveTest(t, Config{}, func(b sluice.Batch[Request]) sluice.Batch[Response] {
		out := make([]Response, 0, b.Len())
		for _, r := range b.Items {
			res := Response{Status: 200}
			if string(r.Target) == "/big" {
				res.Headers = []Header{{Name: []byte("x-big"), Value: bytes.Repeat([]byte("v"), 300)}}
			}
			out = append(out, res)
		}
		return sluice.Batch[Response]{Items: out}
	})
	c := dialH2(t, addr)
	const settingMaxHeaderListSize = 0x6
	c.frame(h2Frame{Type: frameSettings, Payload: settingsPayload([2]uint32{settingMaxHeaderListSize, 200})})
	c.request(1, "GET", "/big")
	c.request(3, "GET", "/small")
	c.flush()

	c.bodies(2)
	if got := h2Status(c.heads[1]); got != 500 {
		t.Errorf("the over-limit response was answered %d, want 500", got)
	}
	if bytes.Contains(c.heads[1], []byte("x-big")) {
		t.Error("the over-limit field was written to the wire")
	}
	if got := h2Status(c.heads[3]); got != 200 {
		t.Errorf("the response under the limit was answered %d, want 200", got)
	}
	if c.sawGoAway {
		t.Error("an over-limit response ended the connection")
	}
}
