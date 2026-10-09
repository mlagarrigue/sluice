package httpstream

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
)

func frame(typ, flags byte, stream uint32, payload []byte) []byte {
	out, err := appendH2Frame(nil, h2Frame{Type: typ, Flags: flags, StreamID: stream, Payload: payload})
	if err != nil {
		panic(err)
	}
	return out
}

// memConn hands a fixed script to the reader and records what came back, with
// no socket in the way.
type memConn struct {
	in  []byte
	pos int
	out []byte
}

func (c *memConn) Read(p []byte) (int, error) {
	if c.pos >= len(c.in) {
		return 0, io.EOF
	}
	n := copy(p, c.in[c.pos:])
	c.pos += n
	return n, nil
}

func (c *memConn) Write(p []byte) (int, error)      { c.out = append(c.out, p...); return len(p), nil }
func (c *memConn) Close() error                     { return nil }
func (c *memConn) LocalAddr() net.Addr              { return nil }
func (c *memConn) RemoteAddr() net.Addr             { return nil }
func (c *memConn) SetDeadline(time.Time) error      { return nil }
func (c *memConn) SetReadDeadline(time.Time) error  { return nil }
func (c *memConn) SetWriteDeadline(time.Time) error { return nil }

func readFrames(t *testing.T, script []byte, cfg H2Config) ([][]h2Frame, error) {
	t.Helper()
	c := &memConn{in: script}
	src := h2Frames(c, cfg)
	var batches [][]h2Frame
	src.Stream()(func(b sluice.Batch[h2Frame]) bool {
		batch := make([]h2Frame, 0, b.Len())
		for _, f := range b.Items {
			f.Payload = append([]byte(nil), f.Payload...) // the batch contract
			batch = append(batch, f)
		}
		batches = append(batches, batch)
		return true
	})
	err := src.Err()
	if errors.Is(err, io.EOF) {
		err = nil
	}
	return batches, err
}

// The point of the layer, and the thing HTTP/1.1 could only manage with a
// client that pipelines: HTTP/2 multiplexes by design, so one read carries
// frames for several streams and the batch is the ordinary case.
func TestFramesFromSeveralStreamsArriveAsOneBatch(t *testing.T) {
	var script []byte
	script = append(script, Preface...)
	script = append(script, frame(frameSettings, 0, 0, nil)...)
	for _, id := range []uint32{1, 3, 5, 7} {
		script = append(script, frame(frameHeaders, flagEndHeaders|flagEndStream, id, []byte{0x82})...)
	}

	batches, err := readFrames(t, script, H2Config{})
	if err != nil {
		t.Fatalf("Err = %v", err)
	}
	if len(batches) == 0 {
		t.Fatal("no batch was delivered")
	}

	streams := map[uint32]bool{}
	for _, f := range batches[0] {
		if f.Type == frameHeaders {
			streams[f.StreamID] = true
		}
	}
	if len(streams) < 4 {
		t.Errorf("the first batch carried %d streams, want 4: %v", len(streams), batches[0])
	}
	t.Logf("first batch: %d frames across %d streams", len(batches[0]), len(streams))
}

func TestFramesRefusesABadPreface(t *testing.T) {
	script := append([]byte("GET / HTTP/1.1\r\nHost: h\r\n\r\n"), 0, 0, 0)
	if _, err := readFrames(t, script, H2Config{}); !errors.Is(err, ErrH2Protocol) {
		t.Fatalf("err = %v, want ErrH2Protocol", err)
	}
}

// The frame layer's refusals. Each is a frame some peer may send and none of
// them has a meaning this package could act on.
func TestFrameShapeRefusals(t *testing.T) {
	tests := []struct {
		name  string
		frame []byte
	}{
		{"SETTINGS on a request stream", frame(frameSettings, 0, 1, nil)},
		{"SETTINGS of a length that is not a multiple of 6", frame(frameSettings, 0, 0, make([]byte, 7))},
		{"a SETTINGS acknowledgement with a payload", frame(frameSettings, flagAck, 0, make([]byte, 6))},
		{"HEADERS on the connection stream", frame(frameHeaders, flagEndHeaders, 0, []byte{0x82})},
		{"DATA on the connection stream", frame(frameData, 0, 0, []byte("x"))},
		{"PING of the wrong length", frame(framePing, 0, 0, make([]byte, 4))},
		{"PING on a request stream", frame(framePing, 0, 1, make([]byte, 8))},
		{"RST_STREAM of the wrong length", frame(frameRSTStream, 0, 1, make([]byte, 3))},
		// PRIORITY of the wrong length is deliberately absent: RFC 9113 §6.3
		// makes it a stream error, so the server resets the one stream — see
		// TestH2PriorityOfTheWrongLengthResetsOnlyItsStream — where everything
		// here ends the connection.
		{"GOAWAY too short", frame(frameGoAway, 0, 0, make([]byte, 4))},
		{"WINDOW_UPDATE of the wrong length", frame(frameWindowUpdate, 0, 0, make([]byte, 8))},
		{"a client that promises a push", frame(framePushPromise, flagEndHeaders, 1, make([]byte, 4))},
		{"CONTINUATION with no open header block", frame(frameContinuation, flagEndHeaders, 1, nil)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			script := append([]byte(Preface), tt.frame...)
			if _, err := readFrames(t, script, H2Config{}); !errors.Is(err, ErrH2Protocol) {
				t.Fatalf("err = %v, want ErrH2Protocol", err)
			}
		})
	}
}

// A frame declaring more than the negotiated maximum is refused on its nine
// header bytes, before the payload it claims is waited for.
func TestFrameLengthIsCheckedBeforeThePayload(t *testing.T) {
	var hdr [frameHeaderSize]byte
	hdr[0], hdr[1], hdr[2] = 0xff, 0xff, 0xff // 16 MiB - 1
	hdr[3] = frameData
	binary.BigEndian.PutUint32(hdr[5:], 1)

	script := append([]byte(Preface), hdr[:]...)
	// No payload follows: if the length were believed, the reader would wait
	// for sixteen megabytes that never come.
	if _, err := readFrames(t, script, H2Config{}); !errors.Is(err, ErrH2Protocol) {
		t.Fatalf("err = %v, want ErrH2Protocol", err)
	}
}

// The CONTINUATION flood: a peer may legally split a header block, and a
// server that does not bound how far can be made to read forever without a
// request ever completing.
func TestContinuationFloodIsBounded(t *testing.T) {
	script := append([]byte(Preface), frame(frameHeaders, 0, 1, []byte{0x82})...) // no END_HEADERS
	for range 64 {
		script = append(script, frame(frameContinuation, 0, 1, make([]byte, 16))...)
	}
	_, err := readFrames(t, script, H2Config{MaxContinuationFrames: 8})
	if !errors.Is(err, ErrH2Protocol) {
		t.Fatalf("err = %v, want ErrH2Protocol", err)
	}
}

// A header block is one stream's, and a frame for another stream in the
// middle of it is refused rather than reordered.
func TestHeaderBlockCannotBeInterleaved(t *testing.T) {
	script := append([]byte(Preface), frame(frameHeaders, 0, 1, []byte{0x82})...)
	script = append(script, frame(frameHeaders, flagEndHeaders, 3, []byte{0x82})...)
	if _, err := readFrames(t, script, H2Config{}); !errors.Is(err, ErrH2Protocol) {
		t.Fatalf("err = %v, want ErrH2Protocol", err)
	}
}

// A block split across CONTINUATION frames, within the bounds, is accepted.
func TestSplitHeaderBlockIsAccepted(t *testing.T) {
	script := append([]byte(Preface), frame(frameHeaders, 0, 1, []byte{0x82})...)
	script = append(script, frame(frameContinuation, 0, 1, []byte{0x84})...)
	script = append(script, frame(frameContinuation, flagEndHeaders, 1, []byte{0x86})...)
	script = append(script, frame(frameData, flagEndStream, 1, []byte("hello"))...)

	batches, err := readFrames(t, script, H2Config{})
	if err != nil {
		t.Fatalf("Err = %v", err)
	}
	var types []byte
	for _, b := range batches {
		for _, f := range b {
			types = append(types, f.Type)
		}
	}
	want := []byte{frameHeaders, frameContinuation, frameContinuation, frameData}
	if string(types) != string(want) {
		t.Errorf("frame types = %v, want %v", types, want)
	}
}

// Round trip: what AppendFrame writes, parseFrame reads back.
func TestFrameRoundTrip(t *testing.T) {
	in := h2Frame{Type: frameData, Flags: flagEndStream, StreamID: 1 << 30, Payload: []byte("payload")}
	raw, err := appendH2Frame(nil, in)
	if err != nil {
		t.Fatal(err)
	}
	out, n, err := parseFrame(raw, H2Config{}.withDefaults())
	if err != nil {
		t.Fatalf("parseFrame returned %v", err)
	}
	if n != len(raw) {
		t.Errorf("consumed %d of %d bytes", n, len(raw))
	}
	if out.Type != in.Type || out.Flags != in.Flags || out.StreamID != in.StreamID || string(out.Payload) != string(in.Payload) {
		t.Errorf("round trip = %+v, want %+v", out, in)
	}
	if !out.EndsStream() {
		t.Error("END_STREAM did not survive")
	}
}

// Framing hands out views into the read buffer, like the HTTP/1.1 side.
func TestFrameParsingAllocatesNothing(t *testing.T) {
	raw := frame(frameData, flagEndStream, 1, []byte("payload"))
	cfg := H2Config{}.withDefaults()
	var sink h2Frame
	allocs := testing.AllocsPerRun(200, func() {
		sink, _, _ = parseFrame(raw, cfg)
	})
	if allocs != 0 {
		t.Errorf("parsing a frame allocated %.1f times", allocs)
	}
	if len(sink.Payload) != 7 {
		t.Errorf("payload = %q", sink.Payload)
	}
}
