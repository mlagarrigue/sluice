package httpstream

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/internal/dgram"
	"github.com/mlagarrigue/sluice/internal/varint"
	"github.com/mlagarrigue/sluice/net/quic"
)

// A maxFrame of zero bounds nothing.
func TestParseH3FramesWithNoBound(t *testing.T) {
	frames, _, _, err := parseH3Frames(nil, appendH3Frame(nil, h3Data, make([]byte, 10)), 0)
	if err != nil || len(frames) != 1 {
		t.Fatalf("%d frames, %v; want the DATA frame", len(frames), err)
	}
}

func TestQPACKStaticIndexPastTheTable(t *testing.T) {
	d := newQPACKDecoder(nil, 0)
	if _, err := d.staticAt(uint64(len(d.static))); err == nil {
		t.Error("an index one past the static table resolved")
	}
	if _, err := d.staticAt(uint64(len(d.static) - 1)); err != nil {
		t.Errorf("the last static index: %v", err)
	}
}

// A section of exactly the list bound decodes, a Huffman value included;
// one byte under it does not.
func TestQPACKListBoundIsExactWithAHuffmanValue(t *testing.T) {
	block := []byte{0x00, 0x00}
	block = appendHPACKInt(block, 3, 3, 0x20)
	block = append(block, "x-a"...)
	value := huffmanEncode("hello world")
	block = appendHPACKInt(block, uint64(len(value)), 7, 0x80)
	block = append(block, value...)
	const size = 3 + 11 + 32
	fields, err := newQPACKDecoder(nil, size).Decode(nil, block)
	if err != nil || len(fields) != 1 || string(fields[0].Value) != "hello world" {
		t.Fatalf("at the bound: %q, %v", fields, err)
	}
	if _, err := newQPACKDecoder(nil, size-1).Decode(nil, block); !errors.Is(err, errHeadersTooLarge) {
		t.Errorf("one byte under: %v, want errHeadersTooLarge", err)
	}
}

func TestH3FieldSectionAtThePeersLimit(t *testing.T) {
	fields := []Header{{Name: []byte("a"), Value: []byte("bb")}}
	if err := checkH3FieldSection(fields, 35); err != nil {
		t.Errorf("at the limit: %v", err)
	}
	if err := checkH3FieldSection(fields, 34); err == nil {
		t.Error("one byte over the limit: accepted")
	}
}

func TestH3KnownCodeAtTheRangesEnds(t *testing.T) {
	for code, want := range map[uint64]bool{
		h3NoError - 1: false, h3NoError: true, h3VersionFallback: true, h3VersionFallback + 1: false,
		qpackDecompressionFailed - 1: false, qpackDecompressionFailed: true,
		qpackDecoderStreamError: true, qpackDecoderStreamError + 1: false,
	} {
		if got := h3KnownCode(code); got != want {
			t.Errorf("h3KnownCode(%#x) = %v, want %v", code, got, want)
		}
	}
}

// reserved reports a value of RFC 9114's reserved 0x1f*N+0x21 space.
func reserved(v uint64) bool { return v >= 0x21 && (v-0x21)%0x1f == 0 }

// The grease varies, stays reserved, and is the exception: in the graceful
// close one time in eight, and as the one extra setting of every SETTINGS.
func TestH3GreaseVaries(t *testing.T) {
	seen := map[uint64]bool{}
	greased := 0
	for range 1024 {
		if code := h3GracefulCode(); code != h3NoError {
			greased++
			seen[code] = true
		}
	}
	if greased == 0 || greased > 512 || len(seen) < 10 {
		t.Errorf("%d of 1024 closes greased, with %d codes", greased, len(seen))
	}

	ids := map[uint64]bool{}
	for range 64 {
		out := appendH3Control(nil, 100)
		_, rest, _ := varint.Read(out)     // the stream type
		_, rest, _ = varint.Read(rest)     // SETTINGS
		_, payload, _ := varint.Read(rest) // its length
		for len(payload) > 0 {
			var id uint64
			id, payload, _ = varint.Read(payload)
			_, payload, _ = varint.Read(payload)
			if id > 0x21 || id == 0x21 {
				if !reserved(id) {
					t.Fatalf("setting %#x is neither defined nor reserved", id)
				}
				ids[id] = true
			}
		}
	}
	if len(ids) < 10 {
		t.Errorf("64 SETTINGS greased with %d identifiers", len(ids))
	}
}

// When bodies stream, the stream window offered is the room for a head
// when that is larger than the transport's own.
func TestQUICConfigStreamedRoomsAHead(t *testing.T) {
	headers := int(quic.DefaultConfig().InitialMaxStreamDataBidiRemote) //nolint:gosec // G115: a window
	p := mustQUICConfig(Config{MaxHeaderBytes: headers})
	if want := uint64(headers) + h3FrameSlack; p.InitialMaxStreamDataBidiRemote != want { //nolint:gosec // G115: positive
		t.Errorf("InitialMaxStreamDataBidiRemote = %d, want %d", p.InitialMaxStreamDataBidiRemote, want)
	}
}

// serverConnWith is the server side of a fresh connection over an
// in-memory pair, with the transport parameters given.
func serverConnWith(t *testing.T, params quic.Config) *quic.Conn {
	t.Helper()
	serverPC, clientPC := dgram.Pair()
	t.Cleanup(func() { _ = serverPC.Close(); _ = clientPC.Close() })
	accepted := make(chan *quic.Conn, 1)
	go func() {
		conn, err := acceptOne(t, serverPC, h3ServerTLS(t), params)
		if err != nil {
			t.Error(err)
		}
		accepted <- conn
	}()
	client, err := quic.Dial(context.Background(), clientPC, serverPC.LocalAddr(), h3ClientTLS(), quic.DefaultConfig())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	conn := <-accepted
	if conn == nil {
		t.FailNow()
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// Manual credit needs windows that hold a request whole — or, when bodies
// stream, its head — and windows of exactly that are enough.
func TestH3CreditAtTheWindowsEdges(t *testing.T) {
	whole := Config{BodyDelivery: BodyWhole, MaxBodyBytes: 1 << 16}.mustResolve()
	streamed := Config{MaxBodyBytes: 1 << 16}.mustResolve()
	wholeStream := uint64(whole.MaxBodyBytes+whole.MaxHeaderBytes) + h3FrameSlack //nolint:gosec // G115: positive
	wholeConn := uint64(4*whole.MaxBodyBytes+whole.MaxHeaderBytes) + h3FrameSlack //nolint:gosec // G115: positive
	head := uint64(streamed.MaxHeaderBytes) + h3FrameSlack                        //nolint:gosec // G115: positive
	for _, tt := range []struct {
		name         string
		cfg          Config
		stream, data uint64
		want         quic.Credit
	}{
		{"whole, at the windows", whole, wholeStream, wholeConn, quic.CreditOnRelease},
		{"whole, a stream window short", whole, wholeStream - 1, wholeConn, quic.CreditOnDelivery},
		{"whole, a connection window short", whole, wholeStream, wholeConn - 1, quic.CreditOnDelivery},
		{"streamed, at the windows", streamed, head, head, quic.CreditOnRelease},
		{"streamed, a stream window short", streamed, head - 1, head, quic.CreditOnDelivery},
	} {
		params := quic.DefaultConfig()
		params.InitialMaxStreamDataBidiRemote, params.InitialMaxData = tt.stream, tt.data
		c := serverConnWith(t, params)
		if got := h3Credit(c, tt.cfg, newH3Assembler(tt.cfg)); got != tt.want {
			t.Errorf("%s: credit %v, want %v", tt.name, got, tt.want)
		}
	}
}

// Transport parameters at each of the bounds ServeH3Conn holds them to are
// served, not refused.
func TestServeH3AcceptsTransportParametersAtTheBounds(t *testing.T) {
	for _, tt := range []struct {
		name   string
		mutate func(*quic.Config)
		cfg    Config
	}{
		{"the stream limit at its cap", func(p *quic.Config) { p.InitialMaxStreamsBidi = h3MaxServeStreams }, Config{}},
		{"the concurrency at its cap", func(*quic.Config) {}, Config{MaxConcurrentStreams: h3MaxServeStreams}},
		{"three unidirectional streams", func(p *quic.Config) { p.InitialMaxStreamsUni = 3 }, Config{}},
		{"every unidirectional stream read", func(p *quic.Config) { p.InitialMaxStreamsUni = maxUniStreams }, Config{}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			params := quic.DefaultConfig()
			tt.mutate(&params)
			_, cancel, served := startH3With(t, tt.cfg, params, quic.DefaultConfig(), echoTarget)
			select {
			case err := <-served:
				t.Fatalf("ServeH3Conn returned %v", err)
			case <-time.After(500 * time.Millisecond):
			}
			cancel()
			<-served
		})
	}
}

// h3Ask sends one request on a new stream and returns the frames of its
// answer.
func h3Ask(t *testing.T, conn *quic.Conn, request []byte) []h3Frame {
	t.Helper()
	s, err := conn.OpenStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.WriteFinal(request); err != nil {
		t.Fatal(err)
	}
	reply, err := io.ReadAll(s)
	if err != nil {
		t.Fatalf("reading the answer: %v", err)
	}
	frames, _, _, err := parseH3Frames(nil, reply, 1<<20)
	if err != nil {
		t.Fatalf("the answer did not parse: %v", err)
	}
	return frames
}

// A streamed 204 sends no DATA; a response that cannot be framed is the
// 500 with its line of body.
func TestH3AnswersAtTheirEdges(t *testing.T) {
	conn, _, _ := startH3(t, quic.DefaultConfig(), func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
		out := make([]Response, b.Len())
		for i := range b.Items {
			switch string(b.Items[i].Target) {
			case "/nc":
				out[i] = Response{Status: 204, Stream: streamOf("x")}
			default:
				out[i] = Response{Status: 200, Body: []byte("x"), Stream: streamOf("y")}
			}
		}
		return sluice.Batch[Response]{Items: out}
	})
	for _, f := range h3Ask(t, conn, h3Request("GET", "/nc")) {
		if f.Type == h3Data {
			t.Errorf("a streamed 204 carried DATA %q", f.Payload)
		}
	}
	var body []byte
	for _, f := range h3Ask(t, conn, h3Request("GET", "/bad")) {
		if f.Type == h3Data {
			body = append(body, f.Payload...)
		}
	}
	if string(body) != string(transportError(500).Body) {
		t.Errorf("the refused response's 500 carried %q", body)
	}
}

// A batch's context is released by its last writer, a batch of one's too.
func TestH3BatchOfOneReleasesItsCalls(t *testing.T) {
	c := serverConnWith(t, quic.DefaultConfig())
	for _, ids := range [][]uint64{{400}, {404, 408}} {
		var calls streamCalls[uint64]
		b := h3Batch{reqs: make([]Request, len(ids)), ids: ids, after: make([]<-chan struct{}, len(ids)), fieldSection: h3NoFieldSectionLimit}
		for i := range b.reqs {
			b.reqs[i].Method = []byte("GET")
		}
		b.ctx, b.calls = calls.register(t.Context(), ids), &calls
		var writers sync.WaitGroup
		answerH3(t.Context(), c, nil, b, echoTarget, writeBoundOf(Config{}.mustResolve()), &writers)
		writers.Wait()
		deadline := time.Now().Add(5 * time.Second)
		for {
			calls.mu.Lock()
			left := len(calls.m)
			calls.mu.Unlock()
			if left == 0 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("a batch of %d left %d streams registered", len(ids), left)
			}
			time.Sleep(time.Millisecond)
		}
	}
}

// The assembler reads no more than maxUniStreams unidirectional streams.
func TestH3AssemblerTracksAtMostItsUniStreams(t *testing.T) {
	a := testAssembler(Config{})
	var frames []quic.Frame
	for i := range maxUniStreams + 1 {
		frames = append(frames, streamFrame(uint64(4*i+2), []byte{0x21}, false)) // a reserved stream type
	}
	if _, _, _, err := a.Frames(frames); err != nil {
		t.Fatal(err)
	}
	if len(a.unis) != maxUniStreams {
		t.Errorf("%d unidirectional streams tracked, want %d", len(a.unis), maxUniStreams)
	}
}

// A frame too long on the control stream is a frame error when it is the
// SETTINGS that opens it, and an unexpected frame when a SETTINGS came
// before it in the same delivery.
func TestH3ControlStreamFrameTooLong(t *testing.T) {
	long := varint.Append(varint.Append(nil, h3Settings), 1<<20)
	for _, tt := range []struct {
		name string
		data []byte
		want uint64
	}{
		{"the opening SETTINGS", append(varint.Append(nil, h3StreamControl), long...), h3FrameError},
		{"a second SETTINGS", append(appendH3Frame(varint.Append(nil, h3StreamControl), h3Settings, nil), long...), h3FrameUnexpected},
	} {
		a := testAssembler(Config{})
		_, _, _, err := a.Frames([]quic.Frame{streamFrame(2, tt.data, false)})
		ce, ok := errors.AsType[h3ConnectionError](err)
		if !ok || ce.Code != tt.want {
			t.Errorf("%s: %v, want the code %#x", tt.name, err, tt.want)
		}
	}
}
