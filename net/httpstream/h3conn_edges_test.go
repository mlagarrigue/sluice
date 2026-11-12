package httpstream

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice/internal/varint"
	"github.com/mlagarrigue/sluice/net/quic"
)

// reservedFrame is a frame of a reserved type (RFC 9114 §7.2.8), which the
// assembler skips, with a payload of n bytes.
func reservedFrame(n int) []byte { return appendH3Frame(nil, 0x21, make([]byte, n)) }

// padTo appends to b a reserved frame that brings it to exactly n bytes.
func padTo(t *testing.T, b []byte, n int) []byte {
	t.Helper()
	for k := n - len(b) - 3; k >= 0; k-- {
		if out := append(append([]byte(nil), b...), reservedFrame(k)...); len(out) == n {
			return out
		}
	}
	t.Fatalf("no reserved frame brings %d bytes to %d", len(b), n)
	return nil
}

// streamingAssembler is an assembler whose bodies stream, as ServeH3Conn
// wires one on windows large enough for manual credit.
func streamingAssembler(cfg Config) *h3Assembler {
	a := testAssembler(cfg)
	a.CreditStream = func(quic.StreamID, uint64) {}
	a.CreditConn = func(uint64) {}
	return a
}

func data(p string) []byte { return appendH3Frame(nil, h3Data, []byte(p)) }

// The per-stream bound on raw bytes is inclusive.
func TestH3AssemblerRawStreamBoundIsExact(t *testing.T) {
	cfg := Config{BodyDelivery: BodyWhole, MaxBodyBytes: 4096, MaxHeaderBytes: 1024}
	bound := testAssembler(cfg).rawStreamBound()
	for n, ok := range map[int]bool{bound: true, bound + 1: false} {
		a := testAssembler(cfg)
		batch, _, failed, err := a.Frames([]quic.Frame{streamFrame(0, padTo(t, h3Request("GET", "/x"), n), true)})
		if err != nil || ok != (batch.Len() == 1 && len(failed) == 0) {
			t.Errorf("%d bytes against a bound of %d: %d requests, %v, %v", n, bound, batch.Len(), failed, err)
		}
	}
}

// The connection-wide bound on bytes held is inclusive, and names itself.
func TestH3AssemblerConnectionBoundIsExact(t *testing.T) {
	cfg := Config{BodyDelivery: BodyWhole, MaxBodyBytes: 4096, MaxHeaderBytes: 1024}
	req := h3Request("GET", "/x")
	for extra, ok := range map[int]bool{0: true, 1: false} {
		a := testAssembler(cfg)
		a.assembling = 4*a.cfg.MaxBodyBytes - len(req) + extra
		batch, _, failed, err := a.Frames([]quic.Frame{streamFrame(0, req, true)})
		if err != nil || ok != (batch.Len() == 1) {
			t.Errorf("%d over: %d requests, %v, %v", extra, batch.Len(), failed, err)
		}
		if !ok && (len(failed) != 1 || !strings.Contains(failed[0].Err.Error(), "over 16384 bytes")) {
			t.Errorf("the refusal: %v", failed)
		}
	}
}

// Before its head, a stream whose body will stream is held to the same
// connection bound on what it keeps, inclusively.
func TestH3AssemblerHeadFirstConnectionBoundIsExact(t *testing.T) {
	cfg := Config{MaxBodyBytes: 128, MaxStreamedBodyBytes: 1 << 12, MaxHeaderBytes: 1024}
	partial := h3Request("GET", "/x")
	partial = partial[:len(partial)-1]
	for extra, ok := range map[int]bool{0: true, 1: false} {
		a := streamingAssembler(cfg)
		a.assembling = 4*a.cfg.MaxBodyBytes - len(partial) + extra
		_, _, failed, err := a.Frames([]quic.Frame{streamFrame(0, partial, false)})
		if err != nil || ok != (len(failed) == 0) {
			t.Errorf("%d over: %v, %v", extra, failed, err)
		}
		if !ok && (len(failed) != 1 || !strings.Contains(failed[0].Err.Error(), "over 512 bytes")) {
			t.Errorf("the refusal: %v", failed)
		}
	}
}

// A FIN inside a frame says how many bytes it was short, inside a frame
// being skipped and inside a DATA frame being streamed.
func TestH3AssemblerFinInsideAFrameSaysHowShort(t *testing.T) {
	skipped := append(h3Request("GET", "/x"), append(varint.Append(varint.Append(nil, 0x21), 100), make([]byte, 10)...)...)
	_, _, _, err := testAssembler(Config{BodyDelivery: BodyWhole}).Frames([]quic.Frame{streamFrame(0, skipped, true)})
	if err == nil || !strings.Contains(err.Error(), ", 90 bytes short") {
		t.Errorf("inside a skipped frame: %v", err)
	}
	streamed := append(h3Request("POST", "/x"), append(varint.Append(varint.Append(nil, h3Data), 100), make([]byte, 10)...)...)
	a := streamingAssembler(Config{MaxBodyBytes: 128, MaxStreamedBodyBytes: 1 << 12})
	_, _, _, err = a.Frames([]quic.Frame{streamFrame(0, streamed, true)})
	if err == nil || !strings.Contains(err.Error(), ", 90 bytes short") {
		t.Errorf("inside a streamed DATA frame: %v", err)
	}
}

// The body bound, the content-length check and the declared bound at their
// edges, on bodies held whole.
func TestH3AssemblerWholeBodyAtItsEdges(t *testing.T) {
	cfg := Config{BodyDelivery: BodyWhole, MaxBodyBytes: 128}
	cl := func(n string) []byte { return h3Request("POST", "/x", [2]string{"content-length", n}) }
	tests := []struct {
		name   string
		stream []byte
		ok     bool
	}{
		{"a body of the bound in one frame", append(h3Request("POST", "/x"), data(strings.Repeat("d", 128))...), true},
		{"one byte over in a second frame", append(append(h3Request("POST", "/x"), data(strings.Repeat("d", 128))...), data("9")...), false},
		{"a declared length of the bound", append(cl("128"), data(strings.Repeat("d", 128))...), true},
		{"a declared zero with a body", append(cl("0"), data("abc")...), false},
	}
	for _, tt := range tests {
		batch, _, failed, err := testAssembler(cfg).Frames([]quic.Frame{streamFrame(0, tt.stream, true)})
		if err != nil || tt.ok != (batch.Len() == 1 && len(failed) == 0) {
			t.Errorf("%s: %d requests, %v, %v", tt.name, batch.Len(), failed, err)
		}
	}
}

// A request refused with a status after its head asked for a 100 waits
// for that 100: the refusal carries the gate.
func TestH3AssemblerRefusalFollowsTheInterim(t *testing.T) {
	a := testAssembler(Config{BodyDelivery: BodyWhole, MaxBodyBytes: 128})
	head := h3Request("POST", "/x", [2]string{"expect", "100-continue"})
	if _, _, _, err := a.Frames([]quic.Frame{streamFrame(0, head, false)}); err != nil {
		t.Fatal(err)
	}
	_, _, failed, err := a.Frames([]quic.Frame{streamFrame(0, data(strings.Repeat("d", 129)), false)})
	if err != nil || len(failed) != 1 || failed[0].Status != 413 || failed[0].After == nil {
		t.Fatalf("the refusal: %+v, %v; want a 413 behind the interim's gate", failed, err)
	}
}

// The rapid-reset allowance is inclusive.
func TestH3AssemblerCancelAllowanceIsExact(t *testing.T) {
	a := testAssembler(Config{MaxConcurrentStreams: 1})
	for i := range 4 {
		a.countCancel(quic.StreamID(4 * i))
	}
	if a.excess != nil {
		t.Fatalf("four cancellations against an allowance of four: %v", a.excess)
	}
	a.countCancel(16)
	if a.excess == nil {
		t.Error("a fifth cancellation was allowed")
	}
}

// Dropping a stream releases what it held, buffer and body, and stops
// counting it.
func TestH3AssemblerDropReleasesWhatItHeld(t *testing.T) {
	a := testAssembler(Config{BodyDelivery: BodyWhole})
	released := 0
	a.Credit = func(_ quic.StreamID, n uint64) { released += int(n) }            //nolint:gosec // G115: a few bytes
	delivered := append(append(h3Request("POST", "/x"), data("abcde")...), 0x00) // and one byte of a frame
	if _, _, _, err := a.Frames([]quic.Frame{streamFrame(0, delivered, false)}); err != nil {
		t.Fatal(err)
	}
	a.drop(0)
	if a.assembling != 0 || released != len(delivered) {
		t.Errorf("after the drop: %d bytes counted, %d of %d released", a.assembling, released, len(delivered))
	}
}

func TestH3AssemblerAdmitTracksTheHighestStream(t *testing.T) {
	a := testAssembler(Config{})
	a.admit(0)
	a.admit(4)
	if a.last != 4 {
		t.Errorf("last = %d, want 4", a.last)
	}
}

// A streamed DATA frame whose rest arrives with the frames after it, and a
// DATA frame past MaxBodyBytes followed by another in one delivery: the
// frames behind are read in the same delivery, and the FIN ends the body.
func TestH3AssemblerStreamedDataFollowedByFrames(t *testing.T) {
	cfg := Config{MaxBodyBytes: 128, MaxStreamedBodyBytes: 1 << 12}
	split := append(h3Request("POST", "/x"), data("0123456789")...)
	cut := len(split) - 5
	big := strings.Repeat("b", 200)
	for _, tt := range []struct {
		name       string
		deliveries [][]byte
		body       string
	}{
		{"a frame finished by the next delivery", [][]byte{split[:cut], append(append([]byte(nil), split[cut:]...), data("xyz")...)}, "0123456789xyz"},
		{"a frame past MaxBodyBytes", [][]byte{append(append(h3Request("POST", "/x"), data(big)...), data("tail")...)}, big + "tail"},
	} {
		a := streamingAssembler(cfg)
		var req *Request
		for i, d := range tt.deliveries {
			batch, _, failed, err := a.Frames([]quic.Frame{streamFrame(0, d, i == len(tt.deliveries)-1)})
			if err != nil || len(failed) != 0 {
				t.Fatalf("%s, delivery %d: %v, %v", tt.name, i, failed, err)
			}
			if batch.Len() == 1 {
				req = &batch.Items[0]
			}
		}
		if req == nil {
			t.Fatalf("%s: the request was never handed over", tt.name)
		}
		got, err := req.ReadAll(nil, 1<<12)
		if err != nil || string(got) != tt.body {
			t.Errorf("%s: %q, %v", tt.name, got, err)
		}
	}
}

// The header bounds at their edges: a HEADERS frame of MaxHeaderBytes is
// not refused for its size, MaxHeaders fields pass in the head and in the
// trailers, one more is refused.
func TestH3AssemblerHeaderBoundsAtTheirEdges(t *testing.T) {
	head := h3Request("GET", "/x")
	frames, _, _, err := parseH3Frames(nil, head, 1<<20)
	if err != nil || len(frames) != 1 {
		t.Fatal(err)
	}
	_, _, failed, _ := testAssembler(Config{MaxHeaderBytes: len(frames[0].Payload)}).Frames([]quic.Frame{streamFrame(0, head, true)})
	for _, f := range failed {
		if strings.Contains(f.Err.Error(), "header bound") {
			t.Errorf("a HEADERS frame of MaxHeaderBytes refused for its size: %v", f.Err)
		}
	}

	trailers := func(n int) []byte {
		fields := make([]Header, n)
		for i := range fields {
			fields[i] = Header{Name: []byte("x-t"), Value: []byte{'a' + byte(i)}}
		}
		return appendH3Frame(nil, h3Headers, appendQPACK(nil, fields))
	}
	cfg := Config{BodyDelivery: BodyWhole, MaxHeaders: 5}
	for _, tt := range []struct {
		name   string
		stream []byte
		ok     bool
	}{
		{"a head of MaxHeaders fields", h3Request("GET", "/x", [2]string{"x-a", "1"}), true},
		{"a head of one more", h3Request("GET", "/x", [2]string{"x-a", "1"}, [2]string{"x-b", "2"}), false},
		{"trailers of MaxHeaders fields", append(append(h3Request("POST", "/x"), data("ab")...), trailers(5)...), true},
		{"trailers of one more", append(append(h3Request("POST", "/x"), data("ab")...), trailers(6)...), false},
	} {
		batch, _, failed, err := testAssembler(cfg).Frames([]quic.Frame{streamFrame(0, tt.stream, true)})
		if err != nil || tt.ok != (batch.Len() == 1 && len(failed) == 0) {
			t.Errorf("%s: %d requests, %v, %v", tt.name, batch.Len(), failed, err)
		}
	}
}

// A streamed body's first wait sends the 100 its head asked for.
func TestH3AssemblerStreamedBodyInvitesItsBody(t *testing.T) {
	a := streamingAssembler(Config{MaxBodyBytes: 128, MaxStreamedBodyBytes: 1 << 12})
	invited := make(chan quic.StreamID, 1)
	a.Interim = func(id quic.StreamID, _ uint64) { invited <- id }
	head := h3Request("POST", "/x", [2]string{"expect", "100-continue"}, [2]string{"content-length", "40"})
	batch, _, failed, err := a.Frames([]quic.Frame{streamFrame(0, head, false)})
	if err != nil || len(failed) != 0 || batch.Len() != 1 {
		t.Fatalf("the head: %d requests, %v, %v", batch.Len(), failed, err)
	}
	req := batch.Items[0]
	done := make(chan error, 1)
	go func() { _, err := req.ReadAll(nil, 1<<12); done <- err }()
	select {
	case id := <-invited:
		if id != 0 {
			t.Errorf("invited stream %d", id)
		}
	case <-time.After(5 * time.Second):
		t.Error("the streamed body never sent the 100 its head asked for")
	}
	if _, _, _, err := a.Frames([]quic.Frame{streamFrame(0, data(strings.Repeat("d", 40)), true)}); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Errorf("the body: %v", err)
	}
}

// A streamed body its handler closed unread is forgotten at the next
// delivery.
func TestH3AssemblerForgetsAClosedBody(t *testing.T) {
	a := streamingAssembler(Config{MaxBodyBytes: 128, MaxStreamedBodyBytes: 1 << 12})
	batch, _, _, err := a.Frames([]quic.Frame{streamFrame(0, h3Request("POST", "/x", [2]string{"content-length", "40"}), false)})
	if err != nil || batch.Len() != 1 || batch.Items[0].body == nil {
		t.Fatalf("the head: %d requests, %v", batch.Len(), err)
	}
	batch.Items[0].body.q.close()
	if _, _, _, err := a.Frames(nil); err != nil {
		t.Fatal(err)
	}
	if a.streams[0] != nil {
		t.Error("a body closed by its handler is still assembled")
	}
}

// A control stream's frame carried across deliveries is released once it
// is consumed, every delivered byte once.
func TestH3AssemblerReleasesAControlFrameCarriedOver(t *testing.T) {
	a := testAssembler(Config{})
	released := 0
	a.Credit = func(_ quic.StreamID, n uint64) { released += int(n) } //nolint:gosec // G115: a few bytes
	payload := varint.Append(varint.Append(nil, h3SettingMaxFieldSection), 1<<10)
	control := appendH3Frame(varint.Append(nil, h3StreamControl), h3Settings, payload)
	cut := len(control) - 2
	for _, d := range [][]byte{control[:cut], control[cut:]} {
		if _, _, _, err := a.Frames([]quic.Frame{streamFrame(2, d, false)}); err != nil {
			t.Fatal(err)
		}
	}
	if released != len(control) {
		t.Errorf("%d of %d control bytes released", released, len(control))
	}
}

// What is kept of a stream's buffer behind a frame: the rest, whatever the
// capacity was.
func TestKeepStreamRestKeepsTheRest(t *testing.T) {
	if got := keepStreamRest(make([]byte, 10, 10000), []byte("ab")); string(got) != "ab" {
		t.Errorf("keepStreamRest = %q, want ab", got)
	}
	if got := keepStreamRest(make([]byte, 10, 10000), nil); len(got) != 0 {
		t.Errorf("keepStreamRest of nothing = %q", got)
	}
	_ = errors.New
}
