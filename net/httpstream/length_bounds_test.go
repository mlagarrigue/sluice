package httpstream

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/net/quic"
)

// A decimal length is a valid length however many digits it has (RFC 9110
// §8.6): one past the body bound — or past the machine word, which no bound
// reaches — is too large, 413, never a small number after a wrap. Under
// MaxStreamedBodyBytes: math.MaxInt, the spelling of no bound, a
// Content-Length of 2^64+5 was read as 5 and served with five bytes of body,
// and a chunk size of ffffffffffffffff was read as -1 and panicked the
// body's reader.
func TestH1LengthsPastTheMachineWordAreTooLarge(t *testing.T) {
	cfg := Config{MaxStreamedBodyBytes: math.MaxInt}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate refuses MaxStreamedBodyBytes: math.MaxInt, the spelling of no bound: %v", err)
	}

	var mu sync.Mutex
	var seen []string
	handle := func(ctx context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
		out := make([]Response, b.Len())
		for i := range b.Items {
			r := b.Items[i]
			var got []byte
			var err error
			if r.Body != nil {
				got = append(got, r.Body...)
			} else {
				var parts [][]byte
				parts, err = r.Stream().Collect()
				got = bytes.Join(parts, nil)
			}
			mu.Lock()
			seen = append(seen, string(got))
			mu.Unlock()
			out[i] = Response{Body: []byte("ok")}
			if errors.Is(err, ErrTooLarge) {
				// A streamed body learns its bound as it goes: the
				// handler reads ErrTooLarge and answers.
				out[i] = Response{Status: 413}
			}
		}
		return sluice.Batch[Response]{Items: out}
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Serve(ctx, ln, cfg, handle) }()
	defer func() { cancel(); <-done }()

	status := func(raw string) string {
		c, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(10 * time.Second))
		if _, err := io.WriteString(c, raw); err != nil {
			t.Fatal(err)
		}
		line, _ := bufio.NewReader(c).ReadString('\n')
		return strings.TrimSpace(line)
	}

	// The length is 2^64 + 5: wrapped, it read as five.
	st := status("POST / HTTP/1.1\r\nHost: h\r\nContent-Length: 18446744073709551621\r\nConnection: close\r\n\r\nhello")
	mu.Lock()
	if !strings.Contains(st, " 413") {
		t.Errorf("Content-Length 2^64+5 answered %q, want 413; handler saw body %q", st, seen)
	}
	seen = nil
	mu.Unlock()

	// Chunked, the body streams: the size reaches the handler's read as
	// ErrTooLarge, not as a wrapped -1.
	st = status("POST / HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\nffffffffffffffff\r\nabc\r\n0\r\n\r\n")
	if !strings.Contains(st, " 413") {
		t.Errorf("chunk size ffffffffffffffff answered %q, want 413", st)
	}
}

// HTTP/2 and HTTP/3 judge a content-length by the body bound, as HTTP/1.1
// does: refused as malformed past 2^31 — a stream window's bound, unrelated
// to lengths — a 3 GB upload under a 4 GiB bound was answered 400, and one
// over the default bound 400 rather than 413. Below 2^31 the bound already
// decided; those cases stay as witnesses.
func TestH2H3LengthsAreJudgedByTheBodyBound(t *testing.T) {
	ok := func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
		out := make([]Response, b.Len())
		for i := range out {
			out[i] = Response{Status: 200, Body: []byte("ok")}
		}
		return sluice.Batch[Response]{Items: out}
	}
	h2 := func(cfg Config, length string) int {
		addr, _ := serveTest(t, cfg, ok)
		c := dialH2(t, addr)
		c.frame(h2Frame{
			Type: frameHeaders, Flags: flagEndHeaders, StreamID: 1,
			Payload: headBlock("POST", "/up", [2]string{"content-length", length}),
		})
		c.flush()
		c.pump(func() bool { return c.heads[1] != nil })
		return h2Status(c.heads[1])
	}
	h3 := func(length string) h3StreamError {
		a := testAssembler(Config{})
		_, _, failed, err := a.Frames([]quic.Frame{
			streamFrame(0, h3Request("POST", "/up", [2]string{"content-length", length}), false),
		})
		if err != nil {
			t.Fatalf("h3 %s: connection error %v", length, err)
		}
		if len(failed) != 1 {
			t.Fatalf("h3 %s: failed = %v, want one refusal", length, failed)
		}
		return failed[0]
	}

	// Witnesses: below 2^31 the bound decides, 413.
	if got := h2(Config{}, "2000000000"); got != 413 {
		t.Fatalf("witness h2 defaults 2e9: %d, want 413", got)
	}
	if f := h3("2000000000"); f.Status != 413 {
		t.Fatalf("witness h3 defaults 2e9: status %d code %#x, want 413", f.Status, f.Code)
	}

	if got := h2(Config{MaxStreamedBodyBytes: 1 << 32}, "3000000000"); got != 200 {
		t.Errorf("h2 MaxStreamedBodyBytes 4 GiB, 3e9: %d, want 200", got)
	}
	if got := h2(Config{}, "3000000000"); got != 413 {
		t.Errorf("h2 defaults, 3e9: %d, want 413", got)
	}
	if f := h3("3000000000"); f.Status != 413 {
		t.Errorf("h3 defaults, 3e9: status %d code %#x err %v, want 413", f.Status, f.Code, f.Err)
	}
	if got := h2(Config{}, "18446744073709551621"); got != 413 {
		t.Errorf("h2 defaults, 2^64+5: %d, want 413", got)
	}
}

// Validate names a request-buffer field past which the buffer's size
// overflows, and a field HTTP/2 announces in a 32-bit SETTINGS value
// (RFC 9113 §6.5.1) past what that value can say.
func TestValidateRefusesBoundsTheBufferOrTheWireCannotHold(t *testing.T) {
	for _, tc := range []struct {
		field string
		cfg   Config
	}{
		{"Config.MaxBodyBytes", Config{MaxBodyBytes: math.MaxInt}},
		{"Config.MaxRequestLineBytes", Config{MaxRequestLineBytes: math.MaxInt}},
		{"Config.MaxHeaderBytes", Config{MaxHeaderBytes: 1 << 33}},
		{"Config.MaxConcurrentStreams", Config{MaxConcurrentStreams: 1 << 33}},
	} {
		err := tc.cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), tc.field) {
			t.Errorf("Validate(%s past its ceiling) = %v, want an error naming the field", tc.field, err)
		}
	}
}

// MaxConcurrentStreams is held to what one connection may cost: past 4096
// it is refused at start, where 2^32 used to be announced as
// SETTINGS_MAX_CONCURRENT_STREAMS 0 — no stream at all for a conformant
// client — 2^32+1 as 1, and any large value sized the request queue every
// accepted HTTP/2 connection allocates. 4096 itself and the default are
// announced as they are.
func TestMaxConcurrentStreamsIsHeldToWhatAConnectionMayCost(t *testing.T) {
	for _, v := range []int{maxConcurrentStreams + 1, 1 << 32, 1<<32 + 1} {
		if err := (Config{MaxConcurrentStreams: v}).Validate(); err == nil || !strings.Contains(err.Error(), "Config.MaxConcurrentStreams") {
			t.Errorf("Validate(MaxConcurrentStreams %d) = %v, want it refused", v, err)
		}
	}
	for _, tc := range []struct{ set, want int }{{0, 100}, {100, 100}, {maxConcurrentStreams, maxConcurrentStreams}} {
		cfg, err := Config{MaxConcurrentStreams: tc.set}.resolve()
		if err != nil {
			t.Fatalf("resolve(MaxConcurrentStreams %d): %v", tc.set, err)
		}
		s := newBenchH2Server(cfg)
		if err := s.hello(); err != nil {
			t.Fatal(err)
		}
		got := -1
		for _, f := range queued(t, s) {
			for p := f.Payload; f.Type == frameSettings && len(p) >= 6; p = p[6:] {
				if binary.BigEndian.Uint16(p) == settingMaxConcurrent {
					got = int(binary.BigEndian.Uint32(p[2:]))
				}
			}
		}
		if got != tc.want {
			t.Errorf("MaxConcurrentStreams %d announced as %d, want %d", tc.set, got, tc.want)
		}
	}
}
