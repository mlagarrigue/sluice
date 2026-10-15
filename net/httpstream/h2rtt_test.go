package httpstream

import (
	"encoding/binary"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
)

// The flow-control window is a throughput ceiling on a link with latency, and
// [H2Config.InitialWindowSize] exists to lift it. This file measures the lift
// rather than asserting it: a 4 MiB upload through a proxy that adds 50 ms
// each way, with the default window and with 1 MiB, and the ratio between
// the two is the number the field's godoc quotes.

// TestH2UploadThroughputAtRTT is the measurement. A ratio of 16 is what the
// arithmetic predicts (1 MiB against 65535 bytes per round trip); the test
// asks for 4, which the default window cannot reach against itself — the
// same comparison with 65535 on both sides measured a ratio of 1 — and
// leaves the rest to noise.
func TestH2UploadThroughputAtRTT(t *testing.T) {
	const size = 4 << 20
	slow := uploadThroughput(t, 0, size)
	fast := uploadThroughput(t, 1<<20, size)
	ratio := fast / slow
	t.Logf("default window %.2f MB/s, 1 MiB window %.2f MB/s, ratio %.1f",
		slow/1e6, fast/1e6, ratio)
	if ratio < 4 {
		t.Fatalf("1 MiB window is only %.1f× the default at 100 ms of RTT, want at least 4×", ratio)
	}
}

// TestH2InitialWindowSizeIsBounded: a window the protocol cannot express is
// lowered to the largest it can, before it reaches the wire, and a negative
// one is the default.
func TestH2InitialWindowSizeIsBounded(t *testing.T) {
	if got := (H2Config{InitialWindowSize: 1 << 31}).withDefaults().InitialWindowSize; got != maxWindow {
		t.Fatalf("1<<31 became %d, want %d", got, maxWindow)
	}
	if got := (H2Config{InitialWindowSize: -1}).withDefaults().InitialWindowSize; got != defaultInitialWindow {
		t.Fatalf("-1 became %d, want %d", got, defaultInitialWindow)
	}

	addr, _ := serveH2(t, H2Config{InitialWindowSize: 1 << 31}, echoTarget)
	c := dialH2(t, addr)
	c.pump(func() bool { return len(c.settings) > 0 })
	if got := c.settings[settingInitialWindowSize]; got != maxWindow {
		t.Fatalf("advertised SETTINGS_INITIAL_WINDOW_SIZE %d, want %d", got, maxWindow)
	}
}

// TestH2ConnWindowFollowsStreams: the connection window is the sum of the
// streams' windows, never below the floor the defaults always granted and
// never past what the protocol can express.
func TestH2ConnWindowFollowsStreams(t *testing.T) {
	for _, tc := range []struct {
		streams, window int
		want            int32
	}{
		{0, 0, 100 * defaultInitialWindow},
		{4, 0, connRecvWindow},
		{100, 1 << 20, 100 << 20},
		{100, 1 << 30, maxWindow},
	} {
		cfg := H2Config{MaxConcurrentStreams: tc.streams, InitialWindowSize: tc.window}.withDefaults()
		if got := connWindow(cfg); got != tc.want {
			t.Errorf("streams=%d window=%d: connection window %d, want %d", tc.streams, tc.window, got, tc.want)
		}
	}
}

// uploadThroughput moves size bytes on one stream through a 100 ms round
// trip to a server granting window per stream (zero for the default), and
// returns the bytes per second the upload achieved, from the request's
// HEADERS to the response's END_STREAM.
func uploadThroughput(t *testing.T, window, size int) float64 {
	t.Helper()
	cfg := H2Config{
		InitialWindowSize: window,
		MaxBodyBytes:      2 * size,
		// The default upload takes seconds at this RTT; the clocks that
		// would otherwise cut it short are not what is measured here.
		ReadTimeout:  2 * time.Minute,
		WriteTimeout: 2 * time.Minute,
		IdleTimeout:  2 * time.Minute,
	}
	addr, _ := serveH2(t, cfg, func(b sluice.Batch[Request]) sluice.Batch[Response] {
		out := make([]Response, 0, b.Len())
		for range b.Items {
			out = append(out, Response{Status: 200})
		}
		return sluice.Batch[Response]{Items: out}
	})
	proxy := delayProxy(t, addr, 50*time.Millisecond)

	conn, err := net.Dial("tcp", proxy)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	u := &rttUploader{t: t, conn: conn, stream: initialWindow, connW: initialWindow, initial: initialWindow}
	u.cond = sync.NewCond(&u.mu)

	var preface []byte
	preface = append(preface, Preface...)
	preface, _ = appendH2Frame(preface, h2Frame{Type: frameSettings})
	u.write(preface)
	go u.read()

	start := time.Now()
	head, _ := appendH2Frame(nil, h2Frame{Type: frameHeaders, Flags: flagEndHeaders, StreamID: 1, Payload: headBlock("POST", "/")})
	u.write(head)
	payload := make([]byte, defaultMaxFrameSize)
	for sent := 0; sent < size; {
		n := u.credit(min(len(payload), size-sent))
		f := h2Frame{Type: frameData, StreamID: 1, Payload: payload[:n]}
		if sent+n == size {
			f.Flags = flagEndStream
		}
		b, _ := appendH2Frame(nil, f)
		u.write(b)
		sent += n
	}
	u.mu.Lock()
	for !u.done && u.err == nil {
		u.cond.Wait()
	}
	err = u.err
	u.mu.Unlock()
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("window %d: %v", window, err)
	}
	return float64(size) / elapsed.Seconds()
}

// rttUploader is the client half of the measurement: a sender that honours
// the server's flow control — the stream window, the connection window and
// SETTINGS_INITIAL_WINDOW_SIZE's retroactive delta — and nothing else, so
// what it measures is the window and not the client.
type rttUploader struct {
	t    *testing.T
	conn net.Conn
	wmu  sync.Mutex // the reader acknowledges SETTINGS while the uploader sends DATA

	mu      sync.Mutex
	cond    *sync.Cond
	stream  int64 // credit on stream 1
	connW   int64 // credit on the connection
	initial int64 // the server's SETTINGS_INITIAL_WINDOW_SIZE as last applied
	done    bool
	err     error
}

func (u *rttUploader) write(b []byte) {
	u.wmu.Lock()
	defer u.wmu.Unlock()
	if _, err := u.conn.Write(b); err != nil {
		u.fail(err)
	}
}

func (u *rttUploader) fail(err error) {
	u.mu.Lock()
	if u.err == nil {
		u.err = err
	}
	u.cond.Broadcast()
	u.mu.Unlock()
}

// credit waits until both windows allow something and returns how much of
// want may go, debiting it.
func (u *rttUploader) credit(want int) int {
	u.mu.Lock()
	defer u.mu.Unlock()
	for min(u.stream, u.connW) <= 0 && u.err == nil {
		u.cond.Wait()
	}
	if u.err != nil {
		u.t.Fatal(u.err)
	}
	n := min(int64(want), u.stream, u.connW)
	u.stream -= n
	u.connW -= n
	return int(n)
}

func (u *rttUploader) read() {
	var in []byte
	tmp := make([]byte, 64<<10)
	for {
		n, err := u.conn.Read(tmp)
		in = append(in, tmp[:n]...)
		for len(in) >= 9 {
			length := int(in[0])<<16 | int(in[1])<<8 | int(in[2])
			if len(in) < 9+length {
				break
			}
			u.frame(in[3], in[4], binary.BigEndian.Uint32(in[5:9])&0x7fffffff, in[9:9+length])
			in = in[9+length:]
		}
		if err != nil {
			u.fail(err)
			return
		}
	}
}

func (u *rttUploader) frame(typ, flags byte, id uint32, payload []byte) {
	u.mu.Lock()
	defer u.mu.Unlock()
	switch typ {
	case frameSettings:
		if flags&flagAck != 0 {
			return
		}
		for p := payload; len(p) >= 6; p = p[6:] {
			if binary.BigEndian.Uint16(p) == settingInitialWindowSize {
				// §6.9.2: a delta against the previous value, applied to
				// the stream already open.
				v := int64(binary.BigEndian.Uint32(p[2:6]))
				u.stream += v - u.initial
				u.initial = v
			}
		}
		ack, _ := appendH2Frame(nil, h2Frame{Type: frameSettings, Flags: flagAck})
		go u.write(ack)
	case frameWindowUpdate:
		inc := int64(binary.BigEndian.Uint32(payload) & 0x7fffffff)
		if id == 0 {
			u.connW += inc
		} else {
			u.stream += inc
		}
	case frameHeaders, frameData:
		if flags&flagEndStream != 0 {
			u.done = true
		}
	case frameRSTStream:
		u.err = &net.OpError{Op: "rst_stream", Err: io.ErrUnexpectedEOF}
	case frameGoAway:
		u.err = &net.OpError{Op: "goaway", Err: io.ErrUnexpectedEOF}
	}
	u.cond.Broadcast()
}

// delayProxy listens on a loopback port and relays every connection to
// target, holding each chunk oneWay in each direction: 100 ms of round trip
// for 50 ms, without tc netem and the privileges it needs. Chunks are queued
// rather than serialised behind the delay, so the proxy adds latency, not a
// bandwidth cap.
func delayProxy(t *testing.T, target string, oneWay time.Duration) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			up, err := net.Dial("tcp", target)
			if err != nil {
				_ = c.Close()
				continue
			}
			wg.Go(func() { delayCopy(up, c, oneWay) })
			wg.Go(func() { delayCopy(c, up, oneWay) })
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		wg.Wait()
	})
	return ln.Addr().String()
}

type stampedChunk struct {
	at time.Time
	b  []byte
}

// delayCopy relays src to dst, each chunk oneWay after it was read. Closing
// dst when src ends is what lets the other direction's relay end too.
func delayCopy(dst, src net.Conn, oneWay time.Duration) {
	defer func() { _ = dst.Close() }()
	queue := make(chan stampedChunk, 4096)
	go func() {
		defer close(queue)
		for {
			buf := make([]byte, 64<<10)
			n, err := src.Read(buf)
			if n > 0 {
				queue <- stampedChunk{at: time.Now(), b: buf[:n]}
			}
			if err != nil {
				return
			}
		}
	}()
	for ch := range queue {
		time.Sleep(time.Until(ch.at.Add(oneWay)))
		if _, err := dst.Write(ch.b); err != nil {
			// Keep draining so the reader is never blocked on a full queue.
			for range queue { //nolint:revive // discarding is the point
			}
			return
		}
	}
}
