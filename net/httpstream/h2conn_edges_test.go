package httpstream

import (
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
)

// gated answers each request with its target, a request for a target in
// gates once that gate is closed; entered receives the target as its call
// starts.
type gated struct {
	gates   map[string]chan struct{}
	entered chan string
}

func newGated(targets ...string) *gated {
	g := &gated{gates: map[string]chan struct{}{}, entered: make(chan string, len(targets))}
	for _, t := range targets {
		g.gates[t] = make(chan struct{})
	}
	return g
}

func (g *gated) handle(ctx context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
	out := make([]Response, b.Len())
	for i := range b.Items {
		target := b.Items[i].Target
		if gate, ok := g.gates[string(target)]; ok {
			g.entered <- string(target)
			select {
			case <-gate:
			case <-ctx.Done():
			}
		}
		out[i] = Response{Status: 200, Body: target}
	}
	return sluice.Batch[Response]{Items: out}
}

// A connection whose one call has been answered while another still runs
// is busy: Close reports it cut.
func TestH2CloseCountsAHandlerStillRunning(t *testing.T) {
	g := newGated("/slow")
	log := &lockedWriter{}
	srv := &Server{Config: Config{Logger: slog.New(slog.NewTextHandler(log, nil))}, Handler: g.handle}
	addr, _ := serveTCP(t, srv)
	c := dialH2(t, addr)
	c.request(1, "GET", "/slow")
	c.flush()
	<-g.entered
	c.request(3, "GET", "/fast")
	c.flush()
	c.pump(func() bool { return c.ended[3] })
	if c.got[3] != "/fast" {
		t.Fatalf("stream 3 answered %q", c.got[3])
	}
	time.Sleep(50 * time.Millisecond) // the responder's turn after the answer
	if err := srv.Close(); err != nil {
		t.Fatal(err)
	}
	close(g.gates["/slow"])
	if !strings.Contains(log.String(), "in flight") {
		t.Errorf("Close with a handler running logged %q, want the cut reported", log.String())
	}
}

// A drained connection answers every request in flight, each in a call of
// its own and past LingerTimeout, says its two GOAWAYs and no third, and
// closes once the last answer is out.
func TestH2DrainAnswersEveryRequestInFlight(t *testing.T) {
	g := newGated("/a", "/b")
	addr, cancel, served := serveH2Direct(t, Config{ShutdownGrace: 10 * time.Second, LingerTimeout: 50 * time.Millisecond, Logger: discardLogger()}, g.handle)
	c := dialH2(t, addr)
	c.request(1, "GET", "/a")
	c.flush()
	<-g.entered
	c.request(3, "GET", "/b")
	c.flush()
	<-g.entered
	cancel()
	c.pump(func() bool { return len(c.pinged) > 0 })
	if len(c.pinged) == 0 {
		t.Fatal("no drain PING")
	}
	c.frame(h2Frame{Type: framePing, Flags: flagAck, Payload: c.pinged[0]})
	c.flush()
	time.Sleep(200 * time.Millisecond) // past LingerTimeout
	close(g.gates["/a"])
	c.pump(func() bool { return c.ended[1] })
	close(g.gates["/b"])
	c.pump(func() bool { return c.ended[3] })
	if c.got[1] != "/a" || c.got[3] != "/b" {
		t.Fatalf("answers %q and %q, want both whole", c.got[1], c.got[3])
	}
	select {
	case err := <-served:
		if err != nil {
			t.Errorf("ServeH2Conn returned %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ServeH2Conn did not return once both were answered")
	}
	c.pump(func() bool { return false }) // to the close
	if len(c.goAways) != 2 {
		t.Errorf("%d GOAWAY frames, want the drain's two", len(c.goAways))
	}
}

// A drain that finds the peer silent after its preface refuses it once
// LingerTimeout has passed: the preface alone does not take the greeting.
func TestH2DrainRefusesAPeerSilentAfterItsPreface(t *testing.T) {
	addr, cancel, served := serveH2Direct(t, Config{ShutdownGrace: 10 * time.Second, LingerTimeout: 50 * time.Millisecond, ReadTimeout: 10 * time.Second}, echoTarget)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := io.WriteString(conn, Preface); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case <-served:
	case <-time.After(3 * time.Second):
		t.Fatal("a peer silent after its preface held the drain past LingerTimeout")
	}
}

// The connection window this end grants goes out at the start as the one
// WINDOW_UPDATE on stream 0 that takes it from the default to its size.
func TestH2ConnectionWindowIsAnnounced(t *testing.T) {
	cfg := Config{}.mustResolve()
	addr, _, _ := serveH2Direct(t, Config{}, echoTarget)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.Write(prefaced()); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var in []byte
	buf := make([]byte, 4096)
	for {
		for len(in) >= frameHeaderSize {
			n := int(in[0])<<16 | int(in[1])<<8 | int(in[2])
			if len(in) < frameHeaderSize+n {
				break
			}
			if in[3] == frameWindowUpdate && binary.BigEndian.Uint32(in[5:9]) == 0 {
				got := binary.BigEndian.Uint32(in[9:13])
				if want := uint32(connWindow(cfg) - defaultInitialWindow); got != want { //nolint:gosec // G115: a window
					t.Errorf("the connection WINDOW_UPDATE grants %d, want %d", got, want)
				}
				return
			}
			in = in[frameHeaderSize+n:]
		}
		n, err := conn.Read(buf)
		if err != nil {
			t.Fatalf("no connection WINDOW_UPDATE: %v", err)
		}
		in = append(in, buf[:n]...)
	}
}

// The peer's SETTINGS_MAX_HEADER_LIST_SIZE is held exactly, in RFC 9113
// §6.5.2's measure, for a head with its Date and its Content-Length and for
// a trailer section; zero is no limit.
func TestH2HeaderListsAtThePeersLimit(t *testing.T) {
	s := &h2Conn{cfg: Config{}.mustResolve()}
	s.date = dateFor(time.Now())
	measure := func(frames []byte) uint32 {
		fields, err := newHPACKDecoder(0, 1<<20).Decode(nil, frames[frameHeaderSize:])
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		for _, f := range fields {
			n += len(f.Name) + len(f.Value) + hpackEntrySize
		}
		return uint32(n) //nolint:gosec // G115: a few fields
	}
	r := Response{Status: 200, Headers: []Header{{Name: []byte("x-a"), Value: []byte("bb")}}, Body: []byte("abc")}
	head := func() error { _, err := s.appendHead(nil, 1, []byte("GET"), r, defaultMaxFrameSize); return err }
	tr := Response{Trailers: func() []Header { return []Header{{Name: []byte("x-t"), Value: []byte("vvv")}} }}
	trailers := func() error { _, err := s.appendTrailers(nil, 1, tr, defaultMaxFrameSize); return err }

	out, err := s.appendHead(nil, 1, []byte("GET"), r, defaultMaxFrameSize)
	if err != nil {
		t.Fatal(err)
	}
	headSize := measure(out)
	out, err = s.appendTrailers(nil, 1, tr, defaultMaxFrameSize)
	if err != nil {
		t.Fatal(err)
	}
	trailerSize := measure(out)
	for _, tt := range []struct {
		name string
		f    func() error
		size uint32
	}{{"head", head, headSize}, {"trailers", trailers, trailerSize}} {
		s.peerMaxHeaderList.Store(tt.size)
		if err := tt.f(); err != nil {
			t.Errorf("%s at the peer's limit of %d: %v", tt.name, tt.size, err)
		}
		s.peerMaxHeaderList.Store(tt.size - 1)
		if err := tt.f(); err == nil {
			t.Errorf("%s one byte over the peer's limit: accepted", tt.name)
		}
		s.peerMaxHeaderList.Store(0)
		if err := tt.f(); err != nil {
			t.Errorf("%s with no limit: %v", tt.name, err)
		}
	}
}

// A block of exactly one frame is one HEADERS, END_HEADERS on it.
func TestHeaderFramesOfExactlyOneFrame(t *testing.T) {
	out, err := appendHeaderFrames(nil, 1, 0, make([]byte, 16), 16)
	if err != nil || len(out) != frameHeaderSize+16 || out[4]&flagEndHeaders == 0 {
		t.Fatalf("%d bytes, flags %#x, %v; want one HEADERS with END_HEADERS", len(out), out[4], err)
	}
}

func TestAddWindowAtItsBounds(t *testing.T) {
	if got, err := addWindow(-maxWindow+1, -1); err != nil || got != -maxWindow {
		t.Errorf("down to -maxWindow: %d, %v", got, err)
	}
	if _, err := addWindow(-maxWindow, -1); err == nil {
		t.Error("below -maxWindow: accepted")
	}
	if got, err := addWindow(maxWindow-1, 1); err != nil || got != maxWindow {
		t.Errorf("up to maxWindow: %d, %v", got, err)
	}
}

// A streamed batch of several chunks, an empty one among them, goes out
// whole, in order.
func TestH2StreamedBatchOfSeveralChunks(t *testing.T) {
	addr, _ := serveTest(t, Config{}, func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
		out := make([]Response, b.Len())
		for i := range out {
			out[i] = Response{Status: 200, Stream: func(yield func(sluice.Batch[[]byte]) bool) {
				if yield(sluice.Batch[[]byte]{Items: [][]byte{[]byte("ab"), nil, []byte("cd")}}) {
					yield(sluice.Batch[[]byte]{Items: [][]byte{[]byte("e")}})
				}
			}}
		}
		return sluice.Batch[Response]{Items: out}
	})
	c := dialH2(t, addr)
	c.request(1, "GET", "/s")
	c.flush()
	if got := c.bodies(1)[1]; got != "abcde" {
		t.Errorf("stream 1 carried %q, want abcde", got)
	}
}

// Two streamed answers of one batch each go to their own stream.
func TestH2TwoStreamedAnswersInOneBatch(t *testing.T) {
	addr, sizes := serveTest(t, Config{}, func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
		out := make([]Response, b.Len())
		for i, r := range b.Items {
			target := string(r.Target)
			out[i] = Response{Status: 200, Stream: func(yield func(sluice.Batch[[]byte]) bool) {
				if yield(sluice.Batch[[]byte]{Items: [][]byte{[]byte(target)}}) {
					yield(sluice.Batch[[]byte]{Items: [][]byte{[]byte("!")}})
				}
			}}
		}
		return sluice.Batch[Response]{Items: out}
	})
	c := dialH2(t, addr)
	c.request(1, "GET", "/one")
	c.request(3, "GET", "/two")
	c.flush()
	bodies := c.bodies(2)
	if got := sizes(); len(got) != 1 || got[0] != 2 {
		t.Fatalf("batches %v: the two requests did not share one; the test cannot tell", got)
	}
	if bodies[1] != "/one!" || bodies[3] != "/two!" {
		t.Errorf("streams carried %q and %q", bodies[1], bodies[3])
	}
}

// A 204's body is not sent, whatever the handler put in it.
func TestH2NoContentSendsNoBody(t *testing.T) {
	addr, _ := serveTest(t, Config{}, func(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
		out := make([]Response, b.Len())
		for i, r := range b.Items {
			out[i] = Response{Status: 200, Body: r.Target}
			if string(r.Target) == "/nc" {
				out[i] = Response{Status: 204, Body: []byte("x")}
			}
		}
		return sluice.Batch[Response]{Items: out}
	})
	c := dialH2(t, addr)
	c.request(1, "GET", "/nc")
	c.flush()
	c.pump(func() bool { return c.ended[1] })
	c.request(3, "GET", "/after")
	c.flush()
	c.pump(func() bool { return c.ended[3] })
	if c.got[1] != "" || c.got[3] != "/after" {
		t.Errorf("the 204 carried %q, the next answer %q", c.got[1], c.got[3])
	}
}
