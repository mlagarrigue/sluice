package httpstream

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/net/quic"
)

// logRecorder is a slog.Handler that keeps every record for a test to read.
type logRecorder struct {
	mu      sync.Mutex
	records []slog.Record
}

func (r *logRecorder) Enabled(context.Context, slog.Level) bool { return true }

func (r *logRecorder) Handle(_ context.Context, rec slog.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = append(r.records, rec.Clone())
	return nil
}

func (r *logRecorder) WithAttrs([]slog.Attr) slog.Handler { return r }
func (r *logRecorder) WithGroup(string) slog.Handler      { return r }

// recordingLogger returns a Logger for a Config and a check, to call once
// the client has seen the 500 or the reset: the log holds exactly one error
// record for the panic, its value quoted, and a stack that runs through
// frame — the handler or producer the test wrote.
func recordingLogger(t *testing.T) (*slog.Logger, func(value, frame string)) {
	t.Helper()
	rec := &logRecorder{}
	return slog.New(rec), func(value, frame string) {
		t.Helper()
		rec.mu.Lock()
		defer rec.mu.Unlock()
		if len(rec.records) != 1 {
			t.Fatalf("the logger holds %d records, want the panic's one", len(rec.records))
		}
		r := rec.records[0]
		if r.Level != slog.LevelError {
			t.Errorf("level %v, want error", r.Level)
		}
		attrs := map[string]string{}
		r.Attrs(func(a slog.Attr) bool {
			attrs[a.Key] = a.Value.String()
			return true
		})
		if attrs["panic"] != value {
			t.Errorf("panic attribute %q, want %q", attrs["panic"], value)
		}
		if !strings.Contains(attrs["stack"], frame) {
			t.Errorf("the stack does not run through %s:\n%s", frame, attrs["stack"])
		}
	}
}

// A handler's panic over HTTP/3 is a 500 on its stream, as over the other
// two, and its value and stack go to Config.Logger: the client sees the 500,
// the operator sees why.
func TestH3HandlerPanicIsLogged(t *testing.T) {
	logger, logged := recordingLogger(t)
	conn, _, _ := startH3With(t, Config{Logger: logger}, quic.DefaultParameters(), quic.DefaultParameters(),
		func(context.Context, sluice.Batch[Request]) sluice.Batch[Response] {
			panic("the caller's own bug")
		})
	s, err := conn.OpenStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(h3Request("GET", "/x")); err != nil {
		t.Fatal(err)
	}
	if err := s.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	if got := h3Status(t, readH3Frame(t, s)); got != "500" {
		t.Errorf("status %s, want 500", got)
	}
	logged("the caller's own bug", t.Name())
}

// The error that ends a connection Serve accepted is returned to nobody: a
// peer that broke the protocol, here the rapid reset of CVE-2023-44487, is a
// warn line with its address; a client that closes after its answer is
// none. The second runs its check after Serve has returned, so a line
// logged at the connection's end could not slip past it.
func TestServeLogsAConnectionEndedInError(t *testing.T) {
	t.Run("rapid reset", func(t *testing.T) {
		rec := &logRecorder{}
		addr, _ := serveTest(t, Config{Logger: slog.New(rec)}, echoTarget)
		c := dialH2(t, addr)
		for i := range uint32(400) {
			id := 2*i + 1
			c.frame(h2Frame{Type: frameHeaders, Flags: flagEndHeaders, StreamID: id, Payload: headBlock("POST", "/upload")})
			c.frame(h2Frame{Type: frameRSTStream, StreamID: id, Payload: []byte{0, 0, 0, 0x8}})
		}
		c.flush()
		if code := goAwayCode(c.waitGoAway()); code != 0xb {
			t.Fatalf("GOAWAY carried code %#x, want ENHANCE_YOUR_CALM (0xb)", code)
		}
		_ = c.conn.Close()
		deadline := time.Now().Add(5 * time.Second)
		for {
			rec.mu.Lock()
			n := len(rec.records)
			var r slog.Record
			if n > 0 {
				r = rec.records[0]
			}
			rec.mu.Unlock()
			if n > 0 {
				if n != 1 || r.Level != slog.LevelWarn {
					t.Fatalf("%d records, the first at %v; want one warn", n, r.Level)
				}
				attrs := map[string]string{}
				r.Attrs(func(a slog.Attr) bool { attrs[a.Key] = a.Value.String(); return true })
				if attrs["proto"] != "h2" || attrs["peer"] != c.conn.LocalAddr().String() || attrs["err"] == "" {
					t.Errorf("attributes %v, want proto h2, the client's address and the error", attrs)
				}
				return
			}
			if time.Now().After(deadline) {
				t.Fatal("no record 5s after the connection was refused")
			}
			time.Sleep(time.Millisecond)
		}
	})
	t.Run("ordinary close", func(t *testing.T) {
		rec := &logRecorder{}
		t.Cleanup(func() { // after serveTest's own: Serve has returned
			rec.mu.Lock()
			defer rec.mu.Unlock()
			if len(rec.records) != 0 {
				var cause string
				rec.records[0].Attrs(func(a slog.Attr) bool {
					if a.Key == "err" {
						cause = a.Value.String()
					}
					return true
				})
				t.Errorf("an ordinary close logged %d records, the first %q: %s", len(rec.records), rec.records[0].Message, cause)
			}
		})
		addr, _ := serveTest(t, Config{Logger: slog.New(rec)}, echoTarget)
		c := dialH2(t, addr)
		c.request(1, "GET", "/a")
		c.flush()
		if got := c.bodies(1)[1]; got != "/a" {
			t.Fatalf("answered %q, want /a", got)
		}
		_ = c.conn.Close()
	})
}
