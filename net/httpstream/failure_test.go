package httpstream

import (
	"context"
	"io"
	"log/slog"
	"net"
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
	conn, _, _ := startH3With(t, Config{Logger: logger}, quic.DefaultConfig(), quic.DefaultConfig(),
		func(context.Context, sluice.Batch[Request]) sluice.Batch[Response] {
			panic("the caller's own bug")
		})
	s, err := conn.OpenStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.WriteFinal(h3Request("GET", "/x")); err != nil {
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
	// HTTP/1.1 too: a request that breaks the protocol is the peer's fault
	// the line exists for; one refused by a bound (431 here) is a conforming
	// client's, and its status says it.
	for _, tc := range []struct {
		name, request string
		status        string
		logged        bool
	}{
		{"http/1.1 malformed", "GET / HTTP/1.1\r\nno colon here\r\n\r\n", "400", true},
		{"http/1.1 past a bound", "GET / HTTP/1.1\r\nHost: h\r\nX: " + strings.Repeat("a", 64<<10) + "\r\n\r\n", "431", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := &logRecorder{}
			addr, _ := serveTest(t, Config{Logger: slog.New(rec)}, echoTarget)
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = conn.Close() }()
			if _, err := conn.Write([]byte(tc.request)); err != nil {
				t.Fatal(err)
			}
			_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			answer, _ := io.ReadAll(conn) // the refusal, then the server's close
			if !strings.HasPrefix(string(answer), "HTTP/1.1 "+tc.status) {
				t.Fatalf("answered %q, want %s", answer, tc.status)
			}
			deadline := time.Now().Add(2 * time.Second)
			for {
				rec.mu.Lock()
				n := len(rec.records)
				var r slog.Record
				if n > 0 {
					r = rec.records[0]
				}
				rec.mu.Unlock()
				switch {
				case n > 0 && !tc.logged:
					t.Fatalf("a refusal by a bound logged %q", r.Message)
				case n > 0:
					attrs := map[string]string{}
					r.Attrs(func(a slog.Attr) bool { attrs[a.Key] = a.Value.String(); return true })
					if r.Level != slog.LevelWarn || attrs["proto"] != "http/1.1" || attrs["err"] == "" {
						t.Errorf("logged %v %q %v, want a warn with proto http/1.1 and the error", r.Level, r.Message, attrs)
					}
					return
				case time.Now().After(deadline):
					if tc.logged {
						t.Fatal("no record 2s after the malformed request was refused")
					}
					return
				}
				time.Sleep(time.Millisecond)
			}
		})
	}
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

// A client that closes in the middle of its request — an upload it
// cancelled, RFC 9112 §8 — ends the connection the ordinary way the
// Config.Logger doc names: it may still get its 400, and no warn is
// written. A request that is malformed is still logged.
func TestAPeerClosingMidRequestIsNotLoggedAsAnError(t *testing.T) {
	warns := func(cfg Config, partial string) []string {
		rec := &logRecorder{}
		cfg.Logger = slog.New(rec)
		addr, _ := serveTest(t, cfg, echoTarget)
		c := dial(t, addr)
		if _, err := io.WriteString(c, partial); err != nil {
			t.Fatal(err)
		}
		if err := c.(*net.TCPConn).CloseWrite(); err != nil {
			t.Fatal(err)
		}
		// The server reports before its two-stage close, so the record, if
		// any, is written once the client reads EOF.
		_, _ = io.Copy(io.Discard, c)
		rec.mu.Lock()
		defer rec.mu.Unlock()
		var out []string
		for _, r := range rec.records {
			if r.Level >= slog.LevelWarn {
				var b strings.Builder
				b.WriteString(r.Message)
				r.Attrs(func(a slog.Attr) bool {
					if a.Key == "err" {
						b.WriteString(" err=" + a.Value.String())
					}
					return true
				})
				out = append(out, b.String())
			}
		}
		return out
	}
	for _, tc := range []struct {
		name    string
		cfg     Config
		partial string
		logged  bool
	}{
		{"a complete request, then the close", Config{}, "GET /a HTTP/1.1\r\nHost: h\r\n\r\n", false},
		{"closed mid-head", Config{}, "POST /up HTTP/1.1\r\nHost: h\r\nContent-Le", false},
		{"closed mid-body, BodyWhole", Config{BodyDelivery: BodyWhole}, "POST /up HTTP/1.1\r\nHost: h\r\nContent-Length: 100\r\n\r\npart", false},
		{"a malformed request", Config{}, "GET /a HTTP/1.1\r\nHost: h\r\nHost: g\r\n\r\n", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if w := warns(tc.cfg, tc.partial); (len(w) > 0) != tc.logged {
				t.Errorf("warn records %q, want logged = %v", w, tc.logged)
			}
		})
	}
}
