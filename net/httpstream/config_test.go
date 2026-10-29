package httpstream

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"net"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/net/quic"
)

// mustResolve is [Config.resolve] for a test whose configuration is valid by
// construction.
func (c Config) mustResolve() Config {
	r, err := c.resolve()
	if err != nil {
		panic(err)
	}
	return r
}

// testAssembler is newH3Assembler on a configuration resolved as ServeH3Conn
// resolves it.
func testAssembler(cfg Config) *h3Assembler { return newH3Assembler(cfg.mustResolve()) }

// mustTransportParameters is [Config.TransportParameters] for a valid
// configuration.
func mustTransportParameters(cfg Config) quic.TransportParameters {
	p, err := cfg.TransportParameters()
	if err != nil {
		panic(err)
	}
	return p
}

func nopHandler(_ context.Context, b sluice.Batch[Request]) sluice.Batch[Response] {
	return sluice.Batch[Response]{Items: make([]Response, b.Len())}
}

// The zero Config is every default, and valid.
func TestValidateAcceptsTheZeroConfig(t *testing.T) {
	if err := (Config{}).Validate(); err != nil {
		t.Errorf("Config{}.Validate() = %v, want nil", err)
	}
}

// A configuration value out of its range is refused at start, by an error
// that names the field and the value, rather than replaced by the default,
// raised to a floor or lowered to a ceiling.
func TestConfigRefusesAnOutOfRangeValue(t *testing.T) {
	for _, tc := range []struct {
		field string
		cfg   Config
		value string
	}{
		{"MaxRequestLineBytes", Config{MaxRequestLineBytes: -1}, "-1"},
		{"MaxHeaderBytes", Config{MaxHeaderBytes: -1}, "-1"},
		{"MaxHeaders", Config{MaxHeaders: -1}, "-1"},
		{"MaxBodyBytes", Config{MaxBodyBytes: -1}, "-1"},
		{"MaxStreamedBodyBytes", Config{MaxStreamedBodyBytes: -1}, "-1"},
		// BodyDelivery names a form, so every value that is neither is out
		// of range, on both sides of the two.
		{"BodyDelivery", Config{BodyDelivery: -1}, "-1"},
		{"BodyDelivery", Config{BodyDelivery: BodyWhole + 1}, "2"},
		{"MaxRequestsPerBatch", Config{MaxRequestsPerBatch: -1}, "-1"},
		{"MaxConcurrentStreams", Config{MaxConcurrentStreams: -1}, "-1"},
		{"MaxRequestsPerConn", Config{MaxRequestsPerConn: -1}, "-1"},
		{"IdleTimeout", Config{IdleTimeout: -time.Second}, "-1s"},
		{"ReadTimeout", Config{ReadTimeout: -time.Second}, "-1s"},
		{"WriteTimeout", Config{WriteTimeout: -time.Second}, "-1s"},
		{"WriteProgressBytes", Config{WriteProgressBytes: -1}, "-1"},
		{"MaxConns", Config{MaxConns: -1}, "-1"},
		{"MaxConnsPerIP", Config{MaxConnsPerIP: -1}, "-1"},
		{"IPv4PrefixBits", Config{IPv4PrefixBits: -1}, "-1"},
		{"IPv4PrefixBits", Config{IPv4PrefixBits: 33}, "33"},
		{"IPv6PrefixBits", Config{IPv6PrefixBits: -1}, "-1"},
		{"IPv6PrefixBits", Config{IPv6PrefixBits: 129}, "129"},
		{"ShutdownGrace", Config{ShutdownGrace: -time.Second}, "-1s"},
		{"LingerTimeout", Config{LingerTimeout: -time.Second}, "-1s"},
		{"KeepAlive", Config{KeepAlive: -time.Second}, "-1s"},
		// RFC 9113 §6.5.2 bounds what this end may advertise: a frame size
		// below 2^14 or over 2^24-1, a window over 2^31-1.
		{"MaxFrameSize", Config{MaxFrameSize: -1}, "-1"},
		{"MaxFrameSize", Config{MaxFrameSize: defaultMaxFrameSize - 1}, "16383"},
		{"MaxFrameSize", Config{MaxFrameSize: maxFrameSize + 1}, "16777216"},
		{"MaxFramesPerBatch", Config{MaxFramesPerBatch: -1}, "-1"},
		{"MaxContinuationFrames", Config{MaxContinuationFrames: -1}, "-1"},
		{"InitialWindowSize", Config{InitialWindowSize: -1}, "-1"},
		{"InitialWindowSize", Config{InitialWindowSize: maxWindow + 1}, "2147483648"},
		{"MaxPendingControlBytes", Config{MaxPendingControlBytes: -1}, "-1"},
		{"MaxWriteBufferBytes", Config{MaxWriteBufferBytes: -1}, "-1"},
	} {
		t.Run(tc.field+"="+tc.value, func(t *testing.T) {
			want := fmt.Sprintf("Config.%s %s", tc.field, tc.value)

			// Validate says it first, with no socket at all: what a
			// deployment checks a configuration it loaded with.
			if err := tc.cfg.Validate(); err == nil || !strings.Contains(err.Error(), want) {
				t.Errorf("Validate: %v, want an error naming %q", err, want)
			}

			// Serve refuses before the listener is read: a closed one would
			// otherwise fail on Accept, with an error that names no field.
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			_ = ln.Close()
			if err := Serve(t.Context(), ln, tc.cfg, nopHandler); err == nil || !strings.Contains(err.Error(), want) {
				t.Errorf("Serve: %v, want an error naming %q", err, want)
			}

			client, server := net.Pipe()
			defer func() { _ = client.Close() }()
			defer func() { _ = server.Close() }()
			src := Requests(server, tc.cfg)
			src.Stream()(func(sluice.Batch[Request]) bool {
				t.Error("Requests yielded a batch on a refused configuration")
				return false
			})
			if err := src.Err(); err == nil || !strings.Contains(err.Error(), want) {
				t.Errorf("Requests: %v, want an error naming %q", err, want)
			}

			if _, err := tc.cfg.TransportParameters(); err == nil || !strings.Contains(err.Error(), want) {
				t.Errorf("TransportParameters: %v, want an error naming %q", err, want)
			}

			// A connection whose peer has gone: served, it would fail on the
			// preface with an error that names no field.
			h2c, h2s := net.Pipe()
			_ = h2c.Close()
			defer func() { _ = h2s.Close() }()
			if err := ServeH2Conn(t.Context(), h2s, tc.cfg, nopHandler); err == nil || !strings.Contains(err.Error(), want) {
				t.Errorf("ServeH2Conn: %v, want an error naming %q", err, want)
			}
		})
	}
}

// Every field whose comment says "Zero means N" resolves to N from the zero
// Config. The defaults are chosen by review, and a review reads the
// comments: a default changed in resolve alone — MaxHeaders from 64 to 65 —
// left every other test green, so the value deployed was not the value
// documented (architecture S3).
func TestZeroConfigResolvesToTheDefaultsItsCommentsName(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "httpstream.go", nil, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	resolved := reflect.ValueOf(Config{}.mustResolve())
	zeroMeans := regexp.MustCompile(`Zero means ([^.;,]+)`)
	figure := regexp.MustCompile(`^(\d+)(?: ?(KiB|MiB|s|ms))?$`)
	words := map[string]time.Duration{"a second": time.Second, "two minutes": 2 * time.Minute}
	units := map[string]int64{"": 1, "KiB": 1 << 10, "MiB": 1 << 20, "s": int64(time.Second), "ms": int64(time.Millisecond)}
	checked := map[string]bool{}
	ast.Inspect(f, func(n ast.Node) bool {
		ts, ok := n.(*ast.TypeSpec)
		if !ok || ts.Name.Name != "Config" {
			return true
		}
		for _, field := range ts.Type.(*ast.StructType).Fields.List {
			m := zeroMeans.FindStringSubmatch(strings.Join(strings.Fields(field.Doc.Text()), " "))
			if m == nil || len(field.Names) != 1 {
				continue
			}
			name, phrase := field.Names[0].Name, strings.TrimSpace(m[1])
			var want int64
			if d, ok := words[phrase]; ok {
				want = int64(d)
			} else if v := figure.FindStringSubmatch(phrase); v != nil {
				n, _ := strconv.ParseInt(v[1], 10, 64)
				want = n * units[v[2]]
			} else {
				continue // "no bound", "MaxBodyBytes", "32 and 64": read by their own tests
			}
			if got := resolved.FieldByName(name).Int(); got != want {
				t.Errorf("Config.%s resolves to %d from zero; its comment says %q", name, got, phrase)
			}
			checked[name] = true
		}
		return false
	})
	for _, name := range []string{"MaxHeaders", "MaxRequestsPerConn", "MaxFramesPerBatch", "MaxBodyBytes", "ReadTimeout"} {
		if !checked[name] {
			t.Errorf("Config.%s's default was not read from its comment: the comment changed shape, or the parser did", name)
		}
	}
}

// TransportParameters refuses a stream limit ServeH3Conn would refuse every
// connection for, rather than lowering it to the one it serves.
func TestTransportParametersRefusesMoreStreamsThanServeH3Serves(t *testing.T) {
	if _, err := (Config{MaxConcurrentStreams: h3MaxServeStreams + 1}).TransportParameters(); err == nil || !strings.Contains(err.Error(), "Config.MaxConcurrentStreams 4097") {
		t.Errorf("TransportParameters: %v, want an error naming MaxConcurrentStreams", err)
	}
	p := mustTransportParameters(Config{MaxConcurrentStreams: h3MaxServeStreams})
	if p.InitialMaxStreamsBidi != h3MaxServeStreams {
		t.Errorf("InitialMaxStreamsBidi = %d, want %d", p.InitialMaxStreamsBidi, h3MaxServeStreams)
	}
}

// Every value inside a range is kept as given, the ends included.
func TestConfigKeepsTheEndsOfTheHTTP2Ranges(t *testing.T) {
	for _, size := range []int{defaultMaxFrameSize, maxFrameSize} {
		if got := (Config{MaxFrameSize: size}).mustResolve().MaxFrameSize; got != size {
			t.Errorf("MaxFrameSize %d became %d", size, got)
		}
	}
	if got := (Config{InitialWindowSize: maxWindow}).mustResolve().InitialWindowSize; got != maxWindow {
		t.Errorf("InitialWindowSize %d became %d", maxWindow, got)
	}
}

// LingerTimeout at zero is a second, or ReadTimeout when that is shorter; a
// value the caller sets is kept, whatever ReadTimeout says.
func TestLingerTimeoutResolves(t *testing.T) {
	for _, tc := range []struct {
		cfg  Config
		want time.Duration
	}{
		{Config{}, time.Second},
		{Config{ReadTimeout: 200 * time.Millisecond}, 200 * time.Millisecond},
		{Config{ReadTimeout: time.Minute}, time.Second},
		{Config{LingerTimeout: 5 * time.Second, ReadTimeout: time.Second}, 5 * time.Second},
	} {
		if got := tc.cfg.mustResolve().LingerTimeout; got != tc.want {
			t.Errorf("%+v: LingerTimeout %v, want %v", tc.cfg, got, tc.want)
		}
	}
}

// A closing connection drains a silent client for LingerTimeout, not for the
// second the drain was hard-wired to.
func TestTearDownLingersForLingerTimeout(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	client, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	server, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close() }()

	const linger = 50 * time.Millisecond
	start := time.Now()
	tearDown(&prefixConn{Conn: server}, Config{LingerTimeout: linger, ReadTimeout: time.Minute}.mustResolve())
	if took := time.Since(start); took < linger || took > 500*time.Millisecond {
		t.Errorf("tearDown took %v against a silent client, want about %v", took, linger)
	}
}
