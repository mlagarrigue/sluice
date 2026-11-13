package main

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"io"
	"strconv"
	"strings"
	"sync"

	"github.com/andybalholm/brotli"
	kpgzip "github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"

	"github.com/mlagarrigue/sluice"
)

// Axis F of the internal study: the buffered body of every answer, encoded
// when the client accepts it. gzip and deflate come from the standard
// library; HTTP's "deflate" is a deflate stream inside the zlib format
// (RFC 9110 §8.4.1.2), not the raw stream some servers send. zstd, br and the
// klauspost gzip are the study's second level (§2.3 bis): what Sluice.go
// would gain from a module outside the root's graph, measured here and only
// here (this module's go.mod; ADR 0002 is untouched).
// Nothing here is an API of web: the study measures this stage first, and
// the measurement decides whether web offers one (study, §8.5).
//
// What is deliberately simple: one encoder per level, reused through a
// sync.Pool, the whole body written in one go and Close'd. A response is
// buffered in R, so the variant is buffered too; under axis E the same
// encoder is flushed once per batch (encodeStream, writeStream), which is
// the "flush per batch" variant of the axis.

const (
	codecNone     = "none"
	codecGzip1    = "gzip1"
	codecGzip6    = "gzip6"
	codecDeflate6 = "deflate6"
	codecGzip1KP  = "gzip1-kp"
	codecGzip6KP  = "gzip6-kp"
	codecZstd     = "zstd"
	codecBr       = "br"
)

// codecNames is every value -compression takes, none first.
var codecNames = []string{
	codecNone, codecGzip1, codecGzip6, codecDeflate6,
	codecGzip1KP, codecGzip6KP, codecZstd, codecBr,
}

// brotliLevel is the quality of the br variant. Brotli's top quality, 11, is
// the default of Node's zlib and aims at static files compressed once; on a
// dynamic answer it ruins the CPU budget (study, §2.3 bis). On this server's
// list (BenchmarkListCompress), 4 costs about what gzip 6 does and writes
// less than half its bytes; 5 and 6 cost 1.6 and 2 times as much and write
// slightly more.
const brotliLevel = 4

// codec is one value of -compression: the token it answers to in
// Accept-Encoding and Content-Encoding, and how it builds an encoder.
type codec struct {
	token     string // "gzip", "deflate", "zstd" or "br"; "" is identity
	newWriter func(io.Writer) resetWriter
	pool      sync.Pool
}

// lookupCodec returns the codec the flag names, or nil for none. The levels
// are constants of this file, so a constructor that refuses one is a bug of
// this file: it panics at the first answer, in every test of the variant.
func lookupCodec(name string) *codec {
	switch name {
	case codecGzip1:
		return &codec{token: "gzip", newWriter: stdGzip(gzip.BestSpeed)}
	case codecGzip6:
		return &codec{token: "gzip", newWriter: stdGzip(gzip.DefaultCompression)}
	case codecDeflate6:
		return &codec{token: "deflate", newWriter: func(w io.Writer) resetWriter {
			return must(zlib.NewWriterLevel(w, zlib.DefaultCompression))
		}}
	case codecGzip1KP:
		return &codec{token: "gzip", newWriter: kpGzip(kpgzip.BestSpeed)}
	case codecGzip6KP:
		return &codec{token: "gzip", newWriter: kpGzip(kpgzip.DefaultCompression)}
	case codecZstd:
		// The library's default level, SpeedDefault (about zstd's level 3).
		// One goroutine per encoder: the default is one per CPU, which a
		// server answering many clients at once pays for nothing. The window
		// stays at the library's 8 MiB, the ceiling RFC 9659 §3 sets for the
		// "zstd" content coding.
		return &codec{token: "zstd", newWriter: func(w io.Writer) resetWriter {
			return must(zstd.NewWriter(w, zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderConcurrency(1)))
		}}
	case codecBr:
		return &codec{token: "br", newWriter: func(w io.Writer) resetWriter {
			return brotli.NewWriterLevel(w, brotliLevel)
		}}
	}
	return nil
}

func stdGzip(level int) func(io.Writer) resetWriter {
	return func(w io.Writer) resetWriter { return must(gzip.NewWriterLevel(w, level)) }
}

// kpGzip is klauspost's gzip, a drop-in replacement of compress/gzip: same
// format, same levels, its own matcher.
func kpGzip(level int) func(io.Writer) resetWriter {
	return func(w io.Writer) resetWriter { return must(kpgzip.NewWriterLevel(w, level)) }
}

func must[W resetWriter](w W, err error) resetWriter {
	if err != nil {
		panic(err)
	}
	return w
}

// resetWriter is what every encoder here has in common: a reset re-targets
// the encoder without rebuilding its tables, and a flush ends the current
// block so that everything written so far can be decoded.
type resetWriter interface {
	io.WriteCloser
	Reset(io.Writer)
	Flush() error
}

// get takes an encoder from the pool, targeted at w; put returns it.
func (c *codec) get(w io.Writer) resetWriter {
	zw, _ := c.pool.Get().(resetWriter)
	if zw == nil {
		return c.newWriter(w)
	}
	zw.Reset(w)
	return zw
}

func (c *codec) put(zw resetWriter) { c.pool.Put(zw) }

// encodeStream is axis F over axis E: every batch of fragments of in comes
// out as one compressed fragment, the encoder flushed so that the client
// can decode what it has, and the final block closes the stream. The
// encoder is taken when the transport starts pulling and returned when the
// stream ends, on the transport's writing goroutine.
func (c *codec) encodeStream(in sluice.Stream[[]byte]) sluice.Stream[[]byte] {
	return func(yield func(sluice.Batch[[]byte]) bool) {
		var buf bytes.Buffer
		zw := c.get(&buf)
		defer c.put(zw)
		emit := func() bool {
			if buf.Len() == 0 {
				return true
			}
			out := bytes.Clone(buf.Bytes())
			buf.Reset()
			return yield(sluice.Batch[[]byte]{Items: [][]byte{out}})
		}
		ok := true
		in(func(batch sluice.Batch[[]byte]) bool {
			for _, frag := range batch.Items {
				if _, err := zw.Write(frag); err != nil {
					ok = false
					return false
				}
			}
			if err := zw.Flush(); err != nil {
				ok = false
				return false
			}
			ok = emit()
			return ok
		})
		if !ok {
			return
		}
		if err := zw.Close(); err != nil {
			return
		}
		emit()
	}
}

// encode compresses body in one write and returns the encoded bytes. The
// writer comes from the pool, so a steady state allocates the output buffer
// and nothing of the encoder.
func (c *codec) encode(body []byte) ([]byte, error) {
	var buf bytes.Buffer
	buf.Grow(len(body) / 4)
	zw := c.get(&buf)
	defer c.put(zw)
	if _, err := zw.Write(body); err != nil {
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// accepts says whether an Accept-Encoding header lets the server answer with
// this codec (RFC 9110 §12.5.3). It is the only hostile input of this file:
// a list of tokens with optional q-values, where "identity" and "*" count,
// and a weight of 0 refuses. An absent header answers identity: §12.5.3
// lets a server pick any coding then, but a client that said nothing is the
// one that may not decode, and the study's generator always says.
//
// The rule is the one a client expects: the codec is used when its token (or
// "*" standing for it) carries a weight above zero. An unparsable member is
// skipped, never fatal; a malformed header therefore degrades to identity,
// which every client can read.
func (c *codec) accepts(header string) bool {
	if c == nil {
		return false
	}
	if header == "" {
		return false
	}
	named, star := false, false
	var namedQ, starQ float64
	for member := range strings.SplitSeq(header, ",") {
		token, params, _ := strings.Cut(member, ";")
		token = strings.TrimSpace(token)
		if token == "" {
			continue
		}
		q, ok := weight(params)
		if !ok {
			continue
		}
		switch {
		case strings.EqualFold(token, c.token):
			named, namedQ = true, q
		case token == "*":
			star, starQ = true, q
		}
	}
	if named {
		return namedQ > 0
	}
	return star && starQ > 0
}

// weight reads the q parameter of one member; absent is 1. Only "q" is
// looked at, and a value outside 0..1 or with more than three decimals is
// clamped rather than refused, as the grammar of §12.4.2 bounds it.
func weight(params string) (float64, bool) {
	q := 1.0
	for p := range strings.SplitSeq(params, ";") {
		name, value, found := strings.Cut(strings.TrimSpace(p), "=")
		if !found || !strings.EqualFold(strings.TrimSpace(name), "q") {
			continue
		}
		value = strings.TrimSpace(value)
		if len(value) > 5 { // "0.123" is the longest the grammar allows
			return 0, false
		}
		f, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return 0, false
		}
		q = min(max(f, 0), 1)
	}
	return q, true
}
