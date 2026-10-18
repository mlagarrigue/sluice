package main

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"io"
	"strconv"
	"strings"
	"sync"

	"github.com/mlagarrigue/sluice"
)

// Axis F of the internal study: the buffered body of every answer, encoded
// with gzip or deflate from the standard library when the client accepts it.
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
)

// codec is one value of -compression: the token it answers to in
// Accept-Encoding and Content-Encoding, and the level it encodes at.
type codec struct {
	token string // "gzip" or "deflate"; "" is identity
	level int
	pool  sync.Pool
}

// lookupCodec returns the codec the flag names, or nil for none.
func lookupCodec(name string) *codec {
	switch name {
	case codecGzip1:
		return &codec{token: "gzip", level: gzip.BestSpeed}
	case codecGzip6:
		return &codec{token: "gzip", level: gzip.DefaultCompression}
	case codecDeflate6:
		return &codec{token: "deflate", level: flate.DefaultCompression}
	}
	return nil
}

// resetWriter is what gzip.Writer and flate.Writer have in common: a reset
// re-targets the encoder without rebuilding its tables, and a flush ends the
// current deflate block so that everything written so far can be decoded.
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

func (c *codec) newWriter(w io.Writer) resetWriter {
	switch c.token {
	case "gzip":
		zw, err := gzip.NewWriterLevel(w, c.level)
		if err != nil {
			panic(err) // the levels are constants of this file
		}
		return zw
	case "deflate":
		zw, err := flate.NewWriter(w, c.level)
		if err != nil {
			panic(err)
		}
		return zw
	}
	panic("codec without a token")
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
