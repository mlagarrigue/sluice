package main

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"fmt"
	"io"
	"slices"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/andybalholm/brotli"
	"github.com/klauspost/compress/zstd"

	"github.com/mlagarrigue/sluice"
)

func TestAcceptEncoding(t *testing.T) {
	g := lookupCodec(codecGzip6)
	d := lookupCodec(codecDeflate6)
	cases := []struct {
		header string
		gzip   bool
		defl   bool
	}{
		{"", false, false},
		{"gzip", true, false},
		{"gzip, deflate", true, true},
		{"GZIP", true, false},
		{"deflate;q=0.5, gzip;q=1.0", true, true},
		{"gzip;q=0", false, false},
		{"gzip;q=0.000", false, false},
		{"gzip; q=0.001", true, false},
		{"*", true, true},
		{"*;q=0", false, false},
		{"*, gzip;q=0", false, true},
		{"identity", false, false},
		{"identity;q=0", false, false},
		{"br", false, false},
		{"gzip;q=abc", false, false},
		{"gzip;q=1.00000", false, false},
		{"gzip;level=9", true, false},
		{"gzip;Q=0", false, false},
		{",,, gzip ,,", true, false},
		{"gzip;q=0, *", false, true},
		{"gzip;q=0, gzip", true, false},
		{";q=1", false, false},
	}
	for _, c := range cases {
		if got := g.accepts(c.header); got != c.gzip {
			t.Errorf("gzip accepts(%q) = %v, want %v", c.header, got, c.gzip)
		}
		if got := d.accepts(c.header); got != c.defl {
			t.Errorf("deflate accepts(%q) = %v, want %v", c.header, got, c.defl)
		}
	}
	var none *codec
	if none.accepts("") {
		t.Error("no codec accepts")
	}
}

func FuzzAcceptEncoding(f *testing.F) {
	for _, s := range []string{"", "gzip", "gzip;q=0.5, *;q=0", "deflate, ;=;;,q=", "*;q=1e308", "gzip;q=-0"} {
		f.Add(s)
	}
	g := lookupCodec(codecGzip1)
	f.Fuzz(func(t *testing.T, header string) {
		// Any input is answered, with one invariant: an unparsable or
		// non-UTF-8 header never panics and the answer is deterministic.
		a, b := g.accepts(header), g.accepts(header)
		if a != b {
			t.Fatalf("accepts(%q) not deterministic", header)
		}
		_ = utf8.ValidString(header)
	})
}

// openDecoder opens the reader a client would for a Content-Encoding token;
// gzip and deflate read their header here, so r must already hold it.
func openDecoder(token string, r io.Reader) (io.Reader, error) {
	switch token {
	case "gzip":
		return gzip.NewReader(r)
	case "deflate":
		return zlib.NewReader(r)
	case "br":
		return brotli.NewReader(r), nil
	case "zstd":
		// One goroutine: the stream is decoded on the caller's, as the
		// generator does.
		zr, err := zstd.NewReader(r, zstd.WithDecoderConcurrency(1))
		if err != nil {
			return nil, err
		}
		return zr.IOReadCloser(), nil
	}
	return nil, fmt.Errorf("no decoder for %q", token)
}

func decoder(tb testing.TB, token string, r io.Reader) io.Reader {
	tb.Helper()
	zr, err := openDecoder(token, r)
	if err != nil {
		tb.Fatal(err)
	}
	return zr
}

// encodedCodecs is every codec but identity.
func encodedCodecs() []string { return codecNames[1:] }

func TestEncodeRoundTrip(t *testing.T) {
	body := bytes.Repeat([]byte(`{"id":1,"total":7,"label":"order-1"},`), 1000)
	for _, name := range encodedCodecs() {
		c := lookupCodec(name)
		for range 3 { // the second pass reuses the pooled writer
			enc, err := c.encode(body)
			if err != nil {
				t.Fatal(err)
			}
			if len(enc) >= len(body) {
				t.Fatalf("%s: %d bytes, not smaller than %d", name, len(enc), len(body))
			}
			got, err := io.ReadAll(decoder(t, c.token, bytes.NewReader(enc)))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, body) {
				t.Fatalf("%s: round trip differs", name)
			}
		}
	}
}

// TestEncodeStreamFlushPerBatch is the property the stream variant exists
// for (study, §2.3 bis): once the fragment a batch came out as is on the
// wire, the client decodes that batch whole, without waiting for the next
// one. Each fragment is released into a pipe only after the previous batch
// was read back; a codec that kept bytes in its encoder would leave the
// reader waiting, and the step times out.
func TestEncodeStreamFlushPerBatch(t *testing.T) {
	rows := syntheticRows(300)
	var batches [][]byte
	for i := 0; i < len(rows); i += 100 {
		batches = append(batches, appendRows(nil, rows[i:i+100], i == 0))
	}
	in := func(yield func(sluice.Batch[[]byte]) bool) {
		for _, b := range batches {
			if !yield(sluice.Batch[[]byte]{Items: [][]byte{b[:len(b)/2], b[len(b)/2:]}}) {
				return
			}
		}
	}
	for _, name := range encodedCodecs() {
		t.Run(name, func(t *testing.T) {
			c := lookupCodec(name)
			var frags [][]byte
			for batch := range c.encodeStream(in) {
				frags = append(frags, batch.Items...)
			}
			if len(frags) != len(batches)+1 { // one per batch, then the end of the stream
				t.Fatalf("%d fragments for %d batches", len(frags), len(batches))
			}
			pr, pw := io.Pipe()
			release := make(chan []byte)
			go func() {
				for f := range release {
					if _, err := pw.Write(f); err != nil {
						return
					}
				}
				pw.Close()
			}()
			got := make(chan []byte)
			go func() {
				defer close(got)
				r, err := openDecoder(c.token, pr)
				if err != nil {
					got <- []byte(err.Error())
					return
				}
				for _, want := range batches {
					b := make([]byte, len(want))
					if _, err := io.ReadFull(r, b); err != nil {
						return
					}
					got <- b
				}
				if rest, err := io.ReadAll(r); err != nil || len(rest) != 0 {
					got <- []byte("trailing")
				}
			}()
			for i, want := range batches {
				release <- frags[i]
				select {
				case b := <-got:
					if !bytes.Equal(b, want) {
						t.Fatalf("batch %d decodes to something else", i)
					}
				case <-time.After(5 * time.Second):
					t.Fatalf("batch %d not decodable from its own fragment", i)
				}
			}
			release <- frags[len(batches)]
			close(release)
			if b, ok := <-got; ok {
				t.Fatalf("after the last batch: %q", b)
			}
		})
	}
}

// BenchmarkListCompress is axis F's cost on the server, the list 1 000 of
// the shape of bench_orders encoded as encode.go writes it: buffered (the
// whole document, one Write and Close) and stream (the three batches the
// server writes for it, listBatchRows being above the list, each flushed).
// The ratio is encoded bytes over the document's. Figures in done.md only.
func BenchmarkListCompress(b *testing.B) {
	rows := syntheticRows(listLimit)
	head := appendListHead(nil, 1, listLimit)
	body := appendRows(nil, rows, true)
	tail := []byte("]}")
	doc := slices.Concat(head, body, tail)
	parts := [][]byte{head, body, tail}
	in := func(yield func(sluice.Batch[[]byte]) bool) {
		for _, p := range parts {
			if !yield(sluice.Batch[[]byte]{Items: [][]byte{p}}) {
				return
			}
		}
	}
	for _, name := range encodedCodecs() {
		c := lookupCodec(name)
		b.Run(name+"/buffered", func(b *testing.B) {
			b.SetBytes(int64(len(doc)))
			b.ReportAllocs()
			var n int
			for b.Loop() {
				enc, err := c.encode(doc)
				if err != nil {
					b.Fatal(err)
				}
				n = len(enc)
			}
			b.ReportMetric(float64(n)/float64(len(doc)), "ratio")
		})
		b.Run(name+"/stream", func(b *testing.B) {
			b.SetBytes(int64(len(doc)))
			b.ReportAllocs()
			var n int
			for b.Loop() {
				n = 0
				for batch := range c.encodeStream(in) {
					n += len(batch.Items[0])
				}
			}
			b.ReportMetric(float64(n)/float64(len(doc)), "ratio")
		})
	}
}
