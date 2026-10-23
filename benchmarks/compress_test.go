package benchmarks

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"testing"
)

// Axis F of the internal study, in process: what each codec and level costs
// on the document the list-1000 scenario answers, before any byte reaches a
// socket. The study's server (comparison/sluice, on main) encodes the same
// way — one stdlib writer per level, reset per response, the whole buffered
// body in one write — so this is the stage's own price, with the ratio it
// buys. What the ratio is worth depends on the link, which only the study's
// throttled network can say (the comparison study); nothing here is a result.
//
// Pair each codec against none with versus.sh:
//
//	internal/bench/scripts/versus.sh 'BenchmarkListCompress/none' 'BenchmarkListCompress/gzip6' 15

// listRow is the row of bench_orders as internal/bench/cmd/pgfixtures writes it:
// id g, total g*7, label "order-g".
type listRow struct {
	ID    int64  `json:"id"`
	Total int64  `json:"total"`
	Label string `json:"label"`
}

type listDocument struct {
	From  int64     `json:"from"`
	Limit int       `json:"limit"`
	Items []listRow `json:"items"`
}

// listDoc1000 is the list-1000 answer from key 50000, encoded once.
func listDoc1000(tb testing.TB) []byte {
	tb.Helper()
	const from = 50000
	doc := listDocument{From: from, Limit: 1000, Items: make([]listRow, 1000)}
	for i := range doc.Items {
		g := int64(from + 1 + i)
		doc.Items[i] = listRow{ID: g, Total: g * 7, Label: fmt.Sprintf("order-%d", g)}
	}
	b, err := json.Marshal(doc)
	if err != nil {
		tb.Fatal(err)
	}
	return b
}

type resetWriter interface {
	io.WriteCloser
	Reset(io.Writer)
}

var listCodecs = []struct {
	name string
	new  func(io.Writer) resetWriter
}{
	{"none", nil},
	{"gzip1", func(w io.Writer) resetWriter { zw, _ := gzip.NewWriterLevel(w, gzip.BestSpeed); return zw }},
	{"gzip6", func(w io.Writer) resetWriter { zw, _ := gzip.NewWriterLevel(w, gzip.DefaultCompression); return zw }},
	{"deflate6", func(w io.Writer) resetWriter { zw, _ := flate.NewWriter(w, flate.DefaultCompression); return zw }},
}

// BenchmarkListCompress encodes the list-1000 document with each codec. The
// "none" row is the copy the others replace — one write of the body into a
// fresh buffer — so the ratio against it is the codec alone. ratio is
// bytes in over bytes out.
func BenchmarkListCompress(b *testing.B) {
	body := listDoc1000(b)
	for _, c := range listCodecs {
		b.Run(c.name, func(b *testing.B) {
			var out int
			var buf bytes.Buffer
			var zw resetWriter
			if c.new != nil {
				zw = c.new(io.Discard)
			}
			b.SetBytes(int64(len(body)))
			b.ReportAllocs()
			for b.Loop() {
				buf = bytes.Buffer{}
				if zw == nil {
					buf.Grow(len(body))
					buf.Write(body)
				} else {
					buf.Grow(len(body) / 4)
					zw.Reset(&buf)
					if _, err := zw.Write(body); err != nil {
						b.Fatal(err)
					}
					if err := zw.Close(); err != nil {
						b.Fatal(err)
					}
				}
				out = buf.Len()
			}
			b.ReportMetric(float64(len(body))/float64(out), "ratio")
			b.ReportMetric(float64(out), "B/resp")
		})
	}
}

// TestListCompressRoundTrips is the sanity check behind the figure: every
// codec's output decodes back to the document.
func TestListCompressRoundTrips(t *testing.T) {
	body := listDoc1000(t)
	for _, c := range listCodecs[1:] {
		var buf bytes.Buffer
		zw := c.new(&buf)
		if _, err := zw.Write(body); err != nil {
			t.Fatal(err)
		}
		if err := zw.Close(); err != nil {
			t.Fatal(err)
		}
		var r io.Reader
		if c.name == "deflate6" {
			r = flate.NewReader(&buf)
		} else {
			zr, err := gzip.NewReader(&buf)
			if err != nil {
				t.Fatal(err)
			}
			r = zr
		}
		got, err := io.ReadAll(r)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, body) {
			t.Fatalf("%s: round trip differs", c.name)
		}
	}
}
