package main

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"io"
	"testing"
)

// TestEncodings: under each protocol and each coding, buffered and in
// flight, every answer is decoded before it is checked. An answer in clear
// is ok and counted apart from the compressed ones; a body cut short and a
// body in a coding the request did not ask for are wrong answers; a raw
// deflate body is decoded and counted.
func TestEncodings(t *testing.T) {
	protos(t, func(t *testing.T, proto string) { //nolint:thelper // a sub-test body
		for _, encoding := range []string{"identity", codingGzip, codingDeflate} {
			for _, stream := range []bool{false, true} {
				name := encoding
				if stream {
					name += "/stream"
				}
				t.Run(name, func(t *testing.T) {
					c := testCampaign(t, proto, "https://unused", "-scenario", "unit", "-rate", "100", "-duration", "500ms", "-conns", "4", "-encoding", encoding)
					draw := c.scenario.draw(c.seed, c.keyspace)
					srv := sleepServer(t, proto, 0, map[int64]fault{
						draw(10).key: inClear, draw(20).key: truncated, draw(30).key: unasked, draw(40).key: rawDeflate,
					})
					srv.stream.Store(stream)
					c.url = srv.URL
					rep := runStage(t, c)
					t.Logf("ok %d of %d bad_key %d compressed %.1f %% wire %.1f B ratio %.2f deflate_raw %d", rep.OK, rep.Sent, rep.BadKey, rep.Compressed, rep.WireBytes, rep.Ratio, rep.DeflateRaw)
					if rep.BadKey != 2 || rep.OK != rep.Sent-2 || rep.Errors+rep.TimedOut != 0 {
						t.Fatalf("bad_key %d ok %d of %d errors %d timed out %d, want the cut and the unasked bodies wrong, the rest ok", rep.BadKey, rep.OK, rep.Sent, rep.Errors, rep.TimedOut)
					}
					if rep.WireBytes <= 0 {
						t.Fatalf("wire %.1f bytes per answer", rep.WireBytes)
					}
					wantCompressed, wantRaw := 0.0, 0
					if encoding != "identity" {
						wantCompressed = 100 * float64(rep.OK-1) / float64(rep.OK) // all but the one in clear
					}
					if encoding == codingDeflate {
						wantRaw = 1
					}
					if rep.Compressed != wantCompressed || rep.DeflateRaw != wantRaw {
						t.Fatalf("compressed %.2f %% deflate_raw %d, want %.2f %% and %d", rep.Compressed, rep.DeflateRaw, wantCompressed, wantRaw)
					}
					if (rep.Ratio > 0) != (encoding != "identity") {
						t.Fatalf("ratio %.2f under %s", rep.Ratio, encoding)
					}
					if rep.Encoding != encoding {
						t.Fatalf("encoding %q published, want %q", rep.Encoding, encoding)
					}
				})
			}
		}
	})
}

// TestDecode: what decode accepts and refuses, on a document the size of
// a list page, where compression pays.
func TestDecode(t *testing.T) {
	doc := listPage(1, 1000)
	enc := func(f func(io.Writer) io.WriteCloser) []byte {
		var buf bytes.Buffer
		zw := f(&buf)
		_, _ = zw.Write(doc)
		_ = zw.Close()
		return buf.Bytes()
	}
	gz := enc(func(w io.Writer) io.WriteCloser { return gzip.NewWriter(w) })
	zl := enc(func(w io.Writer) io.WriteCloser { return zlib.NewWriter(w) })
	raw := enc(func(w io.Writer) io.WriteCloser { fw, _ := flate.NewWriter(w, flate.DefaultCompression); return fw })
	for _, tc := range []struct {
		name, asked, coding string
		body                []byte
		ok, compressed, raw bool
	}{
		{"identity asked, clear", "", "", doc, true, false, false},
		{"identity named", "", "identity", doc, true, false, false},
		{"gzip asked, clear", codingGzip, "", doc, true, false, false},
		{"gzip", codingGzip, "gzip", gz, true, true, false},
		{"gzip, any case", codingGzip, "GZip", gz, true, true, false},
		{"deflate in zlib", codingDeflate, "deflate", zl, true, true, false},
		{"deflate raw", codingDeflate, "deflate", raw, true, true, true},
		{"gzip unasked", "", "gzip", gz, false, false, false},
		{"deflate when gzip asked", codingGzip, "deflate", zl, false, false, false},
		{"gzip cut", codingGzip, "gzip", gz[:len(gz)-4], false, false, false},
		{"zlib cut", codingDeflate, "deflate", zl[:len(zl)/2], false, false, false},
		{"gzip header on a clear body", codingGzip, "gzip", doc, false, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, err := decode(tc.asked, tc.coding, tc.body)
			if (err == nil) != tc.ok {
				t.Fatalf("err %v, want ok %v", err, tc.ok)
			}
			if !tc.ok {
				return
			}
			if !bytes.Equal(d.doc, doc) || d.compressed != tc.compressed || d.raw != tc.raw {
				t.Fatalf("%d bytes compressed %v raw %v, want the document, %v, %v", len(d.doc), d.compressed, d.raw, tc.compressed, tc.raw)
			}
		})
	}
	if r := float64(len(doc)) / float64(len(gz)); r < 3 {
		t.Fatalf("a list page compresses %.1f×, want a document worth compressing", r)
	}
}
