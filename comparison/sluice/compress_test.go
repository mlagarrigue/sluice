package main

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"io"
	"testing"
	"unicode/utf8"
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

func TestEncodeRoundTrip(t *testing.T) {
	body := bytes.Repeat([]byte(`{"id":1,"total":7,"label":"order-1"},`), 1000)
	for _, name := range []string{codecGzip1, codecGzip6, codecDeflate6} {
		c := lookupCodec(name)
		for range 3 { // the second pass reuses the pooled writer
			enc, err := c.encode(body)
			if err != nil {
				t.Fatal(err)
			}
			if len(enc) >= len(body) {
				t.Fatalf("%s: %d bytes, not smaller than %d", name, len(enc), len(body))
			}
			var r io.ReadCloser
			if c.token == "gzip" {
				r, err = gzip.NewReader(bytes.NewReader(enc))
				if err != nil {
					t.Fatal(err)
				}
			} else {
				r = flate.NewReader(bytes.NewReader(enc))
			}
			got, err := io.ReadAll(r)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, body) {
				t.Fatalf("%s: round trip differs", name)
			}
		}
	}
}
