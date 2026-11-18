package main

import (
	"os"
	"strconv"
	"strings"
	"testing"
)

// R reads Accept-Encoding as the sluice program does: the gzip column of
// comparison/accept-encoding.tsv, the table every reading of the study
// plays. R read every parameter but took a weight it could not parse as
// the member's last word, and kept a weight past 1 unclamped.
func TestAcceptsGzipPlaysTheStudysTable(t *testing.T) {
	b, err := os.ReadFile("../../accept-encoding.tsv")
	if err != nil {
		t.Fatal(err)
	}
	rows := 0
	for line := range strings.Lines(string(b)) {
		line = strings.TrimRight(line, "\n")
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		f := strings.Split(line, "\t")
		h, err := strconv.Unquote(f[0])
		if len(f) != 3 || err != nil {
			t.Fatalf("accept-encoding.tsv: %q", line)
		}
		rows++
		if got, want := acceptsGzip(h), f[1] == "true"; got != want {
			t.Errorf("acceptsGzip(%q) = %v, want %v", h, got, want)
		}
	}
	if rows == 0 {
		t.Fatal("accept-encoding.tsv holds no row")
	}
}
