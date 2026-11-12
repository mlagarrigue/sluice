package csvstream_test

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"testing"

	"github.com/mlagarrigue/sluice/encoding/csvstream"
)

const benchRecords = 10_000

func benchInput() []byte {
	var buf bytes.Buffer
	for i := range benchRecords {
		fmt.Fprintf(&buf, "%d,customer %d,%d.%02d\n", i, i%977, i*7, i%100)
	}
	return buf.Bytes()
}

// BenchmarkRead is Read's cost per record, to set against
// BenchmarkReadBareLoop: the gap is what the batch and the bound add.
func BenchmarkRead(b *testing.B) {
	in := benchInput()
	r := bytes.NewReader(in)
	b.SetBytes(int64(len(in)))
	b.ReportAllocs()
	for b.Loop() {
		r.Reset(in)
		n := 0
		for batch := range csvstream.Read(r, csvstream.ReadConfig{}).Stream() {
			n += len(batch.Items)
		}
		if n != benchRecords {
			b.Fatal(n)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*benchRecords), "ns/record")
}

// BenchmarkReadBareBatchLoop is the bare loop that also keeps each batch's
// records in a reused slice, as a batch does: the gap from
// BenchmarkReadBareLoop is the cost of holding a batch (the garbage
// collector's write barriers on the stored records), the gap to
// BenchmarkRead what this package adds on top of it.
func BenchmarkReadBareBatchLoop(b *testing.B) {
	in := benchInput()
	r := bytes.NewReader(in)
	b.SetBytes(int64(len(in)))
	b.ReportAllocs()
	items := make([][]string, 0, 1024)
	for b.Loop() {
		r.Reset(in)
		cr := csv.NewReader(r)
		n := 0
		for {
			rec, err := cr.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
			items = append(items, rec)
			if len(items) == cap(items) {
				n += len(items)
				items = items[:0]
			}
		}
		n += len(items)
		items = items[:0]
		if n != benchRecords {
			b.Fatal(n)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*benchRecords), "ns/record")
}

// BenchmarkReadBareLoop is the standard library's own loop over the same
// input: a csv.Reader read to io.EOF.
func BenchmarkReadBareLoop(b *testing.B) {
	in := benchInput()
	r := bytes.NewReader(in)
	b.SetBytes(int64(len(in)))
	b.ReportAllocs()
	for b.Loop() {
		r.Reset(in)
		cr := csv.NewReader(r)
		n := 0
		for {
			_, err := cr.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
			n++
		}
		if n != benchRecords {
			b.Fatal(n)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*benchRecords), "ns/record")
}
