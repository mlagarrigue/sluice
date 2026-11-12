package jsonl_test

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/mlagarrigue/sluice/encoding/jsonl"
)

const benchRecords = 10_000

type benchEvent struct {
	ID       int     `json:"id"`
	Customer string  `json:"customer"`
	Total    float64 `json:"total"`
}

func benchInput() []byte {
	var buf bytes.Buffer
	for i := range benchRecords {
		fmt.Fprintf(&buf, "{\"id\":%d,\"customer\":\"customer %d\",\"total\":%d.%02d}\n", i, i%977, i*7, i%100)
	}
	return buf.Bytes()
}

// BenchmarkRead is Read's cost per line, to set against
// BenchmarkReadBareLoop: the gap is what the batch and the bound add.
func BenchmarkRead(b *testing.B) {
	in := benchInput()
	r := bytes.NewReader(in)
	b.SetBytes(int64(len(in)))
	b.ReportAllocs()
	for b.Loop() {
		r.Reset(in)
		n := 0
		for batch := range jsonl.Read[benchEvent](r, jsonl.ReadConfig{}).Stream() {
			n += len(batch.Items)
		}
		if n != benchRecords {
			b.Fatal(n)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*benchRecords), "ns/record")
}

// BenchmarkReadBareBatchLoop is the bare loop that also keeps each batch's
// values in a reused slice, as a batch does: the gap from
// BenchmarkReadBareLoop is the cost of holding a batch, the gap to
// BenchmarkRead what this package adds on top of it.
func BenchmarkReadBareBatchLoop(b *testing.B) {
	in := benchInput()
	r := bytes.NewReader(in)
	b.SetBytes(int64(len(in)))
	b.ReportAllocs()
	items := make([]benchEvent, 0, 1024)
	for b.Loop() {
		r.Reset(in)
		sc := bufio.NewScanner(r)
		n := 0
		for sc.Scan() {
			var v benchEvent
			if err := json.Unmarshal(sc.Bytes(), &v); err != nil {
				b.Fatal(err)
			}
			items = append(items, v)
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
// input: a bufio.Scanner's lines, each given to json.Unmarshal.
func BenchmarkReadBareLoop(b *testing.B) {
	in := benchInput()
	r := bytes.NewReader(in)
	b.SetBytes(int64(len(in)))
	b.ReportAllocs()
	for b.Loop() {
		r.Reset(in)
		sc := bufio.NewScanner(r)
		n := 0
		for sc.Scan() {
			var v benchEvent
			if err := json.Unmarshal(sc.Bytes(), &v); err != nil {
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
