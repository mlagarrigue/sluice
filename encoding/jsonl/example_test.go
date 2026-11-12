package jsonl_test

import (
	"fmt"
	"os"
	"strings"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/encoding/jsonl"
)

type order struct {
	ID    int     `json:"id"`
	Total float64 `json:"total"`
}

// Read JSON Lines in batches, double each total, and write the result back
// as JSON Lines, one write per batch.
func ExampleRead() {
	in := strings.NewReader("{\"id\":1,\"total\":40}\n{\"id\":2,\"total\":180.5}\n{\"id\":3,\"total\":95}\n")
	src := jsonl.Read[order](in, jsonl.ReadConfig{BatchRecords: 2})
	w := jsonl.NewWriter[order](os.Stdout)
	doubled := sluice.Convert(src.Stream(), func(o order) order {
		o.Total *= 2
		return o
	})
	var werr error
	for b := range doubled {
		if werr = w.Write(b); werr != nil {
			break
		}
	}
	if err := src.Err(); err != nil {
		fmt.Println("read:", err)
	}
	if werr != nil {
		fmt.Println("write:", werr)
	}
	// Output:
	// {"id":1,"total":80}
	// {"id":2,"total":361}
	// {"id":3,"total":190}
}

// A line that is not one JSON value stops the read, and the error names it.
func ExampleRead_error() {
	in := strings.NewReader("{\"id\":1}\n\n{\"id\":3}\n")
	vs, err := jsonl.Read[order](in, jsonl.ReadConfig{}).Collect()
	fmt.Println(len(vs), err)
	// Output:
	// 1 jsonl: line 2 is empty
}

// A Writer writes one value per line, without escaping <, > and &.
func ExampleNewWriter() {
	w := jsonl.NewWriter[map[string]string](os.Stdout)
	err := w.Write(sluice.Batch[map[string]string]{Items: []map[string]string{
		{"query": "a < b && c > d"},
		{"query": "x"},
	}})
	fmt.Println(err)
	// Output:
	// {"query":"a < b && c > d"}
	// {"query":"x"}
	// <nil>
}
