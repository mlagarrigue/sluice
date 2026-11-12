package csvstream_test

import (
	"fmt"
	"os"
	"strings"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/encoding/csvstream"
)

// Read a CSV file in batches, keep the records whose total passes 100, and
// write them back out as semicolon-separated values, one flush per batch.
func ExampleRead() {
	in := strings.NewReader("id,total\n1,40\n2,180\n3,95\n4,310\n")
	src := csvstream.Read(in, csvstream.ReadConfig{BatchRecords: 2})
	w, err := csvstream.NewWriter(os.Stdout, csvstream.WriteConfig{Comma: ';'})
	if err != nil {
		fmt.Println(err)
		return
	}
	big := sluice.Filter(src.Stream(), func(rec []string) bool {
		return rec[0] == "id" || len(rec[1]) == 3
	})
	var werr error
	for b := range big {
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
	// id;total
	// 2;180
	// 4;310
}

// A Writer quotes what needs quoting and flushes each batch as it is written.
func ExampleNewWriter() {
	w, err := csvstream.NewWriter(os.Stdout, csvstream.WriteConfig{})
	if err != nil {
		fmt.Println(err)
		return
	}
	err = w.Write(sluice.Batch[[]string]{Items: [][]string{
		{"id", "note"},
		{"1", "comma, inside"},
		{"2", `a "quoted" word`},
	}})
	fmt.Println(err)
	// Output:
	// id,note
	// 1,"comma, inside"
	// 2,"a ""quoted"" word"
	// <nil>
}
