package jsonl_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/encoding/jsonl"
)

type event struct {
	ID   int               `json:"id"`
	Tags []string          `json:"tags,omitempty"`
	Meta map[string]string `json:"meta,omitempty"`
}

type counting struct {
	r io.Reader
	n int
}

func (c *counting) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += n
	return n, err
}

type endless byte

func (e endless) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = byte(e)
	}
	return len(p), nil
}

func sizes[T any](src sluice.Source[T]) ([]int, []T) {
	var ns []int
	var all []T
	for b := range src.Stream() {
		ns = append(ns, len(b.Items))
		all = append(all, b.Items...)
	}
	return ns, all
}

func TestReadBatchesFullThenShort(t *testing.T) {
	var in strings.Builder
	for i := range 2500 {
		fmt.Fprintf(&in, "{\"id\":%d,\"tags\":[\"t%d\"]}\n", i, i)
	}
	src := jsonl.Read[event](strings.NewReader(in.String()), jsonl.ReadConfig{BatchRecords: 1000})
	ns, all := sizes(src)
	if err := src.Err(); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(ns) != "[1000 1000 500]" {
		t.Fatalf("batch sizes %v, want [1000 1000 500]", ns)
	}
	if all[0].ID != 0 || all[1999].Tags[0] != "t1999" || all[2499].ID != 2499 {
		t.Fatalf("values kept past their batch changed: %v %v %v", all[0], all[1999], all[2499])
	}
}

func TestReadDecodesIntoAZeroValue(t *testing.T) {
	// The batch's slice is reused: a field absent from a line must not
	// carry the previous value's.
	in := "{\"id\":1,\"meta\":{\"a\":\"1\"}}\n{\"id\":2}\n{\"id\":3,\"tags\":[\"x\"]}\n{\"id\":4}\n"
	src := jsonl.Read[event](strings.NewReader(in), jsonl.ReadConfig{BatchRecords: 2})
	var got []event
	for b := range src.Stream() {
		got = append(got, b.Items...)
	}
	if src.Err() != nil || got[1].Meta != nil || got[3].Tags != nil {
		t.Fatalf("got %+v, err %v", got, src.Err())
	}
}

func TestReadLineEnds(t *testing.T) {
	// "\r\n" ends a line, and the last line may lack its "\n".
	src := jsonl.Read[int](strings.NewReader("1\r\n2\n3"), jsonl.ReadConfig{})
	vs, err := src.Collect()
	if err != nil || fmt.Sprint(vs) != "[1 2 3]" {
		t.Fatalf("got %v, err %v", vs, err)
	}
	vs, err = jsonl.Read[int](strings.NewReader(""), jsonl.ReadConfig{}).Collect()
	if err != nil || len(vs) != 0 {
		t.Fatalf("empty input: %v, %v", vs, err)
	}
}

func TestReadIsStrict(t *testing.T) {
	for name, tc := range map[string]struct {
		in   string
		line string
	}{
		"empty line":       {"1\n2\n\n3\n", "line 3 "},
		"blank line":       {"1\n \r\n3\n", "line 2 "},
		"two values":       {"1\n2 3\n", "line 2:"},
		"syntax":           {"1\n{\"id\":\n", "line 2:"},
		"type":             {"1\n\"x\"\n", "line 2:"},
		"value over lines": {"[1,\n2]\n", "line 1:"},
	} {
		vs, err := jsonl.Read[int](strings.NewReader(tc.in), jsonl.ReadConfig{}).Collect()
		if err == nil || !strings.Contains(err.Error(), tc.line) {
			t.Errorf("%s: err %v, want one naming %q", name, err, tc.line)
		}
		if name == "empty line" && fmt.Sprint(vs) != "[1 2]" {
			t.Errorf("%s: values before the failure %v, want [1 2]", name, vs)
		}
	}
	_, err := jsonl.Read[int](strings.NewReader("\"x\"\n"), jsonl.ReadConfig{}).Collect()
	var te *json.UnmarshalTypeError
	if !errors.As(err, &te) {
		t.Fatalf("a type mismatch is not a *json.UnmarshalTypeError: %v", err)
	}
}

func TestReadBoundStopsAnEndlessLine(t *testing.T) {
	const bound = 1000
	in := &counting{r: io.MultiReader(strings.NewReader("1\n\""), endless('a'))}
	vs, err := jsonl.Read[json.RawMessage](in, jsonl.ReadConfig{MaxRecordBytes: bound}).Collect()
	if !errors.Is(err, jsonl.ErrRecordTooLong) || !strings.Contains(err.Error(), "line 2 ") || len(vs) != 1 {
		t.Fatalf("got %d values, err %v; want 1 and line 2 too long", len(vs), err)
	}
	if in.n > 2+bound+4096 {
		t.Fatalf("read %d bytes for a bound of %d", in.n, bound)
	}
}

func TestReadBoundIsPerLine(t *testing.T) {
	const bound = 5000 // past bufio's buffer, so long lines are assembled
	line := "\"" + strings.Repeat("x", bound-2) + "\"\n"
	vs, err := jsonl.Read[string](strings.NewReader(strings.Repeat(line, 20)), jsonl.ReadConfig{MaxRecordBytes: bound}).Collect()
	if err != nil || len(vs) != 20 || len(vs[19]) != bound-2 {
		t.Fatalf("lines at the bound: %d values, err %v", len(vs), err)
	}
	over := "\"" + strings.Repeat("y", bound-1) + "\"\n"
	_, err = jsonl.Read[string](strings.NewReader(line+over), jsonl.ReadConfig{MaxRecordBytes: bound}).Collect()
	if !errors.Is(err, jsonl.ErrRecordTooLong) {
		t.Fatalf("a line one byte over: err %v", err)
	}
}

func TestReadRefusesABadConfigBeforeReading(t *testing.T) {
	in := &counting{r: strings.NewReader("1\n")}
	src := jsonl.Read[int](in, jsonl.ReadConfig{BatchRecords: -1, MaxRecordBytes: -1})
	err := src.Err()
	if err == nil || !strings.Contains(err.Error(), "BatchRecords") || !strings.Contains(err.Error(), "MaxRecordBytes") {
		t.Fatalf("err %v, want both fields named", err)
	}
	if ns, _ := sizes(src); len(ns) != 0 || in.n != 0 {
		t.Fatalf("a refused config read %d bytes into %v", in.n, ns)
	}
}

func TestReadStopsWhenTheConsumerDoes(t *testing.T) {
	in := &counting{r: endless('\n')}
	src := jsonl.Read[int](io.MultiReader(strings.NewReader(strings.Repeat("1\n", 10)), in), jsonl.ReadConfig{BatchRecords: 2})
	for range src.Stream() {
		break
	}
	if in.n > 4096 {
		t.Fatalf("read %d bytes past the first batch", in.n)
	}
}

type recorder struct {
	calls []string
	err   error
}

func (r *recorder) Write(p []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	r.calls = append(r.calls, string(p))
	return len(p), nil
}

func TestWriterWritesEachBatchInOneCall(t *testing.T) {
	rec := &recorder{}
	w := jsonl.NewWriter[event](rec)
	if err := w.Write(sluice.Batch[event]{Items: []event{{ID: 1, Tags: []string{"<a&b>"}}, {ID: 2}}}); err != nil {
		t.Fatal(err)
	}
	want := "{\"id\":1,\"tags\":[\"<a&b>\"]}\n{\"id\":2}\n"
	if len(rec.calls) != 1 || rec.calls[0] != want {
		t.Fatalf("after one batch the writer holds %q, want one call %q", rec.calls, want)
	}
	if err := w.Write(sluice.Batch[event]{}); err != nil || len(rec.calls) != 1 {
		t.Fatalf("an empty batch wrote %q, err %v", rec.calls, err)
	}
	rec.err = errors.New("disk full")
	if err := w.Write(sluice.Batch[event]{Items: []event{{ID: 3}}}); err == nil {
		t.Fatal("the underlying error was not returned")
	}
	rec.err = nil
	if err := w.Write(sluice.Batch[event]{Items: []event{{ID: 4}}}); err == nil {
		t.Fatal("a Write after a failed one succeeded")
	}
}

func TestWriterRefusesAnUnencodableBatchWhole(t *testing.T) {
	rec := &recorder{}
	w := jsonl.NewWriter[any](rec)
	err := w.Write(sluice.Batch[any]{Items: []any{1, make(chan int), 3}})
	if err == nil || len(rec.calls) != 0 {
		t.Fatalf("err %v, writes %q; want an error and nothing written", err, rec.calls)
	}
	// Not the writer's failure: the next batch goes through.
	if err := w.Write(sluice.Batch[any]{Items: []any{4}}); err != nil || rec.calls[0] != "4\n" {
		t.Fatalf("next batch: %q, err %v", rec.calls, err)
	}
}

func TestRoundTrip(t *testing.T) {
	in := []event{{ID: 1, Tags: []string{"a\nb"}}, {ID: 2, Meta: map[string]string{"k": "\"v\""}}}
	var buf bytes.Buffer
	if err := jsonl.NewWriter[event](&buf).Write(sluice.Batch[event]{Items: in}); err != nil {
		t.Fatal(err)
	}
	if strings.Count(buf.String(), "\n") != 2 {
		t.Fatalf("a value spans lines: %q", buf.String())
	}
	out, err := jsonl.Read[event](&buf, jsonl.ReadConfig{}).Collect()
	if err != nil || fmt.Sprintf("%+v", out) != fmt.Sprintf("%+v", in) {
		t.Fatalf("round trip gave %+v, err %v", out, err)
	}
}
