package csvstream_test

import (
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/mlagarrigue/sluice"
	"github.com/mlagarrigue/sluice/encoding/csvstream"
)

// counting reports how many bytes were read from r.
type counting struct {
	r io.Reader
	n int
}

func (c *counting) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += n
	return n, err
}

// endless yields b forever.
type endless byte

func (e endless) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = byte(e)
	}
	return len(p), nil
}

func sizes[T any](t *testing.T, src sluice.Source[T]) ([]int, []T) {
	t.Helper()
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
		fmt.Fprintf(&in, "%d,name %d\n", i, i)
	}
	src := csvstream.Read(strings.NewReader(in.String()), csvstream.ReadConfig{BatchRecords: 1000})
	ns, all := sizes(t, src)
	if err := src.Err(); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(ns) != "[1000 1000 500]" {
		t.Fatalf("batch sizes %v, want [1000 1000 500]", ns)
	}
	// Records are kept past their batch: each is its own slice.
	if all[0][0] != "0" || all[1999][1] != "name 1999" || all[2499][0] != "2499" {
		t.Fatalf("records kept past their batch changed: %q %q %q", all[0], all[1999], all[2499])
	}
}

func TestReadZeroConfigIsDefaultBatch(t *testing.T) {
	src := csvstream.Read(strings.NewReader(strings.Repeat("a\n", sluice.DefaultBatchSize+1)), csvstream.ReadConfig{})
	ns, _ := sizes(t, src)
	if err := src.Err(); err != nil || fmt.Sprint(ns) != fmt.Sprint([]int{sluice.DefaultBatchSize, 1}) {
		t.Fatalf("sizes %v, err %v", ns, err)
	}
}

func TestReadEmptyInput(t *testing.T) {
	src := csvstream.Read(strings.NewReader(""), csvstream.ReadConfig{})
	ns, _ := sizes(t, src)
	if len(ns) != 0 || src.Err() != nil {
		t.Fatalf("empty input gave batches %v, err %v", ns, src.Err())
	}
}

func TestReadOptions(t *testing.T) {
	in := "# header comment\na; b;\"c;d\"\ne; f;g\"h\n"
	src := csvstream.Read(strings.NewReader(in), csvstream.ReadConfig{
		Comma: ';', Comment: '#', TrimLeadingSpace: true, LazyQuotes: true, FieldsPerRecord: 3,
	})
	recs, err := src.Collect()
	if err != nil {
		t.Fatal(err)
	}
	want := `[[a b c;d] [e f g"h]]`
	if fmt.Sprint(recs) != want {
		t.Fatalf("got %q, want %s", recs, want)
	}
}

func TestReadFieldCountIsAParseError(t *testing.T) {
	src := csvstream.Read(strings.NewReader("a,b\nc,d\ne\nf,g\n"), csvstream.ReadConfig{})
	recs, err := src.Collect()
	var pe *csv.ParseError
	if !errors.As(err, &pe) || !errors.Is(err, csv.ErrFieldCount) || pe.Line != 3 {
		t.Fatalf("err %v, want a field count ParseError on line 3", err)
	}
	// The records before the failure are delivered, not the ones after.
	if len(recs) != 2 {
		t.Fatalf("got %d records before the failure, want 2", len(recs))
	}
}

func TestReadSyntaxErrorNamesItsLine(t *testing.T) {
	src := csvstream.Read(strings.NewReader("a,b\nc,d\ne,\"f\"g\n"), csvstream.ReadConfig{})
	_, err := src.Collect()
	var pe *csv.ParseError
	if !errors.As(err, &pe) || !errors.Is(err, csv.ErrQuote) || pe.Line != 3 || pe.Column == 0 {
		t.Fatalf("err %v, want a quote ParseError on line 3 with its column", err)
	}
}

func TestReadBoundStopsAnEndlessRecord(t *testing.T) {
	const bound = 1000
	for name, r := range map[string]io.Reader{
		"unquoted": endless('a'),
		"quoted":   io.MultiReader(strings.NewReader("x,\""), endless('\n')),
	} {
		in := &counting{r: r}
		src := csvstream.Read(in, csvstream.ReadConfig{MaxRecordBytes: bound})
		_, err := src.Collect()
		if !errors.Is(err, csvstream.ErrRecordTooLong) {
			t.Fatalf("%s: err %v, want ErrRecordTooLong", name, err)
		}
		// One record's bound, plus the buffer csv.Reader reads ahead.
		if in.n > bound+4096 {
			t.Fatalf("%s: read %d bytes for a bound of %d", name, in.n, bound)
		}
	}
}

func TestReadBoundIsPerRecord(t *testing.T) {
	const bound = 64
	line := strings.Repeat("x", bound-1) + "\n" // exactly bound bytes
	in := strings.Repeat(line, 500)
	src := csvstream.Read(strings.NewReader(in), csvstream.ReadConfig{MaxRecordBytes: bound, BatchRecords: 100})
	recs, err := src.Collect()
	if err != nil || len(recs) != 500 {
		t.Fatalf("%d records at the bound: got %d, err %v", 500, len(recs), err)
	}
	// One byte over, after records that fit: refused, with its number.
	src = csvstream.Read(strings.NewReader(line+line+"y"+line), csvstream.ReadConfig{MaxRecordBytes: bound})
	recs, err = src.Collect()
	if !errors.Is(err, csvstream.ErrRecordTooLong) || !strings.Contains(err.Error(), "record 3 ") || len(recs) != 2 {
		t.Fatalf("got %d records, err %v; want 2 and record 3 too long", len(recs), err)
	}
}

func TestReadRefusesABadConfigBeforeReading(t *testing.T) {
	in := &counting{r: strings.NewReader("a,b\n")}
	src := csvstream.Read(in, csvstream.ReadConfig{BatchRecords: -1, MaxRecordBytes: -2, Comma: '"', Comment: '\n'})
	err := src.Err()
	if err == nil {
		t.Fatal("negative and invalid fields accepted")
	}
	for _, field := range []string{"BatchRecords", "MaxRecordBytes", "Comma", "Comment"} {
		if !strings.Contains(err.Error(), field) {
			t.Errorf("error %q does not name %s", err, field)
		}
	}
	if ns, _ := sizes(t, src); len(ns) != 0 || in.n != 0 {
		t.Fatalf("a refused config read %d bytes into %v", in.n, ns)
	}
	if err := csvstream.Read(strings.NewReader(""), csvstream.ReadConfig{Comma: ';', Comment: ';'}).Err(); err == nil {
		t.Fatal("Comment equal to Comma accepted")
	}
}

func TestReadStopsWhenTheConsumerDoes(t *testing.T) {
	in := &counting{r: endless('\n')}
	src := csvstream.Read(io.MultiReader(strings.NewReader(strings.Repeat("a\n", 10)), in), csvstream.ReadConfig{BatchRecords: 2})
	for range src.Stream() {
		break
	}
	if in.n > 4096 {
		t.Fatalf("read %d bytes past the first batch", in.n)
	}
}

// recorder keeps each Write call apart.
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

func TestWriterFlushesEachBatch(t *testing.T) {
	rec := &recorder{}
	w, err := csvstream.NewWriter(rec, csvstream.WriteConfig{Comma: ';'})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Write(sluice.Batch[[]string]{Items: [][]string{{"a", "b;c"}, {"d", "e"}}}); err != nil {
		t.Fatal(err)
	}
	if len(rec.calls) != 1 || rec.calls[0] != "a;\"b;c\"\nd;e\n" {
		t.Fatalf("after one batch the writer holds %q", rec.calls)
	}
	if err := w.Write(sluice.Batch[[]string]{}); err != nil || len(rec.calls) != 1 {
		t.Fatalf("an empty batch wrote %q, err %v", rec.calls, err)
	}
	rec.err = errors.New("disk full")
	if err := w.Write(sluice.Batch[[]string]{Items: [][]string{{"f"}}}); err == nil {
		t.Fatal("the underlying error was not returned")
	}
	rec.err = nil
	if err := w.Write(sluice.Batch[[]string]{Items: [][]string{{"g"}}}); err == nil {
		t.Fatal("a Write after a failed one succeeded")
	}
}

func TestWriterCRLFAndBadComma(t *testing.T) {
	var buf bytes.Buffer
	w, _ := csvstream.NewWriter(&buf, csvstream.WriteConfig{UseCRLF: true})
	_ = w.Write(sluice.Batch[[]string]{Items: [][]string{{"a", "b"}}})
	if buf.String() != "a,b\r\n" {
		t.Fatalf("got %q", buf.String())
	}
	if _, err := csvstream.NewWriter(&buf, csvstream.WriteConfig{Comma: '\n'}); err == nil || !strings.Contains(err.Error(), "Comma") {
		t.Fatalf("a newline Comma: err %v", err)
	}
}

func TestRoundTrip(t *testing.T) {
	in := [][]string{{"id", "note"}, {"1", "comma, inside"}, {"2", "quote \" inside"}, {"3", "line\nbreak"}}
	var buf bytes.Buffer
	w, _ := csvstream.NewWriter(&buf, csvstream.WriteConfig{})
	if err := w.Write(sluice.Batch[[]string]{Items: in}); err != nil {
		t.Fatal(err)
	}
	out, err := csvstream.Read(&buf, csvstream.ReadConfig{}).Collect()
	if err != nil || fmt.Sprintf("%q", out) != fmt.Sprintf("%q", in) {
		t.Fatalf("round trip gave %q, err %v", out, err)
	}
}
