// Package csvstream reads and writes CSV a batch of records at a time, over
// the standard library's encoding/csv.
//
// [Read] turns an [io.Reader] into a [sluice.Source] of records, each a
// []string as [csv.Reader] returns it; a [Writer] writes a batch of records to
// an [io.Writer] and flushes it, one batch per call. The parsing and the
// quoting are encoding/csv's: what this package adds is the batch, and a
// bound on the bytes one record may take, which encoding/csv does not have —
// a line with no end grows its buffer until memory runs out.
//
// In plain words: a file of a million lines arrives as a thousand batches of
// a thousand records, and a line of a gigabyte is refused once it passes
// [ReadConfig.MaxRecordBytes] instead of being held whole.
//
//	src := csvstream.Read(f, csvstream.ReadConfig{})
//	for b := range src.Stream() {
//		for _, rec := range b.Items {
//			// rec[0], rec[1], …
//		}
//	}
//	if err := src.Err(); err != nil {
//		return err // a *csv.ParseError carries the line and column
//	}
//
// The package is named csvstream rather than csv because a caller that reads
// CSV a batch at a time also imports encoding/csv, for [csv.ParseError] and
// [csv.ErrFieldCount] — the rule that named net/httpstream (ADR 0026).
//
// The guide: docs/guide/files.md.
package csvstream

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"

	"github.com/mlagarrigue/sluice"
)

// DefaultMaxRecordBytes is the bound on one record's bytes when
// [ReadConfig.MaxRecordBytes] is zero: 1 MiB.
const DefaultMaxRecordBytes = 1 << 20

// ErrRecordTooLong reports a record whose bytes pass
// [ReadConfig.MaxRecordBytes]. The error [sluice.Source.Err] returns wraps
// it with the record's number and offset; test for it with [errors.Is].
var ErrRecordTooLong = errors.New("csvstream: record exceeds MaxRecordBytes")

// ReadConfig configures [Read]. The zero value reads comma-separated
// records in batches of [sluice.DefaultBatchSize], each at most
// [DefaultMaxRecordBytes].
//
// Comma, Comment, FieldsPerRecord, LazyQuotes and TrimLeadingSpace are
// [csv.Reader]'s fields, with its meanings; they are copied here rather than
// taken as a *csv.Reader because the bound has to sit under the csv.Reader,
// so this package builds it.
type ReadConfig struct {
	// BatchRecords is how many records a batch holds. Zero means
	// [sluice.DefaultBatchSize]; a negative value is refused.
	BatchRecords int
	// MaxRecordBytes bounds the bytes of one record as they stand in the
	// input, quotes and line ends included. A record past it ends the read
	// with [ErrRecordTooLong], having read at most MaxRecordBytes plus one
	// buffer (4 KiB) beyond the record's start. Zero means
	// [DefaultMaxRecordBytes]; a negative value is refused.
	MaxRecordBytes int
	// Comma is the field delimiter. Zero means ','. It may not be '"', '\r',
	// '\n', the Unicode replacement character, or Comment.
	Comma rune
	// Comment, if not zero, starts a comment line, which is skipped. Zero
	// means no comment lines.
	Comment rune
	// FieldsPerRecord is the number of fields each record must have. Zero
	// means the first record's count, as in [csv.Reader]; a negative value
	// means no check.
	FieldsPerRecord int
	// LazyQuotes accepts a quote inside an unquoted field and a lone quote
	// inside a quoted one.
	LazyQuotes bool
	// TrimLeadingSpace ignores the white space that starts a field.
	TrimLeadingSpace bool
}

// readahead is what csv.Reader's bufio.Reader may hold past the end of the
// record it last returned: its default size.
const readahead = 4096

func (c ReadConfig) resolve() (ReadConfig, error) {
	var errs []error
	if c.BatchRecords < 0 {
		errs = append(errs, fmt.Errorf("csvstream: BatchRecords %d is negative; zero means %d", c.BatchRecords, sluice.DefaultBatchSize))
	}
	if c.MaxRecordBytes < 0 {
		errs = append(errs, fmt.Errorf("csvstream: MaxRecordBytes %d is negative; zero means %d", c.MaxRecordBytes, DefaultMaxRecordBytes))
	}
	if c.Comma == 0 {
		c.Comma = ','
	}
	if !validDelim(c.Comma) {
		errs = append(errs, fmt.Errorf("csvstream: Comma %q cannot delimit fields; zero means ','", c.Comma))
	}
	if c.Comment != 0 && !validDelim(c.Comment) {
		errs = append(errs, fmt.Errorf("csvstream: Comment %q cannot start a comment; zero means none", c.Comment))
	}
	if c.Comment == c.Comma {
		errs = append(errs, fmt.Errorf("csvstream: Comment and Comma are both %q", c.Comma))
	}
	if c.BatchRecords == 0 {
		c.BatchRecords = sluice.DefaultBatchSize
	}
	if c.MaxRecordBytes == 0 {
		c.MaxRecordBytes = DefaultMaxRecordBytes
	}
	return c, errors.Join(errs...)
}

// validDelim is encoding/csv's own rule for Comma and Comment.
func validDelim(r rune) bool {
	return r != 0 && r != '"' && r != '\r' && r != '\n' && utf8.ValidRune(r) && r != utf8.RuneError
}

// bounded stops the reads csv.Reader makes at limit bytes into the input,
// which Read moves forward after every record. Past it, it fails with
// ErrRecordTooLong and remembers it did, so that the error csv.Reader makes
// of a record cut short — a quote left open — is read as the bound's.
type bounded struct {
	r       io.Reader
	n       int64 // bytes read so far
	limit   int64
	tripped bool
}

func (b *bounded) Read(p []byte) (int, error) {
	room := b.limit - b.n
	if room <= 0 {
		b.tripped = true
		return 0, ErrRecordTooLong
	}
	if int64(len(p)) > room {
		p = p[:room]
	}
	n, err := b.r.Read(p)
	b.n += int64(n)
	return n, err
}

// Read returns the records of r as a stream of batches of
// [ReadConfig.BatchRecords], the last one shorter.
//
// Each record is a new []string, as csv.Reader returns it with ReuseRecord
// unset, so a record may be kept after its batch; the batch's Items slice is
// reused for the next batch and may not be.
//
// The stream ends at the end of r or at the first record that cannot be read.
// [sluice.Source.Err] then reports why: a configuration refused before
// anything is read, a [*csv.ParseError] with its line and column, an error
// wrapping [ErrRecordTooLong], or r's own error. A record that cannot be read
// is not skipped: an element that could not be produced is an error, not a
// diagnostic, and the records after it are not read. r is read only while the
// stream is consumed, and never closed.
func Read(r io.Reader, cfg ReadConfig) sluice.Source[[]string] {
	cfg, err := cfg.resolve()
	if err != nil {
		return sluice.NewSource(sluice.Empty[[]string](), func() error { return err })
	}
	var readErr error
	stream := func(yield func(sluice.Batch[[]string]) bool) {
		bound := int64(cfg.MaxRecordBytes)
		in := &bounded{r: r, limit: bound + readahead}
		cr := csv.NewReader(in)
		cr.Comma = cfg.Comma
		cr.Comment = cfg.Comment
		cr.FieldsPerRecord = cfg.FieldsPerRecord
		cr.LazyQuotes = cfg.LazyQuotes
		cr.TrimLeadingSpace = cfg.TrimLeadingSpace
		items := make([][]string, 0, cfg.BatchRecords)
		var start int64 // offset of the record being read
		for n := 1; ; n++ {
			rec, err := cr.Read()
			end := cr.InputOffset()
			if err == nil && end-start > bound {
				err = ErrRecordTooLong
			}
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				if in.tripped || errors.Is(err, ErrRecordTooLong) {
					err = fmt.Errorf("csvstream: record %d at byte %d is longer than MaxRecordBytes (%d): %w", n, start, bound, ErrRecordTooLong)
				}
				readErr = err
				// The records read before it are delivered: they were
				// produced, and a consumer that writes them keeps a prefix
				// of the input it can resume after.
				break
			}
			start = end
			in.limit = end + bound + readahead
			items = append(items, rec)
			if len(items) == cfg.BatchRecords {
				if !yield(sluice.Batch[[]string]{Items: items}) {
					return
				}
				items = items[:0]
			}
		}
		if len(items) > 0 {
			yield(sluice.Batch[[]string]{Items: items})
		}
	}
	return sluice.NewSource(stream, func() error { return readErr })
}

// WriteConfig configures a [Writer]. The zero value writes comma-separated
// records with "\n" line ends.
type WriteConfig struct {
	// Comma is the field delimiter. Zero means ','. It follows the rules of
	// [ReadConfig.Comma].
	Comma rune
	// UseCRLF ends each record with "\r\n" rather than "\n", as RFC 4180
	// writes it.
	UseCRLF bool
}

// Writer writes batches of records to an [io.Writer], quoting fields as
// [csv.Writer] does.
type Writer struct {
	cw *csv.Writer
}

// NewWriter returns a Writer over w, or an error naming a WriteConfig field
// that cannot be used.
func NewWriter(w io.Writer, cfg WriteConfig) (*Writer, error) {
	if cfg.Comma == 0 {
		cfg.Comma = ','
	}
	if !validDelim(cfg.Comma) {
		return nil, fmt.Errorf("csvstream: Comma %q cannot delimit fields; zero means ','", cfg.Comma)
	}
	cw := csv.NewWriter(w)
	cw.Comma = cfg.Comma
	cw.UseCRLF = cfg.UseCRLF
	return &Writer{cw: cw}, nil
}

// Write writes the batch's records and flushes them to the underlying writer
// before returning, so a batch is either in the writer or reported: what a
// pipeline has written is what its last successful Write returned. The
// batch is not kept. The first error the underlying writer returns is
// returned, and by every Write after it.
func (w *Writer) Write(b sluice.Batch[[]string]) error {
	for _, rec := range b.Items {
		if err := w.cw.Write(rec); err != nil {
			return err
		}
	}
	w.cw.Flush()
	return w.cw.Error()
}
