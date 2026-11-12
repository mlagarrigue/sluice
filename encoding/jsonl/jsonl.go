// Package jsonl reads and writes JSON Lines — one JSON value per line — a
// batch of values at a time, over the standard library's encoding/json.
//
// [Read] turns an [io.Reader] into a [sluice.Source] of values of a type the
// caller names, each line decoded by [json.Unmarshal]; a [Writer] encodes a
// batch of values, one per line, and writes it in one call to the underlying
// [io.Writer]. What this package adds to encoding/json is the batch, the line
// as the unit — a [json.Decoder] reads values wherever they stand and would
// accept two on one line — and a bound on the bytes one line may take.
//
// In plain words: a log of a million events arrives as a thousand batches of
// a thousand decoded events, and a line of a gigabyte is refused once it
// passes [ReadConfig.MaxRecordBytes] instead of being held whole.
//
//	src := jsonl.Read[Event](f, jsonl.ReadConfig{})
//	for b := range src.Stream() {
//		for _, ev := range b.Items {
//			// ev.ID, ev.At, …
//		}
//	}
//	if err := src.Err(); err != nil {
//		return err // names the line that failed
//	}
//
// The format is read strictly: every line is one JSON value, a line ends at
// "\n" (a "\r" before it is white space to JSON), and an empty line is an
// error, since it holds no value. The last line may lack its "\n".
//
// The guide: docs/guide/files.md.
package jsonl

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/mlagarrigue/sluice"
)

// DefaultMaxRecordBytes is the bound on one line's bytes when
// [ReadConfig.MaxRecordBytes] is zero: 1 MiB.
const DefaultMaxRecordBytes = 1 << 20

// ErrRecordTooLong reports a line whose bytes pass
// [ReadConfig.MaxRecordBytes]. The error [sluice.Source.Err] returns wraps
// it with the line's number; test for it with [errors.Is].
var ErrRecordTooLong = errors.New("jsonl: line exceeds MaxRecordBytes")

// ReadConfig configures [Read]. The zero value reads batches of
// [sluice.DefaultBatchSize] values, each line at most
// [DefaultMaxRecordBytes].
type ReadConfig struct {
	// BatchRecords is how many values a batch holds. Zero means
	// [sluice.DefaultBatchSize]; a negative value is refused.
	BatchRecords int
	// MaxRecordBytes bounds the bytes of one line, its "\n" excluded. A line
	// past it ends the read with [ErrRecordTooLong], having read at most
	// MaxRecordBytes plus one buffer (4 KiB) beyond the line's start. Zero
	// means [DefaultMaxRecordBytes]; a negative value is refused.
	MaxRecordBytes int
}

func (c ReadConfig) resolve() (ReadConfig, error) {
	var errs []error
	if c.BatchRecords < 0 {
		errs = append(errs, fmt.Errorf("jsonl: BatchRecords %d is negative; zero means %d", c.BatchRecords, sluice.DefaultBatchSize))
	}
	if c.MaxRecordBytes < 0 {
		errs = append(errs, fmt.Errorf("jsonl: MaxRecordBytes %d is negative; zero means %d", c.MaxRecordBytes, DefaultMaxRecordBytes))
	}
	if c.BatchRecords == 0 {
		c.BatchRecords = sluice.DefaultBatchSize
	}
	if c.MaxRecordBytes == 0 {
		c.MaxRecordBytes = DefaultMaxRecordBytes
	}
	return c, errors.Join(errs...)
}

// Read returns the values of r, one per line, as a stream of batches of
// [ReadConfig.BatchRecords], the last one shorter.
//
// Each value is decoded into a zero T, so a value may be kept after its
// batch; the batch's Items slice is reused for the next batch and may not
// be.
//
// The stream ends at the end of r or at the first line that cannot be read.
// [sluice.Source.Err] then reports why, naming the line: a configuration
// refused before anything is read, an empty line, a line that is not one
// JSON value of type T (wrapping [json.Unmarshal]'s error, a
// [*json.SyntaxError] or [*json.UnmarshalTypeError]), an error wrapping
// [ErrRecordTooLong], or r's own error. A line that cannot be read is not
// skipped: an element that could not be produced is an error, not a
// diagnostic, and the lines after it are not read. r is read only while the
// stream is consumed, and never closed.
func Read[T any](r io.Reader, cfg ReadConfig) sluice.Source[T] {
	cfg, err := cfg.resolve()
	if err != nil {
		return sluice.NewSource(sluice.Empty[T](), func() error { return err })
	}
	var readErr error
	stream := func(yield func(sluice.Batch[T]) bool) {
		br := bufio.NewReader(r)
		items := make([]T, 0, cfg.BatchRecords)
		var long []byte // a line longer than br's buffer, assembled
		for n := 1; ; n++ {
			line, err := br.ReadSlice('\n')
			if errors.Is(err, bufio.ErrBufferFull) {
				long = append(long[:0], line...)
				for errors.Is(err, bufio.ErrBufferFull) && len(long) <= cfg.MaxRecordBytes {
					line, err = br.ReadSlice('\n')
					long = append(long, line...)
				}
				line = long
			}
			if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, bufio.ErrBufferFull) {
				readErr = err
				break
			}
			if len(line) == 0 && errors.Is(err, io.EOF) {
				break
			}
			line = bytes.TrimSuffix(line, []byte{'\n'})
			if len(line) > cfg.MaxRecordBytes {
				readErr = fmt.Errorf("jsonl: line %d is longer than MaxRecordBytes (%d): %w", n, cfg.MaxRecordBytes, ErrRecordTooLong)
				break
			}
			if len(bytes.TrimSpace(line)) == 0 {
				readErr = fmt.Errorf("jsonl: line %d is empty", n)
				break
			}
			var v T
			if err := json.Unmarshal(line, &v); err != nil {
				readErr = fmt.Errorf("jsonl: line %d: %w", n, err)
				break
			}
			items = append(items, v)
			if len(items) == cfg.BatchRecords {
				if !yield(sluice.Batch[T]{Items: items}) {
					return
				}
				items = items[:0]
			}
			if errors.Is(err, io.EOF) {
				break
			}
		}
		// The values read before a failure are delivered: they were
		// produced, and a consumer that writes them keeps a prefix of the
		// input it can resume after.
		if len(items) > 0 {
			yield(sluice.Batch[T]{Items: items})
		}
	}
	return sluice.NewSource(stream, func() error { return readErr })
}

// Writer writes batches of values to an [io.Writer], one JSON value per line.
type Writer[T any] struct {
	w   io.Writer
	buf bytes.Buffer
	enc *json.Encoder
	err error
}

// NewWriter returns a Writer over w.
//
// Values are encoded as [json.Marshal] encodes them, except that <, > and &
// are written as they are rather than escaped: a data file is not HTML.
func NewWriter[T any](w io.Writer) *Writer[T] {
	wr := &Writer[T]{w: w}
	wr.enc = json.NewEncoder(&wr.buf)
	wr.enc.SetEscapeHTML(false)
	return wr
}

// Write encodes the batch's values and writes them in one call to the
// underlying writer before returning, so a batch is either in the writer or
// reported. A value that cannot be encoded fails the batch before any of it
// is written. The batch is not kept. The first error the underlying writer
// returns is returned, and by every Write after it.
func (w *Writer[T]) Write(b sluice.Batch[T]) error {
	if w.err != nil {
		return w.err
	}
	w.buf.Reset()
	for _, v := range b.Items {
		if err := w.enc.Encode(v); err != nil {
			return fmt.Errorf("jsonl: %w", err)
		}
	}
	if w.buf.Len() == 0 {
		return nil
	}
	if _, err := w.w.Write(w.buf.Bytes()); err != nil {
		w.err = err
		return err
	}
	return nil
}
