package postgres

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/mlagarrigue/sluice/internal/pgwire"
)

// ErrProtocol reports a backend message this package could not parse. It means
// the stream is no longer understood, so the connection is finished — there is
// no way to resynchronise a protocol whose framing has been misread.
var ErrProtocol = errors.New("postgres: malformed backend message")

// ErrMessageTooLarge reports a backend message longer than the reader's
// limit, and ErrMalformedMessage a length field that cannot be right. Both
// are raised by the wire layer and surface through a connection's error.
var (
	ErrMessageTooLarge  = pgwire.ErrMessageTooLarge
	ErrMalformedMessage = pgwire.ErrMalformedMessage
)

// Error is an ErrorResponse from the server, kept as fields rather than
// flattened into a sentence.
//
// Code is the SQLSTATE, and it is the reason this is a struct: it is the only
// part of a server error that is stable across versions, locales and message
// rewordings. A caller that wants to know whether a write hit a unique
// constraint tests Code == "23505"; a caller given only a message string ends
// up matching on text that changes with the server's language setting, which
// is how "duplicate key" checks break in production on a French locale.
type Error struct {
	Severity   string // "ERROR", "FATAL", "PANIC" — not localised when the server sends field V (9.6+)
	Code       string // SQLSTATE, e.g. "23505" for unique_violation
	Message    string
	Detail     string
	Hint       string
	Where      string
	Schema     string
	Table      string
	Column     string
	Constraint string // constraint name
	DataType   string // data type name, when the error concerns one

	// Position is where in the statement text the error lies, counted in
	// characters from 1 — not bytes — and 0 when the server sent none. It is
	// what points a syntax error at its token.
	Position int

	// InternalPosition and InternalQuery are the same pair for a statement the
	// server generated itself, such as the body of a PL/pgSQL function: the
	// position counts into InternalQuery, not into the caller's text.
	InternalPosition int
	InternalQuery    string

	// File, Line and Routine locate the error in the server's own source.
	// None is localised, unlike Message.
	File    string
	Line    string
	Routine string
}

// Error renders the error for a log line. The SQLSTATE comes first because it
// is the part a reader acts on, and the part that does not change with the
// server's language.
func (e *Error) Error() string {
	if e.Detail == "" {
		return fmt.Sprintf("postgres: %s %s: %s", e.Severity, e.Code, e.Message)
	}
	return fmt.Sprintf("postgres: %s %s: %s (%s)", e.Severity, e.Code, e.Message, e.Detail)
}

// ParseError decodes an ErrorResponse or NoticeResponse body: a sequence of
// (type byte, value C string) pairs ending at a zero type byte.
//
// Unknown field types are skipped rather than refused. The protocol reserves
// the right to add them, and a connector that fails on a field it has not
// heard of turns a server upgrade into an outage.
func ParseError(body []byte) (*Error, error) {
	e := &Error{}
	// V is S without the translation, sent by every server since 9.6. It wins
	// whichever order the two arrive in: a caller comparing Severity against
	// "FATAL" must not depend on the server's lc_messages.
	var nonLocalised string
	for len(body) > 0 {
		typ := body[0]
		if typ == 0 {
			if nonLocalised != "" {
				e.Severity = nonLocalised
			}
			return e, nil // the terminator
		}
		body = body[1:]
		val, rest, ok := cstring(body)
		if !ok {
			return nil, fmt.Errorf("%w: unterminated field %q in ErrorResponse", ErrProtocol, typ)
		}
		body = rest
		switch typ {
		case 'S':
			e.Severity = val
		case 'V':
			nonLocalised = val
		case 'C':
			e.Code = val
		case 'M':
			e.Message = val
		case 'D':
			e.Detail = val
		case 'H':
			e.Hint = val
		case 'W':
			e.Where = val
		case 's':
			e.Schema = val
		case 't':
			e.Table = val
		case 'c':
			e.Column = val
		case 'n':
			e.Constraint = val
		case 'd':
			e.DataType = val
		case 'P', 'p':
			// Numeric by the protocol's definition. A value that is not one
			// would otherwise surface as Position 0, which reads as "the
			// server gave none" — a silent loss rather than a refusal.
			n, err := strconv.Atoi(val)
			if err != nil || n < 1 {
				return nil, fmt.Errorf("%w: ErrorResponse position %q is not a positive integer", ErrProtocol, val)
			}
			if typ == 'P' {
				e.Position = n
			} else {
				e.InternalPosition = n
			}
		case 'q':
			e.InternalQuery = val
		case 'F':
			e.File = val
		case 'L':
			e.Line = val
		case 'R':
			e.Routine = val
		}
	}
	// Reaching here means the terminator was missing. Returning what was
	// parsed would be a plausible-looking error with fields silently absent.
	return nil, fmt.Errorf("%w: ErrorResponse without a terminator", ErrProtocol)
}

// cstring splits a null-terminated string off the front of b.
func cstring(b []byte) (s string, rest []byte, ok bool) {
	for i, c := range b {
		if c == 0 {
			return string(b[:i]), b[i+1:], true
		}
	}
	return "", nil, false
}

// Field describes one column of a result, from a RowDescription.
type Field struct {
	Name     string
	TypeOID  uint32
	TypeSize int16
	Format   int16
}

// ParseRowDescription decodes a RowDescription body into fields, appending to
// dst so the caller's slice is reused across statements.
func ParseRowDescription(dst []Field, body []byte) ([]Field, error) {
	if len(body) < 2 {
		return nil, fmt.Errorf("%w: RowDescription shorter than its field count", ErrProtocol)
	}
	n := int(binary.BigEndian.Uint16(body))
	body = body[2:]
	dst = dst[:0]
	for range n {
		name, rest, ok := cstring(body)
		if !ok {
			return nil, fmt.Errorf("%w: unterminated column name in RowDescription", ErrProtocol)
		}
		// After the name: table OID (4), column number (2), type OID (4),
		// type size (2), type modifier (4), format code (2).
		const fixed = 18
		if len(rest) < fixed {
			return nil, fmt.Errorf("%w: RowDescription column %q is truncated", ErrProtocol, name)
		}
		dst = append(dst, Field{
			Name:     name,
			TypeOID:  binary.BigEndian.Uint32(rest[6:10]),
			TypeSize: int16(binary.BigEndian.Uint16(rest[10:12])), //nolint:gosec // G115: the protocol field is int16
			Format:   int16(binary.BigEndian.Uint16(rest[16:18])), //nolint:gosec // G115: the protocol field is int16
		})
		body = rest[fixed:]
	}
	if len(body) != 0 {
		// The same refusal as Rows.appendRow: a message with bytes past its
		// declared content is being read at the wrong offset, and every field
		// above would be plausible nonsense.
		return nil, fmt.Errorf("%w: RowDescription has %d bytes after its last column", ErrProtocol, len(body))
	}
	return dst, nil
}

// Rows accumulates decoded DataRow messages for a whole batch.
//
// Values are packed end to end in one buffer with a table of offsets beside
// them, rather than a slice per row or per field. That is §8.6's columnar
// principle applied to decoding: a thousand-row batch costs a handful of
// allocations that are then reused for the next batch, where the obvious shape
// — [][]byte per row — costs a thousand and a half. It is also what lets a
// generated hydration function walk one column across every row, which is the
// loop a CPU predicts.
//
// A Rows is reset and refilled per batch, so the bytes it hands out are valid
// only until the next batch — the same rule as everything else here.
//
// A caller does not build one: a query does, and every [Row] of a batch
// reaches it through [Row.Rows].
type Rows struct {
	buf []byte // every field's bytes, concatenated

	// ends[i] is where field i stops in buf, and a **negative** value means
	// the field is NULL — stored as ^end, so the offset survives.
	//
	// A separate []bool was clearer and cost an append per field on the
	// hottest loop in the connector: three bounds checks and three capacity
	// tests for every value of every row, 220 000 of them in a wide scan.
	// Folding it in leaves two. The protocol keeps NULL distinct from a
	// zero-length value and so does this — ^0 is -1, which no real end is.
	ends    []int32
	desc    []Field // the description these rows were decoded against
	fields  int     // fields per row, from the RowDescription
	rows    int
	maxRows int
	// maxBytes is the byte bound on a batch's values, resolved to
	// maxRowBytes when the caller set none, so full pays one comparison.
	maxBytes int

	// gen counts the results this accumulator has been initialised for. A
	// [Row] view captures it and refuses to read once it moves on: the
	// accumulator is reused across queries by the connection's scratch, and
	// without the check a view retained past its query would silently read
	// the next query's bytes. Bumped by Rows.start, not by Rows.reset —
	// within one result, batch-to-batch reuse is the ordinary buffer
	// contract.
	gen uint32

	// small counts the batches in a row that fitted the retained sizes
	// while the storage was larger (see Reset).
	small int
}

// What a [Rows] keeps for the life of its connection, and how many batches
// in a row must fit it before larger storage is given back: the rule
// pgwire's Reader keeps for its message buffer. A connection that once
// carried a 60 MB batch kept 60 MB for good — a pool of a hundred that each
// met one held gigabytes.
const (
	rowsRetainBytes = 1 << 20
	rowsRetainEnds  = 1 << 18 // int32 offsets: 1 MiB
	rowsShrinkAfter = 256
)

// maxRowBytes is how many bytes of values one batch may hold, bounded by the
// int32 offsets in [Rows]. A variable rather than a constant so the guard can
// be exercised without allocating two gigabytes; nothing outside the tests
// changes it.
var maxRowBytes = math.MaxInt32

// ErrTooManyRows reports a result larger than the batch bound the caller set.
// It exists for the same reason [distinct.By] takes a bound: a query whose
// result is larger than expected must fail rather than grow until the process
// dies (S1).
var ErrTooManyRows = errors.New("postgres: result exceeds the batch row limit")

// newRows returns an accumulator for results of the given width, holding at
// most maxRows rows per batch, and full once its values reach maxBytes bytes.
// A maxBytes of zero means the 2 GiB the offsets allow, alone; a negative one,
// or one above that ceiling, panics — the bound is the caller's code, not
// data, so a wrong one is a programming error, as an out-of-range
// [Rows.Value] is.
func newRows(fields, maxRows, maxBytes int) *Rows {
	return &Rows{fields: fields, maxRows: maxRows, maxBytes: rowBytes(maxBytes)}
}

// rowBytes resolves a byte bound on a batch, zero to the ceiling the int32
// offsets allow, and panics on one outside it.
func rowBytes(n int) int {
	if n < 0 || n > maxRowBytes {
		panic(fmt.Sprintf("postgres: Rows byte bound %d is outside 0..%d", n, maxRowBytes))
	}
	if n == 0 {
		return maxRowBytes
	}
	return n
}

// start readies the accumulator for a new result described by desc, keeping
// its storage — newRows for the caller that already holds one. A connection
// runs one query at a time, so reusing one accumulator across queries turns
// the row buffer's growth into a one-time cost instead of a per-query one.
//
// It clears through Rows.reset rather than by hand, so a field reset learns
// to clear cannot be missed here — the two staying in step is what keeps a
// reused accumulator from carrying one query's state into the next.
func (r *Rows) start(desc []Field, maxRows, maxBytes int) {
	r.reset()
	r.desc = desc
	r.fields = len(desc)
	r.maxRows = maxRows
	r.maxBytes = rowBytes(maxBytes)
	r.gen++
}

// reset empties the accumulator, keeping its storage for the next batch —
// unless that storage outgrew what an ordinary batch needs and
// rowsShrinkAfter batches in a row have fitted the retained sizes, in which
// case it is given back and the next batch starts small. Only the
// connection that met a large batch pays the check's body; the steady state
// is one comparison.
//
// The byte buffer is not cleared: every byte it hands out is covered by an
// offset written when it was filled, so a stale tail is unreachable rather
// than merely unlikely.
func (r *Rows) reset() {
	if r.rows > 0 && (cap(r.buf) > rowsRetainBytes || cap(r.ends) > rowsRetainEnds) {
		if len(r.buf) <= rowsRetainBytes && len(r.ends) <= rowsRetainEnds {
			r.small++
			if r.small >= rowsShrinkAfter {
				r.buf, r.ends, r.small = nil, nil, 0
			}
		} else {
			r.small = 0
		}
	}
	r.buf = r.buf[:0]
	r.ends = r.ends[:0]
	r.rows = 0
}

// Len reports how many rows are held.
func (r *Rows) Len() int { return r.rows }

// Fields reports the number of columns per row.
func (r *Rows) Fields() int { return r.fields }

// full reports whether the accumulator has reached its row limit or its byte
// bound, which is what a caller polls after each Rows.appendRow to decide when
// to hand the batch downstream.
//
// The byte bound is a threshold, not a refusal: the row that takes the values
// past it is accepted, and the batch is full with it in. So a batch holds at
// most maxBytes plus one row, and a single row wider than the bound still
// travels, in a batch of its own.
func (r *Rows) full() bool { return r.rows >= r.maxRows || len(r.buf) >= r.maxBytes }

// fullOnBytes reports whether the byte bound, not the row limit, is what
// filled the batch: the cut a reader logs.
func (r *Rows) fullOnBytes() bool { return r.rows < r.maxRows && len(r.buf) >= r.maxBytes }

// largestRow returns the size of the widest row held, in bytes of values.
// Cold: it walks the offsets, and is called once per result, when the byte
// bound first cuts a batch.
func (r *Rows) largestRow() int {
	if r.fields == 0 {
		return 0
	}
	widest, start := 0, 0
	for row := range r.rows {
		end := r.ends[(row+1)*r.fields-1]
		if end < 0 {
			end = ^end
		}
		widest = max(widest, int(end)-start)
		start = int(end)
	}
	return widest
}

// appendRow decodes one DataRow body into the accumulator.
func (r *Rows) appendRow(body []byte) error {
	if len(body) < 2 {
		return fmt.Errorf("%w: DataRow shorter than its field count", ErrProtocol)
	}
	n := int(binary.BigEndian.Uint16(body))
	if n != r.fields {
		// The width is fixed by the RowDescription. A DataRow that disagrees
		// means the stream is being read at the wrong offset, and every value
		// after it would be plausible nonsense.
		return fmt.Errorf("%w: DataRow has %d fields, the description declared %d", ErrProtocol, n, r.fields)
	}
	if r.rows >= r.maxRows {
		return fmt.Errorf("%w: %d rows", ErrTooManyRows, r.maxRows)
	}
	// The byte bound (QueryConfig.BatchBytes) is not checked here: it is a
	// threshold Rows.full reports once crossed, and the reader closes the
	// batch on it. What is checked is the ceiling the int32 offsets in ends
	// allow, and the nolint annotations below lean on it: past 2 GiB
	// int32(len(r.buf)) wraps negative, which [Rows.Value] reads as NULL —
	// silent corruption, not an error. A reader that polls Full never reaches
	// it; a caller that appends without polling does, and fails.
	//
	// What a well-formed row adds to buf is its values: the body less the
	// field count and one length per field (a NULL has a length and no
	// value). A malformed row is caught below, after this comparison.
	if len(r.buf) > maxRowBytes-(len(body)-2-4*n) {
		return fmt.Errorf("%w: the batch's values would pass the 2 GiB its offsets allow (%d bytes held)", ErrTooManyRows, len(r.buf))
	}
	body = body[2:]

	for i := range n {
		if len(body) < 4 {
			return fmt.Errorf("%w: DataRow field %d has no length", ErrProtocol, i)
		}
		size := int(int32(binary.BigEndian.Uint32(body))) //nolint:gosec // G115: the protocol field is int32
		body = body[4:]
		if size == -1 {
			// NULL, which the protocol keeps distinct from a zero-length
			// value — and so does this, since merging them would turn a
			// missing value into an empty string with no way back.
			r.ends = append(r.ends, ^int32(len(r.buf))) //nolint:gosec // G115: len(r.buf) is bounded by the maxRowBytes check above
			continue
		}
		if size < 0 || size > len(body) {
			return fmt.Errorf("%w: DataRow field %d declares %d bytes, %d remain", ErrProtocol, i, size, len(body))
		}
		r.buf = append(r.buf, body[:size]...)
		body = body[size:]
		r.ends = append(r.ends, int32(len(r.buf))) //nolint:gosec // G115: len(r.buf) is bounded by the maxRowBytes check above
	}
	if len(body) != 0 {
		return fmt.Errorf("%w: DataRow has %d bytes after its last field", ErrProtocol, len(body))
	}
	r.rows++
	return nil
}

// truncate keeps the first n rows and drops the rest, as if they had never
// been appended. Cold: a query under a demand calls it on the one batch that
// crosses the consumer's limit.
func (r *Rows) truncate(n int) {
	if n >= r.rows {
		return
	}
	k, end := n*r.fields, int32(0)
	if k > 0 {
		if end = r.ends[k-1]; end < 0 {
			end = ^end
		}
	}
	r.buf, r.ends, r.rows = r.buf[:end], r.ends[:k], n
}

// Value returns the bytes of one field, and whether it is NULL. The slice
// borrows the accumulator's buffer and is valid until the next batch.
//
// It panics on an out-of-range row or column, like a slice index: those are
// programming errors, and returning a zero value for them would let a typo in
// a generated hydration function read as legitimate NULLs.
//
// It panics with [IndexOutOfRange], which carries the indices rather than a
// formatted sentence.
func (r *Rows) Value(row, col int) (b []byte, isNull bool) {
	if row < 0 || row >= r.rows || col < 0 || col >= r.fields {
		panic(IndexOutOfRange{Row: row, Col: col, Rows: r.rows, Fields: r.fields})
	}
	i := row*r.fields + col
	end := r.ends[i]
	if end < 0 {
		return nil, true
	}
	start := int32(0)
	if i > 0 {
		// The previous field's end, with the NULL marker undone. A NULL field
		// stores ^end rather than a sentinel, so the boundary is still there.
		if prev := r.ends[i-1]; prev < 0 {
			start = ^prev
		} else {
			start = prev
		}
	}
	return r.buf[start:end], false
}

// IndexOutOfRange is what [Rows.Value] panics with when asked for a field that
// does not exist.
//
// A value rather than a formatted string, for the same reason the rest of this
// package panics with sentinels: whoever recovers one can read the indices
// instead of parsing a sentence.
//
// It also keeps `fmt.Sprintf` out of the body of a function that runs once per
// field per row, which takes it from cost 137 to 66 against the inliner's
// budget of 80 — and **that turned out not to matter**: paired against the
// previous shape it measures as noise, 5 wins out of 14. The bounds check
// itself is what costs, at −4.2% on BenchmarkPGReadDecode when removed
// entirely, and it stays: returning a zero value for an index a caller had no
// business asking for would let a typo in a generated hydration function read
// as a legitimate NULL, which this project ranks below a crash. The number is
// written down so nobody measures it twice.
type IndexOutOfRange struct {
	Row, Col     int // what was asked for
	Rows, Fields int // what the batch holds
}

// Error renders the index asked for against the batch's actual shape.
func (e IndexOutOfRange) Error() string {
	return fmt.Sprintf("postgres: value (%d,%d) out of range for %d rows of %d fields",
		e.Row, e.Col, e.Rows, e.Fields)
}

// String makes the panic readable when it is printed rather than recovered,
// which is how it will usually be met.
func (e IndexOutOfRange) String() string { return e.Error() }
