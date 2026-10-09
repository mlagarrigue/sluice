package postgres

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"
)

// Type OIDs of the types this package encodes. They are the numbers a Parse
// message carries and a RowDescription reports, and naming them is what keeps
// a switch on 1184 from being unreadable.
const (
	OIDBool        uint32 = 16
	OIDBytea       uint32 = 17
	OIDName        uint32 = 19
	OIDInt8        uint32 = 20
	OIDInt2        uint32 = 21
	OIDInt4        uint32 = 23
	OIDText        uint32 = 25
	OIDFloat4      uint32 = 700
	OIDFloat8      uint32 = 701
	OIDBPChar      uint32 = 1042 // char(n): text, blank-padded to n
	OIDVarchar     uint32 = 1043
	OIDDate        uint32 = 1082
	OIDTimestamp   uint32 = 1114
	OIDTimestampTZ uint32 = 1184
	OIDUUID        uint32 = 2950
	OIDUUIDArray   uint32 = 2951
	OIDInt4Array   uint32 = 1007
	OIDInt8Array   uint32 = 1016
	OIDTextArray   uint32 = 1009
)

// ErrCodec reports a value whose bytes do not match the type they were read
// as: the wrong width, a truncated array header, a boolean that is neither.
//
// It always means the stream and the description disagree, never that the data
// is merely surprising — a NULL is reported by its length, not by its content,
// so a decoder that reaches here is decoding something it was told was there.
var ErrCodec = errors.New("postgres: value does not match its declared type")

// pgEpoch is midnight 2000-01-01 UTC, which PostgreSQL counts its timestamps
// and dates from rather than the Unix epoch. Getting this constant wrong
// shifts every timestamp by thirty years and nothing complains, which is the
// class of bug the golden vectors in the tests exist to catch.
var pgEpoch = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

const (
	microsPerSecond = 1_000_000
	secondsPerDay   = 86400
)

// InfinityTime() is what PostgreSQL's 'infinity' decodes to for timestamp,
// timestamptz and date, and the value the encoders recognise to write it
// back; [NegInfinityTime] is its '-infinity' counterpart.
//
// They lie outside what any of the three types can hold as a finite value —
// ten million years either side, past int64 microseconds (±292 000 years)
// and int32 days (±5.8 million years) alike — so no finite value decodes to
// one and decode→encode round-trips. Compare with [time.Time.Equal]. The
// zero time is not a sentinel: 0001-01-01 is an ordinary timestamp.
//
// They are functions rather than variables so that no importer can reassign
// them and silently change what every encoder in the process writes.
func InfinityTime() time.Time { return infinityTime }

// NegInfinityTime() is what '-infinity' decodes to. See [InfinityTime].
func NegInfinityTime() time.Time { return negInfinityTime }

var (
	infinityTime    = time.Date(10_000_000, 1, 1, 0, 0, 0, 0, time.UTC)
	negInfinityTime = time.Date(-10_000_000, 1, 1, 0, 0, 0, 0, time.UTC)
)

// # Scalars
//
// Every encoder appends to dst and returns it, so a batch of parameters is
// built in one buffer with no allocation per value. Every decoder takes the
// bytes as the wire gave them and checks the width before reading, because a
// length is what the server said and a decoder that trusts it reads past its
// slice.

// AppendBool appends a bool in binary format: one byte, 0 or 1.
func AppendBool(dst []byte, v bool) []byte {
	if v {
		return append(dst, 1)
	}
	return append(dst, 0)
}

// DecodeBool decodes a bool.
func DecodeBool(b []byte) (bool, error) {
	if len(b) != 1 {
		return false, fmt.Errorf("%w: bool is %d bytes, want 1", ErrCodec, len(b))
	}
	switch b[0] {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		// Anything else means the column is not a bool, however tempting it is
		// to read it as truthy.
		return false, fmt.Errorf("%w: bool byte is %d, want 0 or 1", ErrCodec, b[0])
	}
}

// AppendInt2 appends an int16.
func AppendInt2(dst []byte, v int16) []byte {
	return binary.BigEndian.AppendUint16(dst, uint16(v)) //nolint:gosec // G115: two's complement, decoded the same way
}

// DecodeInt2 decodes an int16.
func DecodeInt2(b []byte) (int16, error) {
	if len(b) != 2 {
		return 0, fmt.Errorf("%w: int2 is %d bytes, want 2", ErrCodec, len(b))
	}
	return int16(binary.BigEndian.Uint16(b)), nil //nolint:gosec // G115: two's complement
}

// AppendInt4 appends an int32.
func AppendInt4(dst []byte, v int32) []byte {
	return binary.BigEndian.AppendUint32(dst, uint32(v)) //nolint:gosec // G115: two's complement
}

// DecodeInt4 decodes an int32.
func DecodeInt4(b []byte) (int32, error) {
	if len(b) != 4 {
		return 0, fmt.Errorf("%w: int4 is %d bytes, want 4", ErrCodec, len(b))
	}
	return int32(binary.BigEndian.Uint32(b)), nil //nolint:gosec // G115: two's complement
}

// AppendInt8 appends an int64.
func AppendInt8(dst []byte, v int64) []byte {
	return binary.BigEndian.AppendUint64(dst, uint64(v)) //nolint:gosec // G115: two's complement
}

// DecodeInt8 decodes an int64.
func DecodeInt8(b []byte) (int64, error) {
	if len(b) != 8 {
		return 0, fmt.Errorf("%w: int8 is %d bytes, want 8", ErrCodec, len(b))
	}
	return int64(binary.BigEndian.Uint64(b)), nil //nolint:gosec // G115: two's complement
}

// AppendFloat4 appends a float32 in IEEE 754 form.
func AppendFloat4(dst []byte, v float32) []byte {
	return binary.BigEndian.AppendUint32(dst, math.Float32bits(v))
}

// DecodeFloat4 decodes a float32.
//
// NaN and the infinities decode as themselves rather than being rejected:
// they are values PostgreSQL stores and returns, and a decoder that refused
// them would fail on data the server considers ordinary.
func DecodeFloat4(b []byte) (float32, error) {
	if len(b) != 4 {
		return 0, fmt.Errorf("%w: float4 is %d bytes, want 4", ErrCodec, len(b))
	}
	return math.Float32frombits(binary.BigEndian.Uint32(b)), nil
}

// AppendFloat8 appends a float64 in IEEE 754 form.
//
// The bits go out as they are. A text encoding would have to choose a
// precision, and choosing one that is nearly right is how the same input comes
// to produce different rows on different days — the failure the binary format
// removes rather than mitigates.
func AppendFloat8(dst []byte, v float64) []byte {
	return binary.BigEndian.AppendUint64(dst, math.Float64bits(v))
}

// DecodeFloat8 decodes a float64.
func DecodeFloat8(b []byte) (float64, error) {
	if len(b) != 8 {
		return 0, fmt.Errorf("%w: float8 is %d bytes, want 8", ErrCodec, len(b))
	}
	return math.Float64frombits(binary.BigEndian.Uint64(b)), nil
}

// AppendText appends a string. text, varchar, name and bpchar share this form:
// the bytes as they are, with the length carried by the field header.
func AppendText(dst []byte, s string) []byte { return append(dst, s...) }

// DecodeText decodes a string, copying it out of the wire buffer — a string is
// immutable in Go and the buffer is not, so borrowing would hand back a value
// that changes underneath its holder.
func DecodeText(b []byte) string { return string(b) }

// AppendBytea appends raw bytes.
func AppendBytea(dst, b []byte) []byte { return append(dst, b...) }

// AppendTimestampTZ appends a timestamptz: microseconds since 2000-01-01
// UTC.
//
// The value is converted to UTC first. PostgreSQL stores an absolute instant
// and no zone — the zone a client sees is applied on the way out — so sending
// a wall-clock reading from another zone silently shifts the instant.
func AppendTimestampTZ(dst []byte, t time.Time) []byte {
	return AppendInt8(dst, timestampMicros(t))
}

// fromPGMicros turns microseconds since the PostgreSQL epoch into a UTC time.
//
// It goes through [time.Unix] rather than pgEpoch.Add, because Add takes a
// [time.Duration] — int64 nanoseconds, which overflows outside 1678–2262. That
// range excludes dates a timestamp column legitimately holds, and the overflow
// is silent: 3000-01-01 comes back as 1830 with no error anywhere. The
// encoder already avoided the same trap; the decoder did not.
func fromPGMicros(micros int64) time.Time {
	secs := micros / microsPerSecond
	rem := micros % microsPerSecond
	if rem < 0 { // Go truncates toward zero; time.Unix wants a non-negative remainder
		secs, rem = secs-1, rem+microsPerSecond
	}
	return time.Unix(pgEpoch.Unix()+secs, rem*1000).UTC()
}

// The int64 microsecond range, split the way timestampMicros computes it:
// math.MinInt64 is minMicrosSec seconds plus minMicrosRem microseconds, with a
// non-negative remainder.
const (
	maxMicrosSec = math.MaxInt64 / microsPerSecond
	maxMicrosRem = math.MaxInt64 % microsPerSecond
	minMicrosSec = math.MinInt64/microsPerSecond - 1
	minMicrosRem = math.MinInt64%microsPerSecond + microsPerSecond
)

func timestampMicros(t time.Time) int64 {
	switch {
	case t.Equal(InfinityTime()):
		return math.MaxInt64
	case t.Equal(NegInfinityTime()):
		return math.MinInt64
	}
	t = t.UTC()
	// Seconds and nanoseconds separately rather than through UnixNano, which
	// overflows int64 outside 1678–2262 — a range that excludes dates a
	// database legitimately holds.
	secs := t.Unix() - pgEpoch.Unix()
	us := int64(t.Nanosecond()) / 1000
	// Past int64 microseconds the product used to wrap to a plausible value
	// on the other side. It saturates instead, one short of the infinity
	// sentinel: still far outside PostgreSQL's range, so the server refuses
	// it as out of range rather than storing an infinity nobody wrote.
	switch {
	case secs > maxMicrosSec || secs == maxMicrosSec && us > maxMicrosRem:
		return math.MaxInt64 - 1
	case secs < minMicrosSec || secs == minMicrosSec && us < minMicrosRem:
		return math.MinInt64 + 1
	}
	// At the bottom edge secs*microsPerSecond alone is below MinInt64; the
	// wrap is undone by adding us, and Go defines that arithmetic.
	return secs*microsPerSecond + us
}

// DecodeTimestampTZ decodes a timestamptz into a UTC time.
//
// PostgreSQL's infinity and -infinity are the extreme int64 values; they
// decode to [InfinityTime] and [NegInfinityTime] rather than to an error,
// since a row holding one is a row the server was happy to store, and encode
// back to the same extremes.
func DecodeTimestampTZ(b []byte) (time.Time, error) {
	micros, err := DecodeInt8(b)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: timestamptz is %d bytes, want 8", ErrCodec, len(b))
	}
	return timeFromMicros(micros), nil
}

// timeFromMicros maps the two infinities to their sentinels and every other
// value to its instant.
func timeFromMicros(micros int64) time.Time {
	switch micros {
	case math.MaxInt64:
		return InfinityTime()
	case math.MinInt64:
		return NegInfinityTime()
	}
	return fromPGMicros(micros)
}

// AppendTimestamp appends a timestamp *without* time zone: microseconds since
// 2000-01-01, read off the clock the caller wrote.
//
// Its eight bytes are identical to [AppendTimestampTZ]'s and its meaning is
// not. timestamptz is an instant, so it converts to UTC first; timestamp is a
// clock reading, so converting it would store an hour nobody asked for. The
// two encoders therefore differ by exactly the conversion, which is invisible
// for a UTC input and is the whole difference for any other.
func AppendTimestamp(dst []byte, t time.Time) []byte {
	// The sentinels are instants, recognised before the clock is read: one
	// shown in another zone is still the infinity it was decoded from.
	if t.Equal(InfinityTime()) || t.Equal(NegInfinityTime()) {
		return AppendInt8(dst, timestampMicros(t))
	}
	// The clock reading, reinterpreted as if it were UTC — which is what "no
	// zone" means on the wire.
	utc := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC)
	return AppendInt8(dst, timestampMicros(utc))
}

// DecodeTimestamp decodes a timestamp without time zone.
//
// The result carries [time.UTC] as its location because a [time.Time] must
// carry one, not because the value has a zone. Reading it as an instant is the
// mistake this type invites: it is a clock reading, and which instant it names
// depends on a zone the column does not hold.
func DecodeTimestamp(b []byte) (time.Time, error) {
	micros, err := DecodeInt8(b)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: timestamp is %d bytes, want 8", ErrCodec, len(b))
	}
	return timeFromMicros(micros), nil
}

// AppendDate appends a date: whole days since 2000-01-01.
//
// Only the calendar date is sent; a time-of-day in t is dropped, which is what
// the type means rather than a rounding this package chose. The date is the
// one t reads in its own location, as [AppendTimestamp] reads its clock:
// midnight in Paris on 2024-01-01 is the date 2024-01-01, not the UTC date of
// that instant, which is the day before.
//
// [InfinityTime] and [NegInfinityTime] encode as 'infinity' and '-infinity'.
// A date past what int32 days hold saturates one short of them, outside
// PostgreSQL's range, so the server refuses it rather than storing a wrapped
// date or an infinity nobody wrote.
func AppendDate(dst []byte, t time.Time) []byte {
	switch {
	case t.Equal(InfinityTime()):
		return AppendInt4(dst, math.MaxInt32)
	case t.Equal(NegInfinityTime()):
		return AppendInt4(dst, math.MinInt32)
	}
	days := (time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC).Unix() - pgEpoch.Unix()) / secondsPerDay
	switch {
	case days >= math.MaxInt32:
		days = math.MaxInt32 - 1
	case days <= math.MinInt32:
		days = math.MinInt32 + 1
	}
	return AppendInt4(dst, int32(days)) //nolint:gosec // G115: clamped to the int32 range just above
}

// DecodeDate decodes a date into a UTC midnight. 'infinity' and '-infinity'
// decode to [InfinityTime] and [NegInfinityTime].
func DecodeDate(b []byte) (time.Time, error) {
	days, err := DecodeInt4(b)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: date is %d bytes, want 4", ErrCodec, len(b))
	}
	switch days {
	case math.MaxInt32:
		return InfinityTime(), nil
	case math.MinInt32:
		return NegInfinityTime(), nil
	}
	return pgEpoch.AddDate(0, 0, int(days)), nil
}

// AppendUUID appends a UUID's sixteen bytes.
func AppendUUID(dst []byte, u [16]byte) []byte { return append(dst, u[:]...) }

// DecodeUUID decodes a UUID.
func DecodeUUID(b []byte) ([16]byte, error) {
	var u [16]byte
	if len(b) != 16 {
		return u, fmt.Errorf("%w: uuid is %d bytes, want 16", ErrCodec, len(b))
	}
	copy(u[:], b)
	return u, nil
}

// # Arrays
//
// The array encoding is what makes `= ANY($1)` possible, and `= ANY($1)` is
// what makes a batch worth assembling: a thousand keys travel as one parameter
// in one round trip, with zero injection surface because no value ever touches
// SQL text (§8.8).

// arrayHeaderSize is ndim + flags + element OID, then one dimension's length
// and lower bound. This package writes one-dimensional arrays only, which is
// what a key list is.
const arrayHeaderSize = 20

// AppendInt8Array appends a one-dimensional int8[] with no NULLs.
//
// The lower bound is 1, PostgreSQL's default: an array written with any other
// lower bound is a different array to the server, and `= ANY` over it still
// works but the indices a caller reads back would not be the ones it wrote.
func AppendInt8Array(dst []byte, values []int64) []byte {
	dst = appendArrayHeader(dst, OIDInt8, len(values))
	for _, v := range values {
		dst = binary.BigEndian.AppendUint32(dst, 8) // each element's length
		dst = AppendInt8(dst, v)
	}
	return dst
}

// DecodeInt8Array decodes a one-dimensional int8[], appending to dst so the
// caller's slice is reused across batches.
//
// An empty array is zero dimensions, not one dimension of length zero — the
// server sends it that way, and a decoder that expected a dimension header
// would fail on the ordinary result of a query that matched nothing.
func DecodeInt8Array(dst []int64, b []byte) ([]int64, error) {
	body, n, err := arrayBody(b, OIDInt8)
	if err != nil || n == 0 {
		return dst[:0], err
	}
	dst = dst[:0]
	for i := range n {
		// The length and the width are checked together and the value is read
		// inline: this loop is the connector's claim — a thousand values out
		// of one array — and it is watched by the regression gate. The error
		// path is a call, the success path is not.
		if len(body) < 12 || int32(binary.BigEndian.Uint32(body)) != 8 { //nolint:gosec // G115: the protocol field is int32
			return dst[:0], arrayElementError(body, i, 8)
		}
		dst = append(dst, int64(binary.BigEndian.Uint64(body[4:12]))) //nolint:gosec // G115: the protocol field is int64
		body = body[12:]
	}
	if len(body) != 0 {
		return dst[:0], fmt.Errorf("%w: %d bytes after the last array element", ErrCodec, len(body))
	}
	return dst, nil
}

// # numeric lives in numeric.go
//
// It is the one type here that needs more than a fixed-width read, and the one
// where getting it nearly right is worse than not having it: a monetary amount
// rendered through float64 is the silent-wrongness failure this project ranks
// below a crash. [Numeric] keeps the value exactly as the wire holds it, and
// every lossy reading of it has to be named.

// AppendInt4Array appends a one-dimensional int4[] with no NULLs.
func AppendInt4Array(dst []byte, values []int32) []byte {
	dst = appendArrayHeader(dst, OIDInt4, len(values))
	for _, v := range values {
		dst = binary.BigEndian.AppendUint32(dst, 4)
		dst = AppendInt4(dst, v)
	}
	return dst
}

// DecodeInt4Array decodes a one-dimensional int4[], appending to dst.
func DecodeInt4Array(dst []int32, b []byte) ([]int32, error) {
	body, n, err := arrayBody(b, OIDInt4)
	if err != nil || n == 0 {
		return dst[:0], err
	}
	dst = dst[:0]
	for i := range n {
		if len(body) < 8 || int32(binary.BigEndian.Uint32(body)) != 4 { //nolint:gosec // G115: the protocol field is int32
			return dst[:0], arrayElementError(body, i, 4)
		}
		dst = append(dst, int32(binary.BigEndian.Uint32(body[4:8]))) //nolint:gosec // G115: the protocol field is int32
		body = body[8:]
	}
	if len(body) != 0 {
		return dst[:0], fmt.Errorf("%w: %d bytes after the last array element", ErrCodec, len(body))
	}
	return dst, nil
}

// AppendTextArray appends a one-dimensional text[] with no NULLs.
//
// text[] is what turns a batch of string keys into one parameter, which is the
// same trick int8[] does for identifiers and the reason both are here rather
// than only the one this package needed first.
func AppendTextArray(dst []byte, values []string) []byte {
	dst = appendArrayHeader(dst, OIDText, len(values))
	for _, v := range values {
		dst = binary.BigEndian.AppendUint32(dst, uint32(len(v))) //nolint:gosec // G115: bounded by the caller's value
		dst = append(dst, v...)
	}
	return dst
}

// DecodeTextArray decodes a one-dimensional text[], appending to dst.
//
// Elements are copied out: a string is immutable in Go and the wire buffer is
// not, so borrowing would hand back values that change at the next read.
func DecodeTextArray(dst []string, b []byte) ([]string, error) {
	body, n, err := arrayBody(b, OIDText)
	if err != nil || n == 0 {
		return dst[:0], err
	}
	dst = dst[:0]
	for i := range n {
		if len(body) < 4 {
			return dst[:0], fmt.Errorf("%w: array element %d has no length", ErrCodec, i)
		}
		size := int(int32(binary.BigEndian.Uint32(body))) //nolint:gosec // G115: the protocol field is int32
		body = body[4:]
		if size == -1 {
			return dst[:0], fmt.Errorf("%w: array element %d is NULL", ErrCodec, i)
		}
		if size < 0 || size > len(body) {
			return dst[:0], fmt.Errorf("%w: array element %d declares %d bytes, %d remain", ErrCodec, i, size, len(body))
		}
		dst, body = append(dst, string(body[:size])), body[size:]
	}
	if len(body) != 0 {
		return dst[:0], fmt.Errorf("%w: %d bytes after the last array element", ErrCodec, len(body))
	}
	return dst, nil
}

// AppendUUIDArray appends a one-dimensional uuid[] with no NULLs.
//
// uuid[] is here and float8[] is not because this list is about keys: a batch
// of identifiers travelling as one parameter is what `= ANY($1)` is for, and a
// batch of floating-point numbers is not something anything is keyed by.
func AppendUUIDArray(dst []byte, values [][16]byte) []byte {
	dst = appendArrayHeader(dst, OIDUUID, len(values))
	for _, v := range values {
		dst = binary.BigEndian.AppendUint32(dst, 16)
		dst = append(dst, v[:]...)
	}
	return dst
}

// DecodeUUIDArray decodes a one-dimensional uuid[], appending to dst.
func DecodeUUIDArray(dst [][16]byte, b []byte) ([][16]byte, error) {
	body, n, err := arrayBody(b, OIDUUID)
	if err != nil || n == 0 {
		return dst[:0], err
	}
	dst = dst[:0]
	for i := range n {
		if len(body) < 20 || int32(binary.BigEndian.Uint32(body)) != 16 { //nolint:gosec // G115: the protocol field is int32
			return dst[:0], arrayElementError(body, i, 16)
		}
		dst = append(dst, [16]byte(body[4:20]))
		body = body[20:]
	}
	if len(body) != 0 {
		return dst[:0], fmt.Errorf("%w: %d bytes after the last array element", ErrCodec, len(body))
	}
	return dst, nil
}

// appendArrayHeader writes the five words every one-dimensional array starts
// with, so the three encoders cannot disagree about them.
func appendArrayHeader(dst []byte, elem uint32, n int) []byte {
	if n == 0 {
		// An empty array is **zero dimensions**, not one dimension of length
		// zero, and the header stops after the element type — twelve bytes
		// rather than twenty.
		//
		// The decoders here have always known this; the encoders wrote a
		// one-dimensional header with a count of zero, which the server
		// accepts and never produces. That asymmetry is the same shape as
		// every other defect found in this package: a rule written once for a
		// conversion that needs it on both sides.
		dst = binary.BigEndian.AppendUint32(dst, 0)
		dst = binary.BigEndian.AppendUint32(dst, 0)
		return binary.BigEndian.AppendUint32(dst, elem)
	}
	dst = binary.BigEndian.AppendUint32(dst, 1)         // one dimension
	dst = binary.BigEndian.AppendUint32(dst, 0)         // no NULL bitmap
	dst = binary.BigEndian.AppendUint32(dst, elem)      // element type
	dst = binary.BigEndian.AppendUint32(dst, uint32(n)) //nolint:gosec // G115: a list's length
	return binary.BigEndian.AppendUint32(dst, 1)        // lower bound
}

// arrayBody validates an array header against an expected element type and
// returns what follows it, with the element count.
func arrayBody(b []byte, elem uint32) (body []byte, n int, err error) {
	if len(b) < 12 {
		return nil, 0, fmt.Errorf("%w: array header is %d bytes, want at least 12", ErrCodec, len(b))
	}
	ndim := int32(binary.BigEndian.Uint32(b[0:4])) //nolint:gosec // G115: the protocol field is int32
	flags := binary.BigEndian.Uint32(b[4:8])
	got := binary.BigEndian.Uint32(b[8:12])
	// The element type is checked before the empty case: an empty array still
	// names its element type, and an empty text[] read as int8[] is the same
	// mismatch as a full one — accepting it made the check depend on the data.
	switch {
	case got != elem:
		return nil, 0, fmt.Errorf("%w: array element type is OID %d, want %d", ErrCodec, got, elem)
	case flags > 1:
		// The server writes 0 or 1 (has NULLs) and rejects anything else on
		// input; any other value means the stream is being read at the wrong
		// offset, and the rest of the header would be believed from there.
		return nil, 0, fmt.Errorf("%w: array flags are %#x, want 0 or 1", ErrCodec, flags)
	case ndim == 0:
		return nil, 0, nil // the empty array, which the server sends as zero dimensions
	case ndim != 1:
		return nil, 0, fmt.Errorf("%w: array has %d dimensions, this decoder reads one", ErrCodec, ndim)
	case len(b) < arrayHeaderSize:
		return nil, 0, fmt.Errorf("%w: one-dimensional array header is %d bytes, want %d", ErrCodec, len(b), arrayHeaderSize)
	}
	count := int32(binary.BigEndian.Uint32(b[12:16])) //nolint:gosec // G115: the protocol field is int32
	if count < 0 {
		return nil, 0, fmt.Errorf("%w: array length is %d", ErrCodec, count)
	}
	// The lower bound is where the server numbers the first element, and it is
	// not always 1: `'[5:7]={10,20,30}'::bigint[]` sends 5, and `arr[5]` is the
	// first value. These decoders return a Go slice, which is always numbered
	// from zero, so an array with any other lower bound would come back with
	// every index shifted and nothing to say so — a caller computing a
	// position from it would be wrong by a constant it cannot see. Refused
	// rather than silently renumbered, for the same reason a NULL element is.
	if lbound := int32(binary.BigEndian.Uint32(b[16:20])); lbound != 1 { //nolint:gosec // G115: the protocol field is int32
		return nil, 0, fmt.Errorf("%w: array is numbered from %d, and this decoder returns a slice numbered from 0", ErrCodec, lbound)
	}
	return b[arrayHeaderSize:], int(count), nil
}

// arrayElementError explains why a fixed-width element could not be read.
//
// It is called only once a decoder has already found the element wanting, so
// it costs nothing on the path that succeeds — which is why the loops above
// check inline and come here only to say what went wrong.
func arrayElementError(body []byte, i, width int) error {
	if len(body) < 4 {
		return fmt.Errorf("%w: array element %d has no length", ErrCodec, i)
	}
	size := int(int32(binary.BigEndian.Uint32(body))) //nolint:gosec // G115: the protocol field is int32
	if size == -1 {
		// A NULL element in an array of a type this decoder returns by value
		// has nowhere to go. Refusing is the only honest answer — the nullable
		// decoders exist for callers who need the other one.
		return fmt.Errorf("%w: array element %d is NULL", ErrCodec, i)
	}
	if size != width {
		return fmt.Errorf("%w: array element %d is %d bytes, want %d", ErrCodec, i, size, width)
	}
	return fmt.Errorf("%w: array element %d declares %d bytes, %d remain", ErrCodec, i, size, len(body)-4)
}

// DecodeBytea returns the bytes of a bytea value, which the binary format
// carries verbatim. The slice borrows the batch's storage.
func DecodeBytea(b []byte) []byte { return b }
