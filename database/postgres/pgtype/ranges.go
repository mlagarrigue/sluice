package pgtype

import "time"

// The built-in range types and their multiranges (PostgreSQL 14), with the
// OIDs of their arrays. A range's OID is not in its bytes — range_send writes
// the flags and the bounds, nothing else — so it is what a parameter carries
// to say which range it is, and what an array of ranges names as its element.
const (
	OIDInt4Range uint32 = 3904
	OIDNumRange  uint32 = 3906
	OIDTSRange   uint32 = 3908
	OIDTSTZRange uint32 = 3910
	OIDDateRange uint32 = 3912
	OIDInt8Range uint32 = 3926

	OIDInt4RangeArray uint32 = 3905
	OIDNumRangeArray  uint32 = 3907
	OIDTSRangeArray   uint32 = 3909
	OIDTSTZRangeArray uint32 = 3911
	OIDDateRangeArray uint32 = 3913
	OIDInt8RangeArray uint32 = 3927

	OIDInt4Multirange uint32 = 4451
	OIDNumMultirange  uint32 = 4532
	OIDTSMultirange   uint32 = 4533
	OIDTSTZMultirange uint32 = 4534
	OIDDateMultirange uint32 = 4535
	OIDInt8Multirange uint32 = 4536

	OIDInt4MultirangeArray uint32 = 6150
	OIDNumMultirangeArray  uint32 = 6151
	OIDTSMultirangeArray   uint32 = 6152
	OIDTSTZMultirangeArray uint32 = 6153
	OIDDateMultirangeArray uint32 = 6155
	OIDInt8MultirangeArray uint32 = 6157
)

// RangeCodec returns the Codec of a range type over elem: the range's OID and
// its array's, and [AppendRange] and [DecodeRange] bound to elem. It is how a
// range is put in a container — an int4range[] is an [Array] over
// [Int4RangeCodec] — and how a range type a database created is read:
//
//	// CREATE TYPE floatrange AS RANGE (subtype = float8), OIDs looked up
//	floatrange := pgtype.RangeCodec(pgtype.Float8Codec(), oid, arrayOID)
//
// Each call builds the two functions anew; build it once and keep it.
func RangeCodec[T any](elem Codec[T], oid, arrayOID uint32) Codec[Range[T]] {
	return Codec[Range[T]]{
		OID:      oid,
		ArrayOID: arrayOID,
		Append:   func(dst []byte, r Range[T]) []byte { return AppendRange(dst, r, elem) },
		Decode:   func(b []byte) (Range[T], error) { return DecodeRange(b, elem) },
	}
}

// MultirangeCodec returns the Codec of a multirange type whose ranges are
// over elem, as [RangeCodec] does for a range. Its Decode allocates the
// Multirange it returns, which owns its memory as every Codec's value does.
func MultirangeCodec[T any](elem Codec[T], oid, arrayOID uint32) Codec[Multirange[T]] {
	return Codec[Multirange[T]]{
		OID:      oid,
		ArrayOID: arrayOID,
		Append:   func(dst []byte, m Multirange[T]) []byte { return AppendMultirange(dst, m, elem) },
		Decode:   func(b []byte) (Multirange[T], error) { return DecodeMultirange(nil, b, elem) },
	}
}

// The built-in ones are built once: each function below returns a copy of a
// Codec whose functions already exist.
var (
	int4Range = RangeCodec(Int4Codec(), OIDInt4Range, OIDInt4RangeArray)
	int8Range = RangeCodec(Int8Codec(), OIDInt8Range, OIDInt8RangeArray)
	numRange  = RangeCodec(NumericCodec(), OIDNumRange, OIDNumRangeArray)
	tsRange   = RangeCodec(TimestampCodec(), OIDTSRange, OIDTSRangeArray)
	tstzRange = RangeCodec(TimestampTZCodec(), OIDTSTZRange, OIDTSTZRangeArray)
	dateRange = RangeCodec(DateCodec(), OIDDateRange, OIDDateRangeArray)

	int4Multirange = MultirangeCodec(Int4Codec(), OIDInt4Multirange, OIDInt4MultirangeArray)
	int8Multirange = MultirangeCodec(Int8Codec(), OIDInt8Multirange, OIDInt8MultirangeArray)
	numMultirange  = MultirangeCodec(NumericCodec(), OIDNumMultirange, OIDNumMultirangeArray)
	tsMultirange   = MultirangeCodec(TimestampCodec(), OIDTSMultirange, OIDTSMultirangeArray)
	tstzMultirange = MultirangeCodec(TimestampTZCodec(), OIDTSTZMultirange, OIDTSTZMultirangeArray)
	dateMultirange = MultirangeCodec(DateCodec(), OIDDateMultirange, OIDDateMultirangeArray)
)

// Int4RangeCodec returns the Codec of int4range.
func Int4RangeCodec() Codec[Range[int32]] { return int4Range }

// Int8RangeCodec returns the Codec of int8range.
func Int8RangeCodec() Codec[Range[int64]] { return int8Range }

// NumRangeCodec returns the Codec of numrange.
func NumRangeCodec() Codec[Range[Numeric]] { return numRange }

// TSRangeCodec returns the Codec of tsrange, a range of timestamps without
// time zone.
func TSRangeCodec() Codec[Range[time.Time]] { return tsRange }

// TSTZRangeCodec returns the Codec of tstzrange.
func TSTZRangeCodec() Codec[Range[time.Time]] { return tstzRange }

// DateRangeCodec returns the Codec of daterange.
func DateRangeCodec() Codec[Range[time.Time]] { return dateRange }

// Int4MultirangeCodec returns the Codec of int4multirange.
func Int4MultirangeCodec() Codec[Multirange[int32]] { return int4Multirange }

// Int8MultirangeCodec returns the Codec of int8multirange.
func Int8MultirangeCodec() Codec[Multirange[int64]] { return int8Multirange }

// NumMultirangeCodec returns the Codec of nummultirange.
func NumMultirangeCodec() Codec[Multirange[Numeric]] { return numMultirange }

// TSMultirangeCodec returns the Codec of tsmultirange.
func TSMultirangeCodec() Codec[Multirange[time.Time]] { return tsMultirange }

// TSTZMultirangeCodec returns the Codec of tstzmultirange.
func TSTZMultirangeCodec() Codec[Multirange[time.Time]] { return tstzMultirange }

// DateMultirangeCodec returns the Codec of datemultirange.
func DateMultirangeCodec() Codec[Multirange[time.Time]] { return dateMultirange }
