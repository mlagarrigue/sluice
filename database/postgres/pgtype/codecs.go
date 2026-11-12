package pgtype

import (
	"net/netip"
	"time"
)

// Codec is how one PostgreSQL type travels in binary: its OID, the OID of its
// array, and the pair of functions that write and read one value. The
// containers — [Array], [Range], [Multirange] — are written once over a Codec
// rather than once per element type, so a numeric[][] and a tstzrange are the
// same code as an int4[] with a different codec handed in.
//
//	arr, err := pgtype.DecodeArray(arr, raw, pgtype.Int4Codec())
//	params := []postgres.Param{{
//		OID:   pgtype.OIDInt4Array,
//		Value: pgtype.AppendArray(nil, arr, pgtype.Int4Codec()),
//	}}
//
// Every type this package encodes has a function that returns its Codec
// ([Int4Codec], [NumericCodec], [TimestampTZCodec] …). They are functions
// rather than variables so that no caller can reassign one for the whole
// program; each call is a literal the compiler inlines.
//
// A Codec is also how a type this package does not know is plugged in — an
// extension's type, whose OID is looked up at run time, or a composite read
// field by field: fill the four fields and the containers take it as they
// take the built-in ones.
//
// # What Decode may return
//
// The Decode of every Codec here returns a value that owns its memory: the
// codecs of bytea, json and jsonb copy what [DecodeBytea], [DecodeJSON] and
// [DecodeJSONB] borrow. A container decodes many values out of one wire
// buffer that the next batch overwrites, and a value kept from it must not
// change under its owner. A Codec written outside this package should keep
// the same rule.
type Codec[T any] struct {
	// OID is the type's OID: what an array header names as its element type,
	// and what a Param carries for a value of this type.
	OID uint32
	// ArrayOID is the OID of the type's array, what a Param carries for an
	// [Array] of it. Zero when the type has none.
	ArrayOID uint32
	// Append writes one value, without its length: the container writes the
	// length around it.
	Append func(dst []byte, v T) []byte
	// Decode reads one value from exactly the bytes it was sent as.
	Decode func(b []byte) (T, error)

	// decodeElems and appendElems are the element loops of an array of
	// the type (array_fast.go), set by this package's codecs of the common
	// types alone: a Codec built elsewhere keeps its own Decode and Append
	// for every element. decodeElems reads, of n elements, those it expects
	// and returns what follows them; appendElems writes an array with no
	// NULL.
	decodeElems func(dst []T, body []byte, n int) ([]T, []byte, error)
	appendElems func(dst []byte, elems []T) []byte
}

// decodeTextValue is [DecodeText] in the shape of a Codec's Decode. A named
// function rather than a closure built per call, which the compiler would
// have to allocate once the Codec escapes.
func decodeTextValue(b []byte) (string, error) { return DecodeText(b), nil }

// copyJSON is the codec decoder for json: the text, copied.
func copyJSON(b []byte) ([]byte, error) { return cloneBytes(DecodeJSON(b)) }

// copyJSONB is the codec decoder for jsonb: the text after the version byte,
// copied.
func copyJSONB(b []byte) ([]byte, error) {
	v, err := DecodeJSONB(b)
	if err != nil {
		return nil, err
	}
	return cloneBytes(v)
}

// The Codec of each type, in the order of the OID constants. The types that
// share a wire form share their functions and differ by OID only — every
// text-like type is [AppendText] and [DecodeText], every reg* alias is
// [AppendOID] and [DecodeOID] — and the OID is the part an array header and
// the server check.

// BoolCodec returns the Codec of boolean.
func BoolCodec() Codec[bool] {
	return Codec[bool]{OID: OIDBool, ArrayOID: OIDBoolArray, Append: AppendBool, Decode: DecodeBool, decodeElems: decodeBoolElems, appendElems: appendBoolElems}
}

// ByteaCodec returns the Codec of bytea. Its Decode copies.
func ByteaCodec() Codec[[]byte] {
	return Codec[[]byte]{OID: OIDBytea, ArrayOID: OIDByteaArray, Append: AppendBytea, Decode: cloneBytes, decodeElems: decodeByteaElems, appendElems: appendByteaElems}
}

// CharCodec returns the Codec of "char", the one-byte type ([AppendChar]).
func CharCodec() Codec[byte] {
	return Codec[byte]{OID: OIDChar, ArrayOID: OIDCharArray, Append: AppendChar, Decode: DecodeChar}
}

// NameCodec returns the Codec of name.
func NameCodec() Codec[string] {
	return Codec[string]{OID: OIDName, ArrayOID: OIDNameArray, Append: AppendText, Decode: decodeTextValue}
}

// Int8Codec returns the Codec of bigint.
func Int8Codec() Codec[int64] {
	return Codec[int64]{OID: OIDInt8, ArrayOID: OIDInt8Array, Append: AppendInt8, Decode: DecodeInt8, decodeElems: decodeInt8Elems, appendElems: appendInt8Elems}
}

// Int2Codec returns the Codec of smallint.
func Int2Codec() Codec[int16] {
	return Codec[int16]{OID: OIDInt2, ArrayOID: OIDInt2Array, Append: AppendInt2, Decode: DecodeInt2, decodeElems: decodeInt2Elems, appendElems: appendInt2Elems}
}

// Int2VectorCodec returns the Codec of int2vector.
func Int2VectorCodec() Codec[[]int16] {
	return Codec[[]int16]{OID: OIDInt2Vector, ArrayOID: OIDInt2VectorArray, Append: AppendInt2Vector, Decode: func(b []byte) ([]int16, error) {
		return DecodeInt2Vector(nil, b)
	}}
}

// Int4Codec returns the Codec of integer.
func Int4Codec() Codec[int32] {
	return Codec[int32]{OID: OIDInt4, ArrayOID: OIDInt4Array, Append: AppendInt4, Decode: DecodeInt4, decodeElems: decodeInt4Elems, appendElems: appendInt4Elems}
}

// RegprocCodec returns the Codec of regproc.
func RegprocCodec() Codec[uint32] { return oidAlias(OIDRegproc, OIDRegprocArray) }

// TextCodec returns the Codec of text.
func TextCodec() Codec[string] {
	return Codec[string]{OID: OIDText, ArrayOID: OIDTextArray, Append: AppendText, Decode: decodeTextValue, decodeElems: decodeTextElems, appendElems: appendTextElems}
}

// OIDCodec returns the Codec of oid.
func OIDCodec() Codec[uint32] { return oidAlias(OIDOID, OIDOIDArray) }

// TIDCodec returns the Codec of tid.
func TIDCodec() Codec[TID] {
	return Codec[TID]{OID: OIDTID, ArrayOID: OIDTIDArray, Append: AppendTID, Decode: DecodeTID}
}

// XIDCodec returns the Codec of xid.
func XIDCodec() Codec[uint32] {
	return Codec[uint32]{OID: OIDXID, ArrayOID: OIDXIDArray, Append: AppendXID, Decode: DecodeXID}
}

// CIDCodec returns the Codec of cid.
func CIDCodec() Codec[uint32] {
	return Codec[uint32]{OID: OIDCID, ArrayOID: OIDCIDArray, Append: AppendCID, Decode: DecodeCID}
}

// OIDVectorCodec returns the Codec of oidvector.
func OIDVectorCodec() Codec[[]uint32] {
	return Codec[[]uint32]{OID: OIDOIDVector, ArrayOID: OIDOIDVectorArray, Append: AppendOIDVector, Decode: func(b []byte) ([]uint32, error) {
		return DecodeOIDVector(nil, b)
	}}
}

// JSONCodec returns the Codec of json. Its Decode copies.
func JSONCodec() Codec[[]byte] {
	return Codec[[]byte]{OID: OIDJSON, ArrayOID: OIDJSONArray, Append: AppendJSON, Decode: copyJSON}
}

// XMLCodec returns the Codec of xml, text in the client encoding.
func XMLCodec() Codec[string] {
	return Codec[string]{OID: OIDXML, ArrayOID: OIDXMLArray, Append: AppendText, Decode: decodeTextValue}
}

// PointCodec returns the Codec of point.
func PointCodec() Codec[Point] {
	return Codec[Point]{OID: OIDPoint, ArrayOID: OIDPointArray, Append: AppendPoint, Decode: DecodePoint}
}

// LSegCodec returns the Codec of lseg.
func LSegCodec() Codec[LSeg] {
	return Codec[LSeg]{OID: OIDLSeg, ArrayOID: OIDLSegArray, Append: AppendLSeg, Decode: DecodeLSeg}
}

// PathCodec returns the Codec of path.
func PathCodec() Codec[Path] {
	return Codec[Path]{OID: OIDPath, ArrayOID: OIDPathArray, Append: AppendPath, Decode: func(b []byte) (Path, error) {
		return DecodePath(nil, b)
	}}
}

// BoxCodec returns the Codec of box.
func BoxCodec() Codec[Box] {
	return Codec[Box]{OID: OIDBox, ArrayOID: OIDBoxArray, Append: AppendBox, Decode: DecodeBox}
}

// PolygonCodec returns the Codec of polygon.
func PolygonCodec() Codec[[]Point] {
	return Codec[[]Point]{OID: OIDPolygon, ArrayOID: OIDPolygonArray, Append: AppendPolygon, Decode: func(b []byte) ([]Point, error) {
		return DecodePolygon(nil, b)
	}}
}

// LineCodec returns the Codec of line.
func LineCodec() Codec[Line] {
	return Codec[Line]{OID: OIDLine, ArrayOID: OIDLineArray, Append: AppendLine, Decode: DecodeLine}
}

// CIDRCodec returns the Codec of cidr.
func CIDRCodec() Codec[netip.Prefix] {
	return Codec[netip.Prefix]{OID: OIDCIDR, ArrayOID: OIDCIDRArray, Append: AppendCIDR, Decode: DecodeInet}
}

// Float4Codec returns the Codec of real.
func Float4Codec() Codec[float32] {
	return Codec[float32]{OID: OIDFloat4, ArrayOID: OIDFloat4Array, Append: AppendFloat4, Decode: DecodeFloat4, decodeElems: decodeFloat4Elems, appendElems: appendFloat4Elems}
}

// Float8Codec returns the Codec of double precision.
func Float8Codec() Codec[float64] {
	return Codec[float64]{OID: OIDFloat8, ArrayOID: OIDFloat8Array, Append: AppendFloat8, Decode: DecodeFloat8, decodeElems: decodeFloat8Elems, appendElems: appendFloat8Elems}
}

// CircleCodec returns the Codec of circle.
func CircleCodec() Codec[Circle] {
	return Codec[Circle]{OID: OIDCircle, ArrayOID: OIDCircleArray, Append: AppendCircle, Decode: DecodeCircle}
}

// Macaddr8Codec returns the Codec of macaddr8.
func Macaddr8Codec() Codec[[8]byte] {
	return Codec[[8]byte]{OID: OIDMacaddr8, ArrayOID: OIDMacaddr8Array, Append: AppendMacaddr8, Decode: DecodeMacaddr8}
}

// MoneyCodec returns the Codec of money, in the currency's smallest unit
// ([AppendMoney]).
func MoneyCodec() Codec[int64] {
	return Codec[int64]{OID: OIDMoney, ArrayOID: OIDMoneyArray, Append: AppendMoney, Decode: DecodeMoney}
}

// MacaddrCodec returns the Codec of macaddr.
func MacaddrCodec() Codec[[6]byte] {
	return Codec[[6]byte]{OID: OIDMacaddr, ArrayOID: OIDMacaddrArray, Append: AppendMacaddr, Decode: DecodeMacaddr}
}

// InetCodec returns the Codec of inet.
func InetCodec() Codec[netip.Prefix] {
	return Codec[netip.Prefix]{OID: OIDInet, ArrayOID: OIDInetArray, Append: AppendInet, Decode: DecodeInet}
}

// BPCharCodec returns the Codec of char(n), with its padding.
func BPCharCodec() Codec[string] {
	return Codec[string]{OID: OIDBPChar, ArrayOID: OIDBPCharArray, Append: AppendText, Decode: decodeTextValue}
}

// VarcharCodec returns the Codec of varchar.
func VarcharCodec() Codec[string] {
	return Codec[string]{OID: OIDVarchar, ArrayOID: OIDVarcharArray, Append: AppendText, Decode: decodeTextValue}
}

// DateCodec returns the Codec of date.
func DateCodec() Codec[time.Time] {
	return Codec[time.Time]{OID: OIDDate, ArrayOID: OIDDateArray, Append: AppendDate, Decode: DecodeDate, decodeElems: decodeDateElems, appendElems: appendDateElems}
}

// TimeCodec returns the Codec of time without time zone.
func TimeCodec() Codec[TimeOfDay] {
	return Codec[TimeOfDay]{OID: OIDTime, ArrayOID: OIDTimeArray, Append: AppendTime, Decode: DecodeTime}
}

// TimestampCodec returns the Codec of timestamp without time zone.
func TimestampCodec() Codec[time.Time] {
	return Codec[time.Time]{OID: OIDTimestamp, ArrayOID: OIDTimestampArray, Append: AppendTimestamp, Decode: DecodeTimestamp, decodeElems: decodeTimestampElems, appendElems: appendTimestampElems}
}

// TimestampTZCodec returns the Codec of timestamptz.
func TimestampTZCodec() Codec[time.Time] {
	return Codec[time.Time]{OID: OIDTimestampTZ, ArrayOID: OIDTimestampTZArray, Append: AppendTimestampTZ, Decode: DecodeTimestampTZ, decodeElems: decodeTimestampTZElems, appendElems: appendTimestampTZElems}
}

// IntervalCodec returns the Codec of interval.
func IntervalCodec() Codec[Interval] {
	return Codec[Interval]{OID: OIDInterval, ArrayOID: OIDIntervalArray, Append: AppendInterval, Decode: DecodeInterval}
}

// TimeTZCodec returns the Codec of time with time zone.
func TimeTZCodec() Codec[TimeTZ] {
	return Codec[TimeTZ]{OID: OIDTimeTZ, ArrayOID: OIDTimeTZArray, Append: AppendTimeTZ, Decode: DecodeTimeTZ}
}

// BitCodec returns the Codec of bit(n).
func BitCodec() Codec[Bits] {
	return Codec[Bits]{OID: OIDBit, ArrayOID: OIDBitArray, Append: AppendBits, Decode: DecodeBits}
}

// VarbitCodec returns the Codec of bit varying.
func VarbitCodec() Codec[Bits] {
	return Codec[Bits]{OID: OIDVarbit, ArrayOID: OIDVarbitArray, Append: AppendBits, Decode: DecodeBits}
}

// NumericCodec returns the Codec of numeric.
func NumericCodec() Codec[Numeric] {
	return Codec[Numeric]{OID: OIDNumeric, ArrayOID: OIDNumericArray, Append: AppendNumeric, Decode: DecodeNumeric, decodeElems: decodeNumericElems, appendElems: appendNumericElems}
}

// RefcursorCodec returns the Codec of refcursor, a cursor's name.
func RefcursorCodec() Codec[string] {
	return Codec[string]{OID: OIDRefcursor, ArrayOID: OIDRefcursorArray, Append: AppendText, Decode: decodeTextValue}
}

// RegprocedureCodec returns the Codec of regprocedure.
func RegprocedureCodec() Codec[uint32] { return oidAlias(OIDRegprocedure, OIDRegprocedureArray) }

// RegoperCodec returns the Codec of regoper.
func RegoperCodec() Codec[uint32] { return oidAlias(OIDRegoper, OIDRegoperArray) }

// RegoperatorCodec returns the Codec of regoperator.
func RegoperatorCodec() Codec[uint32] { return oidAlias(OIDRegoperator, OIDRegoperatorArray) }

// RegclassCodec returns the Codec of regclass.
func RegclassCodec() Codec[uint32] { return oidAlias(OIDRegclass, OIDRegclassArray) }

// RegtypeCodec returns the Codec of regtype.
func RegtypeCodec() Codec[uint32] { return oidAlias(OIDRegtype, OIDRegtypeArray) }

// UUIDCodec returns the Codec of uuid.
func UUIDCodec() Codec[[16]byte] {
	return Codec[[16]byte]{OID: OIDUUID, ArrayOID: OIDUUIDArray, Append: AppendUUID, Decode: DecodeUUID, decodeElems: decodeUUIDElems, appendElems: appendUUIDElems}
}

// TxidSnapshotCodec returns the Codec of txid_snapshot.
func TxidSnapshotCodec() Codec[Snapshot] {
	return Codec[Snapshot]{OID: OIDTxidSnapshot, ArrayOID: OIDTxidSnapshotArray, Append: AppendSnapshot, Decode: DecodeSnapshot}
}

// PGLSNCodec returns the Codec of pg_lsn.
func PGLSNCodec() Codec[uint64] {
	return Codec[uint64]{OID: OIDPGLSN, ArrayOID: OIDPGLSNArray, Append: AppendPGLSN, Decode: DecodePGLSN}
}

// TSVectorCodec returns the Codec of tsvector.
func TSVectorCodec() Codec[[]Lexeme] {
	return Codec[[]Lexeme]{OID: OIDTSVector, ArrayOID: OIDTSVectorArray, Append: AppendTSVector, Decode: func(b []byte) ([]Lexeme, error) {
		return DecodeTSVector(nil, b)
	}}
}

// TSQueryCodec returns the Codec of tsquery.
func TSQueryCodec() Codec[[]TSQueryItem] {
	return Codec[[]TSQueryItem]{OID: OIDTSQuery, ArrayOID: OIDTSQueryArray, Append: AppendTSQuery, Decode: func(b []byte) ([]TSQueryItem, error) {
		return DecodeTSQuery(nil, b)
	}}
}

// RegconfigCodec returns the Codec of regconfig.
func RegconfigCodec() Codec[uint32] { return oidAlias(OIDRegconfig, OIDRegconfigArray) }

// RegdictionaryCodec returns the Codec of regdictionary.
func RegdictionaryCodec() Codec[uint32] { return oidAlias(OIDRegdictionary, OIDRegdictionaryArray) }

// JSONBCodec returns the Codec of jsonb. Its Decode copies.
func JSONBCodec() Codec[[]byte] {
	return Codec[[]byte]{OID: OIDJSONB, ArrayOID: OIDJSONBArray, Append: AppendJSONB, Decode: copyJSONB}
}

// JSONPathCodec returns the Codec of jsonpath.
func JSONPathCodec() Codec[string] {
	return Codec[string]{OID: OIDJSONPath, ArrayOID: OIDJSONPathArray, Append: AppendJSONPath, Decode: DecodeJSONPath}
}

// RegnamespaceCodec returns the Codec of regnamespace.
func RegnamespaceCodec() Codec[uint32] { return oidAlias(OIDRegnamespace, OIDRegnamespaceArray) }

// RegroleCodec returns the Codec of regrole.
func RegroleCodec() Codec[uint32] { return oidAlias(OIDRegrole, OIDRegroleArray) }

// RegcollationCodec returns the Codec of regcollation.
func RegcollationCodec() Codec[uint32] { return oidAlias(OIDRegcollation, OIDRegcollationArray) }

// PGSnapshotCodec returns the Codec of pg_snapshot.
func PGSnapshotCodec() Codec[Snapshot] {
	return Codec[Snapshot]{OID: OIDPGSnapshot, ArrayOID: OIDPGSnapshotArray, Append: AppendSnapshot, Decode: DecodeSnapshot}
}

// XID8Codec returns the Codec of xid8.
func XID8Codec() Codec[uint64] {
	return Codec[uint64]{OID: OIDXID8, ArrayOID: OIDXID8Array, Append: AppendXID8, Decode: DecodeXID8}
}

// OID8Codec returns the Codec of oid8 (PostgreSQL 19).
func OID8Codec() Codec[uint64] {
	return Codec[uint64]{OID: OIDOID8, ArrayOID: OIDOID8Array, Append: AppendOID8, Decode: DecodeOID8}
}

// RegdatabaseCodec returns the Codec of regdatabase (PostgreSQL 19).
func RegdatabaseCodec() Codec[uint32] { return oidAlias(OIDRegdatabase, OIDRegdatabaseArray) }

// oidAlias is the Codec of oid under another OID: every reg* type is an oid
// on the wire.
func oidAlias(oid, array uint32) Codec[uint32] {
	return Codec[uint32]{OID: oid, ArrayOID: array, Append: AppendOID, Decode: DecodeOID}
}
