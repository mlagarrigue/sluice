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
func BoolCodec() Codec[bool] { return Codec[bool]{OIDBool, OIDBoolArray, AppendBool, DecodeBool} }

// ByteaCodec returns the Codec of bytea. Its Decode copies.
func ByteaCodec() Codec[[]byte] {
	return Codec[[]byte]{OIDBytea, OIDByteaArray, AppendBytea, cloneBytes}
}

// CharCodec returns the Codec of "char", the one-byte type ([AppendChar]).
func CharCodec() Codec[byte] { return Codec[byte]{OIDChar, OIDCharArray, AppendChar, DecodeChar} }

// NameCodec returns the Codec of name.
func NameCodec() Codec[string] {
	return Codec[string]{OIDName, OIDNameArray, AppendText, decodeTextValue}
}

// Int8Codec returns the Codec of bigint.
func Int8Codec() Codec[int64] { return Codec[int64]{OIDInt8, OIDInt8Array, AppendInt8, DecodeInt8} }

// Int2Codec returns the Codec of smallint.
func Int2Codec() Codec[int16] { return Codec[int16]{OIDInt2, OIDInt2Array, AppendInt2, DecodeInt2} }

// Int2VectorCodec returns the Codec of int2vector.
func Int2VectorCodec() Codec[[]int16] {
	return Codec[[]int16]{OIDInt2Vector, OIDInt2VectorArray, AppendInt2Vector, func(b []byte) ([]int16, error) {
		return DecodeInt2Vector(nil, b)
	}}
}

// Int4Codec returns the Codec of integer.
func Int4Codec() Codec[int32] { return Codec[int32]{OIDInt4, OIDInt4Array, AppendInt4, DecodeInt4} }

// RegprocCodec returns the Codec of regproc.
func RegprocCodec() Codec[uint32] { return oidAlias(OIDRegproc, OIDRegprocArray) }

// TextCodec returns the Codec of text.
func TextCodec() Codec[string] {
	return Codec[string]{OIDText, OIDTextArray, AppendText, decodeTextValue}
}

// OIDCodec returns the Codec of oid.
func OIDCodec() Codec[uint32] { return oidAlias(OIDOID, OIDOIDArray) }

// TIDCodec returns the Codec of tid.
func TIDCodec() Codec[TID] { return Codec[TID]{OIDTID, OIDTIDArray, AppendTID, DecodeTID} }

// XIDCodec returns the Codec of xid.
func XIDCodec() Codec[uint32] { return Codec[uint32]{OIDXID, OIDXIDArray, AppendXID, DecodeXID} }

// CIDCodec returns the Codec of cid.
func CIDCodec() Codec[uint32] { return Codec[uint32]{OIDCID, OIDCIDArray, AppendCID, DecodeCID} }

// OIDVectorCodec returns the Codec of oidvector.
func OIDVectorCodec() Codec[[]uint32] {
	return Codec[[]uint32]{OIDOIDVector, OIDOIDVectorArray, AppendOIDVector, func(b []byte) ([]uint32, error) {
		return DecodeOIDVector(nil, b)
	}}
}

// JSONCodec returns the Codec of json. Its Decode copies.
func JSONCodec() Codec[[]byte] { return Codec[[]byte]{OIDJSON, OIDJSONArray, AppendJSON, copyJSON} }

// XMLCodec returns the Codec of xml, text in the client encoding.
func XMLCodec() Codec[string] {
	return Codec[string]{OIDXML, OIDXMLArray, AppendText, decodeTextValue}
}

// PointCodec returns the Codec of point.
func PointCodec() Codec[Point] {
	return Codec[Point]{OIDPoint, OIDPointArray, AppendPoint, DecodePoint}
}

// LSegCodec returns the Codec of lseg.
func LSegCodec() Codec[LSeg] { return Codec[LSeg]{OIDLSeg, OIDLSegArray, AppendLSeg, DecodeLSeg} }

// PathCodec returns the Codec of path.
func PathCodec() Codec[Path] {
	return Codec[Path]{OIDPath, OIDPathArray, AppendPath, func(b []byte) (Path, error) {
		return DecodePath(nil, b)
	}}
}

// BoxCodec returns the Codec of box.
func BoxCodec() Codec[Box] { return Codec[Box]{OIDBox, OIDBoxArray, AppendBox, DecodeBox} }

// PolygonCodec returns the Codec of polygon.
func PolygonCodec() Codec[[]Point] {
	return Codec[[]Point]{OIDPolygon, OIDPolygonArray, AppendPolygon, func(b []byte) ([]Point, error) {
		return DecodePolygon(nil, b)
	}}
}

// LineCodec returns the Codec of line.
func LineCodec() Codec[Line] { return Codec[Line]{OIDLine, OIDLineArray, AppendLine, DecodeLine} }

// CIDRCodec returns the Codec of cidr.
func CIDRCodec() Codec[netip.Prefix] {
	return Codec[netip.Prefix]{OIDCIDR, OIDCIDRArray, AppendCIDR, DecodeInet}
}

// Float4Codec returns the Codec of real.
func Float4Codec() Codec[float32] {
	return Codec[float32]{OIDFloat4, OIDFloat4Array, AppendFloat4, DecodeFloat4}
}

// Float8Codec returns the Codec of double precision.
func Float8Codec() Codec[float64] {
	return Codec[float64]{OIDFloat8, OIDFloat8Array, AppendFloat8, DecodeFloat8}
}

// CircleCodec returns the Codec of circle.
func CircleCodec() Codec[Circle] {
	return Codec[Circle]{OIDCircle, OIDCircleArray, AppendCircle, DecodeCircle}
}

// Macaddr8Codec returns the Codec of macaddr8.
func Macaddr8Codec() Codec[[8]byte] {
	return Codec[[8]byte]{OIDMacaddr8, OIDMacaddr8Array, AppendMacaddr8, DecodeMacaddr8}
}

// MoneyCodec returns the Codec of money, in the currency's smallest unit
// ([AppendMoney]).
func MoneyCodec() Codec[int64] {
	return Codec[int64]{OIDMoney, OIDMoneyArray, AppendMoney, DecodeMoney}
}

// MacaddrCodec returns the Codec of macaddr.
func MacaddrCodec() Codec[[6]byte] {
	return Codec[[6]byte]{OIDMacaddr, OIDMacaddrArray, AppendMacaddr, DecodeMacaddr}
}

// InetCodec returns the Codec of inet.
func InetCodec() Codec[netip.Prefix] {
	return Codec[netip.Prefix]{OIDInet, OIDInetArray, AppendInet, DecodeInet}
}

// BPCharCodec returns the Codec of char(n), with its padding.
func BPCharCodec() Codec[string] {
	return Codec[string]{OIDBPChar, OIDBPCharArray, AppendText, decodeTextValue}
}

// VarcharCodec returns the Codec of varchar.
func VarcharCodec() Codec[string] {
	return Codec[string]{OIDVarchar, OIDVarcharArray, AppendText, decodeTextValue}
}

// DateCodec returns the Codec of date.
func DateCodec() Codec[time.Time] {
	return Codec[time.Time]{OIDDate, OIDDateArray, AppendDate, DecodeDate}
}

// TimeCodec returns the Codec of time without time zone.
func TimeCodec() Codec[TimeOfDay] {
	return Codec[TimeOfDay]{OIDTime, OIDTimeArray, AppendTime, DecodeTime}
}

// TimestampCodec returns the Codec of timestamp without time zone.
func TimestampCodec() Codec[time.Time] {
	return Codec[time.Time]{OIDTimestamp, OIDTimestampArray, AppendTimestamp, DecodeTimestamp}
}

// TimestampTZCodec returns the Codec of timestamptz.
func TimestampTZCodec() Codec[time.Time] {
	return Codec[time.Time]{OIDTimestampTZ, OIDTimestampTZArray, AppendTimestampTZ, DecodeTimestampTZ}
}

// IntervalCodec returns the Codec of interval.
func IntervalCodec() Codec[Interval] {
	return Codec[Interval]{OIDInterval, OIDIntervalArray, AppendInterval, DecodeInterval}
}

// TimeTZCodec returns the Codec of time with time zone.
func TimeTZCodec() Codec[TimeTZ] {
	return Codec[TimeTZ]{OIDTimeTZ, OIDTimeTZArray, AppendTimeTZ, DecodeTimeTZ}
}

// BitCodec returns the Codec of bit(n).
func BitCodec() Codec[Bits] { return Codec[Bits]{OIDBit, OIDBitArray, AppendBits, DecodeBits} }

// VarbitCodec returns the Codec of bit varying.
func VarbitCodec() Codec[Bits] {
	return Codec[Bits]{OIDVarbit, OIDVarbitArray, AppendBits, DecodeBits}
}

// NumericCodec returns the Codec of numeric.
func NumericCodec() Codec[Numeric] {
	return Codec[Numeric]{OIDNumeric, OIDNumericArray, AppendNumeric, DecodeNumeric}
}

// RefcursorCodec returns the Codec of refcursor, a cursor's name.
func RefcursorCodec() Codec[string] {
	return Codec[string]{OIDRefcursor, OIDRefcursorArray, AppendText, decodeTextValue}
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
	return Codec[[16]byte]{OIDUUID, OIDUUIDArray, AppendUUID, DecodeUUID}
}

// TxidSnapshotCodec returns the Codec of txid_snapshot.
func TxidSnapshotCodec() Codec[Snapshot] {
	return Codec[Snapshot]{OIDTxidSnapshot, OIDTxidSnapshotArray, AppendSnapshot, DecodeSnapshot}
}

// PGLSNCodec returns the Codec of pg_lsn.
func PGLSNCodec() Codec[uint64] {
	return Codec[uint64]{OIDPGLSN, OIDPGLSNArray, AppendPGLSN, DecodePGLSN}
}

// TSVectorCodec returns the Codec of tsvector.
func TSVectorCodec() Codec[[]Lexeme] {
	return Codec[[]Lexeme]{OIDTSVector, OIDTSVectorArray, AppendTSVector, func(b []byte) ([]Lexeme, error) {
		return DecodeTSVector(nil, b)
	}}
}

// TSQueryCodec returns the Codec of tsquery.
func TSQueryCodec() Codec[[]TSQueryItem] {
	return Codec[[]TSQueryItem]{OIDTSQuery, OIDTSQueryArray, AppendTSQuery, func(b []byte) ([]TSQueryItem, error) {
		return DecodeTSQuery(nil, b)
	}}
}

// RegconfigCodec returns the Codec of regconfig.
func RegconfigCodec() Codec[uint32] { return oidAlias(OIDRegconfig, OIDRegconfigArray) }

// RegdictionaryCodec returns the Codec of regdictionary.
func RegdictionaryCodec() Codec[uint32] { return oidAlias(OIDRegdictionary, OIDRegdictionaryArray) }

// JSONBCodec returns the Codec of jsonb. Its Decode copies.
func JSONBCodec() Codec[[]byte] {
	return Codec[[]byte]{OIDJSONB, OIDJSONBArray, AppendJSONB, copyJSONB}
}

// JSONPathCodec returns the Codec of jsonpath.
func JSONPathCodec() Codec[string] {
	return Codec[string]{OIDJSONPath, OIDJSONPathArray, AppendJSONPath, DecodeJSONPath}
}

// RegnamespaceCodec returns the Codec of regnamespace.
func RegnamespaceCodec() Codec[uint32] { return oidAlias(OIDRegnamespace, OIDRegnamespaceArray) }

// RegroleCodec returns the Codec of regrole.
func RegroleCodec() Codec[uint32] { return oidAlias(OIDRegrole, OIDRegroleArray) }

// RegcollationCodec returns the Codec of regcollation.
func RegcollationCodec() Codec[uint32] { return oidAlias(OIDRegcollation, OIDRegcollationArray) }

// PGSnapshotCodec returns the Codec of pg_snapshot.
func PGSnapshotCodec() Codec[Snapshot] {
	return Codec[Snapshot]{OIDPGSnapshot, OIDPGSnapshotArray, AppendSnapshot, DecodeSnapshot}
}

// XID8Codec returns the Codec of xid8.
func XID8Codec() Codec[uint64] {
	return Codec[uint64]{OIDXID8, OIDXID8Array, AppendXID8, DecodeXID8}
}

// OID8Codec returns the Codec of oid8 (PostgreSQL 19).
func OID8Codec() Codec[uint64] {
	return Codec[uint64]{OIDOID8, OIDOID8Array, AppendOID8, DecodeOID8}
}

// RegdatabaseCodec returns the Codec of regdatabase (PostgreSQL 19).
func RegdatabaseCodec() Codec[uint32] { return oidAlias(OIDRegdatabase, OIDRegdatabaseArray) }

// oidAlias is the Codec of oid under another OID: every reg* type is an oid
// on the wire.
func oidAlias(oid, array uint32) Codec[uint32] {
	return Codec[uint32]{oid, array, AppendOID, DecodeOID}
}
