package pgtype

import (
	"encoding/binary"
	"fmt"
)

// The server's identifiers and positions, as its send functions write them:
// fixed-width integers for oid and the reg* aliases, xid, cid (four bytes),
// oid8, xid8 and pg_lsn (eight); a block and an offset for tid; one byte for
// "char"; a count and 64-bit transaction IDs for the snapshots; and the two
// vectors, which are arrays with a lower bound of 0.

// AppendOID appends an oid, or any of the reg* aliases — regclass, regtype,
// regproc and the rest — which are an oid on the wire (oidsend): only the
// text form looks the name up.
func AppendOID(dst []byte, v uint32) []byte { return binary.BigEndian.AppendUint32(dst, v) }

// DecodeOID decodes an oid or a reg* alias.
func DecodeOID(b []byte) (uint32, error) { return decodeUint32(b, "oid") }

// AppendOID8 appends an oid8, the 64-bit object identifier of PostgreSQL 19.
func AppendOID8(dst []byte, v uint64) []byte { return binary.BigEndian.AppendUint64(dst, v) }

// DecodeOID8 decodes an oid8.
func DecodeOID8(b []byte) (uint64, error) { return decodeUint64(b, "oid8") }

// AppendXID appends an xid, a 32-bit transaction ID, which wraps around: two
// xids are compared modulo 2^32 by the server, not as numbers.
func AppendXID(dst []byte, v uint32) []byte { return binary.BigEndian.AppendUint32(dst, v) }

// DecodeXID decodes an xid.
func DecodeXID(b []byte) (uint32, error) { return decodeUint32(b, "xid") }

// AppendXID8 appends an xid8, a transaction ID with its epoch, which does not
// wrap.
func AppendXID8(dst []byte, v uint64) []byte { return binary.BigEndian.AppendUint64(dst, v) }

// DecodeXID8 decodes an xid8.
func DecodeXID8(b []byte) (uint64, error) { return decodeUint64(b, "xid8") }

// AppendCID appends a cid, a command number within a transaction.
func AppendCID(dst []byte, v uint32) []byte { return binary.BigEndian.AppendUint32(dst, v) }

// DecodeCID decodes a cid.
func DecodeCID(b []byte) (uint32, error) { return decodeUint32(b, "cid") }

// AppendPGLSN appends a pg_lsn, a position in the write-ahead log. The
// connector's replication names the same number postgres.LSN: convert with
// postgres.LSN(v).
func AppendPGLSN(dst []byte, v uint64) []byte { return binary.BigEndian.AppendUint64(dst, v) }

// DecodePGLSN decodes a pg_lsn.
func DecodePGLSN(b []byte) (uint64, error) { return decodeUint64(b, "pg_lsn") }

// TID is a tid, a row version's physical position: the block of its table
// and the line pointer within the block. It changes when the row is
// updated or the table rewritten, so it names a row version, not a row.
type TID struct {
	Block  uint32
	Offset uint16
}

// AppendTID appends a tid.
func AppendTID(dst []byte, t TID) []byte {
	return binary.BigEndian.AppendUint16(binary.BigEndian.AppendUint32(dst, t.Block), t.Offset)
}

// DecodeTID decodes a tid.
func DecodeTID(b []byte) (TID, error) {
	if len(b) != 6 {
		return TID{}, fmt.Errorf("%w: tid is %d bytes, want 6", ErrCodec, len(b))
	}
	return TID{Block: binary.BigEndian.Uint32(b), Offset: binary.BigEndian.Uint16(b[4:])}, nil
}

// AppendChar appends a "char" — the quoted, one-byte internal type of the
// catalogs (pg_class.relkind), not char(n), which is text ([AppendText]).
func AppendChar(dst []byte, c byte) []byte { return append(dst, c) }

// DecodeChar decodes a "char".
func DecodeChar(b []byte) (byte, error) {
	if len(b) != 1 {
		return 0, fmt.Errorf("%w: \"char\" is %d bytes, want 1", ErrCodec, len(b))
	}
	return b[0], nil
}

// Snapshot is a pg_snapshot or txid_snapshot, which share one layout: the
// oldest transaction still running (Xmin), the first not yet assigned
// (Xmax), and the ones in between still in progress (Xip), as 64-bit IDs
// with their epoch.
type Snapshot struct {
	Xmin, Xmax uint64
	Xip        []uint64
}

// AppendSnapshot appends a pg_snapshot or txid_snapshot.
func AppendSnapshot(dst []byte, s Snapshot) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(s.Xip))) //nolint:gosec // G115: a count bounded by memory well under 2^31
	dst = binary.BigEndian.AppendUint64(dst, s.Xmin)
	dst = binary.BigEndian.AppendUint64(dst, s.Xmax)
	for _, x := range s.Xip {
		dst = binary.BigEndian.AppendUint64(dst, x)
	}
	return dst
}

// DecodeSnapshot decodes a pg_snapshot or txid_snapshot. The count is checked
// against the bytes before anything is allocated for it.
func DecodeSnapshot(b []byte) (Snapshot, error) {
	if len(b) < 20 {
		return Snapshot{}, fmt.Errorf("%w: snapshot is %d bytes, want at least 20", ErrCodec, len(b))
	}
	n := binary.BigEndian.Uint32(b)
	rest := b[20:]
	if n > 1<<31-1 || uint64(len(rest)) != 8*uint64(n) {
		return Snapshot{}, fmt.Errorf("%w: snapshot declares %d transactions in %d bytes", ErrCodec, n, len(rest))
	}
	s := Snapshot{Xmin: binary.BigEndian.Uint64(b[4:]), Xmax: binary.BigEndian.Uint64(b[12:])}
	if n > 0 {
		s.Xip = make([]uint64, n)
		for i := range s.Xip {
			s.Xip[i] = binary.BigEndian.Uint64(rest[8*i:])
		}
	}
	return s, nil
}

// AppendInt2Vector appends an int2vector: an int2[] of one dimension numbered
// from 0, which the catalogs use for column lists (pg_index.indkey). Empty,
// it keeps its dimension, as the server builds and sends it — and refuses
// it back as a parameter (int2vectorrecv: array_recv reads it as zero
// dimensions), as it refuses its own; an empty vector goes as the text ”.
func AppendInt2Vector(dst []byte, values []int16) []byte {
	dst = appendVectorHeader(dst, OIDInt2, len(values))
	for _, v := range values {
		dst = binary.BigEndian.AppendUint32(dst, 2)
		dst = AppendInt2(dst, v)
	}
	return dst
}

// DecodeInt2Vector decodes an int2vector, appending to dst.
func DecodeInt2Vector(dst []int16, b []byte) ([]int16, error) {
	body, n, err := vectorBody(b, OIDInt2)
	if err != nil {
		return dst, err
	}
	for i := range n {
		if len(body) < 6 || binary.BigEndian.Uint32(body) != 2 {
			return dst, arrayElementError(body, i, 2)
		}
		dst = append(dst, int16(binary.BigEndian.Uint16(body[4:]))) //nolint:gosec // G115: the protocol field is int16
		body = body[6:]
	}
	if len(body) != 0 {
		return dst, fmt.Errorf("%w: %d bytes after the last int2vector element", ErrCodec, len(body))
	}
	return dst, nil
}

// AppendOIDVector appends an oidvector: an oid[] of one dimension numbered
// from 0 (pg_proc.proargtypes), kept one-dimensional when empty.
func AppendOIDVector(dst []byte, values []uint32) []byte {
	dst = appendVectorHeader(dst, OIDOID, len(values))
	for _, v := range values {
		dst = binary.BigEndian.AppendUint32(dst, 4)
		dst = AppendOID(dst, v)
	}
	return dst
}

// DecodeOIDVector decodes an oidvector, appending to dst.
func DecodeOIDVector(dst []uint32, b []byte) ([]uint32, error) {
	body, n, err := vectorBody(b, OIDOID)
	if err != nil {
		return dst, err
	}
	for i := range n {
		if len(body) < 8 || binary.BigEndian.Uint32(body) != 4 {
			return dst, arrayElementError(body, i, 4)
		}
		dst = append(dst, binary.BigEndian.Uint32(body[4:]))
		body = body[8:]
	}
	if len(body) != 0 {
		return dst, fmt.Errorf("%w: %d bytes after the last oidvector element", ErrCodec, len(body))
	}
	return dst, nil
}

// appendVectorHeader is the array header of a vector: one dimension, no
// NULL bitmap, the element type, the length, and a lower bound of 0.
func appendVectorHeader(dst []byte, elem uint32, n int) []byte {
	dst = binary.BigEndian.AppendUint32(dst, 1)
	dst = binary.BigEndian.AppendUint32(dst, 0)
	dst = binary.BigEndian.AppendUint32(dst, elem)
	dst = binary.BigEndian.AppendUint32(dst, uint32(n)) //nolint:gosec // G115: a list's length
	return binary.BigEndian.AppendUint32(dst, 0)
}

// vectorBody checks a vector's header — what appendVectorHeader writes and
// the server sends, nothing else — and returns the elements with their count.
func vectorBody(b []byte, elem uint32) (body []byte, n int, err error) {
	if len(b) < arrayHeaderSize {
		return nil, 0, fmt.Errorf("%w: vector header is %d bytes, want %d", ErrCodec, len(b), arrayHeaderSize)
	}
	ndim, flags, got := binary.BigEndian.Uint32(b), binary.BigEndian.Uint32(b[4:]), binary.BigEndian.Uint32(b[8:])
	count, lbound := binary.BigEndian.Uint32(b[12:]), binary.BigEndian.Uint32(b[16:])
	switch {
	case ndim != 1 || flags != 0 || got != elem || lbound != 0:
		return nil, 0, fmt.Errorf("%w: vector header (dimensions %d, flags %d, element OID %d, lower bound %d), want (1, 0, %d, 0)",
			ErrCodec, ndim, flags, got, lbound, elem)
	case uint64(count) > uint64(len(b)):
		// Each element takes at least four bytes; a count past the bytes
		// is refused before the loop believes it.
		return nil, 0, fmt.Errorf("%w: vector declares %d elements in %d bytes", ErrCodec, count, len(b))
	}
	return b[arrayHeaderSize:], int(count), nil
}

func decodeUint32(b []byte, name string) (uint32, error) {
	if len(b) != 4 {
		return 0, fmt.Errorf("%w: %s is %d bytes, want 4", ErrCodec, name, len(b))
	}
	return binary.BigEndian.Uint32(b), nil
}

func decodeUint64(b []byte, name string) (uint64, error) {
	if len(b) != 8 {
		return 0, fmt.Errorf("%w: %s is %d bytes, want 8", ErrCodec, name, len(b))
	}
	return binary.BigEndian.Uint64(b), nil
}
