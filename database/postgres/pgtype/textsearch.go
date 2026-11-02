package pgtype

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// The text search types, as tsvectorsend and tsquerysend write them
// (src/backend/utils/adt/tsvector.c, tsquery.c). The server checks what it
// receives — lexemes in its order and without duplicates, positions rising,
// an operator tree that holds together — and those checks are left to it.

// Lexeme is one word of a tsvector, with the positions it occurs at.
type Lexeme struct {
	Word      string
	Positions []TSPosition
}

// TSPosition is a lexeme's position and weight packed as the server keeps
// them: the position in the low 14 bits (1 to 16383), the weight in the top
// two (3 for A down to 0 for D, the default).
type TSPosition uint16

// Pos is the position, 1 to 16383.
func (p TSPosition) Pos() uint16 { return uint16(p) & 0x3fff }

// Weight is the weight as a letter, 'A' to 'D'.
func (p TSPosition) Weight() byte { return 'D' - byte(p>>14) }

// AppendTSVector appends a tsvector: the count of lexemes, then each word,
// a NUL, the count of its positions and the positions. The lexemes go in
// the order given, which must be the server's — by bytes, the shorter first
// on a common prefix, no duplicate — or the server refuses the value
// ("tsvector lexemes are misordered"), as it does positions that do not
// rise.
func AppendTSVector(dst []byte, lexemes []Lexeme) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(lexemes))) //nolint:gosec // G115: a count bounded by memory
	for _, l := range lexemes {
		dst = append(append(dst, l.Word...), 0)
		dst = binary.BigEndian.AppendUint16(dst, uint16(len(l.Positions))) //nolint:gosec // G115: at most 256 positions, the server's own bound
		for _, p := range l.Positions {
			dst = binary.BigEndian.AppendUint16(dst, uint16(p))
		}
	}
	return dst
}

// DecodeTSVector decodes a tsvector, its lexemes appended to dst. Every
// count is checked against the bytes left before it is believed.
func DecodeTSVector(dst []Lexeme, b []byte) ([]Lexeme, error) {
	if len(b) < 4 {
		return dst, fmt.Errorf("%w: tsvector is %d bytes, want at least 4", ErrCodec, len(b))
	}
	n := binary.BigEndian.Uint32(b)
	b = b[4:]
	if uint64(n) > uint64(len(b))/3 { // a lexeme is at least a byte, a NUL and a count
		return dst, fmt.Errorf("%w: tsvector declares %d lexemes in %d bytes", ErrCodec, n, len(b))
	}
	for i := range n {
		end := bytes.IndexByte(b, 0)
		if end < 0 || len(b) < end+3 {
			return dst, fmt.Errorf("%w: tsvector lexeme %d is cut short", ErrCodec, i)
		}
		l := Lexeme{Word: string(b[:end])}
		npos := int(binary.BigEndian.Uint16(b[end+1:]))
		b = b[end+3:]
		if len(b) < 2*npos {
			return dst, fmt.Errorf("%w: tsvector lexeme %d declares %d positions in %d bytes", ErrCodec, i, npos, len(b))
		}
		if npos > 0 {
			l.Positions = make([]TSPosition, npos)
			for j := range l.Positions {
				l.Positions[j] = TSPosition(binary.BigEndian.Uint16(b[2*j:]))
			}
			b = b[2*npos:]
		}
		dst = append(dst, l)
	}
	if len(b) != 0 {
		return dst, fmt.Errorf("%w: %d bytes after the last tsvector lexeme", ErrCodec, len(b))
	}
	return dst, nil
}

// The operators of a tsquery, as the server numbers them.
const (
	TSNot    byte = 1 // !
	TSAnd    byte = 2 // &
	TSOr     byte = 3 // |
	TSPhrase byte = 4 // <N>, Distance apart
)

// TSQueryItem is one node of a tsquery, in the prefix (Polish) order the
// server keeps them: an operator, then its operands. An operand has Operator
// zero and carries Word, Weight — a mask of the weights it matches, bit 3 A
// down to bit 0 D, zero for any — and Prefix (word:*). An operator carries
// its code and, for TSPhrase, the Distance.
type TSQueryItem struct {
	Operator byte
	Distance uint16
	Word     string
	Weight   byte
	Prefix   bool
}

const (
	tsqVal = 1
	tsqOpr = 2
)

// AppendTSQuery appends a tsquery: the count of its nodes, then each one.
func AppendTSQuery(dst []byte, items []TSQueryItem) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(items))) //nolint:gosec // G115: a count bounded by memory
	for _, it := range items {
		if it.Operator == 0 {
			prefix := byte(0)
			if it.Prefix {
				prefix = 1
			}
			dst = append(append(append(dst, tsqVal, it.Weight, prefix), it.Word...), 0)
			continue
		}
		dst = append(dst, tsqOpr, it.Operator)
		if it.Operator == TSPhrase {
			dst = binary.BigEndian.AppendUint16(dst, it.Distance)
		}
	}
	return dst
}

// DecodeTSQuery decodes a tsquery, its nodes appended to dst.
func DecodeTSQuery(dst []TSQueryItem, b []byte) ([]TSQueryItem, error) {
	if len(b) < 4 {
		return dst, fmt.Errorf("%w: tsquery is %d bytes, want at least 4", ErrCodec, len(b))
	}
	n := binary.BigEndian.Uint32(b)
	b = b[4:]
	if uint64(n) > uint64(len(b))/2 { // a node is at least two bytes
		return dst, fmt.Errorf("%w: tsquery declares %d nodes in %d bytes", ErrCodec, n, len(b))
	}
	for i := range n {
		if len(b) < 2 {
			return dst, fmt.Errorf("%w: tsquery node %d is cut short", ErrCodec, i)
		}
		switch b[0] {
		case tsqVal:
			if len(b) < 4 || b[2] > 1 {
				return dst, fmt.Errorf("%w: tsquery operand %d is malformed", ErrCodec, i)
			}
			end := bytes.IndexByte(b[3:], 0)
			if end < 0 {
				return dst, fmt.Errorf("%w: tsquery operand %d has no end", ErrCodec, i)
			}
			dst = append(dst, TSQueryItem{Weight: b[1], Prefix: b[2] == 1, Word: string(b[3 : 3+end])})
			b = b[4+end:]
		case tsqOpr:
			op := b[1]
			switch {
			case op < TSNot || op > TSPhrase:
				return dst, fmt.Errorf("%w: tsquery operator %d", ErrCodec, op)
			case op == TSPhrase:
				if len(b) < 4 {
					return dst, fmt.Errorf("%w: tsquery phrase %d is cut short", ErrCodec, i)
				}
				dst = append(dst, TSQueryItem{Operator: op, Distance: binary.BigEndian.Uint16(b[2:])})
				b = b[4:]
			default:
				dst = append(dst, TSQueryItem{Operator: op})
				b = b[2:]
			}
		default:
			return dst, fmt.Errorf("%w: tsquery node type %d", ErrCodec, b[0])
		}
	}
	if len(b) != 0 {
		return dst, fmt.Errorf("%w: %d bytes after the last tsquery node", ErrCodec, len(b))
	}
	return dst, nil
}
