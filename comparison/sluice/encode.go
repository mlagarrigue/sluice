package main

import (
	"strconv"
	"unicode/utf8"
)

// The list document written by hand, byte for byte what encoding/json
// writes for listDoc and orderRow, without reflection: under R the encoder
// is about 40 % of the server's CPU on the list of 1 000 (the profile of
// the R preamble), and this is the variant that measures what a hand
// encoder gives back. Only the list goes through it — the other scenarios
// are too small for the encoder to show — and the test compares it to
// json.Marshal on the labels that exercise the escaping.

// appendListDoc is listDoc under json.Marshal: {"from":K,"limit":N,"items":[…]}.
func appendListDoc(dst []byte, from int64, limit int, rows []orderRow) []byte {
	dst = appendListHead(dst, from, limit)
	dst = appendRows(dst, rows, true)
	return append(dst, "]}"...)
}

// appendListHead is the document up to the first row, the fragment a
// streamed answer writes before the store answers.
func appendListHead(dst []byte, from int64, limit int) []byte {
	dst = append(dst, `{"from":`...)
	dst = strconv.AppendInt(dst, from, 10)
	dst = append(dst, `,"limit":`...)
	dst = strconv.AppendInt(dst, int64(limit), 10)
	return append(dst, `,"items":[`...)
}

// appendRows writes the rows without the array's own brackets, a comma
// before the first unless it is the first of the document.
func appendRows(dst []byte, rows []orderRow, first bool) []byte {
	for i := range rows {
		if !first || i > 0 {
			dst = append(dst, ',')
		}
		dst = appendOrderRow(dst, &rows[i])
	}
	return dst
}

// appendOrderRow is orderRow under json.Marshal, fields in declaration order.
func appendOrderRow(dst []byte, r *orderRow) []byte {
	dst = append(dst, `{"id":`...)
	dst = strconv.AppendInt(dst, r.ID, 10)
	dst = append(dst, `,"total":`...)
	dst = strconv.AppendInt(dst, r.Total, 10)
	dst = append(dst, `,"label":`...)
	dst = appendJSONString(dst, r.Label)
	return append(dst, '}')
}

const hexDigits = "0123456789abcdef"

// appendJSONString quotes s as encoding/json does with its HTML escaping
// on, which is json.Marshal's default: `"` and `\` escaped, the control
// characters as \n \r \t \b \f or \u00XX, `<` `>` `&` as \u00XX, U+2028
// and U+2029 as   and  , every invalid byte as �. Runs of
// safe bytes are copied in one append.
func appendJSONString(dst []byte, s string) []byte {
	dst = append(dst, '"')
	start := 0
	for i := 0; i < len(s); {
		b := s[i]
		if b < utf8.RuneSelf {
			if jsonSafe[b] {
				i++
				continue
			}
			dst = append(dst, s[start:i]...)
			switch b {
			case '"', '\\':
				dst = append(dst, '\\', b)
			case '\n':
				dst = append(dst, '\\', 'n')
			case '\r':
				dst = append(dst, '\\', 'r')
			case '\t':
				dst = append(dst, '\\', 't')
			case '\b':
				dst = append(dst, '\\', 'b')
			case '\f':
				dst = append(dst, '\\', 'f')
			default:
				dst = append(dst, '\\', 'u', '0', '0', hexDigits[b>>4], hexDigits[b&0xF])
			}
			i++
			start = i
			continue
		}
		c, size := utf8.DecodeRuneInString(s[i:])
		if c == utf8.RuneError && size == 1 {
			dst = append(dst, s[start:i]...)
			dst = append(dst, '\\', 'u', 'f', 'f', 'f', 'd')
			i++
			start = i
			continue
		}
		if c == ' ' || c == ' ' {
			dst = append(dst, s[start:i]...)
			dst = append(dst, '\\', 'u', '2', '0', '2', hexDigits[c&0xF])
			i += size
			start = i
			continue
		}
		i += size
	}
	dst = append(dst, s[start:]...)
	return append(dst, '"')
}

// jsonSafe marks the ASCII bytes encoding/json copies as they are: the
// printable ones but `"`, `\`, `<`, `>` and `&`.
var jsonSafe = func() (safe [utf8.RuneSelf]bool) {
	for b := range byte(utf8.RuneSelf) {
		safe[b] = b >= 0x20 && b != '"' && b != '\\' && b != '<' && b != '>' && b != '&'
	}
	return safe
}()
