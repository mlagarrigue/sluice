package main

// validJSON reports what json.Valid reports — that b is one JSON value,
// whitespace around it allowed — in one tight pass. json.Valid drives
// encoding/json's scanner, a function call per byte; on a list page
// (~50 KB) that was a fifth of the generator's CPU (CMP-02), the first
// post ahead of QUIC. The grammar is RFC 8259's, with the scanner's two
// leniencies kept so that the answer is the same on every input
// (FuzzValidJSON holds it to json.Valid): a string's bytes are not checked
// as UTF-8, only refused under 0x20, and nesting stops at the scanner's
// 10 000 levels.
func validJSON(b []byte) bool {
	var buf [32]byte
	stack := buf[:0] // the open containers, '{' or '['
	i, n := 0, len(b)
	var ok bool
	for {
		// A value is expected at i.
		i = skipSpace(b, i)
		if i == n {
			return false
		}
		switch c := b[i]; c {
		case '{', '[':
			if len(stack) >= maxNesting {
				return false
			}
			closer := byte('}')
			if c == '[' {
				closer = ']'
			}
			i = skipSpace(b, i+1)
			if i < n && b[i] == closer {
				i++ // an empty container: a value, nothing pushed
				break
			}
			stack = append(stack, c)
			if c == '{' {
				if i, ok = member(b, i); !ok {
					return false
				}
			}
			continue
		case '"':
			if i, ok = scanString(b, i); !ok {
				return false
			}
		case 't':
			if i, ok = literal(b, i, "true"); !ok {
				return false
			}
		case 'f':
			if i, ok = literal(b, i, "false"); !ok {
				return false
			}
		case 'n':
			if i, ok = literal(b, i, "null"); !ok {
				return false
			}
		default:
			if i, ok = scanNumber(b, i); !ok {
				return false
			}
		}
		// After a value: close what it ends, or a comma and the next one.
		for {
			i = skipSpace(b, i)
			if len(stack) == 0 {
				return i == n
			}
			if i == n {
				return false
			}
			top := stack[len(stack)-1]
			switch c := b[i]; {
			case c == ',':
				i++
				if top == '{' {
					if i, ok = member(b, skipSpace(b, i)); !ok {
						return false
					}
				}
			case c == '}' && top == '{', c == ']' && top == '[':
				stack = stack[:len(stack)-1]
				i++
				continue
			default:
				return false
			}
			break
		}
	}
}

// maxNesting is encoding/json's maxNestingDepth: a deeper document is
// refused there, so here too.
const maxNesting = 10000

func skipSpace(b []byte, i int) int {
	for i < len(b) {
		switch b[i] {
		case ' ', '\t', '\n', '\r':
			i++
		default:
			return i
		}
	}
	return i
}

// member reads an object's key and its colon, from the key's quote; the
// value is the caller's.
func member(b []byte, i int) (int, bool) {
	if i == len(b) || b[i] != '"' {
		return i, false
	}
	i, ok := scanString(b, i)
	if !ok {
		return i, false
	}
	i = skipSpace(b, i)
	if i == len(b) || b[i] != ':' {
		return i, false
	}
	return i + 1, true
}

// scanString reads a string from its opening quote to past its closing one.
func scanString(b []byte, i int) (int, bool) {
	for i++; i < len(b); i++ {
		switch c := b[i]; {
		case c == '"':
			return i + 1, true
		case c == '\\':
			i++
			if i == len(b) {
				return i, false
			}
			switch b[i] {
			case '"', '\\', '/', 'b', 'f', 'n', 'r', 't':
			case 'u':
				if i+4 >= len(b) {
					return i, false
				}
				for _, h := range b[i+1 : i+5] {
					if !isHex(h) {
						return i, false
					}
				}
				i += 4
			default:
				return i, false
			}
		case c < 0x20:
			return i, false
		}
	}
	return i, false
}

func isHex(c byte) bool {
	return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F'
}

// scanNumber reads -?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?.
func scanNumber(b []byte, i int) (int, bool) {
	n := len(b)
	if b[i] == '-' {
		i++
	}
	switch {
	case i < n && b[i] == '0':
		i++
	case i < n && b[i] >= '1' && b[i] <= '9':
		i = digits(b, i+1)
	default:
		return i, false
	}
	if i < n && b[i] == '.' {
		j := digits(b, i+1)
		if j == i+1 {
			return j, false
		}
		i = j
	}
	if i < n && (b[i] == 'e' || b[i] == 'E') {
		i++
		if i < n && (b[i] == '+' || b[i] == '-') {
			i++
		}
		j := digits(b, i)
		if j == i {
			return j, false
		}
		i = j
	}
	return i, true
}

func digits(b []byte, i int) int {
	for i < len(b) && b[i] >= '0' && b[i] <= '9' {
		i++
	}
	return i
}

func literal(b []byte, i int, word string) (int, bool) {
	if len(b)-i < len(word) || string(b[i:i+len(word)]) != word {
		return i, false
	}
	return i + len(word), true
}
