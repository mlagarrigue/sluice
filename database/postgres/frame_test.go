package postgres

// frame builds the wire form of a typed message by hand, so the tests do not
// verify the Writer against itself.
func frame(typ byte, body string) []byte {
	out := []byte{typ, 0, 0, 0, 0}
	out[1] = byte((len(body) + 4) >> 24)
	out[2] = byte((len(body) + 4) >> 16)
	out[3] = byte((len(body) + 4) >> 8)
	out[4] = byte(len(body) + 4)
	return append(out, body...)
}
